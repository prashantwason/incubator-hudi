/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.replication.client;

import com.uber.engsec.auth.config.IdentityConfig;
import com.uber.engsec.auth.config.IdentityStrategy;
import com.uber.engsec.auth.utoken.UToken;
import com.uber.engsec.auth.utoken.UTokenCreator;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.hudi.common.util.HoodieTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.uber.engsec.auth.AuthFx;
import com.uber.engsec.auth.config.AuthConfig;
import com.uber.m3.tally.NoopScope;

public abstract class HoodieHttpClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieHttpClient.class);
  protected static final String RPC_CALLER_KEY = "RPC-caller";
  protected static final String RPC_CALLER_DEFAULT = "hudi";
  protected static final String RPC_SERVICE_KEY = "RPC-service";
  protected static final String RPC_PROCEDURE_KEY = "RPC-procedure";
  protected static final String RPC_PROCEDURE_DEFAULT = "_no_procedure";
  protected static final String RPC_METHOD_KEY = "RPC-method";
  protected static final String RPC_METHOD_DEFAULT = "_no_method";
  protected static final String RPC_ENCODING_KEY = "RPC-Encoding";
  protected static final String RPC_ENCODING_DEFAULT = "json";
  protected static final String RPC_UTOKEN_CALLER = "Rpc-Header-Utoken-Caller";
  protected static final String CONTENT_TYPE_KEY = "Content-Type";
  protected static final String CONTENT_TYPE_DEFAULT = "application/json";
  private static final String URL = "http://localhost:%s/%s";
  private static final Integer DEFAULT_PORT = 5436;
  private final RequestConfig requestConfig;
  private final HttpRequestRetryHandler retryHandler;
  private final String serviceName;
  private final boolean useUtoken;
  private final Map<String, CloseableHttpClient> clientsMap;
  private final HoodieHttpClientMetrics metrics;

  public HoodieHttpClient(String serviceName) {
    this(serviceName, true);
  }

  public HoodieHttpClient(String serviceName, boolean useUtoken) {
    this(serviceName, useUtoken, HoodieHttpRequestConfig.newBuilder().build());
  }

  public HoodieHttpClient(String serviceName, HoodieHttpRequestConfig requestConfig) {
    this(serviceName, true, requestConfig);
  }

  public HoodieHttpClient(String serviceName, boolean useUtoken, HoodieHttpRequestConfig requestConfig) {
    this.serviceName = serviceName;
    this.useUtoken = useUtoken;
    this.clientsMap = new HashMap<>();
    this.requestConfig = requestConfig.toRequestConfig();
    this.retryHandler = new DefaultHttpRequestRetryHandler(requestConfig.getNumRetries(), false);
    this.metrics = getMetricsReporter();
  }

  /**
   * Sends a request to a GRPC endpoint. If a client exists for the given RPC procedure, then skip creating a
   * new client and use the existing one. If the client doesn't exist, then a new client will be created for
   * the given procedure.
   * Clients are immutable so the request interceptor cannot be updated. Therefore, a new client needs to be created
   * with a new request interceptor to ensure that all outgoing requests use the correct RPC procedure header.
   *
   * @param request the request to send to the service (HTTPPost, HTTPGet, etc.)
   * @param procedure the RPC procedure which corresponds to the GRPC endpoint
   * @param metricTags the tags to include in the metrics being emitted
   * @return CloseableHttpResponse
   * @throws IOException
   */
  public CloseableHttpResponse execute(final HttpUriRequest request,
                                       String procedure,
                                       HoodieHttpClientMetricTags metricTags) throws IOException {
    CloseableHttpClient client = createOrGetClientForProcedure(procedure);

    try {
      HoodieTimer timer = new HoodieTimer().startTimer();
      CloseableHttpResponse response = client.execute(request);
      long duration = timer.endTimer();

      metrics.reportDuration(metricTags, duration);
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 200) {
        metrics.reportSuccess(metricTags);
      } else {
        metricTags.withErrorMessage(statusCode + " status code");
        metrics.reportFailure(metricTags);
      }

      return response;
    } catch (Exception e) {
      Throwable rootCause = e;
      while (rootCause.getCause() != null) rootCause = rootCause.getCause();
      metricTags.withErrorMessage(rootCause.getMessage());
      metrics.reportFailure(metricTags);
      throw e;
    }
  }

  public CloseableHttpResponse execute(final HttpUriRequest request,
                                       HoodieHttpClientMetricTags metricTags) throws IOException {
    return execute(request, RPC_PROCEDURE_DEFAULT, metricTags);
  }

  public String getURL() {
    return getURL("");
  }

  public String getURL(String endpoint) {
    return String.format(URL, DEFAULT_PORT, endpoint);
  }

  public HoodieHttpClientMetrics getMetricsReporter() {
    return new HoodieHttpClientMetrics();
  }

  /**
   * Create a new client for the given procedure if it is not present in the client map.
   *
   * @param procedure the RPC procedure which corresponds to the GRPC endpoint
   * @returns client
   * @throws IOException
   */
  private CloseableHttpClient createOrGetClientForProcedure(String procedure) throws IOException {
    if (!clientsMap.containsKey(procedure)) {
      CloseableHttpClient newClient = createNewClient(procedure);
      clientsMap.put(procedure, newClient);
      return newClient;
    }

    return clientsMap.get(procedure);
  }

  /**
   * Create new client for the given procedure by passing in the RPC header when creating the
   * request interceptor.
   *
   * @param procedure the RPC procedure which corresponds to the GRPC endpoint
   * @return
   * @throws IOException
   */
  protected CloseableHttpClient createNewClient(String procedure) throws IOException {
    Map<String, String> headers = new HashMap<>();
    headers.put(RPC_PROCEDURE_KEY, procedure);

    return HttpClients.custom()
        .addInterceptorFirst(new HoodieHttpInterceptor(serviceName, headers, useUtoken))
        .setDefaultRequestConfig(requestConfig)
        .setRetryHandler(retryHandler)
        .build();
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, CloseableHttpClient> entry : clientsMap.entrySet()) {
      CloseableHttpClient client = entry.getValue();
      client.close();
    }
    metrics.close();
  }

  /**
   * HoodieHttpInterceptor populates all outgoing requests with the expected RPC headers.
   * This allows us to send requests to GRPC endpoints.
   * For sending requests to GRPC endpoints, the RPC procedure header needs to be updated.
   */
  static class HoodieHttpInterceptor implements HttpRequestInterceptor {
    String serviceName;
    Map<String, String> customHeaders;
    boolean useUtoken;

    HoodieHttpInterceptor(String serviceName, Map<String, String> customHeaders, boolean useUtoken) {
      this.serviceName = serviceName;
      this.customHeaders = customHeaders;
      this.useUtoken = useUtoken;
    }

    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      request.setHeader(RPC_SERVICE_KEY, serviceName);
      request.setHeader(RPC_CALLER_KEY, RPC_CALLER_DEFAULT);
      request.setHeader(RPC_PROCEDURE_KEY, RPC_PROCEDURE_DEFAULT);
      request.setHeader(RPC_METHOD_KEY, RPC_METHOD_DEFAULT);
      request.setHeader(RPC_ENCODING_KEY, RPC_ENCODING_DEFAULT);
      request.setHeader(CONTENT_TYPE_KEY, CONTENT_TYPE_DEFAULT);

      // set the custom headers in the request
      customHeaders.forEach(request::setHeader);

      // add utoken header if enabled
      if (useUtoken) {
        addUtokenHeader(request);
      }

      for (Header header : request.getAllHeaders()) {
        LOG.info(header.getName() + ": " + header.getValue());
      }
    }

    private static AuthConfig getSpireAuthCfg() {
      IdentityConfig identityConfig = new IdentityConfig();
      identityConfig.setStrategies(new HashSet<>());
      identityConfig.getStrategies().add(IdentityStrategy.SPIRE);

      AuthConfig config = new AuthConfig();
      config.setIdentity(identityConfig);
      return config;
    }

    private void addUtokenHeader(HttpRequest request) {
      try (AuthFx authFx = new AuthFx(getSpireAuthCfg(), RPC_CALLER_DEFAULT, new NoopScope())) {
        UTokenCreator uTokenCreator = authFx.getuTokenCreator();
        if (uTokenCreator != null) {
          UToken uToken = uTokenCreator.createSingleHop(serviceName);
          request.setHeader(RPC_UTOKEN_CALLER, uToken.getToken());
        }
      } catch (Exception e) {
        LOG.error("Failed to add uToken header.", e);
      }
    }
  }
}
