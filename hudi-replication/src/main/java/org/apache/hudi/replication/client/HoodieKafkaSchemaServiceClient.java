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

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.hudi.common.config.HoodieClientConfig;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.exception.HoodieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Client for interacting with the Kafka Schema Service.
 * This client provides methods to check onboarding status for datasets.
 */
public class HoodieKafkaSchemaServiceClient extends HoodieHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieKafkaSchemaServiceClient.class);
  private static final String SCHEMA_SERVICE = "schema-service";
  private static final String SCHEMA_ENDPOINT = "/schema-service/api/v6/schema/production/%s/1";
  private static final String API_GET_ONBOARDING_STATUS = "GET_ONBOARDING_STATUS";

  public HoodieKafkaSchemaServiceClient() {
    this(HoodieHttpRequestConfig.newBuilder().build());
  }

  public HoodieKafkaSchemaServiceClient(HoodieConfig config) {
    this(buildHttpRequestConfig(config));
  }

  public HoodieKafkaSchemaServiceClient(HoodieHttpRequestConfig requestConfig) {
    super(SCHEMA_SERVICE, false, requestConfig);
  }

  /**
   * Builds HoodieHttpRequestConfig from HoodieConfig.
   * Extracts schema service timeout and retry settings from the config.
   */
  private static HoodieHttpRequestConfig buildHttpRequestConfig(HoodieConfig config) {
    return HoodieHttpRequestConfig.newBuilder()
        .withConnectTimeoutMs(config.getInt(HoodieClientConfig.SCHEMA_SERVICE_CONNECTION_TIMEOUT_MS))
        .withSocketTimeoutMs(config.getInt(HoodieClientConfig.SCHEMA_SERVICE_SOCKET_TIMEOUT_MS))
        .withConnectionRequestTimeoutMs(config.getInt(HoodieClientConfig.SCHEMA_SERVICE_REQUEST_TIMEOUT_MS))
        .withNumRetries(config.getInt(HoodieClientConfig.SCHEMA_SERVICE_NUM_RETRIES))
        .build();
  }

  /**
   * Checks if a table is onboarded to the schema service.
   * A 200 response indicates the table is onboarded.
   * A 404 response indicates the table is not onboarded (offboarded).
   *
   * @param databaseName The database name
   * @param tableName The table name
   * @return true if the table is onboarded (200), false if offboarded (404)
   * @throws HoodieException if the request fails with an unexpected status code
   */
  public boolean isTableOnboarded(String databaseName, String tableName) throws HoodieException {
    if (databaseName == null || databaseName.trim().isEmpty()) {
      throw new IllegalArgumentException("Database name cannot be null or empty");
    }
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("Table name cannot be null or empty");
    }

    String fullTableName = databaseName + "." + tableName;

    HoodieHttpClientMetricTags metricTags = new HoodieHttpClientMetricTags()
        .withDatasetName(fullTableName)
        .withApiName(API_GET_ONBOARDING_STATUS);

    String endpoint = String.format(SCHEMA_ENDPOINT, fullTableName);
    HttpGet httpGet = new HttpGet(getURL(endpoint));

    try (CloseableHttpResponse response = execute(httpGet, metricTags)) {
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 200) {
        LOG.info("Table {} is onboarded to schema service", fullTableName);
        return true;
      } else if (statusCode == 404) {
        LOG.info("Table {} is not onboarded to schema service", fullTableName);
        return false;
      } else {
        String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
        throw new HoodieException(String.format(
            "Unexpected status code %d when checking onboarding status for table %s: %s",
            statusCode, fullTableName, responseBody));
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to check onboarding status for table: " + fullTableName, e);
    }
  }
}
