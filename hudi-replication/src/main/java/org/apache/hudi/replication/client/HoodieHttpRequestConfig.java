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

import org.apache.http.client.config.RequestConfig;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for individual HTTP requests.
 * Allows overriding default timeout settings on a per-request basis.
 */
public class HoodieHttpRequestConfig {
  public static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 10;
  public static final int DEFAULT_SOCKET_TIMEOUT_SECONDS = 10;
  public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT_SECONDS = 10;
  public static final int DEFAULT_NUM_RETRIES = 3;

  private final int connectTimeoutMs;
  private final int socketTimeoutMs;
  private final int connectionRequestTimeoutMs;
  private final int numRetries;

  private HoodieHttpRequestConfig(Builder builder) {
    this.connectTimeoutMs = builder.connectTimeoutMs;
    this.socketTimeoutMs = builder.socketTimeoutMs;
    this.connectionRequestTimeoutMs = builder.connectionRequestTimeoutMs;
    this.numRetries = builder.numRetries;
  }

  public int getConnectTimeoutMs() {
    return connectTimeoutMs;
  }

  public int getSocketTimeoutMs() {
    return socketTimeoutMs;
  }

  public int getConnectionRequestTimeoutMs() {
    return connectionRequestTimeoutMs;
  }

  public int getNumRetries() {
    return numRetries;
  }

  /**
   * Converts this config to an Apache HttpClient RequestConfig.
   */
  public RequestConfig toRequestConfig() {
    return RequestConfig.custom()
        .setConnectTimeout(connectTimeoutMs)
        .setSocketTimeout(socketTimeoutMs)
        .setConnectionRequestTimeout(connectionRequestTimeoutMs)
        .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int connectTimeoutMs = (int) TimeUnit.SECONDS.toMillis(DEFAULT_CONNECT_TIMEOUT_SECONDS);
    private int socketTimeoutMs = (int) TimeUnit.SECONDS.toMillis(DEFAULT_SOCKET_TIMEOUT_SECONDS);
    private int connectionRequestTimeoutMs = (int) TimeUnit.SECONDS.toMillis(DEFAULT_CONNECTION_REQUEST_TIMEOUT_SECONDS);
    private int numRetries = DEFAULT_NUM_RETRIES;

    public Builder withConnectTimeoutMs(int connectTimeoutMs) {
      this.connectTimeoutMs = connectTimeoutMs;
      return this;
    }

    public Builder withConnectTimeoutSeconds(int connectTimeoutSeconds) {
      this.connectTimeoutMs = (int) TimeUnit.SECONDS.toMillis(connectTimeoutSeconds);
      return this;
    }

    public Builder withSocketTimeoutMs(int socketTimeoutMs) {
      this.socketTimeoutMs = socketTimeoutMs;
      return this;
    }

    public Builder withSocketTimeoutSeconds(int socketTimeoutSeconds) {
      this.socketTimeoutMs = (int) TimeUnit.SECONDS.toMillis(socketTimeoutSeconds);
      return this;
    }

    public Builder withConnectionRequestTimeoutMs(int connectionRequestTimeoutMs) {
      this.connectionRequestTimeoutMs = connectionRequestTimeoutMs;
      return this;
    }

    public Builder withConnectionRequestTimeoutSeconds(int connectionRequestTimeoutSeconds) {
      this.connectionRequestTimeoutMs = (int) TimeUnit.SECONDS.toMillis(connectionRequestTimeoutSeconds);
      return this;
    }

    public Builder withNumRetries(int numRetries) {
      this.numRetries = numRetries;
      return this;
    }

    public HoodieHttpRequestConfig build() {
      return new HoodieHttpRequestConfig(this);
    }
  }
}
