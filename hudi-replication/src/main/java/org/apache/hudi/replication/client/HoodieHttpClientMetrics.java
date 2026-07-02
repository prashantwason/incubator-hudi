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

import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;
import com.uber.m3.tally.m3.M3Reporter;

import java.net.InetSocketAddress;

public class HoodieHttpClientMetrics {
  private static M3Reporter m3Reporter;
  private static final String SERVICE_TAG_KEY = "service";
  private static final String SERVICE_TAG_VALUE = "hoodie";
  private static final String ENV_TAG_KEY = "env";
  private static final String ENV_TAG_VALUE = "production";
  private static final String HOST_NAME = "localhost";
  private static final Integer PORT = 9052;
  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";
  private static final String REQUEST_DURATION = "request_duration";

  HoodieHttpClientMetrics() {
    ImmutableMap.Builder tagBuilder = new ImmutableMap.Builder<>();
    tagBuilder.put(SERVICE_TAG_KEY, SERVICE_TAG_VALUE);
    tagBuilder.put(ENV_TAG_KEY, ENV_TAG_VALUE);
    ImmutableMap<String, String> tags = tagBuilder.build();

    m3Reporter = new M3Reporter.Builder(
        new InetSocketAddress(HOST_NAME, PORT))
        .includeHost(false).commonTags(tags)
        .build();
  }

  /**
   * Emits request success metric.
   *
   * @param tags the metric tags
   */
  public void reportSuccess(HoodieHttpClientMetricTags tags) {
    m3Reporter.reportCounter(SUCCESS, tags.getTagsMap(), 1);
  }

  /**
   * Emits request failure metric.
   *
   * @param tags the metric tags
   */
  public void reportFailure(HoodieHttpClientMetricTags tags) {
    m3Reporter.reportCounter(FAILURE, tags.getTagsMap(), 1);
  }

  /**
   * Emits request duration metric.
   *
   * @param tags the metric tags
   * @param duration the duration of the request
   */
  public void reportDuration(HoodieHttpClientMetricTags tags, long duration) {
    m3Reporter.reportTimer(REQUEST_DURATION, tags.getTagsMap(), Duration.ofMillis(duration));
  }

  public void close() {
    m3Reporter.close();
  }
}
