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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.metrics.Metrics;

import com.codahale.metrics.Timer;

public class HoodieDeltaStreamerMetrics {

  private HoodieMetricsConfig metricsConfig;
  private String tableName;

  public String overallTimerName = null;
  public String hiveSyncTimerName = null;
  private Timer overallTimer = null;
  public Timer hiveSyncTimer = null;

  public HoodieDeltaStreamerMetrics(HoodieMetricsConfig config, String tableName) {
    this.metricsConfig = config;
    this.tableName = tableName;
    if (config.isMetricsOn()) {
      Metrics.init(config);
      this.overallTimerName = getMetricsName("timer", "deltastreamer");
      this.hiveSyncTimerName = getMetricsName("timer", "deltastreamerHiveSync");
    }
  }

  public Timer.Context getOverallTimerContext() {
    if (metricsConfig.isMetricsOn() && overallTimer == null) {
      overallTimer = createTimer(overallTimerName);
    }
    return overallTimer == null ? null : overallTimer.time();
  }

  public Timer.Context getHiveSyncTimerContext() {
    if (metricsConfig.isMetricsOn() && hiveSyncTimer == null) {
      hiveSyncTimer = createTimer(hiveSyncTimerName);
    }
    return hiveSyncTimer == null ? null : hiveSyncTimer.time();
  }

  private Timer createTimer(String name) {
    return metricsConfig.isMetricsOn() ? Metrics.getInstance().getRegistry().timer(name) : null;
  }

  String getMetricsName(String action, String metric) {
    return metricsConfig == null ? null : String.format("%s.%s.%s", tableName, action, metric);
  }

  public void updateDeltaStreamerMetrics(long durationInNs, long hiveSyncNs) {
    if (metricsConfig.isMetricsOn()) {
      Metrics.registerGauge(getMetricsName("deltastreamer", "duration"), getDurationInMs(durationInNs));
      Metrics.registerGauge(getMetricsName("deltastreamer", "hiveSyncDuration"), getDurationInMs(hiveSyncNs));
    }
  }

  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }
}
