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

package org.apache.hudi.metrics;

import org.apache.hudi.config.HoodieMetricsConfig;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Console reporter, which writes the metrics to the standard
 * output.
 *
 * This is useful for unit-tests for testing of metrics system.
 */
public class MetricsConsoleReporter extends MetricsReporter {

  private static final Logger LOG = LogManager.getLogger(MetricsConsoleReporter.class);
  private final ConsoleReporter consoleReporter;
  private final HoodieMetricsConfig config;

  public MetricsConsoleReporter(HoodieMetricsConfig config, MetricRegistry registry) {
    this.config = config;
    this.consoleReporter = ConsoleReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL).build();
  }

  @Override
  public void start() {
    if (consoleReporter != null) {
      consoleReporter.start(30, TimeUnit.SECONDS);
    } else {
      LOG.error("Cannot start as the consoleReporter is null.");
    }
  }

  @Override
  public void report() {
    if (consoleReporter != null) {
      consoleReporter.report();
    } else {
      LOG.error("Cannot report metrics as the consoleReporter is null.");
    }
  }

  @Override
  public Closeable getReporter() {
    return consoleReporter;
  }

  @Override
  public void stop() {
  }
}
