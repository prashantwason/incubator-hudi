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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

public class TestHoodieFileSystemMetrics extends HoodieCommonTestHarness {

  private HoodieWrapperFileSystem fs;

  @Before
  public void start() throws IOException {
    HoodieMetricsConfig metricConfig = HoodieMetricsConfig.newBuilder()
        .withReporterType(MetricsReporterType.CONSOLE.name())
        .on(true)
        .build();
    Metrics.init(metricConfig);

    initPath();
    initMetaClient();
    fs = metaClient.getFs();
    fs.setVerifyChecksum(false);
  }

  @Test
  public void testTimers() throws Exception {
    final Path path1 = Path.mergePaths(new Path(basePath), new Path("/file1"));

    // create
    fs.create(path1);
    assertTrue(Metrics.getInstance().getRegistry().getTimers().get("fs.create").getCount() >= 1);
    assertTrue(Metrics.getInstance().getRegistry().getTimers().get("fs.create").getMeanRate() >= 0);

    // open
    fs.open(path1);
    assertTrue(Metrics.getInstance().getRegistry().getTimers().get("fs.open").getCount() >= 1);
    assertTrue(Metrics.getInstance().getRegistry().getTimers().get("fs.open").getMeanRate() >= 0);

    // delete
    fs.delete(path1);
    assertTrue(Metrics.getInstance().getRegistry().getTimers().get("fs.delete").getCount() >= 1);
    assertTrue(Metrics.getInstance().getRegistry().getTimers().get("fs.delete").getMeanRate() >= 0);
  }
}
