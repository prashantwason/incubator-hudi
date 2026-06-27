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

package org.apache.spark.sql.hudi;

import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HoodieSparkPlugin}'s config injection. The driver plugin's {@code init}
 * only reads {@code pluginContext.conf()}, so we can drive it with a mocked {@link PluginContext}
 * and a real {@link SparkConf} without standing up a Spark session.
 */
class TestHoodieSparkPlugin {

  private static final String WRITE_TABLE_VERSION_SPARK_KEY =
      "spark." + HoodieWriteConfig.WRITE_TABLE_VERSION.key();

  private static void runDriverPluginInit(SparkConf conf) {
    PluginContext pluginContext = mock(PluginContext.class);
    when(pluginContext.conf()).thenReturn(conf);
    DriverPlugin driverPlugin = new HoodieSparkPlugin().driverPlugin();
    // The SparkContext argument is unused by the driver plugin's init, so null is fine here.
    driverPlugin.init(null, pluginContext);
  }

  @Test
  void driverPlugin_injectsWriteTableVersionSixByDefault() {
    SparkConf conf = new SparkConf(false);

    runDriverPluginInit(conf);

    assertEquals(String.valueOf(HoodieTableVersion.SIX.versionCode()),
        conf.get(WRITE_TABLE_VERSION_SPARK_KEY),
        "HoodieSparkPlugin should default " + WRITE_TABLE_VERSION_SPARK_KEY + " to 6");
  }

  @Test
  void driverPlugin_doesNotOverrideExplicitWriteTableVersion() {
    SparkConf conf = new SparkConf(false);
    String explicitVersion = String.valueOf(HoodieTableVersion.NINE.versionCode());
    conf.set(WRITE_TABLE_VERSION_SPARK_KEY, explicitVersion);

    runDriverPluginInit(conf);

    assertEquals(explicitVersion, conf.get(WRITE_TABLE_VERSION_SPARK_KEY),
        "HoodieSparkPlugin must not override an explicitly provided write table version");
  }
}
