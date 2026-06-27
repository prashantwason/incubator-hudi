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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieUberConfigStore;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link HoodieBackedTableMetadataWriter} applies the {@link HoodieUberConfigStore} to
 * its data write config when a metadata writer is created directly (bypassing the write client, as the
 * create/init metadata table procedures and CLI do), and that it skips re-applying when the config was
 * already enriched upstream (the applied marker is present, as on the normal write-client path).
 *
 * <p>The test config is built with the metadata table disabled so construction stops right after the
 * config-store step instead of bootstrapping the metadata table — keeping the assertion focused and cheap.
 */
public class TestMetadataWriterUberConfigStore extends HoodieSparkClientTestHarness {

  private static final String ENFORCED_KEY = "hoodie.test.mdt.enforced";
  private static final String ENFORCED_VALUE = "from-config-store";

  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    HoodieTestUtils.init(storageConf, basePath);
    // Point UberConfigStore at a local temp dir. IS_TESTING=true would normally suppress
    // config-store reads; setTestConfigStorePath overrides that for this test.
    String configStorePath = tempDir.toAbsolutePath().toString();
    writeFile(new File(configStorePath, "hudi_config_enforced.conf"), ENFORCED_KEY + "=" + ENFORCED_VALUE + "\n");
    writeFile(new File(configStorePath, "hudi_config_fallback.conf"), "");
    HoodieUberConfigStore.setTestConfigStorePath(configStorePath);
  }

  @AfterEach
  public void tearDown() throws Exception {
    HoodieUberConfigStore.clearTestConfigStorePath();
    cleanupResources();
  }

  @Test
  public void testDirectlyCreatedWriterAppliesConfigStore() throws Exception {
    HoodieWriteConfig writeConfig = metadataDisabledConfig(new Properties());
    assertFalse(writeConfig.getProps().containsKey(HoodieUberConfigStore.CONFIG_STORE_APPLIED_MARKER));

    HoodieTableConfig tableConfig = buildMetaClient().getTableConfig();
    try (HoodieTableMetadataWriter writer =
             SparkMetadataWriterFactory.create(storageConf, writeConfig, context, tableConfig)) {
      HoodieWriteConfig dataCfg = ((HoodieBackedTableMetadataWriter<?, ?>) writer).dataWriteConfig;
      assertEquals(ENFORCED_VALUE, dataCfg.getProps().getProperty(ENFORCED_KEY),
          "Directly-created metadata writer must enrich the data config via the config store");
      assertTrue(dataCfg.getProps().containsKey(HoodieUberConfigStore.CONFIG_STORE_APPLIED_MARKER),
          "Enriched config must carry the applied marker");
    }
  }

  @Test
  public void testWriterSkipsWhenAlreadyApplied() throws Exception {
    Properties preMarked = new Properties();
    preMarked.setProperty(HoodieUberConfigStore.CONFIG_STORE_APPLIED_MARKER, "true");
    HoodieWriteConfig writeConfig = metadataDisabledConfig(preMarked);

    HoodieTableConfig tableConfig = buildMetaClient().getTableConfig();
    try (HoodieTableMetadataWriter writer =
             SparkMetadataWriterFactory.create(storageConf, writeConfig, context, tableConfig)) {
      HoodieWriteConfig dataCfg = ((HoodieBackedTableMetadataWriter<?, ?>) writer).dataWriteConfig;
      assertFalse(dataCfg.getProps().containsKey(ENFORCED_KEY),
          "A config already marked as applied must not be re-enriched by the metadata writer");
    }
  }

  private HoodieWriteConfig metadataDisabledConfig(Properties extraProps) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProps(extraProps)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
  }

  private HoodieTableMetaClient buildMetaClient() {
    return HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(basePath)
        .build();
  }

  private static void writeFile(File file, String content) throws IOException {
    try (FileWriter fw = new FileWriter(file)) {
      fw.write(content);
    }
  }
}
