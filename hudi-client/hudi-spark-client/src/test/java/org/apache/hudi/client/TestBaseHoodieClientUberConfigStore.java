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

package org.apache.hudi.client;

import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies that HoodieUberConfigStore is applied before buildTransactionManager and
 * buildTimeGenerator inside BaseHoodieClient — HUDI-9064 Bug #1 and Bug #2.
 *
 * Bug #1: The lock-provider validation in HoodieSparkIndexClient.getWriteClient() used to
 * fire before SparkRDDWriteClient was constructed, so UberConfigStore never had a chance to
 * supply hoodie.write.lock.provider.
 *
 * Bug #2: BaseHoodieClient built TransactionManager (LockManager) and TimeGenerator from the
 * un-enriched config before applyConfigStore ran in the 5-arg ctor body, leaving both objects
 * with a stale lockConfiguration that lacked the ZK URL/base_path.
 */
public class TestBaseHoodieClientUberConfigStore extends HoodieSparkClientTestHarness {

  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    HoodieTestUtils.init(storageConf, basePath);
    // Point UberConfigStore at a local temp directory. IS_TESTING=true would normally
    // suppress config-store reads; setTestConfigStorePath overrides that for this test.
    String configStorePath = tempDir.toAbsolutePath().toString();
    writeFile(new File(configStorePath, "hudi_config_enforced.conf"),
        "hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider\n");
    writeFile(new File(configStorePath, "hudi_config_fallback.conf"), "");
    HoodieUberConfigStore.setTestConfigStorePath(configStorePath);
  }

  @AfterEach
  public void tearDown() throws Exception {
    HoodieUberConfigStore.clearTestConfigStorePath();
    cleanupResources();
  }

  /**
   * Regression test for HUDI-9064 Bug #1 and Bug #2.
   *
   * Before the fix:
   * - Bug #1: constructing SparkRDDWriteClient with OCC but no lock provider would throw
   *   IllegalArgumentException (wrapped in HoodieException) because HoodieSparkIndexClient's
   *   validation read localWriteConfig before BaseHoodieClient enriched it.
   * - Bug #2: even if construction succeeded, LockManager.getLockProvider() and
   *   createNewInstantTime(true) would fail because TransactionManager and TimeGenerator were
   *   built from the un-enriched config.
   *
   * After the fix both succeed: the config store supplies the lock provider, and both builders
   * receive the enriched config before snapshotting lockConfiguration.
   */
  @Test
  public void testSparkRDDWriteClientAppliesConfigStoreBeforeBuildingTransactionManagerAndTimeGenerator() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        // OCC triggers the multi-writer lock-provider check but no lock provider is set here —
        // it must come from the test config store (hudi_config_enforced.conf).
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {
      // Bug #1 regression: construction must not throw; enriched config has the provider class.
      assertNotNull(client.getConfig().getLockProviderClass(),
          "Lock provider class must be populated by UberConfigStore before validation");

      // Bug #2 regression (LockManager): getLockProvider() constructs the provider using
      // the lockConfiguration snapshot captured at LockManager construction time.
      // Before the fix this threw because the snapshot lacked hoodie.write.lock.provider.
      assertNotNull(client.getTransactionManager().getLockManager().getLockProvider(),
          "LockManager.getLockProvider() must succeed — lockConfiguration must be enriched");

      // Bug #2 regression (TimeGenerator): createNewInstantTime(true) routes to
      // generateTime(skipLocking=false) which calls getLockProvider().tryLock() via
      // TimeGeneratorBase using its own lockConfiguration snapshot.
      // Before the fix this threw because the TimeGenerator snapshot was un-enriched.
      assertDoesNotThrow(() -> client.createNewInstantTime(true),
          "TimeGenerator must also see the enriched lock config");
    }
  }

  private static void writeFile(File file, String content) throws IOException {
    try (FileWriter fw = new FileWriter(file)) {
      fw.write(content);
    }
  }
}
