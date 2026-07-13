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

package org.apache.hudi.replication.util;

import org.apache.hudi.storage.StoragePath;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.VERSION;
import org.apache.hudi.common.util.StringUtils;

import static org.apache.hudi.replication.util.ReplicationPropertiesManager.REPLICATION_PROPERTIES_FILE;
import static org.apache.hudi.replication.util.ReplicationPropertiesManager.REPLICATION_PROPERTIES_FILE_BACKUP;
import static org.apache.hudi.replication.util.ReplicationPropertiesManager.REPLICATION_PROPERTIES_LOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestReplicationPropertiesManager {

  @TempDir
  public java.nio.file.Path tempDir;
  protected String basePath;
  protected HoodieTableMetaClient metaClient;

  protected void initPath() {
    java.nio.file.Path path = tempDir.resolve("dataset");
    try {
      java.nio.file.Files.createDirectories(path);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
    this.basePath = path.toAbsolutePath().toString();
  }

  protected void initMetaClient() throws IOException {
    if (basePath == null) {
      initPath();
    }
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
  }

  static String METADATA_ENABLED_KEY = "hoodie.table.metadata.enabled";
  static String LAST_REPLICATED_COMMIT = "hoodie.table.replicated.timestamp";
  static String REPLICATION_PROPERTIES_TEST_KEY1 = "hoodie.test.key1";
  static String REPLICATION_PROPERTIES_TEST_KEY2 = "hoodie.test.key2";
  static String REPLICATION_PROPERTIES_TEST_KEY3 = "hoodie.test.key3";
  static String REPLICATION_PROPERTIES_TEST_KEY4 = "hoodie.test.key4";

  /**
   * Functional interface for operations that may need retry with exponential backoff.
   */
  @FunctionalInterface
  public interface RetryableOperation<T> {
    T execute() throws Exception;
  }

  /**
   * Executes a retryable operation with exponential backoff.
   * Retries up to 5 times with exponential backoff starting at 1000ms.
   *
   * @param operation The operation to execute
   * @param <T>       The return type of the operation
   * @return The result of the operation
   * @throws Exception The last exception thrown if all retries fail
   */
  public static <T> T executeWithExponentialBackoff(RetryableOperation<T> operation) throws Exception {
    return executeWithExponentialBackoff(operation, 5, 1000);
  }

  /**
   * Executes a retryable operation with exponential backoff.
   *
   * @param operation      The operation to execute
   * @param maxRetries     Maximum number of retry attempts
   * @param initialDelayMs Initial delay in milliseconds
   * @param <T>            The return type of the operation
   * @return The result of the operation
   * @throws Exception The last exception thrown if all retries fail
   */
  public static <T> T executeWithExponentialBackoff(RetryableOperation<T> operation, int maxRetries, long initialDelayMs) throws Exception {
    Exception lastException = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return operation.execute();
      } catch (Exception e) {
        lastException = e;
        if (attempt == maxRetries) {
          // This was the last attempt, throw the exception
          throw e;
        }
        // Calculate exponential backoff delay: initialDelay * attempt + jitter
        long delayMs = initialDelayMs * attempt + (int) (Math.random() * 100);
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
          throw new RuntimeException("Thread interrupted during retry backoff", ie);
        }
      }
    }
    // This should never be reached, but just in case
    throw lastException;
  }

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  private ReplicationPropertiesManager createManager() {
    return new ReplicationPropertiesManager(metaClient);
  }

  @Test
  public void testSetPropertyBackfillsTableNameWhenMissing() throws Exception {
    // Write replication.properties without hoodie.table.name (simulating an older file).
    StoragePath replicationPropertiesFile = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE);
    Properties propsWithoutName = new Properties();
    propsWithoutName.setProperty(REPLICATION_PROPERTIES_TEST_KEY1, "some-value");
    try (OutputStream out = metaClient.getStorage().create(replicationPropertiesFile, false)) {
      propsWithoutName.store(out, "replication.properties without hoodie.table.name");
    }

    ReplicationPropertiesManager propertiesManager = new ReplicationPropertiesManager(metaClient);

    // setProperty must succeed even when hoodie.table.name is absent:
    // the manager backfills it from hoodie.properties before writing.
    propertiesManager.setProperty(REPLICATION_PROPERTIES_TEST_KEY2, "new-value");

    Properties result = propertiesManager.readProperties();
    assertTrue(result.containsKey(REPLICATION_PROPERTIES_TEST_KEY2));
    assertEquals("new-value", result.getProperty(REPLICATION_PROPERTIES_TEST_KEY2));
    assertTrue(result.containsKey(HoodieTableConfig.NAME.key()),
        "hoodie.table.name should be backfilled from hoodie.properties");
    assertEquals(metaClient.getTableConfig().getTableName(), result.getProperty(HoodieTableConfig.NAME.key()));
    // Original key must still be present
    assertTrue(result.containsKey(REPLICATION_PROPERTIES_TEST_KEY1));
  }

  @Test
  public void testReplicationPropertiesManager() throws Exception {
    StoragePath lockPath = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_LOCK);
    TypedProperties lockProperties = new TypedProperties();
    lockProperties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, metaClient.getMetaPath().toString());
    lockProperties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "10");
    lockProperties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");

    ReplicationPropertiesManager propertiesManager = createManager();

    // Ensure hoodie.properties are not added in replication.properties before update
    Properties props = propertiesManager.readProperties();
    assertFalse(props.containsKey(VERSION.key()));

    // If replication.properties file does not exist, and replication.properties.backup file does not exist,
    // then updating replication.properties should not fail since it will create a new file
    propertiesManager.setProperty(REPLICATION_PROPERTIES_TEST_KEY1, "true");
    props = propertiesManager.readProperties();
    assertTrue(props.containsKey(REPLICATION_PROPERTIES_TEST_KEY1));

    // If replication.properties file does not exist, and replication.properties.backup file does exist,
    // then request should be served using replication.properties.backup
    // Step-1: Creating replication.properties.backup file for testing this Scenario
    StoragePath replicationPropertiesFileBackup = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE_BACKUP);
    metaClient.getStorage().deleteFile(replicationPropertiesFileBackup);
    metaClient.getStorage().deleteFile(new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE));
    Properties replicationPropsBackup = new TypedProperties();
    replicationPropsBackup.setProperty(HoodieTableConfig.NAME.key(), metaClient.getTableConfig().getTableName());
    replicationPropsBackup.setProperty(REPLICATION_PROPERTIES_TEST_KEY1, "true");
    replicationPropsBackup.setProperty(REPLICATION_PROPERTIES_TEST_KEY2, "false");
    try (OutputStream out = metaClient.getStorage().create(replicationPropertiesFileBackup, false)) {
      replicationPropsBackup.store(out, "Adding test props in  replication.properties.backup");
    }
    assertTrue(metaClient.getStorage().exists(replicationPropertiesFileBackup));
    // Step-2: setProperty should read from replication.properties.backup file and add the new property
    propertiesManager.setProperty(REPLICATION_PROPERTIES_TEST_KEY3, "true");
    props = propertiesManager.readProperties();
    assertTrue(props.containsKey(REPLICATION_PROPERTIES_TEST_KEY3));

    // Ensure hoodie.properties are not added in replication.properties after update
    props = propertiesManager.readProperties();
    assertFalse(props.containsKey(VERSION.key()));
    propertiesManager.setProperty(REPLICATION_PROPERTIES_TEST_KEY1, "true");
    props = propertiesManager.readProperties();
    assertTrue(props.containsKey(REPLICATION_PROPERTIES_TEST_KEY1));

    // Ensure that all the props in replication.properties are not missing after update
    // This testcase does not cover concurrency scenarios
    Properties replicationPropsBeforeUpdate = new Properties();
    StoragePath replicationPropertiesFile = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE);
    try (InputStream is = metaClient.getStorage().open(replicationPropertiesFile)) {
      replicationPropsBeforeUpdate.load(is);
    }
    ReplicationPropertiesManager propertiesManagerForUpdate = createManager();
    propertiesManagerForUpdate.setProperty(REPLICATION_PROPERTIES_TEST_KEY4, "false");
    Properties replicationPropsAfterUpdate = propertiesManagerForUpdate.readProperties();
    replicationPropsBeforeUpdate.forEach((key, value) -> assertTrue(replicationPropsAfterUpdate.containsKey(key)));
    assertFalse(replicationPropsAfterUpdate.containsKey(VERSION.key()));
    assertTrue(replicationPropsAfterUpdate.containsKey(REPLICATION_PROPERTIES_TEST_KEY4));

    // adding a new entry should succeed
    propertiesManager.setProperty(METADATA_ENABLED_KEY, "true");
    props = propertiesManager.readProperties();
    assertTrue(props.containsKey(METADATA_ENABLED_KEY));
    assertEquals("true", props.get(METADATA_ENABLED_KEY));

    // create a marker file to mimic another application writing
    metaClient.getStorage().create(lockPath, false).close();

    // Wait for the marker file to timeout and then update the hoodie.properties.
    propertiesManager.setProperty(LAST_REPLICATED_COMMIT, "001");
    props = propertiesManager.readProperties();
    assertTrue(props.containsKey(LAST_REPLICATED_COMMIT));
    assertEquals("001", props.get(LAST_REPLICATED_COMMIT));

    // create a marker file to mimic another application writing
    if (metaClient.getStorage().exists(lockPath)) {
      metaClient.getStorage().deleteFile(lockPath);
    }
    metaClient.getStorage().create(lockPath, false).close();
    Thread.sleep(2000);

    Properties props1 = propertiesManager.readProperties();
    assertEquals(props.keySet().size(), props1.keySet().size());
  }

  @Test
  public void testConcurrentThreadUpdatesToReplicationProperties() throws Exception {
    // Create initial properties file with some test properties
    ReplicationPropertiesManager propertiesManager = createManager();
    Properties initialProps = new Properties();
    initialProps.setProperty(REPLICATION_PROPERTIES_TEST_KEY1, "initial_value");
    initialProps.setProperty(REPLICATION_PROPERTIES_TEST_KEY2, "initial_value");
    propertiesManager.setProperty(initialProps);

    // Number of threads to spawn
    int numThreads = 2;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger totalUpdates = new AtomicInteger(0);

    // Create and start threads
    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      new Thread(() -> {
        try {
          // Wait for all threads to be ready
          startLatch.await();
          // Note: When running on local machine (macOS), file creation is not atomic, when two threads are racing to
          // create the lock file both may report success.
          Thread.sleep(threadId * 10);

          // Each thread gets its own ReplicationPropertiesManager instance
          ReplicationPropertiesManager threadPropertiesManager = createManager();

          // Each thread performs multiple updates to simulate a process with multiple checkpoints
          for (int updateNum = 0; updateNum < 3; updateNum++) {
            Properties props = new Properties();
            String key = String.format("hoodie.thread.%d.checkpoint.%d", threadId, updateNum);
            String value = String.format("value-%d-%d", threadId, updateNum);
            props.setProperty(key, value);
            boolean success = executeWithExponentialBackoff(() -> {
              // update replication.properties
              return threadPropertiesManager.setProperty(props);
            });
            if (success) {
              successCount.incrementAndGet();
            }
            totalUpdates.incrementAndGet();

            // 5-10sec Delay between updates (for local filesystem with checksum file)
            Thread.sleep(5000 + (int) (Math.random() * 5000));
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("Thread " + threadId + " failed with exception: " + e.getMessage());
        } finally {
          endLatch.countDown();
        }
      }).start();
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    endLatch.await(30, TimeUnit.SECONDS);

    // Verify final state
    ReplicationPropertiesManager readPropManager = createManager();
    Properties finalProps = readPropManager.readProperties();

    // Verify all thread updates are present
    for (int i = 0; i < numThreads; i++) {
      for (int updateNum = 0; updateNum < 3; updateNum++) {
        String key = String.format("hoodie.thread.%d.checkpoint.%d", i, updateNum);
        String expectedValue = String.format("value-%d-%d", i, updateNum);

        assertTrue(finalProps.containsKey(key),
            String.format("Missing property %s from thread %d checkpoint %d", key, i, updateNum));
        assertEquals(expectedValue, finalProps.getProperty(key),
            String.format("Incorrect value for property %s from thread %d checkpoint %d", key, i, updateNum));
      }
    }

    // Verify original properties are still present
    assertTrue(finalProps.containsKey(REPLICATION_PROPERTIES_TEST_KEY1));
    assertEquals("initial_value", finalProps.get(REPLICATION_PROPERTIES_TEST_KEY1));
    assertTrue(finalProps.containsKey(REPLICATION_PROPERTIES_TEST_KEY2));
    assertEquals("initial_value", finalProps.get(REPLICATION_PROPERTIES_TEST_KEY2));

    // Verify that all updates were successful
    assertEquals(totalUpdates.get(), successCount.get(),
        "All updates should have been successful. Expected " + totalUpdates.get()
                + " successful updates but got " + successCount.get());
  }

  @Test
  public void testReadReplicationPropertiesWithDifferentCases() throws Exception {
    testReadReplicationPropertiesCase(false, false);
    testReadReplicationPropertiesCase(true, false);
    testReadReplicationPropertiesCase(false, true);
    testReadReplicationPropertiesCase(true, true);
  }

  private void testReadReplicationPropertiesCase(boolean isActualFilePresent, boolean isBackupFilePresent) throws Exception {
    StoragePath primaryPath = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE);
    StoragePath backupPath = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE_BACKUP);

    if (!isActualFilePresent && metaClient.getStorage().exists(primaryPath)) {
      metaClient.getStorage().deleteFile(primaryPath);
    }
    if (!isBackupFilePresent && metaClient.getStorage().exists(backupPath)) {
      metaClient.getStorage().deleteFile(backupPath);
    }

    if (isActualFilePresent) {
      ReplicationPropertiesManager propertiesManager = new ReplicationPropertiesManager(metaClient);
      propertiesManager.readProperties();
      propertiesManager.setProperty("lastTs", "555");
    }

    if (isBackupFilePresent) {
      Properties backupProps = new Properties();
      backupProps.setProperty(HoodieTableConfig.NAME.key(), metaClient.getTableConfig().getTableName());
      backupProps.setProperty(HoodieTableConfig.TYPE.key(), metaClient.getTableConfig().getTableType().toString());
      backupProps.setProperty("lastTs", "777");
      try (OutputStream out = metaClient.getStorage().create(backupPath, true)) {
        backupProps.store(out, "backup");
      }
    }

    Properties readResult = new ReplicationPropertiesManager(metaClient).readProperties();
    String result = readResult.getProperty("lastTs", StringUtils.EMPTY_STRING);
    if (isActualFilePresent) {
      assertEquals("555", result);
    } else if (isBackupFilePresent) {
      assertEquals("777", result);
    } else {
      assertEquals(StringUtils.EMPTY_STRING, result);
    }

    // Cleanup for next iteration
    if (metaClient.getStorage().exists(primaryPath)) {
      metaClient.getStorage().deleteFile(primaryPath);
    }
    if (metaClient.getStorage().exists(backupPath)) {
      metaClient.getStorage().deleteFile(backupPath);
    }
  }
}
