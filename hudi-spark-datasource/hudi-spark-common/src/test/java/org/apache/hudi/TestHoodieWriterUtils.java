/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getMetaClientBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieWriterUtils extends HoodieClientTestBase {

  @Test
  void validateTableConfig() throws IOException {
    HoodieTableMetaClient tableMetaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
        .initTable(storageConf, tempDir.resolve("table1").toString());
    HoodieTableConfig tableConfig = tableMetaClient.getTableConfig();
    TypedProperties properties = TypedProperties.copy(tableConfig.getProps());
    properties.put(HoodieTableConfig.DATABASE_NAME.key(), "databaseFromCatalog");
    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(sparkSession, JavaScalaConverters.convertJavaPropertiesToScalaMap(properties), tableConfig));
  }

  @Test
  void testReturnsKeyWhenTableConfigIsNull() {
    assertEquals("randomKey", HoodieWriterUtils.getKeyInTableConfig("randomKey", null));
  }

  @Test
  void testPayloadClassNameNotVersion9() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, "8");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    assertEquals(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), result);
  }

  @Test
  void testPayloadClassNameVersion9WithLegacyPayload() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "com.example.LegacyPayload");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    assertEquals(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key(), result);
  }

  @Test
  void testPayloadClassNameVersion9WithoutLegacyPayload() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    assertEquals(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), result);
  }

  @Test
  void testRecordMergeModeMappingWithVersion9() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "9");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_MODE.key(), config);
    assertEquals(HoodieTableConfig.RECORD_MERGE_MODE.key(), result);
  }

  @Test
  void testRecordMergeModeMappingWithVersion8() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "8");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_MODE.key(), config);
    assertEquals(HoodieWriteConfig.RECORD_MERGE_MODE.key(), result);
  }

  @Test
  void testRecordMergeStrategyIdMappingWithVersion9() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "9");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key(), config);
    assertEquals(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), result);
  }

  @Test
  void testRecordMergeStrategyIdMappingWithVersion8() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "8");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key(), config);
    assertEquals(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key(), result);
  }

  @Test
  void testFallbackToOriginalKey() {
    HoodieConfig config = new HoodieConfig();
    String result = HoodieWriterUtils.getKeyInTableConfig("my.custom.key", config);
    assertEquals("my.custom.key", result);
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithCustomMergeMode() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name());

    String payloadClass = "com.example.CustomPayload";
    assertFalse(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, config));
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithEmptyPayload() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());

    String payloadClass = "";
    assertTrue(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, config));
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithCommitTimeOrdering() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());

    String payloadClass = "com.example.CustomPayload";
    assertTrue(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, config));
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithEventTimeOrdering() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());

    String payloadClass = "com.example.CustomPayload";
    assertTrue(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, config));
  }

  // ---------------------------------------------------------------------------
  // Fix 1: writer-side qualified `db.table` for hoodie.table.name should be
  // treated as equivalent to the on-disk pair (hoodie.database.name, hoodie.table.name).
  // ---------------------------------------------------------------------------

  @Test
  void validateTableConfig_qualifiedTableName_acceptedWhenSplitMatchesOnDisk() throws IOException {
    HoodieTableMetaClient metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "db1")
        .initTable(storageConf, tempDir.resolve("qualifiedNameOk").toString());
    HoodieTableConfig tableConfig = metaClient.getTableConfig();

    TypedProperties writerParams = TypedProperties.copy(tableConfig.getProps());
    writerParams.put(HoodieTableConfig.NAME.key(), "db1." + RAW_TRIPS_TEST_NAME);

    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(
        sparkSession,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(writerParams),
        tableConfig));
  }

  @Test
  void validateTableConfig_qualifiedTableName_dbPrefixMismatch_logsButDoesNotThrow() throws IOException {
    HoodieTableMetaClient metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "db1")
        .initTable(storageConf, tempDir.resolve("qualifiedNameWrongDb").toString());
    HoodieTableConfig tableConfig = metaClient.getTableConfig();

    TypedProperties writerParams = TypedProperties.copy(tableConfig.getProps());
    writerParams.put(HoodieTableConfig.NAME.key(), "db2." + RAW_TRIPS_TEST_NAME);

    // Config mismatches are now logged instead of throwing.
    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(
        sparkSession,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(writerParams),
        tableConfig));
  }

  @Test
  void validateTableConfig_qualifiedTableName_tableSuffixMismatch_logsButDoesNotThrow() throws IOException {
    HoodieTableMetaClient metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "db1")
        .initTable(storageConf, tempDir.resolve("qualifiedNameWrongTable").toString());
    HoodieTableConfig tableConfig = metaClient.getTableConfig();

    TypedProperties writerParams = TypedProperties.copy(tableConfig.getProps());
    writerParams.put(HoodieTableConfig.NAME.key(), "db1.some_other_table");

    // Config mismatches are now logged instead of throwing.
    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(
        sparkSession,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(writerParams),
        tableConfig));
  }

  // ---------------------------------------------------------------------------
  // Fix 2a: when on-disk hoodie.table.recordkey.fields is unset and the writer
  // supplies a non-empty recordkey, log a WARN instead of throwing.
  // ---------------------------------------------------------------------------

  @Test
  void validateTableConfig_nullOnDiskRecordKey_warnsButDoesNotThrow() throws IOException {
    HoodieTableMetaClient metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
        .initTable(storageConf, tempDir.resolve("nullRecordKeyWarn").toString());
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    // Simulate a legacy table whose hoodie.properties never persisted recordkey.fields.
    tableConfig.getProps().remove(HoodieTableConfig.RECORDKEY_FIELDS.key());
    assertTrue(tableConfig.getInt(HoodieTableConfig.VERSION) > 1,
        "test precondition: table version must be > 1");

    TypedProperties writerParams = TypedProperties.copy(tableConfig.getProps());
    writerParams.put("hoodie.datasource.write.recordkey.field", "unique_key");

    // No metaClient passed -> WARN-only, no backfill, no throw.
    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(
        sparkSession,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(writerParams),
        tableConfig));
  }

  @Test
  void validateTableConfig_mismatchedNonNullRecordKeys_logsButDoesNotThrow() throws IOException {
    HoodieTableMetaClient metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
        .initTable(storageConf, tempDir.resolve("recordKeyMismatch").toString());
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    tableConfig.getProps().setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "k_on_disk");

    TypedProperties writerParams = TypedProperties.copy(tableConfig.getProps());
    writerParams.put("hoodie.datasource.write.recordkey.field", "k_writer");

    // Config mismatches (including record key) are now logged instead of throwing.
    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(
        sparkSession,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(writerParams),
        tableConfig));
  }

  // ---------------------------------------------------------------------------
  // Fix 2b: when a metaClient is provided AND on-disk recordkey is unset, the
  // writer's value is persisted to hoodie.properties via HoodieTableConfig.update.
  // ---------------------------------------------------------------------------

  @Test
  void validateTableConfig_nullOnDiskRecordKey_backfillsWhenMetaClientProvided() throws IOException {
    HoodieTableMetaClient metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
        .initTable(storageConf, tempDir.resolve("nullRecordKeyBackfill").toString());

    // Strip recordkey.fields from on-disk hoodie.properties so this resembles the
    // legacy 0.14 SparkSQL CREATE TABLE state we're trying to heal.
    HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
        Collections.singleton(HoodieTableConfig.RECORDKEY_FIELDS.key()));
    metaClient.reloadTableConfig();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Assertions.assertNull(tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS),
        "test precondition: recordkey.fields must be unset on disk before validate is invoked");
    assertTrue(tableConfig.getInt(HoodieTableConfig.VERSION) > 1,
        "test precondition: table version must be > 1");

    TypedProperties writerParams = TypedProperties.copy(tableConfig.getProps());
    writerParams.put("hoodie.datasource.write.recordkey.field", "unique_key");

    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(
        sparkSession,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(writerParams),
        tableConfig,
        false,
        metaClient));

    // Reload from disk and verify the recordkey was persisted.
    metaClient.reloadTableConfig();
    String persisted = metaClient.getTableConfig().getString(HoodieTableConfig.RECORDKEY_FIELDS);
    assertNotNull(persisted, "recordkey.fields should have been backfilled to hoodie.properties");
    assertEquals("unique_key", persisted);
  }
}
