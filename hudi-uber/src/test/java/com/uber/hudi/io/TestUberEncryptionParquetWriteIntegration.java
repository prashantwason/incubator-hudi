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

package com.uber.hudi.io;

import com.uber.hudi.tools.HoodieToolsFunctionalTest;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests that exercise the InternalRow Parquet write path ({@link HoodieRowCreateHandle})
 * and verify encryption configs reach {@link HoodieRowParquetWriteSupport} via
 * {@link UberEncryptionParquetConfigInjector}.
 */
public class TestUberEncryptionParquetWriteIntegration extends HoodieToolsFunctionalTest {

  private static final String TABLE_NAME = "test_db.test_encryption_table";
  private static final String INSTANT_TIME = "000";
  private static final String PARTITION_PATH = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
  private static final String TEST_SCHEMA =
      "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}";
  private static final StructType TEST_STRUCT_TYPE = new StructType(new StructField[] {
      new StructField("value", DataTypes.StringType, false, Metadata.empty())
  });

  @BeforeEach
  @Override
  public void setUp() throws java.io.IOException {
    EncryptionWriteConfigCapture.clear();
    super.setUp();
  }

  @AfterEach
  @Override
  public void tearDown() {
    EncryptionWriteConfigCapture.clear();
    super.tearDown();
  }

  @Test
  public void testBulkInsertInjectsEncryptionConfigsAtWriteSupport() throws Exception {
    HoodieWriteConfig writeConfig = buildWriteConfigWithCapturingWriteSupport();

    writeViaRowCreateHandle(writeConfig, null);

    List<EncryptionWriteConfigCapture.Snapshot> captures =
        EncryptionWriteConfigCapture.getCaptures();
    assertFalse(captures.isEmpty(), "Expected at least one Parquet write capture");

    EncryptionWriteConfigCapture.Snapshot snapshot = captures.get(0);
    assertEquals(TABLE_NAME, snapshot.getHadoopHoodieTableName());
    assertEquals(TABLE_NAME, snapshot.getHoodieConfigTableName());
    assertEquals(TABLE_NAME, snapshot.getHadoopWriterTargetTableName());
    assertEquals(TABLE_NAME, snapshot.getHadoopContextualTargetTableName());
    assertNotNull(snapshot.getHadoopPartitionPath());
    assertFalse(snapshot.getHadoopPartitionPath().isEmpty());
    assertEquals(snapshot.getHadoopPartitionPath(), snapshot.getHoodieConfigPartitionPath());
  }

  @Test
  public void testBulkInsertDoesNotOverridePresetHadoopEncryptionTableNames() throws Exception {
    String productionTable = "production.shadow_table";
    HoodieWriteConfig writeConfig = buildWriteConfigWithCapturingWriteSupport();

    writeViaRowCreateHandle(writeConfig, table -> {
      table.getStorageConf().set(
          EncryptionParquetWriterConfigKeys.HADOOP_DATA_WRITER_TARGET_TABLE_NAME, productionTable);
      table.getStorageConf().set(
          EncryptionParquetWriterConfigKeys.HADOOP_DATA_CONTEXTUAL_TARGET_TABLE_NAME, productionTable);
    });

    List<EncryptionWriteConfigCapture.Snapshot> captures =
        EncryptionWriteConfigCapture.getCaptures();
    assertFalse(captures.isEmpty());

    for (EncryptionWriteConfigCapture.Snapshot snapshot : captures) {
      assertEquals(productionTable, snapshot.getHadoopWriterTargetTableName());
      assertEquals(productionTable, snapshot.getHadoopContextualTargetTableName());
      assertEquals(TABLE_NAME, snapshot.getHadoopHoodieTableName());
    }
  }

  private HoodieWriteConfig buildWriteConfigWithCapturingWriteSupport() {
    HoodieStorageConfig storageConfig = HoodieStorageConfig.newBuilder()
        .withParquetConfigInjectorClass(UberEncryptionParquetConfigInjector.class.getName())
        .build();
    storageConfig.setValue(
        HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS,
        CapturingHoodieRowParquetWriteSupport.class.getName());

    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .forTable(TABLE_NAME)
        .withSchema(TEST_SCHEMA)
        .withPopulateMetaFields(false)
        .withMarkersType(MarkerType.DIRECT.name())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withStorageConfig(storageConfig)
        .build();
  }

  private void writeViaRowCreateHandle(HoodieWriteConfig writeConfig,
                                       java.util.function.Consumer<HoodieTable> tableCustomizer)
      throws Exception {
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    if (tableCustomizer != null) {
      tableCustomizer.accept(table);
    }

    String fileId = UUID.randomUUID().toString();
    HoodieRowCreateHandle handle = new HoodieRowCreateHandle(
        table, writeConfig, PARTITION_PATH, fileId, INSTANT_TIME,
        0, 0L, 0L, TEST_STRUCT_TYPE);

    InternalRow row = new GenericInternalRow(new Object[] {UTF8String.fromString("test-value")});
    handle.write(row);

    WriteStatus writeStatus = handle.close();
    assertFalse(writeStatus.hasErrors());
  }
}
