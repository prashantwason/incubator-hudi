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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HADOOP_ENCRYPTION_TARGET_TABLE_NAMES;
import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HOODIE_FILE_WRITER_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * Tests for {@link UberEncryptionParquetConfigInjector}.
 */
public class TestUberEncryptionParquetConfigInjector {

  private static final String BASE_PATH = "hdfs:///test/hudi/table1";
  private static final String TABLE_NAME = "db.test_table";

  @Test
  public void testInjectsTableAndPartitionPathOnStorageAndHoodieConfig() {
    HoodieConfig hoodieConfig = new HoodieConfig();
    hoodieConfig.setValue(HoodieCommonConfig.BASE_PATH, BASE_PATH);
    hoodieConfig.setValue(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, TABLE_NAME);

    StoragePath dataFilePath =
        new StoragePath(BASE_PATH + "/par1/par2/test_file.parquet");
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration());

    UberEncryptionParquetConfigInjector injector = new UberEncryptionParquetConfigInjector();
    Pair<StorageConfiguration, HoodieConfig> result =
        injector.injectConfig(dataFilePath, storageConf, hoodieConfig);

    assertNotSame(storageConf, result.getLeft());
    assertNotSame(hoodieConfig, result.getRight());

    assertEquals("par1/par2",
        result.getRight().getString(HOODIE_FILE_WRITER_PARTITION_PATH));
    assertEquals("par1/par2",
        result.getLeft().getString(HOODIE_FILE_WRITER_PARTITION_PATH, ""));

    assertEquals(TABLE_NAME,
        result.getLeft().getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, ""));
    for (String key : HADOOP_ENCRYPTION_TARGET_TABLE_NAMES) {
      assertEquals(TABLE_NAME, result.getLeft().getString(key, ""),
          "Expected " + key + " to be set to table name");
    }
  }

  @Test
  public void testDoesNotOverridePresetHadoopEncryptionTargetTableNames() {
    HoodieConfig hoodieConfig = new HoodieConfig();
    hoodieConfig.setValue(HoodieCommonConfig.BASE_PATH, BASE_PATH);
    hoodieConfig.setValue(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, TABLE_NAME);

    String productionTableName = "production_table";
    Configuration hadoopConf = new Configuration();
    for (String key : HADOOP_ENCRYPTION_TARGET_TABLE_NAMES) {
      hadoopConf.set(key, productionTableName);
    }
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(hadoopConf);

    StoragePath dataFilePath = new StoragePath(BASE_PATH + "/par1/test_file.parquet");

    UberEncryptionParquetConfigInjector injector = new UberEncryptionParquetConfigInjector();
    Pair<StorageConfiguration, HoodieConfig> result =
        injector.injectConfig(dataFilePath, storageConf, hoodieConfig);

    for (String key : HADOOP_ENCRYPTION_TARGET_TABLE_NAMES) {
      assertEquals(productionTableName, result.getLeft().getString(key, ""),
          "Expected " + key + " to retain pre-set production table name");
    }
    assertEquals(TABLE_NAME,
        result.getLeft().getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, ""));
    assertEquals("par1",
        result.getRight().getString(HOODIE_FILE_WRITER_PARTITION_PATH));
  }

  @Test
  public void testDoesNotInjectTableNameWhenEmptyString() {
    HoodieConfig hoodieConfig = new HoodieConfig();
    hoodieConfig.setValue(HoodieCommonConfig.BASE_PATH, BASE_PATH);
    hoodieConfig.setValue(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "");

    StoragePath dataFilePath = new StoragePath(BASE_PATH + "/par1/test_file.parquet");
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration());

    UberEncryptionParquetConfigInjector injector = new UberEncryptionParquetConfigInjector();
    Pair<StorageConfiguration, HoodieConfig> result =
        injector.injectConfig(dataFilePath, storageConf, hoodieConfig);

    assertEquals("",
        result.getLeft().getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, ""));
    for (String key : HADOOP_ENCRYPTION_TARGET_TABLE_NAMES) {
      assertEquals("", result.getLeft().getString(key, ""),
          "Expected " + key + " to remain unset when table name is empty");
    }
    assertEquals("par1",
        result.getRight().getString(HOODIE_FILE_WRITER_PARTITION_PATH));
  }
}
