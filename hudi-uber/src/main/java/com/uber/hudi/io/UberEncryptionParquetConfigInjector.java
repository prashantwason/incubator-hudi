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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.HoodieParquetConfigInjector;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.hudi.common.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HADOOP_ENCRYPTION_TARGET_TABLE_NAMES;
import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HOODIE_FILE_WRITER_PARTITION_PATH;

/**
 * Uber {@link HoodieParquetConfigInjector} that populates table and partition-path hints on both
 * Hadoop storage configuration and {@link HoodieConfig} for Parquet CLAC/encryption metadata
 * resolution.
 *
 * <p>Mirrors the release-branch {@code setTableAndPartitionPathForEncryption} behavior, wired
 * through the OSS 1.2 {@link HoodieParquetConfigInjector} extension point.
 */
public class UberEncryptionParquetConfigInjector implements HoodieParquetConfigInjector {

  private static final Logger LOG = LoggerFactory.getLogger(UberEncryptionParquetConfigInjector.class);

  @Override
  public Pair<StorageConfiguration, HoodieConfig> injectConfig(
      StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
    StorageConfiguration<?> copiedStorageConf = storageConf.newInstance();
    HoodieConfig copiedHoodieConfig = HoodieConfig.copy(hoodieConfig.getProps());

    String relativePartitionPath = getRelativePartitionPath(copiedHoodieConfig, path);
    copiedHoodieConfig.setValue(HOODIE_FILE_WRITER_PARTITION_PATH, relativePartitionPath);
    copiedStorageConf.set(HOODIE_FILE_WRITER_PARTITION_PATH, relativePartitionPath);

    String tableName = copiedHoodieConfig.getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    if (!StringUtils.isNullOrEmpty(tableName)) {
      copiedStorageConf.set(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, tableName);

      // Populate hadoop writer table name fields only when unset, so staging writes can shadow
      // production table names for encryption key resolution (HUDI-7484).
      if (HADOOP_ENCRYPTION_TARGET_TABLE_NAMES.stream()
          .allMatch(key -> copiedStorageConf.getString(key, "").isEmpty())) {
        for (String key : HADOOP_ENCRYPTION_TARGET_TABLE_NAMES) {
          copiedStorageConf.set(key, tableName);
        }
      }
    }

    LOG.debug("Injected encryption parquet writer configs for path {}: partitionPath={}",
        path, relativePartitionPath);
    return Pair.of(copiedStorageConf, copiedHoodieConfig);
  }

  private static String getRelativePartitionPath(HoodieConfig hoodieConfig, StoragePath dataFilePath) {
    String basePath = hoodieConfig.getString(HoodieCommonConfig.BASE_PATH);
    StoragePath partitionPath = dataFilePath.getParent();
    return FSUtils.getRelativePartitionPath(new StoragePath(basePath), partitionPath);
  }
}
