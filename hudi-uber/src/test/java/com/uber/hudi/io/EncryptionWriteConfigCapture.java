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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HADOOP_DATA_CONTEXTUAL_TARGET_TABLE_NAME;
import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HADOOP_DATA_WRITER_TARGET_TABLE_NAME;
import static com.uber.hudi.io.EncryptionParquetWriterConfigKeys.HOODIE_FILE_WRITER_PARTITION_PATH;

/**
 * Thread-local capture of encryption-related Hadoop/Hudi configs observed at Parquet write time.
 * Used by {@link CapturingHoodieRowParquetWriteSupport} in tests to assert injector behavior on
 * real write paths without depending on Uber's {@code CryptoMetadataRetriever}.
 */
public final class EncryptionWriteConfigCapture {

  private static final ThreadLocal<List<Snapshot>> CAPTURES =
      ThreadLocal.withInitial(ArrayList::new);

  private EncryptionWriteConfigCapture() {}

  /**
   * Records encryption-related keys from the Hadoop conf and Hoodie config passed to write support.
   */
  public static void record(Configuration hadoopConf, HoodieConfig hoodieConfig) {
    CAPTURES.get().add(new Snapshot(
        hadoopConf.get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY),
        hadoopConf.get(HOODIE_FILE_WRITER_PARTITION_PATH),
        hadoopConf.get(HADOOP_DATA_WRITER_TARGET_TABLE_NAME),
        hadoopConf.get(HADOOP_DATA_CONTEXTUAL_TARGET_TABLE_NAME),
        hoodieConfig.getString(HOODIE_FILE_WRITER_PARTITION_PATH),
        hoodieConfig.getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY)));
  }

  public static List<Snapshot> getCaptures() {
    return Collections.unmodifiableList(new ArrayList<>(CAPTURES.get()));
  }

  public static void clear() {
    CAPTURES.get().clear();
  }

  /** Immutable view of encryption configs at a single Parquet file write. */
  public static final class Snapshot {
    private final String hadoopHoodieTableName;
    private final String hadoopPartitionPath;
    private final String hadoopWriterTargetTableName;
    private final String hadoopContextualTargetTableName;
    private final String hoodieConfigPartitionPath;
    private final String hoodieConfigTableName;

    Snapshot(
        String hadoopHoodieTableName,
        String hadoopPartitionPath,
        String hadoopWriterTargetTableName,
        String hadoopContextualTargetTableName,
        String hoodieConfigPartitionPath,
        String hoodieConfigTableName) {
      this.hadoopHoodieTableName = hadoopHoodieTableName;
      this.hadoopPartitionPath = hadoopPartitionPath;
      this.hadoopWriterTargetTableName = hadoopWriterTargetTableName;
      this.hadoopContextualTargetTableName = hadoopContextualTargetTableName;
      this.hoodieConfigPartitionPath = hoodieConfigPartitionPath;
      this.hoodieConfigTableName = hoodieConfigTableName;
    }

    public String getHadoopHoodieTableName() {
      return hadoopHoodieTableName;
    }

    public String getHadoopPartitionPath() {
      return hadoopPartitionPath;
    }

    public String getHadoopWriterTargetTableName() {
      return hadoopWriterTargetTableName;
    }

    public String getHadoopContextualTargetTableName() {
      return hadoopContextualTargetTableName;
    }

    public String getHoodieConfigPartitionPath() {
      return hoodieConfigPartitionPath;
    }

    public String getHoodieConfigTableName() {
      return hoodieConfigTableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Snapshot snapshot = (Snapshot) o;
      return Objects.equals(hadoopHoodieTableName, snapshot.hadoopHoodieTableName)
          && Objects.equals(hadoopPartitionPath, snapshot.hadoopPartitionPath)
          && Objects.equals(hadoopWriterTargetTableName, snapshot.hadoopWriterTargetTableName)
          && Objects.equals(hadoopContextualTargetTableName, snapshot.hadoopContextualTargetTableName)
          && Objects.equals(hoodieConfigPartitionPath, snapshot.hoodieConfigPartitionPath)
          && Objects.equals(hoodieConfigTableName, snapshot.hoodieConfigTableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          hadoopHoodieTableName,
          hadoopPartitionPath,
          hadoopWriterTargetTableName,
          hadoopContextualTargetTableName,
          hoodieConfigPartitionPath,
          hoodieConfigTableName);
    }
  }
}
