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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;

public class HoodieStorageReaderFactory {

  public static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieStorageReader<R> getStorageReader(
      Configuration conf, Path path) throws IOException {
    final String extension = FSUtils.getFileExtension(path.toString());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetStorageReader(conf, path);
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileStorageReader(conf, path);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieStorageReader<R> newParquetStorageReader(
      Configuration conf, Path path) throws IOException {
    return new HoodieParquetReader<>(conf, path);
  }

  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieStorageReader<R> newHFileStorageReader(
      Configuration conf, Path path) throws IOException {

    CacheConfig cacheConfig = new CacheConfig(conf);
    return new HoodieHFileReader<>(conf, path, cacheConfig);
  }
}
