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

package org.apache.hudi.common.metadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCommitMetadata;
import org.apache.hudi.avro.model.HoodieCompactionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieMetadataException;

/**
 * A MetadataStore which fetches all information from the file system. It does not maintain
 * any database or additional information.
 */
public class MetadataStoreSimple extends MetadataStore {
  private String basePath;
  private FileSystem fs;

  public MetadataStoreSimple(String basePath, Configuration conf) {
    super(basePath, conf);
    this.fs = FSUtils.getFs(basePath, configuration);
  }

  @Override
  public void create(String instantTime) {
    throw new HoodieMetadataException("Create is not supported");
  }

  @Override
  public boolean init() {
    return true;
  }

  @Override
  public void commit(String instantTime) {
    return;
  }

  @Override
  public void merge(String string) {
    return;
  }

  @Override
  public void update(HoodieCommitMetadata metadata, String instantTime) {
    return;
  }

  @Override
  public void update(HoodieCleanMetadata metadata, String instantTime) throws IOException  {
    return;
  }

  @Override
  public void update(HoodieRollbackMetadata metadata, String instantTime) throws IOException  {
    return;
  }

  @Override
  public void update(HoodieSavepointMetadata metadata, String instantTime) throws IOException  {
    return;
  }

  @Override
  public void update(HoodieCompactionMetadata metadata, String instantTime) throws IOException  {
    return;
  }

  @Override
  public void update(org.apache.hudi.common.model.HoodieCommitMetadata metadata, String instantTime) throws IOException  {
    return;
  }

  @Override
  public String getFilePath(int fileId) throws Exception {
    throw new HoodieMetadataException("getFilePath is not supported");
  }

  @Override
  public List<String> getAllPartitions() throws IOException  {
    return FSUtils.getAllFoldersWithPartitionMetaFile(FSUtils.getFs(basePath, configuration), basePath);
  }

  @Override
  public List<String> getAllFilesInPartition(String partitionPath) throws IOException  {
    FileStatus[] statuses = fs.listStatus(FSUtils.getPartitionPath(basePath, partitionPath));
    return Arrays.asList(statuses).stream().map(status -> status.getPath().toString()).collect(Collectors.toList());
  }

  @Override
  public void scanFiles(int startFileId, BiFunction<Integer, String, Boolean> iter) throws Exception  {
    return;
  }

  @Override
  public Stats getStats()  {
    return new Stats();
  }
}
