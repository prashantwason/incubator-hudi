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

package org.apache.hudi.common.table.view;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.metadata.MetadataStore;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 */
public class MetaStoreFileSystemView extends AbstractTableFileSystemView {
  private MetadataStore metaStore;
  private AbstractTableFileSystemView delegateView;
  private static final Logger LOG = LogManager.getLogger(MetaStoreFileSystemView.class);

  public MetaStoreFileSystemView(MetadataStore metadata, HoodieTableMetaClient metaClient,
                                 AbstractTableFileSystemView view) {
    this.metaStore = metadata;
    this.delegateView = view;
    init(metaClient, metaClient.getCommitsTimeline().filterCompletedInstants());
  }

  @Override
  protected FileStatus[] listPartition(Path partitionPath) throws IOException {
    List<String> filesInPartition = metaStore.getAllFilesInPartition(partitionPath.toString());
    String basePath = metaStore.getBasePath();
    return filesInPartition.stream()
        .map(filePath -> new FileStatus(0, false, 0, 0, 0, 0, null, null, null, new Path(basePath, filePath)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    delegateView.init(metaClient, visibleActiveTimeline);
    super.init(metaClient, visibleActiveTimeline);
  }

  @Override
  public void close() {
    delegateView.close();
  }

  @Override
  protected void resetViewState() {
    delegateView.resetViewState();
  }

  @Override
  protected boolean isPendingCompactionScheduledForFileId(HoodieFileGroupId fgId) {
    return delegateView.isPendingCompactionScheduledForFileId(fgId);
  }

  @Override
  void resetPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    delegateView.resetPendingCompactionOperations(operations);
  }

  @Override
  void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    delegateView.addPendingCompactionOperations(operations);
  }

  @Override
  void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    delegateView.removePendingCompactionOperations(operations);
  }

  @Override
  protected Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(HoodieFileGroupId fileGroupId) {
    return delegateView.getPendingCompactionOperationWithInstant(fileGroupId);
  }

  @Override
  Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations() {
    return delegateView.fetchPendingCompactionOperations();
  }

  @Override
  boolean isPartitionAvailableInStore(String partitionPath) {
    return delegateView.isPartitionAvailableInStore(partitionPath);
  }

  @Override
  void storePartitionView(String partitionPath, List<HoodieFileGroup> fileGroups) {
    delegateView.storePartitionView(partitionPath, fileGroups);
  }

  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partitionPath) {
    return delegateView.fetchAllStoredFileGroups(partitionPath);
  }

  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups() {
    return delegateView.fetchAllStoredFileGroups();
  }

  @Override
  boolean isClosed() {
    return delegateView.isClosed();
  }
}
