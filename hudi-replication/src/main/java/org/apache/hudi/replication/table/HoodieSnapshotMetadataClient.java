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

package org.apache.hudi.replication.table;

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Provides an API for reading metadata
 * 1) for latest commit.
 * 2) as of given commit.
 * 
 * This only includes base files (parquet) today. But can be extended to return other metadata information.
 */
public class HoodieSnapshotMetadataClient {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HoodieSnapshotMetadataClient.class);

  private HoodieTableMetaClient metaClient;
  private final FileSystemBackedTableMetadata fsMetadata;

  /**
   * Constructor to create metadata client.
   * 
   * @param conf storage configuration bag.
   * @param basePath Table location absolute path.
   */
  public HoodieSnapshotMetadataClient(StorageConfiguration<?> conf, String basePath) {
    this(HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build());
  }

  /**
   * Create HoodieIncrementalMetadataClient from HoodieTableMetaClient.
   */
  public HoodieSnapshotMetadataClient(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    this.fsMetadata = new FileSystemBackedTableMetadata(
        engineContext, metaClient.getTableConfig(), metaClient.getStorage(),
        metaClient.getBasePath().toString());
  }

  /**
   * Returns the latest set of data files from the given partition, without partition metadata.
   * Note: Only base files are returned.
   */
  public Stream<StoragePath> getLatestSnapshotFiles(String partitionPath) {
    StoragePath path = partitionPath.isEmpty() ? metaClient.getBasePath() : new StoragePath(metaClient.getBasePath(), partitionPath);
    HoodieStorage storage = metaClient.getStorage();

    try {
      boolean isHoodiePartition = HoodiePartitionMetadata.hasPartitionMetadata(storage, path);

      if (isHoodiePartition) {
        HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(
                fsMetadata, metaClient, metaClient.getActiveTimeline().filterCompletedInstants());
        return fileSystemView.getLatestBaseFiles(partitionPath).map(bf -> new StoragePath(bf.getPath()));
      } else {
        return storage.listDirectEntries(path).stream()
                .filter(StoragePathInfo::isFile)
                .map(StoragePathInfo::getPath);
      }
    } catch (IOException e) {
      String msg = String.format("Failed to get snapshot files for partition: %s", partitionPath);
      LOG.error(msg, e);
      throw new HoodieIOException(msg, e);
    }
  }

  /**
   * Returns the latest set of data files that represent rows in table, along with the partition_metadata.
   * Note that this only returns 'base' files and not log files.
   */
  public Stream<StoragePath> getLatestSnapshotFilesWithPartitionMetadata(String partitionPath) {
    StoragePath path = partitionPath.isEmpty() ? metaClient.getBasePath() : new StoragePath(metaClient.getBasePath(), partitionPath);
    HoodieStorage storage = metaClient.getStorage();

    try {
      boolean isHoodiePartition = HoodiePartitionMetadata.hasPartitionMetadata(storage, path);

      if (isHoodiePartition) {
        HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(
                fsMetadata, metaClient, metaClient.getActiveTimeline().filterCompletedInstants());
        Stream<StoragePath> paths = fileSystemView.getLatestBaseFiles(partitionPath).map(bf -> new StoragePath(bf.getPath()));
        return Stream.concat(paths, Stream.of(getPartitionMetafile(partitionPath)));
      } else {
        return storage.listDirectEntries(path).stream()
                .filter(StoragePathInfo::isFile)
                .map(StoragePathInfo::getPath);
      }
    } catch (IOException e) {
      String msg = String.format("Failed to get snapshot files with partition metadata for partition: %s", partitionPath);
      LOG.error(msg, e);
      throw new HoodieIOException(msg, e);
    }
  }

  /**
   *
   * Returns set of data files that represent rows in table as of specified instant.
   * Note that this only returns 'base' files and not log files.
   * Also, note that this only works if corresponding base files are not removed by Cleaner.
   */
  public Stream<StoragePath> getSnapshotFilesAt(String instant, String partitionPath) {
    StoragePath path = partitionPath.isEmpty() ? metaClient.getBasePath() : new StoragePath(metaClient.getBasePath(), partitionPath);
    HoodieStorage storage = metaClient.getStorage();

    try {
      boolean isHoodiePartition = HoodiePartitionMetadata.hasPartitionMetadata(storage, path);

      if (isHoodiePartition) {
        HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(fsMetadata, metaClient, metaClient.getActiveTimeline());
        return fileSystemView.getLatestBaseFilesBeforeOrOn(partitionPath, instant).map(bf -> new StoragePath(bf.getPath()));
      } else {
        return storage.listDirectEntries(path).stream()
                .filter(StoragePathInfo::isFile)
                .map(StoragePathInfo::getPath);
      }
    } catch (IOException e) {
      String msg = String.format("Failed to get snapshot files at instant %s for partition: %s", instant, partitionPath);
      LOG.error(msg, e);
      throw new HoodieIOException(msg, e);
    }
  }

  /**
   * Returns set of data files that represent rows in table as of specified instant, along with partition_metadata.
   * Note that this only returns 'base' files and not log files.
   * Also, note that this only works if corresponding base files are not removed by Cleaner.
   */
  public Stream<StoragePath> getSnapshotFilesAtWithPartitionMetadata(String instant, String partitionPath) {
    StoragePath path = partitionPath.isEmpty() ? metaClient.getBasePath() : new StoragePath(metaClient.getBasePath(), partitionPath);
    HoodieStorage storage = metaClient.getStorage();

    try {
      boolean isHoodiePartition = HoodiePartitionMetadata.hasPartitionMetadata(storage, path);

      if (isHoodiePartition) {
        HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(fsMetadata, metaClient, metaClient.getActiveTimeline());
        Stream<StoragePath> paths = fileSystemView.getLatestBaseFilesBeforeOrOn(partitionPath, instant).map(bf -> new StoragePath(bf.getPath()));
        return Stream.concat(paths, Stream.of(getPartitionMetafile(partitionPath)));
      } else {
        return storage.listDirectEntries(path).stream()
                .filter(StoragePathInfo::isFile)
                .map(StoragePathInfo::getPath);
      }
    } catch (IOException e) {
      String msg = String.format("Failed to get snapshot files at instant %s with partition metadata for partition: %s", instant, partitionPath);
      LOG.error(msg, e);
      throw new HoodieIOException(msg, e);
    }
  }

  /**
   * Returns the latest commit instant time on hoodie table.
   */
  public Option<String> getLatestInstant() {
    return this.metaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime);
  }

  /**
   * reload active timeline to read new commits (if any).
   */
  public void reload() {
    this.metaClient.reloadActiveTimeline();
  }

  /**
   * Getter for metaClient.
   */
  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  /**
   * Returns the partition metafile path for the given partition.
   */
  private StoragePath getPartitionMetafile(String partitionPath) {
    StoragePath partitionMetafile = new StoragePath(new StoragePath(metaClient.getBasePath(), partitionPath),
        HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
    return partitionMetafile;
  }
}
