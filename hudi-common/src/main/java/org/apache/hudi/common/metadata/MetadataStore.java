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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCommitMetadata;
import org.apache.hudi.avro.model.HoodieCompactionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataMigrator;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 */
public abstract class MetadataStore {
  protected String basePath;
  protected String metadataPath;
  protected Configuration configuration;
  protected Logger logger;
  private static final ReentrantReadWriteLock GLOBAL_LOCK = new ReentrantReadWriteLock();
  private static final Map<String, MetadataStore> INSTANCES = new HashMap<>();

  public MetadataStore(String basePath, Configuration conf) {
    this.basePath = basePath;
    if (basePath.endsWith("/")) {
      this.basePath = basePath.substring(0, basePath.length() - 1);
    }
    this.metadataPath = getMetadataDirectory(basePath);
    this.configuration = conf;
    this.logger = LogManager.getLogger(this.getClass());
  }

  public static MetadataStore get(String basePath, Configuration conf) {
    try {
      Logger logger = LogManager.getLogger(MetadataStore.class);
      GLOBAL_LOCK.writeLock().lock();
      if (!INSTANCES.containsKey(basePath)) {
        try {
          MetadataStoreProposal2 metaStore = new MetadataStoreProposal2(basePath, conf);
          if (metaStore.init()) {
            // Metadata is valid so can use this
            logger.info("Initialize Metastore at " + basePath);
            INSTANCES.put(basePath, metaStore);
          } else {
            logger.warn("Metastore not found at " + basePath);
            INSTANCES.put(basePath, new MetadataStoreSimple(basePath, conf));
          }
        } catch (Exception e) {
          logger.warn("Failed to initialize Metastore at " + basePath + ": " + e);
          INSTANCES.put(basePath, new MetadataStoreSimple(basePath, conf));
        }
      }

      return INSTANCES.get(basePath);
    } finally {
      GLOBAL_LOCK.writeLock().unlock();
    }
  }

  public static void create(HoodieTableMetaClient metaClient, boolean clean) throws Exception {
    String basePath = metaClient.getBasePath();
    // Get the latest completed instant time
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    String latestInstantTime = timeline.filterCompletedInstants().lastInstant()
        .orElse(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "000"))
        .getTimestamp();

    Path metadataPath = new Path(getMetadataDirectory(basePath));

    FileSystem fs = FSUtils.getFs(basePath, metaClient.getHadoopConf());
    if (clean) {
      fs.delete(metadataPath, true);
      fs.mkdirs(metadataPath);
    } else {
      try {
        FileStatus[] statuses = fs.listStatus(metadataPath);
        if (statuses.length > 0) {
          throw new HoodieMetadataException("Metadata directory (" + metadataPath + ") not empty");
        }
      } catch (FileNotFoundException ex) {
        fs.mkdirs(metadataPath);
      }
    }

    // Create metadata at this instant
    MetadataStore metadata = new MetadataStoreProposal2(basePath, metaClient.getHadoopConf());
    metadata.create(latestInstantTime);
    GLOBAL_LOCK.writeLock().lock();
    INSTANCES.put(basePath, metadata);
    GLOBAL_LOCK.writeLock().unlock();
  }

  public static void update(HoodieTableMetaClient metaClient, boolean merge) {
    MetadataStore metastore = get(metaClient.getBasePath(), metaClient.getHadoopConf());
    Stats stats = metastore.getStats();

    // Get the instants completed since the time master was created
    HoodieTimeline timeline = metaClient.getActiveTimeline()
        .filterCompletedInstants().findInstantsAfter(stats.masterInstantTime, Integer.MAX_VALUE);

    if (timeline.empty()) {
      return;
    }

    GLOBAL_LOCK.writeLock().lock();
    final int numInstants = timeline.countInstants();
    int[] counter = {0};
    timeline.getInstants().forEach(instant -> {
      try {
        byte[] data = timeline.getInstantDetails(instant).get();
        String instantTime = instant.getTimestamp();
        GenericRecord record;
        switch (instant.getAction()) {
          case HoodieActiveTimeline.COMMIT_ACTION:
          case HoodieActiveTimeline.DELTA_COMMIT_ACTION:
            try {
              record = HoodieAvroUtils.jsonBytesToAvro(data, HoodieCommitMetadata.SCHEMA$);
              HoodieCommitMetadata metadata = (HoodieCommitMetadata)SpecificData.get()
                      .deepCopy(HoodieCommitMetadata.SCHEMA$, record);
              metastore.update(metadata, instantTime);
            } catch (AvroTypeException ex) {
              org.apache.hudi.common.model.HoodieCommitMetadata metadata =
                  org.apache.hudi.common.model.HoodieCommitMetadata.fromBytes(data, org.apache.hudi.common.model.HoodieCommitMetadata.class);
              metastore.update(metadata, instantTime);
            }
            break;
          case HoodieActiveTimeline.CLEAN_ACTION:
            record = HoodieAvroUtils.jsonBytesToAvro(data, HoodieCleanMetadata.SCHEMA$);
            HoodieCleanMetadata cleanMetadata = (HoodieCleanMetadata)SpecificData.get()
                    .deepCopy(HoodieCleanMetadata.SCHEMA$, record);
            CleanMetadataMigrator metadataMigrator = new CleanMetadataMigrator(metaClient);
            cleanMetadata = metadataMigrator.upgradeToLatest(cleanMetadata, cleanMetadata.getVersion());
            metastore.update(cleanMetadata, instantTime);
            break;
          case HoodieActiveTimeline.ROLLBACK_ACTION:
            record = HoodieAvroUtils.jsonBytesToAvro(data, HoodieRollbackMetadata.SCHEMA$);
            HoodieRollbackMetadata rollbackMetadata = (HoodieRollbackMetadata)SpecificData.get()
                .deepCopy(HoodieRollbackMetadata.SCHEMA$, record);
            metastore.update(rollbackMetadata, instantTime);
            break;
          case HoodieActiveTimeline.SAVEPOINT_ACTION:
            record = HoodieAvroUtils.jsonBytesToAvro(data, HoodieSavepointMetadata.SCHEMA$);
            HoodieSavepointMetadata savepointMetadata = (HoodieSavepointMetadata)SpecificData.get()
                .deepCopy(HoodieSavepointMetadata.SCHEMA$, record);
            metastore.update(savepointMetadata, instantTime);
            break;
          case HoodieActiveTimeline.COMPACTION_ACTION:
            record = HoodieAvroUtils.jsonBytesToAvro(data, HoodieCompactionMetadata.SCHEMA$);
            HoodieCompactionMetadata compactionMetadata = (HoodieCompactionMetadata)SpecificData.get()
                .deepCopy(HoodieCompactionMetadata.SCHEMA$, record);
            metastore.update(compactionMetadata, instantTime);
          default:
            throw new HoodieMetadataException("Unknown type of instant " + instant);
        }

        if (!merge || ++counter[0] == numInstants) {
          metastore.commit(instantTime);
        }
      } catch (IOException ex) {
        throw new HoodieMetadataException("Failed to update from instant " + instant + ": " + ex);
      }
    });

    GLOBAL_LOCK.writeLock().unlock();

  }


  public abstract void create(String instantTime) throws IOException;

  public abstract boolean init() throws Exception;

  public abstract void commit(String instantTime) throws IOException;

  public abstract void merge(String string) throws Exception;

  public abstract void update(HoodieCommitMetadata metadata, String instantTime) throws IOException;

  public abstract void update(HoodieCleanMetadata metadata, String instantTime) throws IOException;

  public abstract void update(HoodieRollbackMetadata metadata, String instantTime) throws IOException;

  public abstract void update(HoodieSavepointMetadata metadata, String instantTime) throws IOException;

  public abstract void update(HoodieCompactionMetadata metadata, String instantTime) throws IOException;

  public abstract void update(org.apache.hudi.common.model.HoodieCommitMetadata metadata, String instantTime) throws IOException;

  public abstract List<String> getAllPartitions() throws IOException;

  public abstract List<String> getAllFilesInPartition(String partitionPath) throws IOException;

  public abstract void scanFiles(int startFileId, BiFunction<Integer, String, Boolean> iter) throws Exception;

  public abstract String getFilePath(int fileId) throws Exception;

  public String getBasePath() {
    return basePath;
  }

  protected String getRelativePath(String path) {
    int index = path.indexOf(basePath);
    if (index != -1) {
      return path.substring(index + basePath.length() + 1);
    }

    return path;
  }

  public static String getMetadataDirectory(String basePath) {
    // TODO: Remove this eventually as metata should be stored in the .hoodie folder
    // return String.format("%s/%s/metadata", basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    return String.format("/user/%s/consolidated_metadata/%s", System.getProperty("user.name"),
        new Path(basePath).getName().toString());
  }

  public static class Stats {
    public long metadataTotalSize = 0;
    public Map<String, Long> metadataFileSizes = new HashMap<>();
    public int fileCount = 0;
    public int deletedFileCount = 0;
    public int partitionCount = 0;
    public int maxFileId = 0;
    public String masterInstantTime;
  }

  public abstract Stats getStats();
}
