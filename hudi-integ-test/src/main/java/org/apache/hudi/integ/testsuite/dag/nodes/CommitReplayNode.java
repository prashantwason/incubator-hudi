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

package org.apache.hudi.integ.testsuite.dag.nodes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieWriteOperation;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteWriter;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;
import org.apache.hudi.integ.testsuite.generator.ExistingFileRecordGenerationIterator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a node which reads records from a specific commit and writes the records again.
 */
public class CommitReplayNode extends InsertNode {

  private HoodieActiveTimeline timeline;
  private byte[] originalMetadataBytes;
  private HoodieInstant originalInstant;
  private HoodieCommitMetadata originalCommitMetadata;
  private String replayCommitTime;
  private HoodieTableMetaClient metaClient;
  private Registry registry;
  private long recordReadingTimeInMsec;

  public CommitReplayNode(Config config) {
    super(config);
    registry = Registry.getRegistry("HoodieTestSuite");
  }

  @Override
  protected void generate(DeltaGenerator deltaGenerator) throws Exception {
    // No data is generated.
  }

  @Override
  protected JavaRDD<WriteStatus> ingest(HoodieTestSuiteWriter hoodieTestSuiteWriter, Option<String> commitTime)
      throws Exception {
    // CommitReplayNode only works when the source is HoodieIncrSource
    if (!hoodieTestSuiteWriter.getCfg().sourceClassName.equals(HoodieIncrSource.class.getName())) {
      throw new HoodieException(this.getClass().getSimpleName() + " requires the source to be "
          + HoodieIncrSource.class.getName());
    }

    this.replayCommitTime = commitTime.get();
    final String originalInstantTime = config.getInstantTime();

    // Read the records which are to be ingested
    metaClient = new HoodieTableMetaClient(hoodieTestSuiteWriter.getConfiguration(),
        hoodieTestSuiteWriter.getCfg().targetBasePath);
    timeline = metaClient.reloadActiveTimeline();
    originalInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, originalInstantTime);
    originalMetadataBytes = timeline.getInstantDetails(originalInstant).get();
    originalCommitMetadata = HoodieCommitMetadata.fromBytes(originalMetadataBytes, HoodieCommitMetadata.class);

    log.info(String.format("Commit at %s has following counts: inserts=%d, updates=%d, deletes=%d, partitions=%d, "
        + "totalRecords=%d, totalBytes=%d",
        originalInstantTime, originalCommitMetadata.fetchTotalInsertRecordsWritten(),
        originalCommitMetadata.fetchTotalUpdateRecordsWritten(), originalCommitMetadata.getTotalRecordsDeleted(),
        originalCommitMetadata.fetchTotalPartitionsWritten(),
        originalCommitMetadata.fetchTotalRecordsWritten(), originalCommitMetadata.fetchTotalBytesWritten()));

    final String insertKeySuffix = "-" + commitTime;
    long t1 = System.currentTimeMillis();
    JavaRDD<HoodieRecord> recordsRDD = readRecordsForCommit(hoodieTestSuiteWriter, originalCommitMetadata, metaClient,
        originalInstantTime, config.usePreppedApi(), insertKeySuffix);
    final int numFilesWrittenInParallel = originalCommitMetadata.getFileIdAndRelativePaths().size();

    // Input parallelism should be the same as the number of files created (to match the existing commit)
    log.info("Read all input records for commit with " + recordsRDD.getNumPartitions() + " partitions. "
        + "Number of files written in parallel = " + numFilesWrittenInParallel);

    // Compute and persist the RDD so that the records are loaded. Without doing this, the time to load the records
    // from existing data files will also be counted within the commit time and the comparison results would not be
    // correct.
    if (recordsRDD.getStorageLevel() == StorageLevel.NONE()) {
      recordsRDD.persist(StorageLevel.MEMORY_AND_DISK());
    }
    log.info("Records RDD was persisted at: " + recordsRDD.getStorageLevel());

    // Laod the records by executing all the stages. To prevent OOM, we need to load across a large number of
    // partitions.
    recordsRDD = recordsRDD.repartition(1024);
    recordsRDD.count();
    recordReadingTimeInMsec = System.currentTimeMillis() - t1;

    final boolean insertOnly = originalCommitMetadata.fetchTotalUpdateRecordsWritten() == 0;
    if (insertOnly) {
      if (config.useBulkInsert()) {
        Option<BulkInsertPartitioner> bulkInsertPartitioner = Option.of(new ReplayBulkInsertPartitioner(numFilesWrittenInParallel));
        String api = config.usePreppedApi() ? "bulkInsertPrepped" : "bulkInsert";
        log.info("Replaying commit data {} from instant time {} using {}", this.getName(), originalInstantTime, api);
        if (config.usePreppedApi()) {
          this.result = hoodieTestSuiteWriter.bulkInsertPrepped(recordsRDD, commitTime, bulkInsertPartitioner);
        } else {
          this.result = hoodieTestSuiteWriter.bulkInsert(recordsRDD, commitTime, bulkInsertPartitioner);
        }
      } else {
        String api = config.usePreppedApi() ? "insertPrepped" : "insert";
        log.info("Replaying commit data {} from instant time {} using {}", this.getName(), originalInstantTime, api);
        if (config.usePreppedApi()) {
          this.result = hoodieTestSuiteWriter.insertPrepped(recordsRDD, commitTime);
        } else {
          this.result = hoodieTestSuiteWriter.insert(recordsRDD, commitTime);
        }
      }
    } else {
      if (config.useMultiWrite()) {
        HoodieWriteOperation insertOp;
        JavaRDD<HoodieRecord> insertRdd = recordsRDD.filter(r -> r.getKey().getRecordKey().endsWith(insertKeySuffix))
            .map(r -> {
              HoodieRecord<HoodieRecordPayload> rr = r;
              return rr;
            });
        if (config.useBulkInsert()) {
          Option<BulkInsertPartitioner<JavaRDD<HoodieRecord>>> bulkInsertPartitioner =
              Option.of(new ReplayBulkInsertPartitioner(numFilesWrittenInParallel));
          insertOp = HoodieWriteOperation.bulkInsert(insertRdd, bulkInsertPartitioner);
        } else {
          insertOp = HoodieWriteOperation.insert(insertRdd);
        }

        HoodieWriteOperation upsertOp;
        JavaRDD<HoodieRecord<HoodieRecordPayload>> upsertRdd = recordsRDD.filter(r -> r.getKey().getRecordKey().endsWith(insertKeySuffix))
            .map(r -> {
              HoodieRecord<HoodieRecordPayload> rr = r;
              return rr;
            });
        if (config.usePreppedApi()) {
          upsertOp = HoodieWriteOperation.upsertPrepped(upsertRdd);
        } else {
          upsertOp = HoodieWriteOperation.upsert(upsertRdd);
        }

        List<HoodieWriteOperation> operations = new ArrayList<>(2);
        operations.add(insertOp);
        operations.add(upsertOp);
        this.result = hoodieTestSuiteWriter.write(operations, commitTime);
      } else {
        String api = config.usePreppedApi() ? "upsertPrepped" : "upsert";
        log.info("Replaying commit data {} from instant time {} using {}", this.getName(), originalInstantTime, api);
        if (config.usePreppedApi()) {
          this.result = hoodieTestSuiteWriter.upsertPrepped(recordsRDD, commitTime);
        } else {
          this.result = hoodieTestSuiteWriter.upsert(recordsRDD, commitTime);
        }
      }
    }

    return this.result;
  }

  private JavaRDD<HoodieRecord> readRecordsForCommit(HoodieTestSuiteWriter hoodieTestSuiteWriter,
      HoodieCommitMetadata commitMetadata, HoodieTableMetaClient metaClient, String commitTime, boolean tagLocation,
      String insertKeySuffix) throws IOException {
    log.info("Loading records for commit " + commitTime);

    final String basePath = metaClient.getBasePath();
    Map<String, String> regularFileIdToFullPath = commitMetadata.getFileIdAndFullPaths(basePath);
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats()
        .values().stream()
        .flatMap(s -> s.stream())
        .collect(Collectors.toList());

    JavaSparkContext jsc = hoodieTestSuiteWriter.getSparkContext();
    if (regularFileIdToFullPath.isEmpty()) {
      return jsc.emptyRDD();
    }

    // Read the schema
    Path parquetPath = new Path(regularFileIdToFullPath.values().iterator().next());
    Schema commitSchema = readSchemaForCommit(commitMetadata, true, parquetPath, jsc.hadoopConfiguration());
    String schemaStr = commitSchema.toString();
    Schema commitSchemaNoMetafields = readSchemaForCommit(commitMetadata, false, parquetPath, jsc.hadoopConfiguration());
    String schemaNoMetadataStr = commitSchemaNoMetafields.toString();

    final String payloadClassName = hoodieTestSuiteWriter.getCfg().payloadClassName;
    final KeyGenerator keygen = createKeygen();

    SerializableConfiguration conf = new SerializableConfiguration(jsc.hadoopConfiguration());

    // Divide the record generation across multiple parallel operations.
    // Reading 1M records takes about 20mins. So dividing the read into 100K chunks ensures the complete reading
    // will complete in order of few minutes.
    final int maxRecordsPerOperations = 100000;
    List<Pair<String, long[]>> operations = new LinkedList<>();
    allWriteStats.forEach(wstat -> {
      long numInserts = wstat.getNumInserts();
      long numDeletes = wstat.getNumDeletes();
      long numUpdateWrites = wstat.getNumUpdateWrites();
      long offset = 0;

      while (true) {
        long numRecords = 0;
        // offset, numInsert, numUpsert, numDelete
        long[] op = {offset, 0, 0, 0};
        if (numInserts > 0) {
          numRecords = Math.min(numInserts, maxRecordsPerOperations);
          op[1] = numRecords;
          numInserts -= numRecords;
        } else if (numUpdateWrites > 0) {
          numRecords = Math.min(numUpdateWrites, maxRecordsPerOperations);
          op[2] = numRecords;
          numUpdateWrites -= numRecords;
        } else if (numDeletes > 0) {
          numRecords = Math.min(numDeletes, maxRecordsPerOperations);
          op[3] = numRecords;
          numDeletes -= numRecords;
        } else {
          break;
        }

        offset += numRecords;
        operations.add(Pair.of(wstat.getPath(), op));
      }
    });

    Registry registryData = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName());
    Registry registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder");

    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(operations, operations.size())
        .mapPartitions(opitr -> {
          HoodieWrapperFileSystem.setMetricsRegistry(registryData, registryMeta);
          Pair<String, long[]> op = opitr.next();
          Path baseFilePath = FSUtils.getPartitionPath(basePath, op.getKey());
          log.info("Loading records for file " + baseFilePath);

          Schema readerSchema = new Schema.Parser().parse(schemaStr);
          Schema finalSchema = new Schema.Parser().parse(schemaNoMetadataStr);
          return new ExistingFileRecordGenerationIterator(conf.get(), baseFilePath,
              readerSchema, Option.of(finalSchema), keygen, payloadClassName, Option.of(insertKeySuffix),
              op.getValue()[3], op.getValue()[1], op.getValue()[2], op.getValue()[0], tagLocation);
        }, true);

    return recordsRDD;
  }

  private static Schema readSchemaForCommit(HoodieCommitMetadata commitMetadata, boolean includeMetadataFields,
                                            Path parquetPath,
                                            Configuration conf) {
    String schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
    Schema schema = new Schema.Parser().parse(schemaStr);
    if (includeMetadataFields) {
      schema = HoodieAvroUtils.addMetadataFields(schema);
    }

    return schema;
  }

  private KeyGenerator createKeygen() {
    TypedProperties props = new TypedProperties();
    props.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), HoodieRecord.RECORD_KEY_METADATA_FIELD);
    props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    return new SimpleKeyGenerator(props);
  }

  @Override
  public void printResults(ExecutionContext executionContext) {
    final String originalCommitTime = config.getInstantTime();

    // Retrieve metadata for the replay commit
    HoodieInstant replayInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, replayCommitTime);
    HoodieCommitMetadata replayMetadata;
    try {
      replayMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(replayInstant).get(),
          HoodieCommitMetadata.class);
    } catch (IOException e) {
      log.error("Failed to load metadata for commit " + originalCommitTime, e);
      // cannot publish any metrics
      return;
    }

    // Log the entire commit metadata for debugging
    log.info("===== Replay Metadata");
    log.info(new String(timeline.getInstantDetails(replayInstant).get()));
    log.info("===== Original Metadata");
    log.info(new String(originalMetadataBytes));

    // Comparison of original and replay metadata
    HoodieCommitMetadata o = originalCommitMetadata;
    HoodieCommitMetadata r = replayMetadata;
    log.info("CommitReplayNode stats after replaying commit " + originalCommitTime + ": originalCommit / replayCommit");
    log.info(String.format("  Operation Type: %s / %s", o.getOperationType().toString(), r.getOperationType().toString()));
    log.info(String.format("  Commit instant time: %s / %s", originalCommitTime, replayCommitTime));

    // Publish the replay metrics
    publishMetric("CommitDuration", "Total Commit Duration", durationFromInstant(originalInstant),
        durationFromInstant(replayInstant) - recordReadingTimeInMsec);
    publishMetric("BytesWritten", "Total Bytes Written", o.fetchTotalBytesWritten(), r.fetchTotalBytesWritten());
    publishMetric("FilesInsert", "Total Files Inserted",  o.fetchTotalFilesInsert(), r.fetchTotalFilesInsert());
    publishMetric("FilesUpdated", "Total Files Updated", o.fetchTotalFilesUpdated(), r.fetchTotalFilesUpdated());
    publishMetric("PartitionsWritten", "Total Partitions Written", o.fetchTotalPartitionsWritten(), r.fetchTotalPartitionsWritten());
    publishMetric("RecordsInserted", "Total Records Inserted", o.fetchTotalInsertRecordsWritten(), r.fetchTotalInsertRecordsWritten());
    publishMetric("RecordsWritten", "Total Records Written", o.fetchTotalRecordsWritten(), r.fetchTotalRecordsWritten());
    publishMetric("RecordsUpdated", "Total Records Updated", o.fetchTotalUpdateRecordsWritten(), r.fetchTotalUpdateRecordsWritten());
    publishMetric("RecordsDeleted", "Total Records Deleted", o.getTotalRecordsDeleted(), r.getTotalRecordsDeleted());
    publishMetric("Errors", "Total Errors", o.fetchTotalWriteErrors(), r.fetchTotalWriteErrors());
    publishMetric("CreateTime", "Total Create Time", o.getTotalCreateTime(), r.getTotalCreateTime());
    publishMetric("UpsertTime", "Total Upsert Time", o.getTotalUpsertTime(), r.getTotalUpsertTime());
    publishMetric("ScanTime", "Total Scan Time", o.getTotalScanTime(), r.getTotalScanTime());
  }

  private void publishMetric(String metricName, String desc, long origValue, long replayValue) {
    registry.add("original" + metricName, origValue);
    registry.add("replay" + metricName, replayValue);
    log.info(String.format("  %s: %d / %d", desc, origValue, replayValue));
  }

  /**
   * Returns the time duration in seconds from the time the given instant was created to its last modification time.
   *
   * For a commit instant, this will the time from when startCommit is called to the time commit actually
   * completes. Hence, it provides an approximation of the time it took to perform this commit.
   *
   * @param instant Instant to return the duration
   * @return Duration in seconds
   */
  private long durationFromInstant(HoodieInstant instant) {
    try {
      FileStatus status = metaClient.getFs().getFileStatus(new Path(metaClient.getMetaPath(), instant.getFileName()));
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
      Date date = sdf.parse(instant.getTimestamp());
      return status.getModificationTime() - date.getTime();
    } catch (Exception e) {
      log.error("Failed to calculate commit duration for instant " + instant, e);
      return -1;
    }
  }

  private static class ReplayBulkInsertPartitioner implements BulkInsertPartitioner<JavaRDD<HoodieRecord>> {
    private static Logger LOG = LoggerFactory.getLogger(ReplayBulkInsertPartitioner.class);
    private int numOriginalPartitions;

    public ReplayBulkInsertPartitioner(int numOriginalPartitions) {
      LOG.info("Partitioner created with numOriginalPartitions=" + numOriginalPartitions);
      this.numOriginalPartitions = numOriginalPartitions;
    }

    @Override
    public JavaRDD<HoodieRecord> repartitionRecords(JavaRDD<HoodieRecord> records, int outputSparkPartitions) {
      // Maintain the number of original partitions
      if (records.getNumPartitions() == numOriginalPartitions) {
        LOG.info("No need to partition as input already has " + records.getNumPartitions() + "=" + numOriginalPartitions
            + " partitions");
        return records;
      }

      LOG.info("Repartitioning input RDD from " + records.getNumPartitions() + " to " + numOriginalPartitions + " partitions");
      return records.sortBy(
          v1 -> String.format("%s %s", v1.getPartitionPath(), v1.getRecordKey()), true, numOriginalPartitions);
    }

    @Override
    public boolean arePartitionRecordsSorted() {
      // Since the records are read in order from existing data files, they must be already sorted.
      return true;
    }
  }
}
