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

package org.apache.hudi.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class HoodieMetadata {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadata.class);

  private static final String METADATA_TABLE_REL_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR
      + "metadata";
  private static final String METADATA_TABLE_NAME_SUFFIX = "_metadata";

  private JavaSparkContext jsc;
  private String basePath;
  private boolean shouldAssumeDatePartitioning;
  private String metadataBasePath;
  private HoodieWriteConfig config;
  private String tableName;
  private HoodieTableMetaClient metaClient;
  private Schema schema;

  public HoodieMetadata(JavaSparkContext jsc, HoodieWriteConfig config) {
    this.jsc = jsc;
    this.basePath = config.getBasePath();
    this.shouldAssumeDatePartitioning = config.shouldAssumeDatePartitioning();
    this.metadataBasePath = config.getBasePath() + Path.SEPARATOR + METADATA_TABLE_REL_PATH;
    this.tableName = config.getTableName() + METADATA_TABLE_NAME_SUFFIX;
    this.config = HoodieWriteConfig.newBuilder().withPath(metadataBasePath.toString())
        .withTimelineLayoutVersion(config.getTimelineLayoutVersion())
        .withConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .build();
  }

  /**
   * Initialize the metadata table if it does not exist.
   * Update the metadata to bring it in sync with the file system. This can happen in two ways:
   * 1. If the table did not exist, then file and partition listing is used
   * 2. If the table already exists, the Instants from Timeline are read in order and changes determined
   *
   * The above logic has been chosen because it is faster to perform #1 at scale rather than read all the Instants
   * which are large in size (AVRO or JSON encoded and not compressed) and incur considerable IO for de-serialization
   * and decoding.
   *
   * @throws IOException
   */
  public void initAndSyncMetadataTable() throws IOException {
    FileSystem fs = FSUtils.getFs(config.getBasePath(), jsc.hadoopConfiguration());
    boolean exists = fs.exists(new Path(metadataBasePath));

    if (!exists) {
      LOG.info("Creating a new metadata table at " + metadataBasePath);
      HoodieTableMetaClient.initTableType(jsc.hadoopConfiguration(), metadataBasePath.toString(),
          HoodieTableType.MERGE_ON_READ.toString(), tableName, "archived", null,
          HoodieFileFormat.HFILE.toString());
    }

    LOG.info("Loading metadata table from " + metadataBasePath);
    metaClient = ClientUtils.createMetaClient(jsc.hadoopConfiguration(), config, true);

    if (!exists) {
      initFromFilesystem();
    } else {
      syncFromInstants();
    }
  }

  private void initFromFilesystem() throws IOException {
    // List all partitions in the basePath of the containing dataset
    LOG.info("Initializing metadata table by using file listings");
    FileSystem fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    List<String> partitions = FSUtils.getAllPartitionPaths(fs, basePath, shouldAssumeDatePartitioning);
    LOG.info("Found " + partitions.size() + " partitions");

    // List all paritions in parallel and collect the list of files in them
    JavaPairRDD<String, List<String>> partitionFileListRDD = jsc.parallelize(partitions).mapToPair(partition -> {
      try {
        FileStatus[] statuses = fs.listStatus(new Path(basePath, partition));
        List<String> fileList = new ArrayList<>(statuses.length);
        for (FileStatus status : statuses) {
          if (status.isFile()) {
            String filePath = status.getPath().toString();
            fileList.add(filePath); // TODO: relative only
          }
        }

        return new Tuple2<>(partition, fileList);
      } catch (Exception e) {
        throw new RuntimeException("Failed to list files in partition " + partition, e);
      }
    });

    // Collect the list of partitions and file lists
    List<Tuple2<String, List<String>>> partitionFileList = partitionFileListRDD.collect();

    // Create a HoodieCommitMetadata with writeStats for all discovered files
    int[] stats = {0};
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    partitionFileList.forEach(t -> {
      t._2.forEach(filePath -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPath(filePath);
        writeStat.setPartitionPath(t._1);
        metadata.addWriteStat(t._1, writeStat);
      });
      stats[0] += t._2.size();
    });

    LOG.info("Committing " + partitionFileList.size() + " partitions and " + stats[0] + " files to metadata");
    update(metadata);
  }

  private static void syncFromInstants() {
    // TODO Auto-generated method stub

  }

  public void update(HoodieCommitMetadata metadata) {
    List<GenericRecord> records = new LinkedList<>();
    metadata.getPartitionToWriteStats().forEach((partition, writeStats) -> {
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        String fileName = pathWithPartition.substring(partition.length() + 1);
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(key, v);
      });
    });

    logger.info("Updated from " + instantTime + ".commit. #fileupdates=" + newFilesMap.size());
  }
}
