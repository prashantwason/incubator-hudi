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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSchemaUUIDDeletion extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestSchemaUUIDDeletion.class);
  private static final Schema SCHEMA = getSchemaFromResource(TestSchemaUUIDDeletion.class, "/exampleSchema.txt");
  private static final Schema SCHEMA_UUID = getSchemaFromResource(TestSchemaUUIDDeletion.class, "/exampleSchema_withUUID.txt");

  private final String partitionPath = "2016/01/31";
  private String firstCommitTime;

  private HoodieWriteConfig makeHoodieClientConfig(String basePath, Schema schema) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schema.toString())
        .withParallelism(2,  2).withAvroSchemaValidate(true).build();
  }

  @Test
  public void testFieldRemovalFromScheama() throws Exception {
    firstCommitTime = HoodieTestUtils.makeNewCommitTime();

    // Create dataset 1 which contain the UUID column in the schema
    String basePathUUID = basePath + Path.SEPARATOR + "uuid";
    createDataset(basePathUUID, SCHEMA_UUID, true);

    // Create dataset 2 which does not contain the UUID column in the schema
    String basePathNoUUID = basePath + Path.SEPARATOR + "nouuid";
    createDataset(basePathNoUUID, SCHEMA, false);

    // Move the file from non-uuid path to the uuid path
    FileStatus[] allFilesUUID = getIncrementalFiles(basePathUUID, partitionPath, "0", -1);
    assertEquals(1, allFilesUUID.length);
    FileStatus[] allFilesNoUUID = getIncrementalFiles(basePathNoUUID, partitionPath, "0", -1);
    assertEquals(1, allFilesNoUUID.length);
    LOG.info("===Copying " + allFilesNoUUID[0].getPath().toString() + " ----> " + allFilesUUID[0].getPath().toString());
    assertTrue(FileUtil.copy(fs, allFilesNoUUID[0].getPath(), fs, allFilesUUID[0].getPath(), false, hadoopConf));

    // Ensure file names remain the same
    FileStatus[] allFilesUUID_1 = getIncrementalFiles(basePathUUID, partitionPath, "0", -1);
    assertEquals(1, allFilesUUID_1.length);
    FileStatus[] allFilesNoUUID_1 = getIncrementalFiles(basePathNoUUID, partitionPath, "0", -1);
    assertEquals(1, allFilesNoUUID_1.length);
    assertEquals(allFilesUUID[0].getPath().toString(), allFilesUUID_1[0].getPath().toString());
    assertEquals(allFilesNoUUID[0].getPath().toString(), allFilesNoUUID_1[0].getPath().toString());

    // Ensure we can read from the files using their existing schema
    assertTrue(readParquet(allFilesUUID[0].getPath(), 1));
    assertTrue(readParquet(allFilesNoUUID[0].getPath(), 1));

    // At this point the new schema does not have the UUID column at all. So we only have SCHEAM.

    // Update dataset 1 which now does not contain the UUID column in the files with schema having no UUID column
    try {
      updateDataset(basePathUUID, SCHEMA, false);
      assertFalse(true, "Should have raised exception");
    } catch (Exception e) {
     LOG.info("+++++++++++++ Expected exception " + e);
    }

    Thread.sleep(1000);

    // Update dataset 1 which now does not contain the UUID column in the files with schema having UUID column
    updateDataset(basePathUUID, SCHEMA_UUID, false);

    // Update dataset 2 which does not contain the UUID column in the schema
    updateDataset(basePathNoUUID, SCHEMA, false);

    // This should work as we are passing a schema with NULL default field
    //updateDataset(basePathUUID, SCHEMA_UUID, false);
    //updateDataset(basePathNoUUID, SCHEMA_UUID, false);
  }

  private void updateDataset(String basePath, Schema schema, boolean withUUID) throws Exception {
    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    if (withUUID) {
      updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15, \"uuid\":\"uuid1\"}";
    }

    // Record inserted
    String insertRecordStr = "{\"_row_key\":\"99999999-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":20}";

    RawTripTestPayload updateRowChanges1 = new RawTripTestPayload(updateRecordStr1);
    HoodieRecord updatedRecord1 = new HoodieRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);

    RawTripTestPayload rowChange4 = new RawTripTestPayload(insertRecordStr);
    HoodieRecord insertedRecord1 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<HoodieRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    //Thread.sleep(1000);
    HoodieWriteConfig config = makeHoodieClientConfig(basePath, schema);
    HoodieWriteClient writeClient = getHoodieWriteClient(config);

    // First commit
    String newCommitTime = HoodieTestUtils.makeNewCommitTime();
    //metaClient = HoodieTableMetaClient.reload(metaClient);
    writeClient.startCommitWithTime(newCommitTime);
    List<WriteStatus> statuses = writeClient.upsert(jsc.parallelize(updatedRecords), newCommitTime).collect();

    FileStatus[] allFiles = getIncrementalFiles(basePath, partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);

    // Check whether the record has been updated
    Path updatedParquetFilePath = allFiles[0].getPath();
    BloomFilter updatedFilter =
        ParquetUtils.readBloomFilterFromParquetMetadata(hadoopConf, updatedParquetFilePath);

    ParquetReader updatedReader = ParquetReader.builder(new AvroReadSupport<>(), updatedParquetFilePath).build();
    int index = 0;
    GenericRecord newRecord;
    LOG.info("==================== Records from " + basePath + " after update") ;
    while ((newRecord = (GenericRecord) updatedReader.read()) != null) {
      //assertEquals(newRecord.get("_row_key").toString(), records.get(index).getRecordKey());
      //if (index == 0) {
      //  assertEquals("15", newRecord.get("number").toString());
      //}
      LOG.info(index + " " + newRecord.toString());
      index++;
    }
    updatedReader.close();
  }

  private void createDataset(String basePath, Schema schema, boolean withUUID) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = makeHoodieClientConfig(basePath, schema);

    // First commit
    HoodieWriteClient writeClient = getHoodieWriteClient(config, true);
    writeClient.startCommitWithTime(firstCommitTime);

    // Get a records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    if (withUUID) {
      recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12, \"uuid\":\"uuid1\"}";
    }

    List<HoodieRecord> records = new ArrayList<>();
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    records.add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));

    // Insert new records
    writeClient.insert(jsc.parallelize(records, 1), firstCommitTime);

    FileStatus[] allFiles = getIncrementalFiles(basePath, partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read the parquet file, check the record content
    Path parquetFilePath = allFiles[0].getPath();
    List<GenericRecord> fileRecords = ParquetUtils.readAvroRecords(hadoopConf, parquetFilePath);
    GenericRecord newRecord;
    int index = 0;
    LOG.info("==================== Records from " + basePath + " after insert") ;
    for (GenericRecord record : fileRecords) {
      //System.out.println("Got :" + record.get("_row_key").toString() + ", Exp :" + records.get(index).getRecordKey());
      assertEquals(records.get(index).getRecordKey(), record.get("_row_key").toString());
      LOG.info(index + " " + record.toString());
      index++;
    }
  }

  private boolean readParquet(Path parquetFilePath, int expectedRecords) {
    List<GenericRecord> fileRecords = ParquetUtils.readAvroRecords(hadoopConf, parquetFilePath);
    GenericRecord newRecord;
    int index = 0;
    for (GenericRecord record : fileRecords) {
      //System.out.println("Got :" + record.get("_row_key").toString() + ", Exp :" + records.get(index).getRecordKey());
      //assertEquals(records.get(index).getRecordKey(), record.get("_row_key").toString());
      index++;
    }

    return index == expectedRecords;

  }

  private FileStatus[] getIncrementalFiles(String basePath, String partitionPath, String startCommitTime,
                                           int numCommitsToPull)
          throws Exception {
    // initialize parquet input format
    HoodieParquetInputFormat hoodieInputFormat = new HoodieParquetInputFormat();
    JobConf jobConf = new JobConf(hadoopConf);
    hoodieInputFormat.setConf(jobConf);
    HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    setupIncremental(jobConf, startCommitTime, numCommitsToPull);
    FileInputFormat.setInputPaths(jobConf, Paths.get(basePath, partitionPath).toString());
    return hoodieInputFormat.listStatus(jobConf);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
    String modePropertyName =
            String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
            String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
            String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }
}
