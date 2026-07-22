/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.runner

import org.apache.hudi.HoodieSparkSQLUtils
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

class RunNonHoodiePartitionsTest extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testReadingNonHoodiePartitions(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_non_hoodie_partition_table"
    val basepath = getBasePath(tableName)
    cleanup(tableName, basepath)
    // Insert data into hudi table
    createInserts(database, tableName, SaveMode.Append, isHudiTable = true)

    // Then delete _hoodie_partition_metadata file and run row count checks.
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basepath).setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sessionState.newHadoopConf()))
      .build()
    val storage = metaClient.getStorage
    val partitionsList = metaClient.getStorage.listDirectEntries(new StoragePath(basepath))
    assert(partitionsList != null && !partitionsList.isEmpty)
    // Get the first partition directory
    val firstPartition = partitionsList.stream()
      .filter(s => s.isDirectory && !s.getPath.getName.startsWith("."))
      .findFirst()
    assert(firstPartition.isPresent)
    val partitionPath = firstPartition.get().getPath
    log.info(s"PartitionPath: $partitionPath")
    val pathOption = HoodiePartitionMetadata.getPartitionMetafilePath(storage, partitionPath)
    assert(pathOption.isPresent)
    log.info(s"PathOption: ${pathOption.get()}")
    storage.deleteFile(pathOption.get())
    val df = spark.sql(s"select * from $database.$tableName")
    df.show(100, false)
    assert(20 == df.count())
  }

  def testDatasetWithMissingHoodiePartitionMetadataFile(): Unit = {
    val tableName = "rawdata.kafka_hp_scheduled_ride_job_state_changes_nodedup"
    val df = spark.sql(
      s"""
         | select datestr, count(1) from $tableName
         | where datestr >= '2018-02-01'
         | and datestr <= '2018-12-31'
         | group by datestr
         | order by datestr""".stripMargin)
    df.show(1000, false)
    val rowCount = df.count()
    log.info("Row count seen : " + rowCount)
    assert(rowCount > 0, "Row count should be greater than 0")
  }
}
