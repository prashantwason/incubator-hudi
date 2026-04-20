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

import org.apache.hudi.{DataSourceReadOptions, HoodieSparkSQLUtils}
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineUtils}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors

import scala.collection.mutable.ArrayBuffer

class RunHudiComparisonTests extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  //TODO: Run queries on non-partitioned dataset and multi-key partitioned datasets
  def testPartitionPruning(): Unit = {
    val datasets = Seq("rawdata.schemaless_userstore_udr_entities_rows", "rawdata.schemaless_mezzanine_trips_rows",
      "rawdata.schemaless_hbostore_udr_contact_rows", "rawdata.schemaless_jobstore_orders_rows",
      "rawdata.docstore_docstore_rolodex_csv_rolodex_query_rows")
    val startDate = LocalDate.now().minusDays(365)
    val xPartitions = 30
    executeRowCount(datasets, startDate, xPartitions)
  }

  def testQueriesOnHudiAndNonPartitions(): Unit = {
    val datasets = Seq(
      "rawdata.kafka_hp_scheduled_ride_job_state_changes_nodedup")
    val baseDate = LocalDate.of(2018, 2, 1)
    val xPartitions = 28
    executeRowCount(datasets, baseDate, xPartitions)
  }

  private def executeRowCount(datasets: Seq[String], baseDate: LocalDate, xPartitions: Int): Unit = {
    val startDateFormatted = baseDate.format(formatter)
    val endDateFormatted = baseDate.plusDays(xPartitions).format(formatter)
    log.info(s"Start date is $startDateFormatted and end date is $endDateFormatted")

    val logList = ArrayBuffer[String]()
    for (dataset <- datasets) {
      try {
        val timer = startAndGetHoodieTimer()
        spark.sparkContext.setJobGroup(s"Counting rows in $xPartitions partitions", s"Counting rows in $dataset")
        val results = spark.sql(s"SELECT datestr, count(1) FROM $dataset "
            + s"WHERE datestr >= '$startDateFormatted' AND datestr < '$endDateFormatted' GROUP BY datestr")
          .collectAsList()
        val duration = timer.endTimer()
        var currList = ArrayBuffer[String]()
        results.forEach(row => currList += s"Row count for $dataset is ${row.get(0)}, ${row.get(1)}")
        currList = currList.sorted
        logList += s"Duration to collect row counts for last $xPartitions partitions in $dataset is $duration ms"
        logList ++= currList
        assert(results.size() == xPartitions, s"Expected $xPartitions partitions but got ${results.size()}")
      } catch {
        case e: Exception =>
          log.error(s"Error running row counts test on $dataset", e)
          logList += s"Error running row counts test on $dataset"
      }
    }
    logList.foreach(log.info)
  }

  def testPointQueries(): Unit = {
    val point_queries: Seq[(String, String)] = Seq(
      ("rawdata.schemaless_userstore_udr_entities_rows", "SELECT * FROM rawdata.schemaless_userstore_udr_entities_rows WHERE datestr = '2024-01-01' and _hoodie_record_key = 'e9e8363b-340e-5132-9054-54ce4e1e3ab1'"),
      ("rawdata.schemaless_mezzanine_trips_rows", "SELECT * FROM rawdata.schemaless_mezzanine_trips_rows WHERE datestr ='2024-01-01' and _hoodie_record_key = 'cb2a1128-fb9a-4cad-829b-38694d33deb1'"),
      ("rawdata.schemaless_hbostore_udr_contact_rows", "SELECT * FROM rawdata.schemaless_hbostore_udr_contact_rows WHERE datestr ='2024-01-01' and _hoodie_record_key = '556cc633-f589-4a6c-a852-9eb4323b7150'"),
      ("rawdata.schemaless_userstore_udr_entities_rows", "SELECT _hoodie_record_key, datestr FROM rawdata.schemaless_userstore_udr_entities_rows WHERE datestr >= '2024-01-01' and datestr <= '2024-01-02' and _hoodie_record_key = 'e9e8363b-340e-5132-9054-54ce4e1e3ab1'")
    )

    val sb = new StringBuilder()
    for (query_pair <- point_queries) {
      try {
        val timer = startAndGetHoodieTimer()
        spark.sparkContext.setJobGroup("Point queries", s"Running on ${query_pair._1}")
        val result = spark.sql(query_pair._2)
        result.show()
        val rows = result.count()
        val duration = timer.endTimer()
        sb.append(s"Duration to run point query on ${query_pair._1} is $duration with row count $rows").append("\n")
        assert(rows == 1, s"Expected 1 row but got $rows")
      } catch {
        case e: Exception =>
          log.error(s"Error running partition pruning test on ${query_pair._1}", e)
          sb.append(s"Error running partition pruning test on ${query_pair._1}").append("\n")
      }
    }
    log.info(sb.toString())
  }

  def testLoadingBasepaths(): Unit = {
    val datasets = Seq(
      "rawdata.schemaless_userstore_udr_entities_rows",
      "rawdata.schemaless_mezzanine_trips_rows",
      "rawdata.schemaless_hbostore_udr_contact_rows",
      "rawdata.kafka_hp_scheduled_ride_job_state_changes_nodedup",
      "rawdata.kafka_hp_event_user_nodedup"
    )
    for (dataset <- datasets) {
      val timer = startAndGetHoodieTimer()
      spark.sparkContext.setJobGroup("Test spark.read.load API", s"Running on $dataset")
      val basePath = HoodieSparkSQLUtils.getBasePathFromTableName(spark, dataset)
      var df = spark.read.format("hudi").load(basePath)
      log.info(s"Loaded basepath $basePath applying filter criteria.")
      df = df.filter("datestr = '2024-01-01'")
      val rows = df.count()
      val duration = timer.endTimer()
      log.info(s"Duration to collect row count of a partition using spark.read.format method on $dataset is $duration and rows are $rows")
      assert(rows > 0, s"Expected rows to be greater than 0 but got $rows")
    }
  }

  def testIncrementalQueries(): Unit = {
    val datasets = Seq(
      "rawdata.schemaless_userstore_udr_entities_rows"
    )
    for (dataset <- datasets) {
      val basePath = HoodieSparkSQLUtils.getBasePathFromTableName(spark, dataset)
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .build
      val nonClusteringIngestionCommitsTimeline = metaClient.getActiveTimeline
        .getCommitsTimeline
        .filterCompletedInstants
        .filter(instant => !TimelineUtils.isClusteringCommit(metaClient, instant))

      val completedCommitsTimeline = if (metaClient.getTableConfig.getTableType == HoodieTableType.MERGE_ON_READ) {
        nonClusteringIngestionCommitsTimeline.filter((instant: HoodieInstant) => instant.getAction == HoodieTimeline.DELTA_COMMIT_ACTION)
      } else {
        nonClusteringIngestionCommitsTimeline.filterCompletedInstants()
      }
      val lastTwoInstantsList: java.util.List[HoodieInstant] = completedCommitsTimeline
        .getReverseOrderedInstants
        .limit(2)
        .collect(Collectors.toList[HoodieInstant])
      val (beginInstant, endInstant) = if (lastTwoInstantsList.size == 2) {
        (lastTwoInstantsList.get(1).requestedTime, lastTwoInstantsList.get(0).requestedTime)
      } else {
        (null, null)
      }
      log.info(s"For $dataset, beginInstant is $beginInstant and endInstant is $endInstant")

      val timer = startAndGetHoodieTimer()
      if (beginInstant != null) {
        val sourceDF = spark.read.format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.START_COMMIT.key, beginInstant)
          .option(DataSourceReadOptions.END_COMMIT.key, endInstant)
          .load(basePath)
        val rowCount = sourceDF.count()
        log.info(s"Row count for $dataset is $rowCount with beginInstant: $beginInstant "
          + s"and endInstant: $endInstant has executed in ${timer.endTimer()} ms")
        assert(rowCount > 0, s"Expected row count to be greater than 0 but got $rowCount")
      }
    }
  }
}
