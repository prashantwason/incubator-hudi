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

import org.apache.hudi.common.model.HoodieRecord

import org.apache.spark.sql.{Encoders, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class RunHudiHiveJoinSQLs extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testJoinQueryOnHudiTables(): Unit = {
    val database = "rawdatatmp"
    val tableName1 = "hudi_trips_cow_test_join_query1"
    val tableName2 = "hudi_trips_cow_test_join_query2"

    cleanup(tableName1, getBasePath(tableName1))
    cleanup(tableName2, getBasePath(tableName2))

    // First create insert into 1st dataset
    createInserts(database, tableName1, SaveMode.Overwrite, isHudiTable = true)

    // Create 2nd dataset with contents from 1st dataset
    var sourceDf = spark.read.format("hudi").load(getBasePath(tableName1))
    val colsToDrop: Array[String] = HoodieRecord.HOODIE_META_COLUMNS.asScala.toArray
    sourceDf = sourceDf.drop(colsToDrop: _*)
    writeToHudiTable(sourceDf, database, tableName2, SaveMode.Overwrite, getBasePath(tableName2), mutable.Map[String, String]())
    val intersectionUuidsSorted = sourceDf.select("uuid").map(r => r.getString(0))(Encoders.STRING).sort().collect().sorted

    // Do inserts each on 1st and 2nd dataset
    createInserts(database, tableName1, SaveMode.Append, isHudiTable = true)
    createInserts(database, tableName2, SaveMode.Append, isHudiTable = true)

    // Run join queries and verify results
    runAllJoinQueriesAndVerify(database, tableName1, tableName2, intersectionUuidsSorted)
  }


  def testJoinQueryOnHiveTables(): Unit = {
    val database = "rawdatatmp"
    val tableName1 = "hive_trips_cow_test_join_query3"
    val tableName2 = "hive_trips_cow_test_join_query4"

    cleanup(tableName1, getBasePath(tableName1))
    cleanup(tableName2, getBasePath(tableName2))

    // First create insert into 1st dataset
    createInserts(database, tableName1, SaveMode.Overwrite, isHudiTable = false)

    // Create 2nd dataset with contents from 1st dataset
    var sourceDf = spark.sql(s"select * from $database.$tableName1")
    val colsToDrop: Array[String] = HoodieRecord.HOODIE_META_COLUMNS.asScala.toArray
    sourceDf = sourceDf.drop(colsToDrop: _*)
    writeToHiveTable(sourceDf, database, tableName2, SaveMode.Overwrite, getBasePath(tableName2))
    val intersectionUuidsSorted = sourceDf.select("uuid").map(r => r.getString(0))(Encoders.STRING).sort().collect().sorted

    // Do inserts each on 1st and 2nd dataset
    createInserts(database, tableName1, SaveMode.Append, isHudiTable = false)
    createInserts(database, tableName2, SaveMode.Append, isHudiTable = false)

    // Run join queries and verify results
    runAllJoinQueriesAndVerify(database, tableName1, tableName2, intersectionUuidsSorted)
  }

  def testJoinQueryOnHudiAndHiveTables(): Unit = {
    val database = "rawdatatmp"
    val tableName1 = "hudi_trips_cow_test_join_query5"
    val tableName2 = "hive_trips_cow_test_join_query6"

    cleanup(tableName1, getBasePath(tableName1))
    cleanup(tableName2, getBasePath(tableName2))

    // First create insert into 1st dataset
    createInserts(database, tableName1, SaveMode.Overwrite, isHudiTable = true)

    // Create 2nd dataset with contents from 1st dataset
    var sourceDf = spark.read.format("hudi").load(getBasePath(tableName1))
    val colsToDrop: Array[String] = HoodieRecord.HOODIE_META_COLUMNS.asScala.toArray
    sourceDf = sourceDf.drop(colsToDrop: _*)
    writeToHiveTable(sourceDf, database, tableName2, SaveMode.Overwrite, getBasePath(tableName2))
    val intersectionUuidsSorted = sourceDf.select("uuid").map(r => r.getString(0))(Encoders.STRING).sort().collect().sorted

    // Do inserts each on 1st and 2nd dataset
    createInserts(database, tableName1, SaveMode.Append, isHudiTable = true)
    createInserts(database, tableName2, SaveMode.Append, isHudiTable = false)

    // Run join queries and verify results
    runAllJoinQueriesAndVerify(database, tableName1, tableName2, intersectionUuidsSorted)
  }

  private def runAllJoinQueriesAndVerify(database: String, tableName1: String, tableName2: String, intersectionUuidsSorted: Array[String]): Unit = {
    val uuidsFromFirstDatasetSorted = spark.sql(s"select uuid from $database.$tableName1")
      .map(r => r.getString(0))(Encoders.STRING).collect()
      .sorted
    val uuidsFromSecondDatasetSorted = spark.sql(s"select uuid from $database.$tableName2")
      .map(r => r.getString(0))(Encoders.STRING).collect()
      .sorted
    val unionUuidsSorted = (intersectionUuidsSorted ++ uuidsFromFirstDatasetSorted ++ uuidsFromSecondDatasetSorted).distinct.sorted
    log.info(s"Intersection UUIDs: ${intersectionUuidsSorted.mkString(",\n")}")
    log.info(s"UUIDs from 1st dataset: ${uuidsFromFirstDatasetSorted.mkString(",\n")}")
    log.info(s"UUIDs from 2nd dataset: ${uuidsFromSecondDatasetSorted.mkString(",\n")}")
    log.info(s"Union UUIDs: ${unionUuidsSorted.mkString(",\n")}")

    runJoinQueryAndVerify(s"$database.$tableName1", s"$database.$tableName2", SQL_JOIN.INNER_JOIN.toString, Array("uuid"), intersectionUuidsSorted, fullScan = true)
    runJoinQueryAndVerify(s"$database.$tableName1", s"$database.$tableName2", SQL_JOIN.LEFT_JOIN.toString, Array("uuid"), uuidsFromFirstDatasetSorted, fullScan = true)
    runJoinQueryAndVerify(s"$database.$tableName1", s"$database.$tableName2", SQL_JOIN.RIGHT_JOIN.toString, Array("uuid"), uuidsFromSecondDatasetSorted, fullScan = true)
    runJoinQueryAndVerify(s"$database.$tableName1", s"$database.$tableName2", SQL_JOIN.FULL_OUTER_JOIN.toString, Array("uuid"), unionUuidsSorted, fullScan = true)
    runJoinQueryAndVerify(s"$database.$tableName1", s"$database.$tableName2", SQL_JOIN.CROSS_JOIN.toString, Array("uuid"),
      expectedRowCount = uuidsFromFirstDatasetSorted.length.toLong * uuidsFromSecondDatasetSorted.length.toLong, fullScan = true)
  }

  private def runJoinQueryAndVerify(fullTableName1: String, fullTableName2: String, joinType: String,
                   columnsToJoin: Array[String], expectedUUIDs: Array[String] = Array.empty,
                   expectedRowCount: Long = -1, fullScan: Boolean): Unit = {
    spark.sparkContext.setJobGroup("SQL Join query", s"SQL join query between $fullTableName1 and $fullTableName2")
    var sqlStr = s"SELECT * FROM $fullTableName1 $joinType $fullTableName2"
    if (joinType != SQL_JOIN.CROSS_JOIN.toString) {
      for ((col, idx) <- columnsToJoin.zipWithIndex) {
        val keyword = if (idx == 0) "ON" else "AND"
        sqlStr = sqlStr.concat(s" $keyword $fullTableName1.$col = $fullTableName2.$col")
      }
    }
    log.info(s"Executing sql: $sqlStr")
    var sqlDF = spark.sql(sqlStr)
    if (!fullScan) {
      sqlDF = sqlDF.limit(10)
    }
    sqlDF.show(100, truncate = false)

    if (joinType == SQL_JOIN.CROSS_JOIN.toString) {
      val rowCount = sqlDF.count()
      assert(rowCount == expectedRowCount, s"Cross join row count doesn't match. Expected: $expectedRowCount, Got: $rowCount")
    } else if (joinType == SQL_JOIN.INNER_JOIN.toString) {
      val uuids: Array[String] = sqlDF.select(s"$fullTableName1.${columnsToJoin.head}")
        .map(r => r.getString(0))(Encoders.STRING).collect().sorted
      assert(uuids.sameElements(expectedUUIDs), s"UUIDs don't match. Expected: ${expectedUUIDs.mkString(",\n")}, Got: ${uuids.mkString(",\n")}")
    } else if (joinType == SQL_JOIN.LEFT_JOIN.toString) {
      val uuids: Array[String] = sqlDF.select(s"$fullTableName1.${columnsToJoin.head}")
        .map(r => r.getString(0))(Encoders.STRING).collect().sorted
      assert(uuids.sameElements(expectedUUIDs), s"UUIDs don't match. Expected: ${expectedUUIDs.mkString(",\n")}, Got: ${uuids.mkString(",\n")}")
    } else if (joinType == SQL_JOIN.RIGHT_JOIN.toString) {
      val uuids: Array[String] = sqlDF.select(s"$fullTableName2.${columnsToJoin.head}")
        .map(r => r.getString(0))(Encoders.STRING).collect().sorted
      assert(uuids.sameElements(expectedUUIDs), s"UUIDs don't match. Expected: ${expectedUUIDs.mkString(",\n")}, Got: ${uuids.mkString(",\n")}")
    } else if (joinType == SQL_JOIN.FULL_OUTER_JOIN.toString) {
      val uuids: Array[(String, String)] = sqlDF.select(s"$fullTableName1.${columnsToJoin.head}", s"$fullTableName2.${columnsToJoin.head}")
        .map(r => Tuple2(r.getString(0), r.getString(1)))(Encoders.tuple(Encoders.STRING, Encoders.STRING)).collect()
      val totalNonNullsInLeft = uuids.map(r => r._1).count(_ != null)
      val totalNullsInLeft = uuids.map(r => r._1).count(_ == null)
      val totalNonNullsInRight = uuids.map(r => r._2).count(_ != null)
      val totalNullsInRight = uuids.map(r => r._2).count(_ == null)
      assert(40 == totalNonNullsInLeft, s"Total non-nulls in left table should be 20, but got $totalNonNullsInLeft")
      assert(20 == totalNullsInLeft, s"Total nulls in left table should be 20, but got $totalNullsInLeft")
      assert(40 == totalNonNullsInRight, s"Total non-nulls in right table should be 20, but got $totalNonNullsInRight")
      assert(20 == totalNullsInRight, s"Total nulls in right table should be 20, but got $totalNullsInRight")
    } else {
      throw new IllegalArgumentException(s"Unsupported join type: $joinType")
    }
  }

  def testJoinQueryOnTwoHudiProductionTables(): Unit = {
    spark.sparkContext.setJobGroup("Test join query on two hudi prod datasets",
      s"Join query between dwh.dim_city and dwh.fact_trip_payment")
    val sqlQuery = "select trips.trip_uuid, city.city_name, city.country_name, city.is_operational, trips.amount_usd" +
      " from dwh.dim_city as city join dwh.fact_trip_payment as trips " +
      " on city.city_id = trips.city_id and trips.city_id = 1 " +
      " and datestr = '2024-01-01' and trips.amount_usd > 10 order by trips.trip_uuid"
    explainQueryAndExecute(sqlQuery)
    log.info(s"Completed running join query on two hudi production tables")
  }

  def testJoinQueryOnHudiAndHiveProductionTables(): Unit = {
    spark.sparkContext.setJobGroup("Test join query on hudi and hive prod datasets",
      s"Join query between dwh.fact_trip_payment and dwh.dim_client")
    val sqlQuery = "select trips.trip_uuid, client.country_id, client.role, client.language, trips.amount_usd" +
      " from dwh.fact_trip_payment trips join dwh.dim_client as client" +
      " on trips.driver_uuid = client.user_uuid " +
      " and trips.datestr = '2024-01-01' and trips.amount_usd > 10 order by trips.trip_uuid"
    explainQueryAndExecute(sqlQuery)
    log.info(s"Completed running join query on hudi and hive production tables")
  }

  def testJoinQueryOnHudiAndHivePartitionedProductionTables(): Unit = {
    spark.sparkContext.setJobGroup("Test join query on hudi and hive partitioned prod datasets",
      s"Join query between dwh.fact_trip_payment and dwh.fact_trip")
    val sqlQuery = "select trip_payments.trip_uuid, trip_payments.classification, trip_payments.category, " +
      " trip_payments.jobtype, trip_payments.amount_usd, trip_payments.currency_code, trip_payments.datestr, trips.datestr " +
      " from dwh.fact_trip_payment trip_payments join dwh.fact_trip as trips on trip_payments.trip_uuid = trips.uuid " +
      " and trip_payments.datestr >= '2024-01-01' and trip_payments.datestr <= '2024-01-10' and trips.datestr = '2024-01-01'" +
      " and trip_payments.city_id = 1 order by trip_payments.trip_uuid"
    explainQueryAndExecute(sqlQuery)
    log.info(s"Completed running join query on hudi and hive partitioned production tables")
  }

  def testJoinQueryOnTwoHiveProductionTables(): Unit = {
    spark.sparkContext.setJobGroup("Test join query on two hive prod datasets",
      s"Join query between dwh.fact_trip and dwh.dim_client")
    val sqlQuery = "select trips.uuid, trips.currency_code, client.country_id, client.role, client.language" +
      " from dwh.fact_trip as trips join dwh.dim_client as client on trips.driver_uuid = client.user_uuid" +
      " and trips.datestr >= '2024-01-01' and trips.datestr <= '2024-01-03' and trips.city_id = 1 order by trips.uuid"
    explainQueryAndExecute(sqlQuery)
    log.info(s"Completed running join query on two hive production tables")
  }

  private def explainQueryAndExecute(sqlText: String): Unit = {
    val df = spark.sql(sqlText)
    /**
     * df.explain(true)
     * df.queryExecution.executedPlan
     */
    df.explain(true)
    log.info(s"Physical plan that will be executed ${df.queryExecution.executedPlan}")
    // Show results.
    val rowCount = df.count()
    log.info(s"For query $sqlText, Row count: $rowCount")
    assert(rowCount > 0, s"Expected row count to be greater than 0, but got $rowCount")
  }
}
