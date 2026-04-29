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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

class RunHudiCRUDOperations extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testHudiCreateTableCommand(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_trips_cow_using_new_syntax"
    cleanup(tableName, getBasePath(tableName))
    val sqlStr = readSqlFromResource("create_table_using_new_syntax.sql")
    spark.sql(sqlStr)
    tableExists(database, tableName)
    assert(spark.sql(s"select * from $database.$tableName").count() == 0)
  }

  def testNewHudiCreateSQLFromHudiSource(): Unit = {
    val database = "rawdatatmp"
    val sourceTableName = "hudi_trips_cow_from_select_source"
    cleanup(sourceTableName, getBasePath(sourceTableName))
    val sqlStr = readSqlFromResource("create_table_from_select_statement.sql")
    spark.sql(sqlStr)
    tableExists(database, sourceTableName)
    assert(spark.sql(s"select * from $database.$sourceTableName").count() == 1)

    val targetTableName = "hudi_trips_cow_from_select_statement"
    cleanup(targetTableName, getBasePath(targetTableName))
    val sqlStr2 = readSqlFromResource("create_table_from_select_statement_2.sql")
    spark.sql(sqlStr2)
    tableExists(database, targetTableName)
    assert(spark.sql(s"select * from $database.$targetTableName").count() == 1)
  }

  def testHudiInsertSqlCommand(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_trips_cow_test_insert_sql"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)
    assert(tableExists(database, tableName))

    spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'rider1', 10.0, 1000)")
    spark.sql(
      s"""INSERT INTO $database.$tableName VALUES
         |(2, 'rider2', 20.0, 2000),
         |(3, 'rider3', 30.0, 3000)""".stripMargin)
    runSqlQueryWithAsserts(database, tableName, fullScan = true, expectedVal = 3)
    log.info("INSERT INTO SQL test executed successfully")
  }

  def testHudiUpdateSqlCommand(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_trips_cow_test_update_sql"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)
    spark.sql(
      s"""INSERT INTO $database.$tableName VALUES
         |(1, 'rider1', 10.0, 1000),
         |(2, 'rider2', 20.0, 2000)""".stripMargin)

    spark.sql(s"UPDATE $database.$tableName SET price = 99.0, ts = 5000 WHERE id = 1")

    val updated = spark.sql(s"SELECT price FROM $database.$tableName WHERE id = 1").collect()
    assert(updated.length == 1, s"Expected exactly 1 row for id=1 but got ${updated.length}")
    val updatedPrice = updated(0).getDouble(0)
    assert(updatedPrice == 99.0, s"Expected price 99.0 after UPDATE but got $updatedPrice")
    runSqlQueryWithAsserts(database, tableName, fullScan = true, expectedVal = 2)
    log.info("UPDATE SQL test executed successfully")
  }

  def testHudiDeleteSqlCommand(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_trips_cow_test_delete_sql"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)
    spark.sql(
      s"""INSERT INTO $database.$tableName VALUES
         |(1, 'rider1', 10.0, 1000),
         |(2, 'rider2', 20.0, 2000),
         |(3, 'rider3', 30.0, 3000)""".stripMargin)

    spark.sql(s"DELETE FROM $database.$tableName WHERE id = 2")

    val survivors = spark.sql(s"SELECT id FROM $database.$tableName ORDER BY id")
      .collect().map(_.getInt(0)).toSeq
    assert(survivors == Seq(1, 3), s"Expected ids Seq(1, 3) after DELETE but got $survivors")
    runSqlQueryWithAsserts(database, tableName, fullScan = true, expectedVal = 2)
    log.info("DELETE SQL test executed successfully")
  }

  def testInsertOverwriteWithSourceHudi(): Unit = {
    val database = "rawdatatmp"
    val sourceHudiTable = "hudi_trips_insert_overwrite_source"
    cleanup(sourceHudiTable, getBasePath(sourceHudiTable))
    testInsertOverwriteHelper(database, sourceHudiTable, isSourceHudiTable = true)
  }

  def testInsertOverwriteWithSourceHive(): Unit = {
    val database = "rawdatatmp"
    val sourceHiveTable = "hive_trips_insert_overwrite_source"
    cleanup(sourceHiveTable, getBasePath(sourceHiveTable))
    testInsertOverwriteHelper(database, sourceHiveTable, isSourceHudiTable = false)
  }

  /**
   * This method tests following cases for insert-overwrite on hive table
   * 1. Insert into hive table without partition
   * 2. Insert into hive table with a specific partition
   * 3. Insert into hive table with multiple partitions
   * 4. Insert into hudi table without partition
   */
  private def testInsertOverwriteHelper(database: String, sourceTable: String, isSourceHudiTable: Boolean): Unit = {
    createInserts(database, sourceTable, SaveMode.Overwrite, isSourceHudiTable)
    val tableSuffix = if (isSourceHudiTable) "_hudi" else "_hive"

    // 1. Insert into hive table without partitions from a hudi table
    var targetTableName = "hive_trips_insert_overwrite_with_all_partitions" + tableSuffix
    cleanup(targetTableName, getBasePath(targetTableName))
    // Register the target table as hive table and insert data into it.
    createHiveTestTable(database, targetTableName, getBasePath(targetTableName), true, isSourceHudiTable)
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
    val sqlStr =
      s"""
         | INSERT OVERWRITE TABLE
         | $database.$targetTableName
         | (SELECT * from $database.$sourceTable)
         |""".stripMargin
    log.info("Executing sql: " + sqlStr)
    spark.sql(sqlStr)
    runSqlQueryWithAsserts(database, targetTableName, true, 20)

    // 2. Insert into hive table with a specific partition from a hudi table
    targetTableName = "hive_trips_insert_overwrite_with_specific_partition" + tableSuffix
    cleanup(targetTableName, getBasePath(targetTableName))
    // Register the target table as hive table and insert data into it.
    createHiveTestTable(database, targetTableName, getBasePath(targetTableName), true, isSourceHudiTable)
    val sqlStr2 =
      s"""
         | INSERT OVERWRITE TABLE
         | $database.$targetTableName
         | PARTITION(partitionpath) (SELECT * from $database.$sourceTable where partitionpath = '2025-01-01')
         |""".stripMargin
    spark.sql(sqlStr2)
    var df = spark.sql(s"select * from $database.$targetTableName")
    df.show(100, false)
    assert(df.count() > 0)

    // 3. Insert into hive table with multiple partitions
    targetTableName = "hive_trips_insert_overwrite_with_specific_partitions" + tableSuffix
    cleanup(targetTableName, getBasePath(targetTableName))
    // Register the target table as hive table and insert data into it.
    createHiveTestTable(database, targetTableName, getBasePath(targetTableName), true, isSourceHudiTable)
    val sqlStr3 =
      s"""
         | INSERT OVERWRITE TABLE
         | $database.$targetTableName
         | PARTITION(partitionpath) (SELECT * from $database.$sourceTable where partitionpath in ('2025-01-01', '2025-01-02'))
         |""".stripMargin
    spark.sql(sqlStr3)
    df = spark.sql(s"select * from $database.$targetTableName")
    df.show(100, false)
    assert(df.count() > 0)
  }

  def testSparkHoodieConfigPropagationDisablesMetadataTable(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_spark_config_propagation_test"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.conf.set("spark." + HoodieMetadataConfig.ENABLE.key, "false")
    try {
      spark.sql(
        s"""
           |CREATE TABLE $database.$tableName (
           |  id INT,
           |  name STRING,
           |  price DOUBLE,
           |  ts BIGINT
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$basePath'
           |""".stripMargin)

      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'rider1', 10.0, 1000)")
      assert(tableExists(database, tableName))
      assert(spark.sql(s"SELECT * FROM $database.$tableName").count() == 1)

      val metadataTablePath = new Path(basePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH)
      val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      assert(!fs.exists(metadataTablePath),
        s"Metadata table should not exist when spark.${HoodieMetadataConfig.ENABLE.key} is set to false")
      log.info("Verified: metadata table was not created when disabled via spark.hoodie config")
    } finally {
      spark.conf.unset("spark." + HoodieMetadataConfig.ENABLE.key)
      cleanup(tableName, basePath)
    }
  }

  def insertOverwriteToHudiDataset(): Unit = {
    val database = "rawdatatmp"
    val sourceHudiTable = "hudi_trips_insert_overwrite_source2"
    cleanup(sourceHudiTable, getBasePath(sourceHudiTable))
    createInserts(database, sourceHudiTable, SaveMode.Overwrite, isHudiTable = true)
    // val targetTableName = "hudi_trips_insert_overwrite_with_all_partitions"
    val targetTableName = "hudi_trips_external_table_test"
    cleanup(targetTableName, getBasePath(targetTableName))

    // Create hudi table with CREATE SQL
    val createSqlStr = readSqlFromResource("test_create_external_table.sql")
    spark.sql(createSqlStr)
    assert(tableExists(database, targetTableName))
    val basepath = spark.sessionState.catalog.externalCatalog.getTable(database, targetTableName).location.toString
    try {
      HoodieTableMetaClient.builder().setBasePath(basepath).setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration)).build()
      throw new AssertionError("Expecting TableNotFoundException but metaClient is created successfully")
    } catch {
      case e: TableNotFoundException => log.info(s"Expected exception ${e.getClass} is thrown")
    }

    // Insert overwrite on an empty table should fail.
    try {
      spark.sql("SET hive.exec.dynamic.partition = true")
      spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
      val sqlStr =
        s"""
           | INSERT OVERWRITE TABLE
           | $database.$targetTableName
           | (SELECT * from $database.$sourceHudiTable)
           |""".stripMargin
      spark.sql(sqlStr)
      throw new AssertionError("Expecting an exception but query executed successfully")
    } catch {
      case _: TableNotFoundException =>
        log.info(s"Expected exception TableNotFoundException is thrown")
    }

    // select query should nto fail and should return 0 results.
    val df = spark.sql(s"select * from $database.$targetTableName")
    assert(df.count() == 0)
  }
}
