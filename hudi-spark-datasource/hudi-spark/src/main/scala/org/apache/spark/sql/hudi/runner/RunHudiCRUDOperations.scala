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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

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

  /**
   * Matrix coverage for SQL UPDATE across:
   *   hoodie.spark.sql.optimized.writes.enable in {true, false}
   *   table type in {cow, mor}
   *   column style in {partial, all}  (partial = update one non-PK col, all = update every non-PK col)
   *
   * Per-cell failures are accumulated and reported as a single AssertionError so the run log
   * shows the complete pass/fail grid even when an early cell breaks.
   */
  def testHudiUpdateSqlCommand(): Unit = {
    val database = "rawdatatmp"
    val results = ListBuffer[(String, Option[Throwable])]()
    for {
      optimized <- Seq(true, false)
      tableType <- Seq("cow", "mor")
      style <- Seq("partial", "all")
    } {
      val cellLabel = s"flag=$optimized type=$tableType style=$style"
      val tableName = s"hudi_dml_update_${tableType}_opt_${optimized}_$style"
      results += runOptimizedWritesCell("UPDATE", cellLabel, optimized, database, tableName, () => {
        val basePath = getBasePath(tableName)
        setupOptimizedWritesTestTable(database, tableName, basePath, tableType)
        val updateSql = if (style == "partial") {
          s"UPDATE $database.$tableName SET fare = 999.99 WHERE id = 3"
        } else {
          s"UPDATE $database.$tableName SET name = 'r3_new', fare = 999.99, ts = ts + 1 WHERE id = 3"
        }
        log.info(s"[UPDATE] cell $cellLabel: executing $updateSql")
        spark.sql(updateSql)
        val expectedSnapshotRow = if (style == "partial") {
          (3, "r3", 999.99, 1002L, "2025-01-02")
        } else {
          (3, "r3_new", 999.99, 1003L, "2025-01-02")
        }
        val expectedROBaselineRow = (3, "r3", 30.0, 1002L, "2025-01-02")
        assertViaSnapshotAndRO(
          database, tableName, basePath, tableType,
          expectedSnapshotCount = 5,
          expectedROCount = 5,
          snapshotMutationCheck = (view: String) => {
            assertRow(view, id = 3, expected = expectedSnapshotRow, label = "snapshot")
          },
          roMutationCheck = (view: String) => {
            assertRow(view, id = 3, expected = expectedROBaselineRow,
              label = "RO baseline (no compaction)")
          })
      })
    }
    failIfAnyCellsFailed("UPDATE", results.toSeq)
  }

  /**
   * Matrix coverage for SQL DELETE across:
   *   hoodie.spark.sql.optimized.writes.enable in {true, false}
   *   table type in {cow, mor}
   *
   * No column-style dimension because DELETE has no SET clause.
   */
  def testHudiDeleteSqlCommand(): Unit = {
    val database = "rawdatatmp"
    val results = ListBuffer[(String, Option[Throwable])]()
    for {
      optimized <- Seq(true, false)
      tableType <- Seq("cow", "mor")
    } {
      val cellLabel = s"flag=$optimized type=$tableType"
      val tableName = s"hudi_dml_delete_${tableType}_opt_$optimized"
      results += runOptimizedWritesCell("DELETE", cellLabel, optimized, database, tableName, () => {
        val basePath = getBasePath(tableName)
        setupOptimizedWritesTestTable(database, tableName, basePath, tableType)
        val deleteSql = s"DELETE FROM $database.$tableName WHERE id = 3"
        log.info(s"[DELETE] cell $cellLabel: executing $deleteSql")
        spark.sql(deleteSql)
        assertViaSnapshotAndRO(
          database, tableName, basePath, tableType,
          expectedSnapshotCount = 4,
          expectedROCount = 5,
          snapshotMutationCheck = (view: String) => {
            val cnt = spark.sql(s"SELECT count(*) FROM $view WHERE id = 3")
              .collectAsList().get(0).getLong(0)
            assert(cnt == 0, s"snapshot: expected id=3 deleted, but found $cnt rows")
          },
          roMutationCheck = (view: String) => {
            // RO sees only base files; the delete is in a log file and not yet compacted,
            // so the row should still be visible on the RO path.
            val cnt = spark.sql(s"SELECT count(*) FROM $view WHERE id = 3")
              .collectAsList().get(0).getLong(0)
            assert(cnt == 1, s"RO: expected baseline id=3 still visible (no compaction), but found $cnt rows")
          })
      })
    }
    failIfAnyCellsFailed("DELETE", results.toSeq)
  }

  /**
   * Matrix coverage for SQL MERGE INTO across:
   *   hoodie.spark.sql.optimized.writes.enable in {true, false}
   *   table type in {cow, mor}
   *   column style in {partial, all}  (partial = UPDATE SET fare = s.fare, all = UPDATE SET *)
   *
   * Source CTE has one matched row (id=3 -> fare=777.0) and one unmatched row (id=99).
   * RO assertion intentionally does not check the inserted row's visibility because MOR-insert
   * routing (base file vs. log file) is not a stable contract.
   */
  def testHudiMergeIntoSqlCommand(): Unit = {
    val database = "rawdatatmp"
    val results = ListBuffer[(String, Option[Throwable])]()
    for {
      optimized <- Seq(true, false)
      tableType <- Seq("cow", "mor")
      style <- Seq("partial", "all")
    } {
      val cellLabel = s"flag=$optimized type=$tableType style=$style"
      val tableName = s"hudi_dml_merge_${tableType}_opt_${optimized}_$style"
      results += runOptimizedWritesCell("MERGE", cellLabel, optimized, database, tableName, () => {
        val basePath = getBasePath(tableName)
        setupOptimizedWritesTestTable(database, tableName, basePath, tableType)
        val updateClause = if (style == "partial") {
          // Hudi MERGE INTO requires the precombine field (`ts`) to appear in the SET clause
          // for partial updates; otherwise: MergeIntoFieldResolutionException for `ts`.
          "UPDATE SET fare = s.fare, ts = s.ts"
        } else {
          "UPDATE SET *"
        }
        val insertClause = if (style == "partial") {
          "INSERT (id, name, fare, ts, partitionpath) VALUES (s.id, s.name, s.fare, s.ts, s.partitionpath)"
        } else {
          "INSERT *"
        }
        val mergeSql =
          s"""
             | MERGE INTO $database.$tableName t
             | USING (
             |   SELECT 3 AS id, 'r3_merged' AS name, 777.0 AS fare, 2000L AS ts, '2025-01-02' AS partitionpath
             |   UNION ALL
             |   SELECT 99 AS id, 'r99' AS name, 88.0 AS fare, 2001L AS ts, '2025-01-03' AS partitionpath
             | ) s
             | ON t.id = s.id
             | WHEN MATCHED THEN $updateClause
             | WHEN NOT MATCHED THEN $insertClause
             |""".stripMargin
        log.info(s"[MERGE] cell $cellLabel: executing $mergeSql")
        spark.sql(mergeSql)
        val expectedSnapshotIdThreeRow = if (style == "partial") {
          // partial: only fare and ts are taken from source; name stays as baseline.
          (3, "r3", 777.0, 2000L, "2025-01-02")
        } else {
          // all (UPDATE SET *): entire row is replaced by source.
          (3, "r3_merged", 777.0, 2000L, "2025-01-02")
        }
        val expectedSnapshotIdNinetyNineRow = (99, "r99", 88.0, 2001L, "2025-01-03")
        val expectedROBaselineIdThreeRow = (3, "r3", 30.0, 1002L, "2025-01-02")
        assertViaSnapshotAndRO(
          database, tableName, basePath, tableType,
          expectedSnapshotCount = 6,
          expectedROCount = -1, // RO row count is not asserted for MERGE - see roMutationCheck.
          snapshotMutationCheck = (view: String) => {
            assertRow(view, id = 3, expected = expectedSnapshotIdThreeRow, label = "snapshot")
            assertRow(view, id = 99, expected = expectedSnapshotIdNinetyNineRow,
              label = "snapshot (inserted row)")
          },
          roMutationCheck = (view: String) => {
            // Assert only the matched UPDATE has not landed in base files yet.
            // The inserted id=99 row's RO visibility depends on small-file packing and is not asserted.
            assertRow(view, id = 3, expected = expectedROBaselineIdThreeRow,
              label = "RO baseline (no compaction)")
          })
      })
    }
    failIfAnyCellsFailed("MERGE", results.toSeq)
  }

  private def setupOptimizedWritesTestTable(database: String, tableName: String,
                                            basePath: String, tableType: String): Unit = {
    cleanup(tableName, basePath)
    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  fare DOUBLE,
         |  ts BIGINT,
         |  partitionpath STRING
         |) USING hudi
         |PARTITIONED BY (partitionpath)
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'r1', 10.0, 1000, '2025-01-01'),
         |  (2, 'r2', 20.0, 1001, '2025-01-01'),
         |  (3, 'r3', 30.0, 1002, '2025-01-02'),
         |  (4, 'r4', 40.0, 1002, '2025-01-02'),
         |  (5, 'r5', 50.0, 1003, '2025-01-02')
         |""".stripMargin)
  }

  private def runOptimizedWritesCell(opLabel: String, cellLabel: String, optimized: Boolean,
                                     database: String, tableName: String,
                                     body: () => Unit): (String, Option[Throwable]) = {
    val flagKey = DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES.key()
    spark.conf.set(flagKey, optimized.toString)
    val basePath = getBasePath(tableName)
    try {
      body()
      log.info(s"[$opLabel] cell $cellLabel -> PASS")
      (cellLabel, None)
    } catch {
      case t: Throwable =>
        log.error(s"[$opLabel] cell $cellLabel -> FAIL (${truncate(t.toString, 240)})", t)
        (cellLabel, Some(t))
    } finally {
      try cleanup(tableName, basePath) catch {
        case t: Throwable => log.warn(s"[$opLabel] cleanup failed for $tableName: ${t.getMessage}")
      }
      spark.conf.unset(flagKey)
    }
  }

  private def assertViaSnapshotAndRO(database: String, tableName: String, basePath: String,
                                     tableType: String, expectedSnapshotCount: Long,
                                     expectedROCount: Long,
                                     snapshotMutationCheck: String => Unit,
                                     roMutationCheck: String => Unit): Unit = {
    // For COW, the HMS-registered Hudi table reads correctly via SQL.
    // For MOR, on this stack the HoodieSparkPlugin injects
    //   spark.hoodie.datasource.read.file.index.list.file.statuses.using.ro.path.filter=true
    // session-wide, which causes every read (including the _rt Hive view and even the DF
    // reader with QUERY_TYPE=snapshot) to list only base files. We override that flag per-read
    // here so the snapshot DF actually merges log files.
    val snapshotDf = if (tableType == "mor") {
      spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
        .option(DataSourceReadOptions.FILE_INDEX_LIST_FILE_STATUSES_USING_RO_PATH_FILTER.key, "false")
        .load(basePath)
    } else {
      spark.sql(s"SELECT * FROM $database.$tableName")
    }
    val snapshotView = s"${tableName}_snapshot_view"
    snapshotDf.createOrReplaceTempView(snapshotView)
    spark.sql(s"SELECT * FROM $snapshotView").show(20, false)
    val snapshotCount = spark.sql(s"SELECT count(*) FROM $snapshotView")
      .collectAsList().get(0).getLong(0)
    assert(snapshotCount == expectedSnapshotCount,
      s"snapshot: expected $expectedSnapshotCount rows, got $snapshotCount")
    snapshotMutationCheck(snapshotView)

    if (tableType == "mor") {
      val roDf = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
        .load(basePath)
      val roView = s"${tableName}_ro_view"
      roDf.createOrReplaceTempView(roView)
      roDf.show(20, false)
      if (expectedROCount >= 0) {
        val roCount = roDf.count()
        assert(roCount == expectedROCount,
          s"RO: expected $expectedROCount rows, got $roCount")
      }
      roMutationCheck(roView)
    }
  }

  private def failIfAnyCellsFailed(opLabel: String,
                                   results: Seq[(String, Option[Throwable])]): Unit = {
    val failures = results.collect { case (label, Some(t)) => (label, t) }
    if (failures.nonEmpty) {
      val summary = failures.map { case (label, t) =>
        s"  - $label -> ${truncate(t.toString, 300)}"
      }.mkString("\n")
      // Use RuntimeException (not AssertionError) so HoodieSparkSqlWriterRunner's
      // `case e: Exception` catch records this method as failed via reportStatusMetrics
      // instead of letting the Error propagate and abort runAllTests.
      val aggregate = new RuntimeException(
        s"$opLabel: ${failures.size} of ${results.size} cells failed:\n$summary")
      failures.foreach { case (_, t) => aggregate.addSuppressed(t) }
      throw aggregate
    }
    log.info(s"$opLabel: all ${results.size} cells passed")
  }

  /**
   * Assert that exactly one row exists for the given id and matches the expected
   * (id, name, fare, ts, partitionpath) tuple. Used by the UPDATE/MERGE matrix tests so
   * the partial vs all-column behaviour is actually validated end-to-end, not just `fare`.
   */
  private def assertRow(view: String, id: Int,
                        expected: (Int, String, Double, Long, String),
                        label: String): Unit = {
    val rows = spark.sql(
      s"SELECT id, name, fare, ts, partitionpath FROM $view WHERE id = $id")
      .collectAsList()
    assert(rows.size == 1,
      s"$label: expected exactly 1 row for id=$id, got ${rows.size}")
    val row = rows.get(0)
    val actual = (row.getInt(0), row.getString(1), row.getDouble(2),
      row.getLong(3), row.getString(4))
    assert(actual == expected,
      s"$label: expected row $expected for id=$id, got $actual")
  }

  private def truncate(s: String, max: Int): String =
    if (s == null || s.length <= max) s else s.substring(0, max) + "..."

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
