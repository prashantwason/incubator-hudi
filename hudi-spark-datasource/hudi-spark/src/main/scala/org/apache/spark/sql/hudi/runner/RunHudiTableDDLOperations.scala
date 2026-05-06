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

import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

/**
 * Integration tests for Hudi Table DDL commands.
 *
 * This category currently covers SHOW PARTITIONS
 * (ShowHoodieTablePartitionsCommand). ALTER TABLE ADD/DROP/RENAME PARTITION
 * and MSCK REPAIR TABLE will land here in follow-up PRs.
 *
 * Run from drogon with:
 *   TEST_CATEGORY=hudi-table-ddl TEST_NAME=                           # all
 *   TEST_CATEGORY=hudi-table-ddl TEST_NAME=testShowPartitionsMultiLevel
 */
class RunHudiTableDDLOperations extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)
  private val database = "rawdatatmp"

  // SHOW PARTITIONS on a non-partitioned hudi table should return zero rows.
  def testShowPartitionsNonPartitioned(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_non_partitioned"
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

    spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")

    assertShowPartitions(tableName, Seq.empty)
  }

  // SHOW PARTITIONS on a single-level partitioned table returns one row per
  // distinct partition value, hive-style encoded.
  def testShowPartitionsSinglePartition(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_single"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT,
         |  datestr STRING
         |) USING hudi
         |PARTITIONED BY (datestr)
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'a1', 10.0, 1000, '2025-01-01'),
         |  (2, 'a2', 11.0, 1001, '2025-01-02'),
         |  (3, 'a3', 12.0, 1002, '2025-01-03')
         |""".stripMargin)

    assertShowPartitions(tableName, Seq(
      "datestr=2025-01-01",
      "datestr=2025-01-02",
      "datestr=2025-01-03"
    ))
  }

  // SHOW PARTITIONS on a multi-level partitioned table returns one row per
  // distinct (year, month, day) tuple, slash-joined hive style.
  def testShowPartitionsMultiLevel(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_multi"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT,
         |  year STRING,
         |  month STRING,
         |  day STRING
         |) USING hudi
         |PARTITIONED BY (year, month, day)
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'a1', 10.0, 1000, '2025', '01', '01'),
         |  (2, 'a2', 11.0, 1001, '2025', '01', '02'),
         |  (3, 'a3', 12.0, 1002, '2025', '02', '01'),
         |  (4, 'a4', 13.0, 1003, '2026', '01', '01')
         |""".stripMargin)

    assertShowPartitions(tableName, Seq(
      "year=2025/month=01/day=01",
      "year=2025/month=01/day=02",
      "year=2025/month=02/day=01",
      "year=2026/month=01/day=01"
    ))
  }

  // SHOW PARTITIONS with a PARTITION(...) spec filters the listing. Both a
  // prefix match (year+month) and an exact match (year+month+day) are checked.
  def testShowPartitionsWithSpecFilter(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_spec_filter"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT,
         |  year STRING,
         |  month STRING,
         |  day STRING
         |) USING hudi
         |PARTITIONED BY (year, month, day)
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'a1', 10.0, 1000, '2025', '01', '01'),
         |  (2, 'a2', 11.0, 1001, '2025', '01', '02'),
         |  (3, 'a3', 12.0, 1002, '2025', '02', '01'),
         |  (4, 'a4', 13.0, 1003, '2026', '01', '01')
         |""".stripMargin)

    // Prefix match: (year=2025, month=01) selects two of the four partitions.
    assertShowPartitions(
      tableName,
      Seq("year=2025/month=01/day=01", "year=2025/month=01/day=02"),
      partitionSpec = Some("PARTITION(year='2025', month='01')"))

    // Exact match returns the single matching partition.
    assertShowPartitions(
      tableName,
      Seq("year=2025/month=01/day=01"),
      partitionSpec = Some("PARTITION(year='2025', month='01', day='01')"))
  }

  // SHOW PARTITIONS on a slash-separated-date partitioned table returns the
  // partition column value in dash form (yyyy-MM-dd), not the slash-encoded
  // physical path. Exercises the useSlashSeparatedDatePartitioning branch in
  // ShowHoodieTablePartitionsCommand.
  def testShowPartitionsSlashSeparatedDate(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_slash_date"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT,
         |  datestr STRING
         |) USING hudi
         |PARTITIONED BY (datestr)
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  'hoodie.datasource.write.slash.separated.date.partitioning' = 'true'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'a1', 10.0, 1000, '2026-01-05'),
         |  (2, 'a2', 11.0, 1001, '2026-01-06'),
         |  (3, 'a3', 12.0, 1002, '2026-02-10')
         |""".stripMargin)

    assertShowPartitions(tableName, Seq(
      "datestr=2026-01-05",
      "datestr=2026-01-06",
      "datestr=2026-02-10"
    ))
  }

  // After ALTER TABLE DROP PARTITION the partition disappears from the
  // listing; re-inserting into it brings it back.
  def testShowPartitionsAfterDropAndRecreate(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_drop_recreate"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT,
         |  datestr STRING
         |) USING hudi
         |PARTITIONED BY (datestr)
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'a1', 10.0, 1000, '2025-06-01'),
         |  (2, 'a2', 11.0, 1001, '2025-06-02')
         |""".stripMargin)
    assertShowPartitions(tableName, Seq("datestr=2025-06-01", "datestr=2025-06-02"))

    spark.sql(s"ALTER TABLE $database.$tableName DROP PARTITION (datestr='2025-06-01')")
    assertShowPartitions(tableName, Seq("datestr=2025-06-02"))

    spark.sql(s"INSERT INTO $database.$tableName VALUES (3, 'a3', 12.0, 1002, '2025-06-01')")
    assertShowPartitions(tableName, Seq("datestr=2025-06-01", "datestr=2025-06-02"))
  }

  // INSERT OVERWRITE TABLE replaces all data; SHOW PARTITIONS afterwards
  // reflects only the partitions present in the overwrite.
  def testShowPartitionsAfterInsertOverwrite(): Unit = {
    val tableName = "hudi_table_ddl_show_partitions_overwrite"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    spark.sql(
      s"""
         |CREATE TABLE $database.$tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts BIGINT,
         |  datestr STRING
         |) USING hudi
         |PARTITIONED BY (datestr)
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         |)
         |LOCATION '$basePath'
         |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $database.$tableName VALUES
         |  (1, 'a1', 10.0, 1000, '2025-07-01'),
         |  (2, 'a2', 11.0, 1001, '2025-07-02'),
         |  (3, 'a3', 12.0, 1002, '2025-07-03')
         |""".stripMargin)
    assertShowPartitions(tableName, Seq(
      "datestr=2025-07-01",
      "datestr=2025-07-02",
      "datestr=2025-07-03"
    ))

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE $database.$tableName VALUES
         |  (4, 'a4', 13.0, 1003, '2025-07-01'),
         |  (5, 'a5', 14.0, 1004, '2025-07-02')
         |""".stripMargin)
    assertShowPartitions(tableName, Seq(
      "datestr=2025-07-01",
      "datestr=2025-07-02"
    ))
  }

  /**
   * Run SHOW PARTITIONS (optionally with a PARTITION spec) on the given table
   * and assert the returned rows match `expected` as a sorted set.
   */
  private def assertShowPartitions(
      tableName: String,
      expected: Seq[String],
      partitionSpec: Option[String] = None): Unit = {
    val sql = partitionSpec match {
      case Some(spec) => s"SHOW PARTITIONS $database.$tableName $spec"
      case None       => s"SHOW PARTITIONS $database.$tableName"
    }
    val rows = spark.sql(sql).collect().map(_.getString(0)).toSeq.sorted
    val expectedSorted = expected.sorted
    log.info(s"[$tableName] $sql -> ${rows.mkString("[", ", ", "]")}")
    assert(rows == expectedSorted,
      s"SHOW PARTITIONS mismatch for $database.$tableName " +
        s"(spec=${partitionSpec.getOrElse("none")}). " +
        s"Expected ${expectedSorted.mkString("[", ", ", "]")}, " +
        s"got ${rows.mkString("[", ", ", "]")}")
  }
}
