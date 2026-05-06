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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Integration tests for Hudi Table DDL commands. Two coverage areas:
 *
 *   1. SHOW PARTITIONS (ShowHoodieTablePartitionsCommand) — 7 scenarios.
 *   2. ALTER TABLE — 48 scenarios covering ADD COLUMNS, CHANGE COLUMN,
 *      RENAME COLUMN, RENAME TO, ADD PARTITION, DROP PARTITION across
 *      COW + MOR.
 *
 * Each command's behavior is exercised end-to-end against the integ env's
 * HMS-backed catalog and HDFS, then asserted across three independent
 * surfaces: the Spark/HMS catalog (schema, partitions, comments), the
 * Hudi meta-client (timeline, table config, on-disk schema), and the
 * data path (SELECT after the DDL).
 *
 * ALTER TABLE methods follow the naming pattern test<Command><Variant>
 * (e.g. testRenameColumnMor) so the runner reports per-variant pass/fail
 * metrics and a single MOR failure can be re-run via TEST_NAME=...Mor
 * without re-running its COW counterpart.
 *
 * Run from drogon with:
 *   TEST_CATEGORY=hudi-table-ddl TEST_NAME=                           # all
 *   TEST_CATEGORY=hudi-table-ddl TEST_NAME=testShowPartitionsMultiLevel
 */
class RunHudiTableDDLOperations extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)
  private val database = "rawdatatmp"

  // ===========================================================================
  // SHOW PARTITIONS — ShowHoodieTablePartitionsCommand
  // ===========================================================================

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

  // ===========================================================================
  // ALTER TABLE ADD COLUMNS — AlterHoodieTableAddColumnsCommand
  // ===========================================================================

  def testAddColumnsCow(): Unit = runAddColumnsHappyPath("cow")
  def testAddColumnsMor(): Unit = runAddColumnsHappyPath("mor")

  private def runAddColumnsHappyPath(tableType: String): Unit = {
    val tableName = s"ddl_add_cols_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createBasicTable(tableName, tableType)
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")

      spark.sql(s"ALTER TABLE $database.$tableName ADD COLUMNS (ext0 STRING, ext1 DOUBLE)")

      // Catalog reflects new columns
      val catalogTable = getCatalogTable(tableName)
      val userFields = HoodieSqlCommonUtils.removeMetaFields(catalogTable.schema).fields.map(_.name)
      assertEqualsSeq(Seq("id", "name", "price", "ts", "ext0", "ext1"), userFields, "catalog schema")

      // Meta-client reflects new columns
      val metaClient = openMetaClient(basePath)
      val avroSchema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
      val avroFieldNames = avroSchema.getFields.asScala.map(_.name).toSet
      assert(avroFieldNames.contains("ext0"), s"meta-client schema missing ext0: $avroFieldNames")
      assert(avroFieldNames.contains("ext1"), s"meta-client schema missing ext1: $avroFieldNames")

      // Old row reads back with NULLs for new cols, post-add insert preserves values
      spark.sql(s"INSERT INTO $database.$tableName VALUES (2, 'a2', 12.0, 1001, 'x', 2.5)")
      val rows = spark.sql(s"SELECT id, ext0, ext1 FROM $database.$tableName ORDER BY id").collect()
      assert(rows.length == 2, s"expected 2 rows, got ${rows.length}")
      assert(rows(0).getString(1) == null, "row id=1 ext0 should be NULL")
      assert(rows(1).getString(1) == "x", s"row id=2 ext0 should be 'x', got '${rows(1).getString(1)}'")
    } finally cleanup(tableName, basePath)
  }

  def testAddColumnsWithCommentCow(): Unit = runAddColumnsWithComment("cow")
  def testAddColumnsWithCommentMor(): Unit = runAddColumnsWithComment("mor")

  private def runAddColumnsWithComment(tableType: String): Unit = {
    val tableName = s"ddl_add_cols_comment_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createBasicTable(tableName, tableType)
      spark.sql(s"ALTER TABLE $database.$tableName ADD COLUMNS (dt STRING COMMENT 'data time')")
      val catalogTable = getCatalogTable(tableName)
      val dtField = catalogTable.schema.fields.find(_.name == "dt").getOrElse(
        throw new AssertionError(s"dt column missing in catalog schema: ${catalogTable.schema.fieldNames.mkString(",")}"))
      val comment = dtField.getComment().getOrElse("")
      assert(comment == "data time", s"expected comment 'data time', got '$comment'")
    } finally cleanup(tableName, basePath)
  }

  def testAddColumnsDuplicateColumnFailsCow(): Unit = runAddColumnsDuplicateFails("cow")
  def testAddColumnsDuplicateColumnFailsMor(): Unit = runAddColumnsDuplicateFails("mor")

  private def runAddColumnsDuplicateFails(tableType: String): Unit = {
    val tableName = s"ddl_add_cols_dup_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createBasicTable(tableName, tableType)
      expectFailure(
        s"ALTER TABLE $database.$tableName ADD COLUMNS (name STRING)",
        "already exists in the table")
    } finally cleanup(tableName, basePath)
  }

  /**
   * MOR-only: AlterHoodieTableAddColumnsCommand.refreshSchema also refreshes the
   * `<tbl>_ro` and `<tbl>_rt` HMS entries when they exist. This test only asserts
   * the refresh if those entries were created by HMS sync — otherwise it logs and
   * passes (matches the source's own existence-guarded refresh).
   */
  def testAddColumnsRefreshesRoRtForMor(): Unit = {
    val tableName = "ddl_add_cols_rort_mor"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    cleanup(s"${tableName}_ro", basePath)
    cleanup(s"${tableName}_rt", basePath)
    try {
      createBasicTable(tableName, "mor")
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")
      spark.sql(s"ALTER TABLE $database.$tableName ADD COLUMNS (ext0 STRING)")

      Seq(s"${tableName}_ro", s"${tableName}_rt").foreach { sideTable =>
        val ident = new TableIdentifier(sideTable, Some(database))
        if (spark.sessionState.catalog.tableExists(ident)) {
          val schema = spark.sessionState.catalog.getTableMetadata(ident).schema
          val names = HoodieSqlCommonUtils.removeMetaFields(schema).fields.map(_.name).toSet
          assert(names.contains("ext0"),
            s"${ident.unquotedString} schema missing ext0 after ADD COLUMNS: $names")
        } else {
          log.info(s"$sideTable not present in HMS; skipping refresh assertion")
        }
      }
    } finally {
      cleanup(s"${tableName}_ro", basePath)
      cleanup(s"${tableName}_rt", basePath)
      cleanup(tableName, basePath)
    }
  }

  def testAddColumnsOnPartitionedTableCow(): Unit = runAddColumnsOnPartitioned("cow")
  def testAddColumnsOnPartitionedTableMor(): Unit = runAddColumnsOnPartitioned("mor")

  private def runAddColumnsOnPartitioned(tableType: String): Unit = {
    val tableName = s"ddl_add_cols_part_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT, dt STRING",
        tableType = tableType,
        partitionedBy = Some("dt"))
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000, '2025-01-01')")
      spark.sql(s"ALTER TABLE $database.$tableName ADD COLUMNS (ext0 DOUBLE)")
      // Old partition still queryable, new column is NULL there
      val rows = spark.sql(s"SELECT id, dt, ext0 FROM $database.$tableName WHERE dt = '2025-01-01'").collect()
      assert(rows.length == 1, s"expected 1 row, got ${rows.length}")
      assert(rows(0).get(2) == null, s"ext0 should be NULL on pre-add row, got ${rows(0).get(2)}")
      // Partition column is still last in catalog schema
      val schema = getCatalogTable(tableName).schema
      val userFields = HoodieSqlCommonUtils.removeMetaFields(schema).fields.map(_.name)
      assert(userFields.last == "dt",
        s"partition col should remain last; got order: ${userFields.mkString(",")}")
    } finally cleanup(tableName, basePath)
  }

  // ===========================================================================
  // ALTER TABLE CHANGE COLUMN — AlterHoodieTableChangeColumnCommand
  // ===========================================================================

  def testChangeColumnCommentCow(): Unit = runChangeColumnComment("cow")
  def testChangeColumnCommentMor(): Unit = runChangeColumnComment("mor")

  private def runChangeColumnComment(tableType: String): Unit = {
    val tableName = s"ddl_change_col_comment_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createBasicTable(tableName, tableType)
      spark.sql(s"ALTER TABLE $database.$tableName CHANGE COLUMN id id INT COMMENT 'primary id'")
      val catalogTable = getCatalogTable(tableName)
      val idField = catalogTable.schema.fields(catalogTable.schema.fieldIndex("id"))
      assert(idField.getComment().contains("primary id"),
        s"expected comment 'primary id', got ${idField.getComment()}")

      // Schema does not leak hoodie meta fields
      val metaClient = openMetaClient(basePath)
      val resolved = new TableSchemaResolver(metaClient).getTableSchema(false)
      val fieldNames = resolved.getFields.asScala.map(_.name).toSet
      val leaked = fieldNames intersect Set("_hoodie_commit_time", "_hoodie_record_key")
      assert(leaked.isEmpty, s"hoodie meta fields should not appear in user schema, found: $leaked")
    } finally cleanup(tableName, basePath)
  }

  def testChangeColumnTypeIncompatibleFailsCow(): Unit = runChangeColumnTypeIncompatibleFails("cow")
  def testChangeColumnTypeIncompatibleFailsMor(): Unit = runChangeColumnTypeIncompatibleFails("mor")

  private def runChangeColumnTypeIncompatibleFails(tableType: String): Unit = {
    val tableName = s"ddl_change_col_type_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createBasicTable(tableName, tableType)
      expectFailure(
        s"ALTER TABLE $database.$tableName CHANGE COLUMN id id BIGINT",
        "ALTER TABLE CHANGE COLUMN is not supported for changing column")
    } finally cleanup(tableName, basePath)
  }

  def testChangeColumnUnderOccCow(): Unit = runChangeColumnUnderOCC("cow")
  def testChangeColumnUnderOccMor(): Unit = runChangeColumnUnderOCC("mor")

  private def runChangeColumnUnderOCC(tableType: String): Unit = {
    val tableName = s"ddl_change_col_occ_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT",
        tableType = tableType,
        extraTblProps = Map(
          "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",
          "hoodie.clean.failed.writes.policy" -> "LAZY",
          "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider"
        ))
      spark.sql(s"ALTER TABLE $database.$tableName CHANGE COLUMN id id INT COMMENT 'primary id'")
      val catalogTable = getCatalogTable(tableName)
      val idField = catalogTable.schema.fields(catalogTable.schema.fieldIndex("id"))
      assert(idField.getComment().contains("primary id"),
        s"expected comment 'primary id', got ${idField.getComment()}")
    } finally cleanup(tableName, basePath)
  }

  /**
   * COW-only: with services configured to be eager, ALTER TABLE statements must NOT
   * trigger rollback / clean / archive. Counts each timeline before and after.
   */
  def testAlterDoesNotTriggerServicesCow(): Unit = {
    val tableName = "ddl_alter_no_services_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT",
        tableType = "cow",
        extraTblProps = Map(
          "hoodie.metadata.enable" -> "false",
          "hoodie.clean.commits.retained" -> "100",
          "hoodie.clustering.inline" -> "true",
          "hoodie.clustering.inline.max.commits" -> "1",
          "hoodie.keep.max.commits" -> "300",
          "hoodie.keep.min.commits" -> "200"
        ))
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")
      spark.sql(s"INSERT INTO $database.$tableName VALUES (2, 'a2', 20.0, 2000)")

      val metaClient = openMetaClient(basePath)
      val rollbackBefore = metaClient.getActiveTimeline.getRollbackTimeline.countInstants()
      val cleanBefore = metaClient.getActiveTimeline.getCleanerTimeline.countInstants()
      metaClient.getArchivedTimeline().reload()
      val archiveBefore = metaClient.getArchivedTimeline().countInstants()

      spark.sql(s"ALTER TABLE $database.$tableName CHANGE COLUMN id id INT COMMENT 'pk'")
      spark.sql(s"ALTER TABLE $database.$tableName SET TBLPROPERTIES ('hoodie.clean.commits.retained' = '1')")
      spark.sql(s"ALTER TABLE $database.$tableName SET TBLPROPERTIES ('hoodie.keep.max.commits' = '3')")
      spark.sql(s"ALTER TABLE $database.$tableName SET TBLPROPERTIES ('hoodie.keep.min.commits' = '2')")
      spark.sql(s"ALTER TABLE $database.$tableName CHANGE COLUMN id id INT COMMENT 'primary id'")

      metaClient.reloadActiveTimeline()
      val rollbackAfter = metaClient.getActiveTimeline.getRollbackTimeline.countInstants()
      val cleanAfter = metaClient.getActiveTimeline.getCleanerTimeline.countInstants()
      metaClient.getArchivedTimeline().reload()
      val archiveAfter = metaClient.getArchivedTimeline().countInstants()
      assert(rollbackAfter == rollbackBefore,
        s"ALTER must not produce rollbacks; before=$rollbackBefore after=$rollbackAfter")
      assert(cleanAfter == cleanBefore,
        s"ALTER must not produce cleans; before=$cleanBefore after=$cleanAfter")
      assert(archiveAfter == archiveBefore,
        s"ALTER must not produce archives; before=$archiveBefore after=$archiveAfter")
    } finally cleanup(tableName, basePath)
  }

  // ===========================================================================
  // ALTER TABLE RENAME COLUMN — AlterTableCommand (UPDATE / RenameColumn)
  // ===========================================================================

  def testRenameColumnCow(): Unit = runRenameColumn("cow")
  def testRenameColumnMor(): Unit = runRenameColumn("mor")

  private def runRenameColumn(tableType: String): Unit = {
    val tableName = s"ddl_rename_col_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      withConf(
        "hoodie.schema.on.read.enable" -> "true",
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true"
      ) {
        createBasicTable(tableName, tableType, extraTblProps = Map("hoodie.schema.on.read.enable" -> "true"))
        spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")
        spark.sql(s"ALTER TABLE $database.$tableName RENAME COLUMN name TO fullname")

        val catalogTable = getCatalogTable(tableName)
        val fields = HoodieSqlCommonUtils.removeMetaFields(catalogTable.schema).fields.map(_.name).toSet
        assert(fields.contains("fullname"),
          s"catalog schema should contain renamed column 'fullname', got: $fields")
        assert(!fields.contains("name"),
          s"catalog schema should not contain old name 'name', got: $fields")

        val rows = spark.sql(s"SELECT id, fullname FROM $database.$tableName WHERE id = 1").collect()
        assert(rows.length == 1, s"expected 1 row, got ${rows.length}")
        assert(rows(0).getString(1) == "a1",
          s"renamed column should retain prior value 'a1', got '${rows(0).getString(1)}'")
      }
    } finally cleanup(tableName, basePath)
  }

  /**
   * Without hoodie.schema.on.read.enable=true, RENAME COLUMN is not supported and must
   * raise a recognizable failure.
   */
  def testRenameColumnSchemaOnReadDisabledFailsCow(): Unit = {
    val tableName = "ddl_rename_col_no_sor_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      // schema-on-read disabled both at table level and session level
      withConf("hoodie.schema.on.read.enable" -> "false") {
        createBasicTable(tableName, "cow", extraTblProps = Map("hoodie.schema.on.read.enable" -> "false"))
        expectFailureAny(
          s"ALTER TABLE $database.$tableName RENAME COLUMN name TO fullname",
          Seq("schema.on.read", "schema on read", "not supported", "Unsupported", "v2 tables"))
      }
    } finally cleanup(tableName, basePath)
  }

  // ===========================================================================
  // ALTER TABLE RENAME TO — AlterHoodieTableRenameCommand
  // ===========================================================================

  def testRenameTableCow(): Unit = runRenameTableExternal("cow")
  def testRenameTableMor(): Unit = runRenameTableExternal("mor")

  private def runRenameTableExternal(tableType: String): Unit = {
    val tableName = s"ddl_rename_to_$tableType"
    val newName = s"${tableName}_renamed"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    cleanup(newName, basePath)
    try {
      createBasicTable(tableName, tableType)
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")

      spark.sql(s"ALTER TABLE $database.$tableName RENAME TO $database.$newName")

      assert(!spark.sessionState.catalog.tableExists(new TableIdentifier(tableName, Some(database))),
        s"old table $tableName should not exist in HMS")
      assert(spark.sessionState.catalog.tableExists(new TableIdentifier(newName, Some(database))),
        s"new table $newName should exist in HMS")

      val metaClient = openMetaClient(basePath)
      assert(metaClient.getTableConfig.getTableName == newName,
        s"meta-client tableName should be '$newName', got '${metaClient.getTableConfig.getTableName}'")

      val rows = spark.sql(s"SELECT id, name FROM $database.$newName").collect()
      assert(rows.length == 1, s"renamed table should still have 1 row, got ${rows.length}")
    } finally {
      cleanup(newName, basePath)
      cleanup(tableName, basePath)
    }
  }

  def testRenameTableManagedNoLocationCow(): Unit = runRenameTableManaged("cow")
  def testRenameTableManagedNoLocationMor(): Unit = runRenameTableManaged("mor")

  private def runRenameTableManaged(tableType: String): Unit = {
    val tableName = s"ddl_rename_managed_$tableType"
    val newName = s"${tableName}_renamed"
    cleanup(tableName, getBasePath(tableName))
    cleanup(newName, getBasePath(newName))
    try {
      // Managed table — no LOCATION clause. HMS picks the warehouse path.
      spark.sql(
        s"""
           |CREATE TABLE $database.$tableName (
           |  id INT, name STRING, price DOUBLE, ts BIGINT
           |) USING hudi
           |TBLPROPERTIES (
           |  type = '$tableType',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |""".stripMargin)
      spark.sql(s"ALTER TABLE $database.$tableName RENAME TO $database.$newName")
      assert(!spark.sessionState.catalog.tableExists(new TableIdentifier(tableName, Some(database))),
        s"old managed table $tableName should not exist after rename")
      assert(spark.sessionState.catalog.tableExists(new TableIdentifier(newName, Some(database))),
        s"new managed table $newName should exist after rename")
    } finally {
      // Drop using current registered name; either may be present depending on test outcome
      Try(spark.sql(s"DROP TABLE IF EXISTS $database.$tableName"))
      Try(spark.sql(s"DROP TABLE IF EXISTS $database.$newName"))
    }
  }

  def testRenameTableExternalLocationPreservedCow(): Unit = runRenameTableExternalLocationPreserved("cow")
  def testRenameTableExternalLocationPreservedMor(): Unit = runRenameTableExternalLocationPreserved("mor")

  private def runRenameTableExternalLocationPreserved(tableType: String): Unit = {
    val tableName = s"ddl_rename_loc_$tableType"
    val newName = s"${tableName}_renamed"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    cleanup(newName, basePath)
    try {
      createBasicTable(tableName, tableType)
      val oldPath = getCatalogTable(tableName).location.toString

      spark.sql(s"ALTER TABLE $database.$tableName RENAME TO $database.$newName")

      val newPath = spark.sessionState.catalog.getTableMetadata(
        new TableIdentifier(newName, Some(database))).location.toString
      assert(oldPath == newPath,
        s"external table physical path should be preserved on rename; old='$oldPath' new='$newPath'")
    } finally {
      cleanup(newName, basePath)
      cleanup(tableName, basePath)
    }
  }

  // ===========================================================================
  // ALTER TABLE ADD PARTITION — AlterHoodieTableAddPartitionCommand
  // (Not sensitive to COW vs MOR; running variants on COW only.)
  // ===========================================================================

  def testAddPartitionOnNonPartitionedFailsCow(): Unit = {
    val tableName = "ddl_add_part_nonpart_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, dt STRING",
        tableType = "cow",
        partitionedBy = None)
      expectFailure(
        s"ALTER TABLE $database.$tableName ADD PARTITION (dt = '2023-08-01')",
        "is a non-partitioned table that is not allowed to add partition")
    } finally cleanup(tableName, basePath)
  }

  def testAddPartitionWithLocationFailsCow(): Unit = {
    val tableName = "ddl_add_part_loc_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, dt STRING",
        tableType = "cow",
        partitionedBy = Some("dt"))
      expectFailure(
        s"ALTER TABLE $database.$tableName ADD PARTITION (dt='2023-08-01') LOCATION '/tmp/path'",
        "Hoodie table does not support specify partition location explicitly")
    } finally cleanup(tableName, basePath)
  }

  def testAddPartitionIfNotExistsCow(): Unit = {
    val tableName = "ddl_add_part_ifne_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, dt STRING",
        tableType = "cow",
        partitionedBy = Some("dt"))
      spark.sql(s"ALTER TABLE $database.$tableName ADD PARTITION (dt='2023-08-01')")
      assertShowPartitions(tableName, Seq("dt=2023-08-01"))

      // IF NOT EXISTS is silent on duplicate
      spark.sql(s"ALTER TABLE $database.$tableName ADD IF NOT EXISTS PARTITION (dt='2023-08-01')")
      // Bare form raises
      expectFailure(
        s"ALTER TABLE $database.$tableName ADD PARTITION (dt='2023-08-01')",
        "Partition metadata already exists for path")
    } finally cleanup(tableName, basePath)
  }

  def testAddPartitionSinglePartitionHiveStyleTrueCow(): Unit =
    runAddPartitionSingleHiveStyle(hiveStyle = true)
  def testAddPartitionSinglePartitionHiveStyleFalseCow(): Unit =
    runAddPartitionSingleHiveStyle(hiveStyle = false)

  private def runAddPartitionSingleHiveStyle(hiveStyle: Boolean): Unit = {
    val tableName = s"ddl_add_part_single_hs_${hiveStyle}_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, dt STRING",
        tableType = "cow",
        partitionedBy = Some("dt"),
        extraTblProps = Map("hoodie.datasource.write.hive_style_partitioning" -> hiveStyle.toString))
      spark.sql(s"ALTER TABLE $database.$tableName ADD PARTITION (dt='2023-08-01')")
      val expected = if (hiveStyle) Seq("dt=2023-08-01") else Seq("2023-08-01")
      assertShowPartitions(tableName, expected)
    } finally cleanup(tableName, basePath)
  }

  def testAddPartitionMultiLevelHiveStyleTrueCow(): Unit =
    runAddPartitionMultiHiveStyle(hiveStyle = true)
  def testAddPartitionMultiLevelHiveStyleFalseCow(): Unit =
    runAddPartitionMultiHiveStyle(hiveStyle = false)

  private def runAddPartitionMultiHiveStyle(hiveStyle: Boolean): Unit = {
    val tableName = s"ddl_add_part_multi_hs_${hiveStyle}_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, year STRING, month STRING, day STRING",
        tableType = "cow",
        partitionedBy = Some("year, month, day"),
        extraTblProps = Map("hoodie.datasource.write.hive_style_partitioning" -> hiveStyle.toString))
      spark.sql(s"ALTER TABLE $database.$tableName ADD PARTITION (year='2023', month='08', day='01')")
      val expected = if (hiveStyle) Seq("year=2023/month=08/day=01") else Seq("2023/08/01")
      assertShowPartitions(tableName, expected)
    } finally cleanup(tableName, basePath)
  }

  def testAddPartitionUrlEncodeTrueCow(): Unit = runAddPartitionUrlEncode(urlEncode = true)
  def testAddPartitionUrlEncodeFalseCow(): Unit = runAddPartitionUrlEncode(urlEncode = false)

  private def runAddPartitionUrlEncode(urlEncode: Boolean): Unit = {
    val tableName = s"ddl_add_part_urlenc_${urlEncode}_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, p_a STRING, p_b STRING",
        tableType = "cow",
        partitionedBy = Some("p_a, p_b"),
        extraTblProps = Map("hoodie.datasource.write.partitionpath.urlencode" -> urlEncode.toString))
      spark.sql(s"ALTER TABLE $database.$tableName ADD PARTITION (p_a='url%a', p_b='key=val')")
      val expected =
        if (urlEncode) Seq("p_a=url%25a/p_b=key%3Dval")
        else Seq("p_a=url%a/p_b=key=val")
      assertShowPartitions(tableName, expected)
    } finally cleanup(tableName, basePath)
  }

  // ===========================================================================
  // ALTER TABLE DROP PARTITION — AlterHoodieTableDropPartitionCommand
  // ===========================================================================

  def testDropPartitionOnNonPartitionedFailsCow(): Unit = runDropPartitionOnNonPartitionedFails("cow")
  def testDropPartitionOnNonPartitionedFailsMor(): Unit = runDropPartitionOnNonPartitionedFails("mor")

  private def runDropPartitionOnNonPartitionedFails(tableType: String): Unit = {
    val tableName = s"ddl_drop_part_nonpart_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, dt STRING",
        tableType = tableType,
        partitionedBy = None)
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a', 'v1', '2021-10-01')")
      expectFailure(
        s"ALTER TABLE $database.$tableName DROP PARTITION (dt='2021-10-01')",
        "is a non-partitioned table that is not allowed to drop partition")
    } finally cleanup(tableName, basePath)
  }

  def testDropSinglePartitionUrlEncodeTrueCow(): Unit = runDropSinglePartitionUrlEncode(urlEncode = true)
  def testDropSinglePartitionUrlEncodeFalseCow(): Unit = runDropSinglePartitionUrlEncode(urlEncode = false)

  private def runDropSinglePartitionUrlEncode(urlEncode: Boolean): Unit = {
    val tableName = s"ddl_drop_part_single_urlenc_${urlEncode}_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, dt STRING",
        tableType = "cow",
        partitionedBy = Some("dt"),
        extraTblProps = Map("hoodie.datasource.write.partitionpath.urlencode" -> urlEncode.toString))
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a', 'v1', '2021-10-01'), (2, 'b', 'v1', '2021-10-02')")
      spark.sql(s"ALTER TABLE $database.$tableName DROP PARTITION (dt='2021-10-01')")

      val rows = spark.sql(s"SELECT dt FROM $database.$tableName ORDER BY dt").collect()
      assert(rows.length == 1, s"expected 1 row after drop, got ${rows.length}")
      assert(rows(0).getString(0) == "2021-10-02",
        s"expected only 2021-10-02 partition to remain, got ${rows(0).getString(0)}")
    } finally cleanup(tableName, basePath)
  }

  def testDropMultiLevelPartitionHiveStyleTrueCow(): Unit =
    runDropMultiLevelHiveStyle(hiveStyle = true)
  def testDropMultiLevelPartitionHiveStyleFalseCow(): Unit =
    runDropMultiLevelHiveStyle(hiveStyle = false)

  private def runDropMultiLevelHiveStyle(hiveStyle: Boolean): Unit = {
    val tableName = s"ddl_drop_part_multi_hs_${hiveStyle}_cow"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id BIGINT, name STRING, ts STRING, year STRING, month STRING, day STRING",
        tableType = "cow",
        partitionedBy = Some("year, month, day"),
        extraTblProps = Map("hoodie.datasource.write.hive_style_partitioning" -> hiveStyle.toString))
      spark.sql(
        s"""
           |INSERT INTO $database.$tableName VALUES
           |  (1, 'a', 'v1', '2021', '10', '01'),
           |  (2, 'b', 'v1', '2021', '10', '02')
           |""".stripMargin)

      // Not specifying all partition columns must fail
      expectFailure(
        s"ALTER TABLE $database.$tableName DROP PARTITION (year='2021', month='10')",
        "All partition columns need to be specified for Hoodie's partition")

      // Specifying all keys succeeds
      spark.sql(s"ALTER TABLE $database.$tableName DROP PARTITION (year='2021', month='10', day='01')")
      val rows = spark.sql(s"SELECT id, year, month, day FROM $database.$tableName ORDER BY id").collect()
      assert(rows.length == 1, s"expected 1 surviving row, got ${rows.length}")
      assert(rows(0).getLong(0) == 2L, s"expected id=2 to remain, got ${rows(0).getLong(0)}")
    } finally cleanup(tableName, basePath)
  }

  def testDropPartitionWildcardsCow(): Unit = runDropPartitionWildcards("cow")
  def testDropPartitionWildcardsMor(): Unit = runDropPartitionWildcards("mor")

  private def runDropPartitionWildcards(tableType: String): Unit = {
    val tableName = s"ddl_drop_part_wild_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT, partition_date_col STRING",
        tableType = tableType,
        partitionedBy = Some("partition_date_col"))
      spark.sql(
        s"""
           |INSERT INTO $database.$tableName VALUES
           |  (1, 'a1', 10.0, 1000, '2023-08-01'),
           |  (2, 'a2', 10.0, 1000, '2023-08-02'),
           |  (3, 'a3', 10.0, 1000, '2023-09-01')
           |""".stripMargin)
      spark.sql(s"ALTER TABLE $database.$tableName DROP PARTITION (partition_date_col='2023-08-*')")

      val rows = spark.sql(
        s"SELECT DISTINCT partition_date_col FROM $database.$tableName ORDER BY partition_date_col").collect()
      assert(rows.length == 1, s"expected only 2023-09-01 to remain, got ${rows.length} partitions")
      assert(rows(0).getString(0) == "2023-09-01",
        s"expected '2023-09-01', got '${rows(0).getString(0)}'")
    } finally cleanup(tableName, basePath)
  }

  def testDropPartitionBlockedByPendingClusteringCow(): Unit = runDropPartitionBlockedByClustering("cow")
  def testDropPartitionBlockedByPendingClusteringMor(): Unit = runDropPartitionBlockedByClustering("mor")

  private def runDropPartitionBlockedByClustering(tableType: String): Unit = {
    val tableName = s"ddl_drop_part_blk_clust_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT",
        tableType = tableType,
        partitionedBy = Some("ts"))
      spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")
      spark.sql(s"INSERT INTO $database.$tableName VALUES (2, 'a2', 10.0, 1001)")
      spark.sql(s"INSERT INTO $database.$tableName VALUES (3, 'a3', 10.0, 1002)")

      val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, scala.Option(s"$database.$tableName"))
      try {
        val instant = client.scheduleClustering(HOption.empty()).get()
        log.info(s"Scheduled clustering instant: $instant")
        expectFailure(
          s"ALTER TABLE $database.$tableName DROP PARTITION (ts=1002)",
          "Failed to drop partitions")
      } finally client.close()
    } finally cleanup(tableName, basePath)
  }

  def testDropPartitionBlockedByPendingCompactionMor(): Unit = {
    val tableName = "ddl_drop_part_blk_compact_mor"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT",
        tableType = "mor",
        partitionedBy = Some("ts"),
        extraTblProps = Map("hoodie.index.type" -> "INMEMORY"))

      withConf("hoodie.compact.inline" -> "false", "hoodie.compact.schedule.inline" -> "false") {
        spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (2, 'a2', 10.0, 1001)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (3, 'a3', 10.0, 1002)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (4, 'a4', 10.0, 1003)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (5, 'a5', 10.0, 1004)")

        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, scala.Option(s"$database.$tableName"))
        try {
          val instant = client.scheduleCompaction(HOption.empty())
          assert(instant.isPresent, "compaction plan should be scheduled")
          log.info(s"Scheduled compaction instant: ${instant.get()}")
          expectFailure(
            s"ALTER TABLE $database.$tableName DROP PARTITION (ts=1002)",
            "Failed to drop partitions")
        } finally client.close()
      }
    } finally cleanup(tableName, basePath)
  }

  def testDropPartitionBlockedByPendingLogCompactionMor(): Unit = {
    val tableName = "ddl_drop_part_blk_logcompact_mor"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT",
        tableType = "mor",
        partitionedBy = Some("ts"),
        extraTblProps = Map("hoodie.index.type" -> "INMEMORY"))

      withConf("hoodie.compact.inline" -> "false", "hoodie.compact.schedule.inline" -> "false") {
        // Same partition each time so log-compaction has multiple log blocks for one file group
        spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'a1', 10.0, 1000)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (2, 'a2', 10.0, 1000)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (3, 'a3', 10.0, 1000)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (4, 'a4', 10.0, 1000)")
        spark.sql(s"INSERT INTO $database.$tableName VALUES (5, 'a5', 10.0, 1000)")

        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, scala.Option(s"$database.$tableName"))
        try {
          val instant = client.scheduleLogCompaction(HOption.empty())
          assert(instant.isPresent, "log-compaction plan should be scheduled")
          log.info(s"Scheduled log-compaction instant: ${instant.get()}")
          expectFailure(
            s"ALTER TABLE $database.$tableName DROP PARTITION (ts=1000)",
            "Failed to drop partitions")
        } finally client.close()
      }
    } finally cleanup(tableName, basePath)
  }

  def testDropPartitionPreservesSchemaInCommitMetadataCow(): Unit =
    runDropPartitionPreservesSchema("cow")
  def testDropPartitionPreservesSchemaInCommitMetadataMor(): Unit =
    runDropPartitionPreservesSchema("mor")

  private def runDropPartitionPreservesSchema(tableType: String): Unit = {
    val tableName = s"ddl_drop_part_schema_$tableType"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    try {
      createTable(
        tableName,
        columns = "id INT, name STRING, price DOUBLE, ts BIGINT, dt STRING",
        tableType = tableType,
        partitionedBy = Some("dt"))
      spark.sql(
        s"""
           |INSERT INTO $database.$tableName VALUES
           |  (1, 'a1', 10.0, 1000, '01'),
           |  (2, 'a2', 10.0, 1000, '02'),
           |  (3, 'a3', 10.0, 1000, '03')
           |""".stripMargin)

      spark.sql(s"ALTER TABLE $database.$tableName DROP PARTITION (dt='01')")

      val metaClient = openMetaClient(basePath)
      val replaceTimeline = metaClient.getActiveTimeline.getCompletedReplaceTimeline
      val lastInstant = replaceTimeline.lastInstant().get()
      val commitMetadata = metaClient.getActiveTimeline.readCommitMetadata(lastInstant)
      val schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY)
      assert(schemaStr != null && schemaStr.nonEmpty, "drop-partition replace-commit must persist a schema")
      val schema = HoodieSchema.parse(schemaStr)
      val fields = schema.getFields.asScala.map(_.name()).toSet
      val expected = Set("id", "name", "price", "ts", "dt")
      assert(fields == expected,
        s"replace-commit schema should be exactly user fields $expected, got $fields")
      val leakedMeta = fields.intersect(Set("_hoodie_commit_time", "_hoodie_record_key", "_hoodie_partition_path"))
      assert(leakedMeta.isEmpty,
        s"replace-commit schema must not include hoodie meta fields, found $leakedMeta")
    } finally cleanup(tableName, basePath)
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

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

  private def createBasicTable(tableName: String, tableType: String,
                               extraTblProps: Map[String, String] = Map.empty): Unit = {
    createTable(
      tableName,
      columns = "id INT, name STRING, price DOUBLE, ts BIGINT",
      tableType = tableType,
      partitionedBy = None,
      extraTblProps = extraTblProps)
  }

  private def createTable(tableName: String,
                          columns: String,
                          tableType: String,
                          partitionedBy: Option[String] = None,
                          extraTblProps: Map[String, String] = Map.empty): Unit = {
    val basePath = getBasePath(tableName)
    val baseProps = Map(
      "type" -> tableType,
      "primaryKey" -> "id",
      "preCombineField" -> "ts"
    )
    val mergedProps = baseProps ++ extraTblProps
    val tblPropsClause = mergedProps.map { case (k, v) => s"  '$k' = '$v'" }.mkString(",\n")
    val partitionClause = partitionedBy.map(p => s"PARTITIONED BY ($p)").getOrElse("")
    val ddl =
      s"""
         |CREATE TABLE $database.$tableName (
         |  $columns
         |) USING hudi
         |TBLPROPERTIES (
         |$tblPropsClause
         |)
         |$partitionClause
         |LOCATION '$basePath'
         |""".stripMargin
    log.info(s"Creating table $database.$tableName:\n$ddl")
    spark.sql(ddl)
  }

  private def getCatalogTable(tableName: String): org.apache.spark.sql.catalyst.catalog.CatalogTable = {
    spark.sessionState.catalog.refreshTable(new TableIdentifier(tableName, Some(database)))
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Some(database)))
  }

  private def openMetaClient(basePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
  }

  private def assertEqualsSeq(expected: Seq[String], actual: Seq[String], context: String): Unit = {
    assert(expected == actual,
      s"$context mismatch: expected=${expected.mkString("[", ",", "]")} actual=${actual.mkString("[", ",", "]")}")
  }

  /**
   * Run `body` with the given Spark conf entries set; restore prior values (or unset) on exit.
   * Replaces the unit-suite `withSQLConf` helper which isn't available in the integ runner.
   */
  private def withConf[T](kvs: (String, String)*)(body: => T): T = {
    val prev = kvs.map { case (k, _) => k -> Try(spark.conf.get(k)).toOption }
    kvs.foreach { case (k, v) => spark.conf.set(k, v) }
    try body
    finally prev.foreach {
      case (k, Some(v)) => spark.conf.set(k, v)
      case (k, None)    => Try(spark.conf.unset(k))
    }
  }

  /** Asserts `sql` fails and the exception chain contains `msgContains`. */
  private def expectFailure(sql: String, msgContains: String): Unit = {
    expectFailureAny(sql, Seq(msgContains))
  }

  /** Asserts `sql` fails and the exception chain contains at least one of `candidates`. */
  private def expectFailureAny(sql: String, candidates: Seq[String]): Unit = {
    val ex: Throwable =
      try { spark.sql(sql); null }
      catch { case e: Throwable => e }
    if (ex == null) {
      throw new AssertionError(s"Expected failure for SQL: $sql")
    }
    val full = Iterator
      .iterate[Throwable](ex)(t => t.getCause)
      .takeWhile(_ != null)
      .map(t => Option(t.getMessage).getOrElse(""))
      .mkString(" | ")
    if (!candidates.exists(full.contains)) {
      throw new AssertionError(
        s"Expected message to contain one of ${candidates.mkString("[", ",", "]")} for SQL [$sql], got: $full")
    }
  }
}
