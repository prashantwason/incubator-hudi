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

import org.apache.hudi.{DataSourceWriteOptions, QuickstartUtils}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, HiveSyncConfigHolder}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.spark.sql.{Encoders, SaveMode}
import org.slf4j.LoggerFactory

import java.util.{Collections, Properties}

import scala.collection.JavaConverters._

/**
 * End-to-end integration tests for backwards-compatibility with legacy Hudi tables created via
 * Hudi 0.14 SparkSQL `CREATE TABLE ... USING HUDI` against a Hudi 1.2 DataSource writer (e.g.
 * hudi-pyspark / hudi_writer.py). The production failure that motivates this class:
 *
 *   - pre_promote_task creates the table via SparkSQL `CREATE EXTERNAL TABLE db.t (...) USING HUDI ...`
 *     without `'primaryKey'` in TBLPROPERTIES. HoodieCatalogTable persists `hoodie.table.name=t`
 *     (bare) + `hoodie.DEFAULT_DATABASE.name=db` separately, and leaves `hoodie.table.recordkey.fields` unset.
 *   - The hudi_writer.py job then runs `df.write.format("hudi")` with `hoodie.table.name = "db.t"`
 *     (qualified, the legacy hudi-pyspark drogon convention) and `hoodie.datasource.write.recordkey.field`.
 *   - Pre-fix, this hit two writer-side validators in 1.2:
 *       1. HoodieWriterUtils.validateTableConfig → `Config conflict: hoodie.table.name: db.t  t`
 *       2. HoodieSparkSqlWriter.handleSaveModes  → `hoodie table with name t already exists ...
 *          can not append data to the table with another name db.t`
 *     Plus, when 'primaryKey' was missing on disk, validateTableConfig also threw
 *     `Config conflict: RecordKey: <writer> null`.
 *
 * The fixes (HoodieWriterUtils.shouldIgnoreConfig carve-out for NAME, handleSaveModes
 * normalization, and HoodieTableConfig.update-based recordkey backfill) all live on the writer
 * side and require no caller changes. These tests assert that running the legacy hudi-pyspark
 * shape against an existing Hudi-0.14-DDL-shaped table now succeeds end-to-end.
 *
 * Setup uses SparkSQL `CREATE TABLE ... USING HUDI` because the DataSource writer stores
 * whatever `hoodie.table.name` is supplied (e.g., it would store the qualified form),
 * whereas HoodieCatalogTable splits the qualified identifier and writes the canonical
 * (bare name + separate DEFAULT_DATABASE) shape — which is the actual on-disk shape produced by
 * the failing production pipeline.
 */
class RunLegacyTableCompatTests extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)
  lazy val DEFAULT_DATABASE: String = getDatabase()

  /** Build a metaClient for the table at basePath (used to inspect or modify on-disk hoodie.properties). */
  private def buildMetaClient(basePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
  }

  /**
   * Create the Hudi table via SparkSQL `CREATE EXTERNAL TABLE ... USING HUDI` so that
   * HoodieCatalogTable produces the canonical on-disk shape: bare `hoodie.table.name`
   * + separate `hoodie.DEFAULT_DATABASE.name`. When `includePrimaryKey` is false the DDL omits
   * `'primaryKey'`, matching the legacy Hudi-0.14 SparkSQL shape that leaves
   * `hoodie.table.recordkey.fields` unset on disk.
   */
  private def createHudiTableViaSql(tableName: String, basePath: String, includePrimaryKey: Boolean): Unit = {
    val tblProps = if (includePrimaryKey) {
      "TBLPROPERTIES ('primaryKey' = 'uuid', 'preCombineField' = 'ts')"
    } else {
      // Intentionally no primaryKey — mimics the legacy 0.14 DDL that produced the failure.
      "TBLPROPERTIES ('govern' = 'false')"
    }
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE $DEFAULT_DATABASE.$tableName (
         |  uuid STRING,
         |  ts TIMESTAMP,
         |  rider STRING,
         |  driver STRING,
         |  begin_lat DOUBLE,
         |  begin_lon DOUBLE,
         |  end_lat DOUBLE,
         |  end_lon DOUBLE,
         |  fare DOUBLE
         |)
         |USING HUDI
         |PARTITIONED BY (partitionpath STRING)
         |LOCATION '$basePath'
         |$tblProps
         |""".stripMargin)
  }

  /**
   * Append rows via the DataSource API with `hoodie.table.name` supplied in the qualified
   * "<db>.<table>" form (the legacy hudi-pyspark convention). This is the call path that
   * mimics the failing hudi_writer.py job. Hive sync is enabled so partitions register with
   * HMS — the integ-test runner sets `spark.hoodie.datasource.read.file.index.list.partitions.from.catalog=true`,
   * so reads consult Hive for the partition list and an unsynced write reads back as 0 rows.
   */
  private def appendViaDataSource(tableName: String, basePath: String, recordKeyField: String): Unit = {
    val records = QuickstartUtils.convertToStringList(dataGen.generateInserts(20))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(spark.createDataset(recordsRDD)(Encoders.STRING))
    inputDF.write.format("hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      // Qualified "db.table" form — the legacy hudi-pyspark convention that triggered
      // the production failure.
      .option(HoodieTableConfig.NAME.key(), s"$DEFAULT_DATABASE.$tableName")
      .option(RECORDKEY_FIELD.key(), recordKeyField)
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true")
      // Hive sync options — required so the read path (which reads partitions from HMS)
      // sees the partitions written by this Append. Mirrors the existing
      // RunOperationsBase.writeToHudiTable convention.
      .option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key(), "true")
      .option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), HiveSyncMode.HMS.name())
      .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), DEFAULT_DATABASE)
      .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), tableName)
      .option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "partitionpath")
      .option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
        classOf[HiveStylePartitionValueExtractor].getName)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  /**
   * Fix 1 + handleSaveModes — table created via SparkSQL `CREATE TABLE ... USING HUDI` has
   * bare `hoodie.table.name` + separate `hoodie.DEFAULT_DATABASE.name` on disk. A subsequent
   * DataSource Append that supplies the qualified `db.table` form for `hoodie.table.name`
   * must succeed (Fix 1 carve-out in HoodieWriterUtils.shouldIgnoreConfig + the normalized
   * comparison in HoodieSparkSqlWriter.handleSaveModes).
   */
  def testAppendWithQualifiedTableNameSucceeds(): Unit = {
    val tableName = "legacy_qualified_name_compat"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    try {
      createHudiTableViaSql(tableName, basePath, includePrimaryKey = true)

      val initialTableConfig = buildMetaClient(basePath).getTableConfig
      val onDiskRawName = initialTableConfig.getString(HoodieTableConfig.NAME)
      val onDiskRawDb = initialTableConfig.getString(HoodieTableConfig.DATABASE_NAME)
      log.info(s"On-disk after CREATE TABLE — hoodie.table.name=$onDiskRawName, hoodie.DEFAULT_DATABASE.name=$onDiskRawDb")
      assert(onDiskRawName == tableName,
        s"on-disk hoodie.table.name should be the bare table name '$tableName' (HoodieCatalogTable shape), was: $onDiskRawName")
      assert(onDiskRawDb == DEFAULT_DATABASE,
        s"on-disk hoodie.DEFAULT_DATABASE.name should be '$DEFAULT_DATABASE' (HoodieCatalogTable shape), was: $onDiskRawDb")

      // Append with qualified "<db>.<table>" form — pre-fix this would throw either
      // `Config conflict: hoodie.table.name: db.t  t` (validateTableConfig) or
      // `hoodie table with name t ... can not append data ... with another name db.t`
      // (handleSaveModes), depending on which validator fired first.
      appendViaDataSource(tableName, basePath, recordKeyField = "uuid")
      runDataFrameReaderWithAsserts(DEFAULT_DATABASE, tableName, expectedVal = 20)
      log.info("PASSED: qualified hoodie.table.name accepted on Append against bare on-disk shape")
    } finally {
      cleanup(tableName, basePath)
    }
  }

  /**
   * Fix 2a + 2b — table created via Hudi 0.14 SparkSQL `CREATE TABLE ... USING HUDI` without
   * `'primaryKey'` leaves `hoodie.table.recordkey.fields` unset on disk. A DataSource Append
   * that supplies a recordkey at write time must succeed (Fix 2a WARN) AND backfill the
   * recordkey to on-disk hoodie.properties via HoodieTableConfig.update (Fix 2b).
   */
  def testAppendWithMissingOnDiskRecordKeyBackfills(): Unit = {
    val tableName = "legacy_missing_recordkey_compat"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    try {
      createHudiTableViaSql(tableName, basePath, includePrimaryKey = false)

      val seedTableConfig = buildMetaClient(basePath).getTableConfig
      assert(seedTableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS) == null,
        "test precondition: hoodie.table.recordkey.fields must be unset on disk after a CREATE TABLE without 'primaryKey'")
      assert(seedTableConfig.getInt(HoodieTableConfig.VERSION) > 1,
        "test precondition: table version must be > 1 for the recordkey check to fire")
      log.info(s"On-disk after CREATE TABLE — hoodie.table.recordkey.fields=${seedTableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS)}, " +
        s"hoodie.table.version=${seedTableConfig.getInt(HoodieTableConfig.VERSION)}")

      // Pre-fix this throws `Config conflict: RecordKey: uuid  null` from validateTableConfig.
      // With Fix 2a + 2b, the writer succeeds and the recordkey is persisted to disk.
      appendViaDataSource(tableName, basePath, recordKeyField = "uuid")

      val backfilledTableConfig = buildMetaClient(basePath).getTableConfig
      val persistedRecordKey = backfilledTableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS)
      assert(persistedRecordKey == "uuid",
        s"hoodie.table.recordkey.fields should have been backfilled to 'uuid' on disk, was: $persistedRecordKey")
      log.info(s"PASSED: backfilled hoodie.table.recordkey.fields=$persistedRecordKey on disk")

      runDataFrameReaderWithAsserts(DEFAULT_DATABASE, tableName, expectedVal = 20)
    } finally {
      cleanup(tableName, basePath)
    }
  }

  /**
   * Combined scenario matching the original production failure
   * (ONE_ETL_hudi_regression_hudi_writer_partition_upsert_staging): SparkSQL `CREATE TABLE
   * ... USING HUDI` without `'primaryKey'`, then DataSource Append with both qualified
   * `hoodie.table.name` AND a recordkey that is unset on disk. All three fix points must
   * cooperate (Fix 1 + handleSaveModes + Fix 2a + Fix 2b).
   */
  def testAppendWithQualifiedNameAndMissingRecordKeyBothApply(): Unit = {
    val tableName = "legacy_combined_compat"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    try {
      createHudiTableViaSql(tableName, basePath, includePrimaryKey = false)
      appendViaDataSource(tableName, basePath, recordKeyField = "uuid")

      val finalTableConfig = buildMetaClient(basePath).getTableConfig
      assert(finalTableConfig.getString(HoodieTableConfig.NAME) == tableName,
        s"on-disk hoodie.table.name should remain bare '$tableName', was: ${finalTableConfig.getString(HoodieTableConfig.NAME)}")
      assert(finalTableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS) == "uuid",
        s"hoodie.table.recordkey.fields should have been backfilled to 'uuid', was: ${finalTableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS)}")
      runDataFrameReaderWithAsserts(DEFAULT_DATABASE, tableName, expectedVal = 20)
      log.info("PASSED: combined qualified-name + missing-recordkey legacy compat")
    } finally {
      cleanup(tableName, basePath)
    }
  }

  /**
   * handleSaveModes — legacy-fixture variant. The other tests reach the qualified-name branch
   * by *passing* the qualified `hoodie.table.name` at write time against a 1.x-shaped on-disk
   * file (NAME=bare, DEFAULT_DATABASE_NAME=set). This test instead mutates the on-disk
   * `.hoodie/hoodie.properties` to the actual 0.14 shape (NAME=db.table, no DEFAULT_DATABASE_NAME)
   * before the Append, so `tableConfig.getTableName()` returns the bare form via the
   * HoodieTableConfig workaround and `handleSaveModes` is the validator that fires.
   *
   * Without the strip-prefix fix in `handleSaveModes`, this Append throws
   * "hoodie table with name <bare> already exists ... can not append data ... with another
   * name <db>.<bare>" — the failure shape seen on tables that were originally created by
   * Hudi 0.14 (rather than created by 1.x and written-to with a qualified name).
   */
  def testAppendOnPreSeededLegacyProperties(): Unit = {
    val tableName = "legacy_seeded_compat"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    try {
      createHudiTableViaSql(tableName, basePath, includePrimaryKey = true)
      rewriteHoodiePropertiesAsLegacy(basePath, qualifiedName = s"$DEFAULT_DATABASE.$tableName")

      val mutatedTableConfig = buildMetaClient(basePath).getTableConfig
      val rawName = mutatedTableConfig.getString(HoodieTableConfig.NAME)
      val rawDb = mutatedTableConfig.getString(HoodieTableConfig.DATABASE_NAME)
      log.info(s"On-disk after mutation — hoodie.table.name=$rawName, hoodie.DEFAULT_DATABASE.name=$rawDb")
      assert(rawName == s"$DEFAULT_DATABASE.$tableName",
        s"on-disk hoodie.table.name should be the qualified form '$DEFAULT_DATABASE.$tableName', was: $rawName")
      assert(rawDb == null || rawDb.isEmpty,
        s"on-disk hoodie.DEFAULT_DATABASE.name should be unset (legacy 0.14 shape), was: $rawDb")

      appendViaDataSource(tableName, basePath, recordKeyField = "uuid")
      runDataFrameReaderWithAsserts(DEFAULT_DATABASE, tableName, expectedVal = 20)
      log.info("PASSED: Append succeeded against pre-seeded legacy hoodie.properties shape")
    } finally {
      cleanup(tableName, basePath)
    }
  }

  /**
   * Mutate `<basePath>/.hoodie/hoodie.properties` to the on-disk shape Hudi 0.14 wrote:
   * `hoodie.table.name=<db>.<table>` (qualified) and no `hoodie.DEFAULT_DATABASE.name`. The checksum
   * is recomputed by `HoodieTableConfig.updateAndDeleteProps`, so the file remains internally
   * consistent for subsequent reads.
   */
  private def rewriteHoodiePropertiesAsLegacy(basePath: String, qualifiedName: String): Unit = {
    val metaClient = buildMetaClient(basePath)
    val updates = new Properties()
    updates.setProperty(HoodieTableConfig.NAME.key(), qualifiedName)
    val deletes = Collections.singleton(HoodieTableConfig.DATABASE_NAME.key())
    HoodieTableConfig.updateAndDeleteProps(metaClient.getStorage, metaClient.getMetaPath, updates, deletes)
    log.info(s"Rewrote $basePath/.hoodie/hoodie.properties: NAME=$qualifiedName, removed DEFAULT_DATABASE_NAME")
  }
}
