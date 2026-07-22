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
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieTableMetadata

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

/**
 * Sanity integration tests guarding the "default table version = 6" behaviour.
 *
 * This build can write both table version 6 and table version 9, but version 6 is the configured
 * default in this fork (injected as the spark.hoodie.write.table.version session default). These
 * tests assert that common operations create v6 tables and never silently auto-upgrade them to v9 —
 * for both the data table and its metadata table (MDT), which are both Hudi tables and therefore
 * both carry a table version.
 *
 * None of these tests set `hoodie.write.table.version` explicitly: they deliberately rely on the
 * default so that a regression in the default (or an unexpected auto-upgrade) is caught here.
 */
class RunTableVersionSixTests extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Case 1: a single insert (one write) that registers the table via Hive sync must not upgrade
   * the table version. The write uses HMS Hive sync inline (see RunOperationsBase.writeToHudiTable),
   * and we additionally drive the `hive_sync` procedure as an explicit registration step.
   */
  def testSingleInsertThenHiveSyncStaysOnVersionSix(): Unit = {
    val database = getDatabase()
    val tableName = "tv6_single_insert_hive_sync"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    // Single write; inline HMS Hive sync registers the table in the metastore.
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)

    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = false)
    log.info("testSingleInsertThenHiveSyncStaysOnVersionSix passed")
  }

  /**
   * Case 2: insert followed by upserts with the record-level index enabled must not upgrade the
   * table version. The record index lives inside the metadata table, so this asserts both the data
   * table and the MDT remain on version 6.
   */
  def testInsertUpsertWithRecordIndexStaysOnVersionSix(): Unit = {
    val database = getDatabase()
    val tableName = "tv6_insert_upsert_record_index"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    // Create insert should use table version 6.
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = true)

    // Now execute create record_index procedure this should still keep the table version 6.
    spark.sql(s"CREATE INDEX record_index ON $database.$tableName (uuid)")
    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = true)

    // Do one upsert.
    createUpserts(database, tableName, SaveMode.Append, isHudiTable = true)
    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = true)
    log.info("testInsertUpsertWithRecordIndexStaysOnVersionSix passed")
  }

  /**
   * Case 3: creating a table with the new Spark SQL syntax must not auto-upgrade it to v9 — neither
   * at CREATE time nor after the first write.
   */
  def testCreateTableUsingNewSyntaxStaysOnVersionSix(): Unit = {
    val database = getDatabase()
    val tableName = "tv6_create_table_new_syntax"
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
    // Table is initialized at CREATE time; version must be 6.
    assertTableVersionIsSix(basePath, "data-table (after create)")

    // First write must not trigger an auto-upgrade either.
    spark.sql(s"INSERT INTO $database.$tableName VALUES (1, 'rider1', 10.0, 1000)")
    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = false)
    log.info("testCreateTableUsingNewSyntaxStaysOnVersionSix passed")
  }

  /**
   * Case 4: delete then re-create the metadata table and (re)build the record index. After this
   * MDT lifecycle, both the data table and the recreated metadata table must still be on version 6.
   *
   * Flow: insert (creates data table + MDT) -> simple insert -> delete_metadata_table ->
   * create_metadata_table -> CREATE INDEX record_index -> assert both tables on v6.
   */
  def testMetadataTableRecreateWithRecordIndexStaysOnVersionSix(): Unit = {
    val database = getDatabase()
    val tableName = "tv6_mdt_recreate_record_index"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    // First write creates the data table and the metadata table (MDT enabled by default).
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    // A simple additional insert; deliberately do NOT clear the MDT here.
    createInserts(database, tableName, SaveMode.Append, isHudiTable = true)
    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = true)

    // Delete the metadata table using the delete procedure.
    spark.sql(s"call delete_metadata_table(table => '$database.$tableName')").show(false)
    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    assert(!metadataTableExists(basePath),
      s"Metadata table should be absent after delete_metadata_table at $mdtBasePath")

    // Re-create the metadata table using the create procedure.
    spark.sql(s"call create_metadata_table(table => '$database.$tableName')").show(false)

    // Build the record index explicitly.
    spark.sql(s"CREATE INDEX record_index ON $database.$tableName (uuid)")

    // Both the data table and the recreated MDT must still be on version 6.
    assertDataAndMetadataTableVersionIsSix(basePath, requireMetadataTable = true)
    log.info("testMetadataTableRecreateWithRecordIndexStaysOnVersionSix passed")
  }

  // -------- common version-verification utilities --------

  /**
   * Common utility: assert that the Hudi table rooted at `basePath` is on table version SIX,
   * including its timeline layout version. Works for any Hudi table — the data table and the
   * metadata table both have their own hoodie.properties carrying a table version. Table version 6
   * uses timeline layout version 1, whereas table versions 8/9 use layout version 2, so the layout
   * check catches an upgrade even if the version code somehow lagged.
   */
  private def assertTableVersionIsSix(basePath: String, label: String): Unit = {
    val storageConf = HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration)
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf)
      .build()
    val version = metaClient.getTableConfig.getTableVersion
    assert(version == HoodieTableVersion.SIX,
      s"[$label] expected table version SIX (6) but got $version (code ${version.versionCode()}) at $basePath")
    val layoutVersion: Int = metaClient.getTimelineLayoutVersion.getVersion
    assert(layoutVersion == TimelineLayoutVersion.VERSION_1.intValue(),
      s"[$label] expected timeline layout version 1 but got $layoutVersion at $basePath")
    log.info(s"[$label] verified table version is SIX and timeline layout is 1 at $basePath")
  }

  /**
   * Common utility: assert the data table at `basePath` is on version SIX and, when present (or
   * required), that its metadata table is also on version SIX.
   */
  private def assertDataAndMetadataTableVersionIsSix(basePath: String, requireMetadataTable: Boolean): Unit = {
    assertTableVersionIsSix(basePath, "data-table")

    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    if (metadataTableExists(basePath)) {
      assertTableVersionIsSix(mdtBasePath, "metadata-table")
    } else if (requireMetadataTable) {
      throw new AssertionError(s"Expected metadata table to exist at $mdtBasePath but it was not found")
    } else {
      log.info(s"Metadata table not present at $mdtBasePath; skipping MDT version check")
    }
  }

  private def metadataTableExists(basePath: String): Boolean = {
    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val mdtPath = new Path(mdtBasePath)
    val fs = mdtPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(mdtPath)
  }
}
