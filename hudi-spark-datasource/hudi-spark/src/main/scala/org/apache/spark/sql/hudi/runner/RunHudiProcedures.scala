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

import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import java.util.Properties

class RunHudiProcedures extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testHudiShowCommitsProcedure(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_show_commits_procedure"

    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying more inserts on existing table")
    for (i <- 1 to 2) {
      createInserts(database, tableName, SaveMode.Append, isHudiTable = true)
      log.info(s"Insert iteration $i done")
    }
    log.info("Inserts done")
    spark.sql(s"call show_commits(table => '$database.$tableName', limit => 10);").show()
    log.info("Show commits procedure test executed successfully")
  }

  def testHudiCleanProcedure(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_run_clean_procedure"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying upserts on existing table")
    for (i <- 1 to 5) {
      createUpserts(database, tableName, SaveMode.Append, isHudiTable = true)
      log.info(s"Upsert done with iteration $i")
    }
    spark.sql(s"call run_clean(table => '$database.$tableName', retain_commits => 1)").show()
    log.info("Successfully executed clean test case")
  }

  def testHudiArchivalProcedure(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_run_archival_procedure"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying upserts on existing table")
    for (i <- 1 to 5) {
      createUpserts(database, tableName, SaveMode.Append, isHudiTable = true)
      log.info(s"Upsert done with iteration $i")
    }
    spark.sql(s"call archive_commits(table => '$database.$tableName'" +
      s", min_commits => 2, max_commits => 3, retain_commits => 1, enable_metadata => false)").show()
    spark.sql(s"call show_commits(table => '$database.$tableName', limit => 10);").show()
    log.info("Successfully executed archival test case")
  }

  def testHudiClusteringProcedure(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_run_clustering_procedure"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying more inserts on existing table")
    for (i <- 1 to 2) {
      createInserts(database, tableName, SaveMode.Append, isHudiTable = true)
      log.info(s"Insert iteration $i done")
    }
    log.info("Inserts done")
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 60)
    spark.sql(s"call run_clustering(table => '$database.$tableName', order => 'ts', show_involved_partition => true)").show()
    spark.sql(s"call show_commits(table => '$database.$tableName', limit => 10);").show()
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 60)
    log.info("Clustering procedure test executed successfully")
  }

  def testHudiUpgradeOrDowngradeProcedure(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_trips_cow_test_upgrade_downgrade_procedure"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)

    val storageConf = HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration)
    var metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val initialVersion = metaClient.getTableConfig.getTableVersion
    log.info(s"Initial table version: $initialVersion")
    assert(initialVersion == HoodieTableVersion.current(),
      s"Expected initial table version ${HoodieTableVersion.current()} but got $initialVersion")

    // Downgrade to the previous major (EIGHT == 1.0).
    val downgradeRows = spark.sql(
      s"call downgrade_table(table => '$database.$tableName', to_version => 'EIGHT')").collect()
    assert(downgradeRows.length == 1 && downgradeRows(0).getBoolean(0),
      s"downgrade_table did not return success row, got ${downgradeRows.toSeq}")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assert(metaClient.getTableConfig.getTableVersion == HoodieTableVersion.EIGHT,
      s"Expected table version EIGHT after downgrade but got ${metaClient.getTableConfig.getTableVersion}")

    // Upgrade back to the current version.
    val targetVersion = HoodieTableVersion.current().name()
    val upgradeRows = spark.sql(
      s"call upgrade_table(table => '$database.$tableName', to_version => '$targetVersion')").collect()
    assert(upgradeRows.length == 1 && upgradeRows(0).getBoolean(0),
      s"upgrade_table did not return success row, got ${upgradeRows.toSeq}")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assert(metaClient.getTableConfig.getTableVersion == HoodieTableVersion.current(),
      s"Expected table version ${HoodieTableVersion.current()} after upgrade but got ${metaClient.getTableConfig.getTableVersion}")
    log.info("Upgrade/Downgrade procedure test executed successfully")
  }

  def testCompactionProcedure(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_trips_mor_test_run_compaction_procedure"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    // Disable inline compaction so the run_compaction procedure drives it explicitly.
    spark.conf.set("hoodie.compact.inline", "false")
    spark.conf.set("hoodie.compact.schedule.inline", "false")
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
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$basePath'
           |""".stripMargin)

      spark.sql(
        s"""INSERT INTO $database.$tableName VALUES
           |(1, 'a1', 10.0, 1000),
           |(2, 'a2', 20.0, 2000)""".stripMargin)
      // Updates against MOR generate log files that compaction will roll up. Produce enough delta
      // commits (1 insert + 5 updates = 6) to cross the default scheduling threshold of 5.
      for (i <- 1 to 5) {
        spark.sql(s"UPDATE $database.$tableName SET price = price + $i WHERE id = 1")
      }

      val scheduleResult = spark.sql(
        s"call run_compaction(op => 'schedule', table => '$database.$tableName')").collect()
      assert(scheduleResult.length == 1,
        s"Expected exactly 1 scheduled compaction instant but got ${scheduleResult.length}")
      val scheduledTs = scheduleResult(0).getString(0)
      log.info(s"Scheduled compaction instant: $scheduledTs")

      val runResult = spark.sql(
        s"call run_compaction(op => 'run', table => '$database.$tableName', timestamp => $scheduledTs)").collect()
      assert(runResult.length == 1, s"Expected 1 run row but got ${runResult.length}")
      val state = runResult(0).getString(2)
      assert(state == "COMPLETED", s"Expected compaction state COMPLETED but got $state")

      runSqlQueryWithAsserts(database, tableName, fullScan = true, expectedVal = 2)
      log.info("Compaction procedure test executed successfully")
    } finally {
      spark.conf.unset("hoodie.compact.inline")
      spark.conf.unset("hoodie.compact.schedule.inline")
    }
  }

  def testHudiRepairOverwritePropsProcedure(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_repair_overwrite_props_procedure"
    val newTableName = "hudi_trips_repair_overwrite_props_procedure_renamed"
    val tableBasePath = getBasePath(tableName)
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    val tableRenamePropertiesFile = new Path(getBasePath("misc"), "hoodie_table_rename.properties")

    // Delete and recreate props file with new table name.
    val metaClient = HoodieTableMetaClient.builder().setBasePath(tableBasePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration)).build()
    val fs = new Path(tableBasePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(tableRenamePropertiesFile)) {
      fs.delete(tableRenamePropertiesFile, false)
    }
    val outputStream = fs.create(tableRenamePropertiesFile, true)
    try {
      val properties = new Properties()
      properties.setProperty(HoodieTableConfig.NAME.key(), s"$database.$newTableName")
      properties.setProperty(HoodieTableConfig.DATABASE_NAME.key(), database)
      properties.store(outputStream, "Hoodie table rename properties")
      log.info(s"Properties file created at $tableRenamePropertiesFile")
    } finally {
      outputStream.close()
    }

    val sqlStr = s"call repair_overwrite_hoodie_props(table => '$database.$tableName', " +
      s"new_props_file_path => '$tableRenamePropertiesFile')"
    log.info(s"Executing SQL: $sqlStr")
    spark.sql(sqlStr).show()
    metaClient.reloadTableConfig()
    val fullTableName = s"$database.$newTableName"
    assert(fullTableName == metaClient.getTableConfig.getTableName,
      s"Expected table name '$fullTableName' but got '${metaClient.getTableConfig.getTableName}'")
    assert(database == metaClient.getTableConfig.getDatabaseName,
      s"Expected database '$database' but got '${metaClient.getTableConfig.getDatabaseName}'")
  }
}
