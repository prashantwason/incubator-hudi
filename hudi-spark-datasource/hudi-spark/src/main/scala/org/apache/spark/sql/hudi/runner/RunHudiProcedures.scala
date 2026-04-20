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

import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import java.util.Properties

class RunHudiProcedures extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testHudiShowCommitsProcedure(): Unit = {
    val database = "rawdatatmp"
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
    val database = "rawdatatmp"
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
    val database = "rawdatatmp"
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
    val database = "rawdatatmp"
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
    throw new UnsupportedOperationException("Not implemented yet")
  }

  def testCompactionProcedure(): Unit = {
    throw new UnsupportedOperationException("Not implemented yet")
  }

  def testHudiRepairOverwritePropsProcedure(): Unit = {
    val database = "rawdatatmp"
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
