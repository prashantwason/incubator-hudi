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

import org.apache.hudi.{DataSourceWriteOptions, HoodieVersion}
import org.apache.hudi.DataSourceWriteOptions.HIVE_SYNC_MODE
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, HiveSyncConfigHolder}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

class RunHudiBasicOperations extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testHudiDFInsertTable(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_insert_table_v2"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying inserts on existing table")
    val df = spark.sql(s"select * from $database.$tableName")
    df.show(20, false)
    val rowCount = df.count()
    assert(20 == rowCount, s"Expected 20 records found $rowCount")
  }

  def testHudiDFUpsertTable(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_upsert_table"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying upserts on existing table")
    for (i <- 1 to 2) {
      createUpserts(database, tableName, SaveMode.Append, isHudiTable = true)
      log.info(s"Upsert done with iteration $i")
      val df = spark.sql(s"select * from $database.$tableName order by _hoodie_record_key")
      df.show(200, false)
      val rowCount = df.count()
      assert(20 == rowCount, s"Expected 20 records but got $rowCount")
    }
    val df2 = spark.sql(s"select DISTINCT ${HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName} from $database.$tableName")
    df2.show(200, false)
    val commitsCount = df2.count()
    assert(3 == commitsCount, s"Expected 3 commits but got $commitsCount")
  }

  def testHudiSqlQuery(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_sql_query"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying SQL queries")
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 20)
  }

  def testHudiDFQuery(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_df_read"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying DataFrame queries")
    runDataFrameReaderWithAsserts(database, tableName, 20)
  }

  def testRepeatedSQLQueriesOnHudiDataset(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_repeated_sql"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    log.info("First insert done, now trying sql queries on the hudi table")
    for (i <- 1 to 2) {
      runSqlQueryWithAsserts(database, tableName, fullScan = true, 20)
      log.info(s"SQL query iteration $i done")
    }
  }

  def testHudiDropTableCommand(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_drop_table"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = true)
    assert(tableExists(database, tableName), s"Table $database.$tableName should exist")
    log.info("First insert done, now trying to drop table")
    cleanup(tableName, getBasePath(tableName))
    assert(!tableExists(database, tableName), s"Table $database.$tableName should not exist")
  }

  def testInsertingDataWithoutDatabase(): Unit = {
    val df = spark.sql("select * from dwh.dim_city limit 100")
    val tableName = "hudi_trips_integration_test_table"
    cleanup(tableName, getBasePath(tableName))
    df.write
      .format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "city_id")
      .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "update_epoch")
      .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "mega_region")
      .save(getBasePath(tableName))
    //TODO: Add an assertion after fixing. https://t3.uberinternal.com/browse/HUDI-6642
  }

  def testStackedDataFrameAPIOperationsOnHudi(): Unit = {
    // Insert 2 records each in two partitions
    val inputDf = spark.sql(
      """
      SELECT 1 AS id, 'a1' AS name, 10 AS price, 100 AS dt, '2025-04-21' AS datestr
      UNION ALL
      SELECT 2, 'a2', 15, 200, '2025-04-21'
      UNION ALL
      SELECT 3, 'a3', 20, 300, '2025-04-22'
      UNION ALL
      SELECT 4, 'a4', 25, 400, '2025-04-22'
      """)
    // Show the entries
    inputDf.show()

    // Write data into dataset and register into Hive Metastore
    val database = getDatabase()
    val tableName = "hudi_trips_test_table"
    cleanup(tableName, getBasePath(tableName))
    inputDf.write
      .format("hudi")
      .mode(SaveMode.Append)
      .option("hoodie.table.name", s"$database.$tableName")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "dt")
      .option("hoodie.datasource.write.partitionpath.field", "datestr")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.hive_sync.enable", "true")
      .option("hoodie.datasource.hive_sync.mode", "HMS")
      .option("hoodie.datasource.hive_sync.database", database)
      .option("hoodie.datasource.hive_sync.table", tableName)
      .option("hoodie.datasource.hive_sync.partition_fields", "datestr")
      .save(getBasePath(tableName))
    // Validate the row count
    runDataFrameReaderWithAsserts(database, tableName, 4)

    val updateDf = spark.sql(
      """
      SELECT 1 AS id, 'a11' AS name, 12 AS price, 101 AS dt, '2025-04-21' AS
      datestr
    """)
    updateDf.write
      .format("hudi")
      .mode(SaveMode.Append)
      .option("hoodie.table.name", s"$database.$tableName")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "dt")
      .option("hoodie.datasource.write.partitionpath.field", "datestr")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.hive_sync.enable", "true")
      .option("hoodie.datasource.hive_sync.mode", "HMS")
      .option("hoodie.datasource.hive_sync.database", database)
      .option("hoodie.datasource.hive_sync.table", tableName)
      .option("hoodie.datasource.hive_sync.partition_fields", "datestr")
      .save(getBasePath(tableName))
    // Validate the row count
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 4)

    // Insert overwrite partition
    val insertOverwriteDf = spark.sql(
      """
      SELECT 5 AS id, 'a5' AS name, 10 AS price, 500 AS dt, '2025-04-22' AS datestr
      UNION ALL
      SELECT 6, 'a6', 40, 600, '2025-04-22'
      """)

    insertOverwriteDf.write
      .format("hudi")
      .mode(SaveMode.Append)
      .option("hoodie.table.name", s"$database.$tableName")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "dt")
      .option("hoodie.datasource.write.partitionpath.field", "datestr")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.datasource.write.operation", "insert_overwrite")
      .option("hoodie.datasource.hive_sync.enable", "true")
      .option("hoodie.datasource.hive_sync.mode", "HMS")
      .option("hoodie.datasource.hive_sync.database", database)
      .option("hoodie.datasource.hive_sync.table", tableName)
      .option("hoodie.datasource.hive_sync.partition_fields", "datestr")
      .save(getBasePath(tableName))
    // Validate the row count
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 4)
  }

  def testHudiCreateTableWithDataFrameAPI(): Unit = {
    val database = getDatabase()
    val tableName = "hudi_trips_cow_test_save_as_table"
    cleanup(tableName, getBasePath(tableName))
    val records = Seq(
      (1, "Trip from SFO to SJC", "2025-04-16"),
      (2, "Trip from SJC to OAK", "2025-04-16"),
      (3, "Trip from OAK to SFO", "2025-04-17"))

    val schema = StructType(Seq(
      StructField("trip_id", IntegerType, nullable = true),
      StructField("details", StringType, nullable = true),
      StructField("datestr", StringType, nullable = true)
    ))
    val rdd = spark.sparkContext.parallelize(records)
    val rowRDD = rdd.map { case (trip_id, details, datastr) => Row(trip_id, details, datastr) }
    val df = spark.createDataFrame(rowRDD, schema)

    df.write.format("hudi")
      .mode(SaveMode.Append)
      //.partitionBy("datestr")
      // We must set this property with the table name otherwise we get an error message that this
      // is missing.
      .option(HoodieTableConfig.NAME.key(), database + "." + tableName)
      // This must match the partitionBy above
      // .option(HoodieTableConfig.PARTITION_FIELDS.key(), "datestr")
      .option("hoodie.datasource.write.partitionpath.field", "datestr")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)

      .option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key(), "true")
      .option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), "HMS")
      .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), database)
      .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), tableName)
      .option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "datestr")
      .option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), classOf[HiveStylePartitionValueExtractor].getName)
      .save(getBasePath(tableName))
    // Validate the row count
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 3)

    val records2 = Seq(
      (4, "Trip from OAK to LAX", "2025-04-17"))

    val rdd2 = spark.sparkContext.parallelize(records2)
    val rowRDD2 = rdd2.map { case (trip_id, details, datastr) => Row(trip_id, details, datastr) }
    val df2 = spark.createDataFrame(rowRDD2, schema)

    df2.write.format("hudi")
      .mode(SaveMode.Append)
      //.partitionBy("datestr")
      // We must set this property with the table name otherwise we get an error message that this
      // is missing.
      .option(HoodieTableConfig.NAME.key(), database + "." + tableName)
      // This must match the partitionBy above
      .option(HoodieTableConfig.PARTITION_FIELDS.key(), "datestr")
      .option("hoodie.datasource.write.partitionpath.field", "datestr")
      // Must be the same as above
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key(), "datestr")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(HIVE_SYNC_MODE.key(), "HMS")
      // we want to upsert partitions that this job calculates
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED.key(), "true")
      .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), database)
      .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), tableName)
      .save(getBasePath(tableName))
    // Validate the row count
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 3)
  }

  def testHudiVersion(): Unit = {
    val version = HoodieVersion.get()
    log.info(s"Running with Hoodie version $version")
    log.info(s"Running with Hoodie major version as ${HoodieVersion.major()}")
    log.info(s"Running with Hoodie minor version as ${HoodieVersion.minor()}")
    log.info(s"Running with Hoodie patch version as ${HoodieVersion.patch()}")

    ValidationUtils.checkState(version.split("\\.").length > 2, s"Version $version is not valid")
  }
}
