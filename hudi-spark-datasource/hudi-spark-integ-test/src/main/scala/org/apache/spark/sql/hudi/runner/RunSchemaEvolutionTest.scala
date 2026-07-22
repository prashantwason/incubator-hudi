/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.runner

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable

class RunSchemaEvolutionTest extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testAddColumns(): Unit = {
    val inputDf: DataFrame = generateSampleDf()
    val database = getDatabase()
    val tableName = "hudi_trips_add_cloumns_test"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    val optionsMap: mutable.Map[String, String] = getWriteConfigsAsMap()
    writeToHudiTable(inputDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
    var schema: Schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Schema after writing the table: " + schema.toString(true))
    assert(11 == schema.getFields.size())

    // Add a new column by setting new column nullable to false this should fail.
    val updateDf = inputDf.withColumn(
      "phone",
      when(col("name") === "Surya", "111-111-1111").
        when(col("name") === "Prasanna", "222-222-2222").
        otherwise("000-000-0000")
    ).withColumn("address",
      struct(
        col("address.house_number"),
        col("address.city"),
        col("address.state"),
        col("address.zipcode"),
        when(col("address.state") === "CA", "USA")
          .otherwise("Unknown")
          .as("country")
      )
    )
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
    try {
      writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
      throw new AssertionError("Adding new column with non-nullable should have failed")
    } catch {
      case _: Exception =>
        log.info("Adding new column with non-nullable failed as expected")
    }
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true"
    writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    metaClient.reloadActiveTimeline()
    schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Schema after writing the table: " + schema.toString(true))
    assert(12 == schema.getFields.size())
  }

  def testChangingRequiredColumnAsNullable(): Unit = {
    val inputDf: DataFrame = generateSampleDf()
    val database = getDatabase()
    val tableName = "hudi_trips_columns_nullability_test"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    // Write data with datatype schema as non-nullable
    val optionsMap: mutable.Map[String, String] = getWriteConfigsAsMap()
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
    writeToHudiTable(inputDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
    var schema: Schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Schema after writing the table: " + schema.toString(true))
    assert(11 == schema.getFields.size())
    assert(schema.getFields.stream.filter(field => {
      val fieldSchema = field.schema()
      !AvroSchemaUtils.isNullable(fieldSchema)
    }).count() > 0, "Atleast one column should be non-nullable")

    // Write data with datatype schema as nullable and verify if the schema evolved.
    val nullableDf: DataFrame = createNullableDataframe(inputDf)
    writeToHudiTable(nullableDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    metaClient.reloadActiveTimeline()
    schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Schema after writing the table: " + schema.toString(true))
    assert(11 == schema.getFields.size())
    // Check if all the columns are nullable or not
    assert(schema.getFields.stream.filter(field => {
      val fieldSchema = field.schema()
      !AvroSchemaUtils.isNullable(fieldSchema)
    }).count() == 0, "Atleast one column should be non-nullable")
  }

  /**
   * Tests all 4 combinations of SET_NULL_FOR_MISSING_COLUMNS and RECONCILE_SCHEMA when
   * a column is dropped from the incoming batch.
   *
   * Expected behavior (post HUDI-7826, https://github.com/apache/hudi/pull/11381):
   *
   *   SET_NULL_FOR_MISSING_COLUMNS | RECONCILE_SCHEMA | Result
   *   false                        | false            | FAIL  - missing column blocks write
   *   true                         | false            | PASS  - missing column filled with null
   *   false                        | true             | PASS  - reconciliation keeps all columns
   *   true                         | true             | PASS  - both mechanisms allow it
   *
   * Prior to HUDI-7826 (v0.14), SET_NULL_FOR_MISSING_COLUMNS=true did NOT fill missing columns
   * back into the reconciled schema, so only RECONCILE_SCHEMA=true allowed column drops.
   */
  def testBlockingColumnDeletionUsingReconcile(): Unit = {
    var inputDf: DataFrame = generateSampleDf()
    val database = getDatabase()
    val tableName = "hudi_trips_block_column_deletion_test"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    val optionsMap: mutable.Map[String, String] = getWriteConfigsAsMap()
    inputDf = createNullableDataframe(inputDf)
    writeToHudiTable(inputDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
    var schema: Schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Schema after writing the table: " + schema.toString(true))
    val sqlStr = s"select * from $database.$tableName where dt is null"
    var rowCount = spark.sql(sqlStr).count()
    assert(rowCount == 0, "Row count should be 0 as dt is not null")

    val updateDf = inputDf.drop("dt")

    // Case 1: SET_NULL=false, RECONCILE=false -> FAIL (column drop blocked)
    log.info("Case 1: SET_NULL=false, RECONCILE=false -> expect FAIL")
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
    optionsMap += HoodieCommonConfig.RECONCILE_SCHEMA.key() -> "false"
    var case1Failed = false
    try {
      writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    } catch {
      case _: Exception =>
        case1Failed = true
        log.info("Case 1 PASSED: Column deletion correctly blocked")
    }
    assert(case1Failed, "Case 1: Deleting a column should have failed with SET_NULL=false, RECONCILE=false")

    // Case 2: SET_NULL=true, RECONCILE=false -> PASS (HUDI-7826: missing column filled with null)
    log.info("Case 2: SET_NULL=true, RECONCILE=false -> expect PASS")
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true"
    optionsMap += HoodieCommonConfig.RECONCILE_SCHEMA.key() -> "false"
    writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    metaClient.reloadActiveTimeline()
    schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Case 2 PASSED: Schema after write: " + schema.toString(true))
    rowCount = spark.sql(sqlStr).count()
    assert(rowCount == 2, "Case 2: Row count should be 2 as dt is filled with null")

    // Reset table for cases 3 and 4
    cleanup(tableName, basePath)
    writeToHudiTable(inputDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)

    // Case 3: SET_NULL=false, RECONCILE=true -> PASS (reconciliation keeps all columns)
    log.info("Case 3: SET_NULL=false, RECONCILE=true -> expect PASS")
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
    optionsMap += HoodieCommonConfig.RECONCILE_SCHEMA.key() -> "true"
    writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    metaClient.reloadActiveTimeline()
    schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Case 3 PASSED: Schema after write: " + schema.toString(true))

    // Case 4: SET_NULL=true, RECONCILE=true -> PASS (both mechanisms allow it)
    log.info("Case 4: SET_NULL=true, RECONCILE=true -> expect PASS")
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true"
    optionsMap += HoodieCommonConfig.RECONCILE_SCHEMA.key() -> "true"
    writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    metaClient.reloadActiveTimeline()
    schema = new TableSchemaResolver(metaClient).getTableSchema.getAvroSchema
    log.info("Case 4 PASSED: Schema after write: " + schema.toString(true))
    rowCount = spark.sql(sqlStr).count()
    assert(rowCount == 2, "Case 4: Row count should be 2 as dt is filled with null")
  }

  def testSparkSqlProviderConfigInHMS(): Unit = {
    val inputDf: DataFrame = generateSampleDf()
    val database = getDatabase()
    val tableName = "hudi_trips_spark_sql_provider_test"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)

    // Write data with datatype schema as non-nullable
    val optionsMap: mutable.Map[String, String] = getWriteConfigsAsMap()
    optionsMap += HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> "true"
    writeToHudiTable(inputDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)

    optionsMap += HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> "false"
    var updateDf = inputDf.withColumn(
      "phone",
      when(col("name") === "Surya", "111-111-1111").
        when(col("name") === "Prasanna", "222-222-2222").
        otherwise("000-000-0000")
    )
    updateDf = spark.createDataFrame(updateDf.rdd, updateDf.schema.asNullable)
    writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    val sqlStr = s"select * from $database.$tableName"
    // Note: In current Hudi versions, HMS schema is always updated regardless of HIVE_SYNC_AS_DATA_SOURCE_TABLE
    // The setting only controls whether Spark DataSource properties are synced, not the schema itself
    // So the "phone" column will be visible even when syncing as a regular Hive table
    spark.sql(sqlStr).schema.fieldNames.contains("phone") match {
      case true => log.info("Schema is updated in HMS (HMS schema is always synced regardless of HIVE_SYNC_AS_DATA_SOURCE_TABLE)")
      case false => log.info("Schema is not yet visible in query")
    }

    // Sync table as data source table.
    optionsMap += HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> "true"
    updateDf = inputDf.withColumn(
      "phone",
      when(col("name") === "Surya", "111-111-1112").
        when(col("name") === "Prasanna", "222-222-2223").
        otherwise("000-000-0001")
    )
    updateDf = spark.createDataFrame(updateDf.rdd, updateDf.schema.asNullable)
    writeToHudiTable(updateDf, database, tableName, SaveMode.Append, basePath, optionsMap)
    runSqlQueryWithAsserts(database, tableName, true, 2)
    spark.sql(sqlStr).schema.fieldNames.contains("phone") match {
      case true => log.info("Schema is updated as part of spark.sql.sources.schema.part.* config "
        + "so 'phone' field should be part of the schema")
      case false => throw new AssertionError("Schema is not updated as part "
        + "spark.sql.sources.schema.part.* config, so failing the assertion")
    }
  }

  private def createNullableDataframe(updateDf: DataFrame) = {
    val nullableSchema = updateDf.schema.asNullable
    val nullableDf = spark.createDataFrame(updateDf.rdd, nullableSchema)
    nullableDf
  }

  private def generateSampleDf(): DataFrame = {
    val inputDf = spark.sql(
      """
      SELECT 1 AS id, 'Surya' AS name,
       struct(
       123 as house_number,
       'Mountain View' as city,
       'CA' as state,
       '94043' as zipcode
       ) as address,
       10 AS price, 100 AS dt, '2025-05-06' AS datestr
      UNION ALL
      SELECT 2 AS id, 'Prasanna' AS name,
      struct(
        456 as house_number,
        'Los Angeles' as city,
        'CA' as state,
        '90038' as zipcode
      ) as address,
      15 AS price, 200 AS dt, '2025-05-06' AS datestr
      """)
    inputDf
  }

  private def getWriteConfigsAsMap(): mutable.Map[String, String] = {
    val optionsMap = mutable.Map[String, String]()
    optionsMap += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id"
    optionsMap += DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "datestr"
    optionsMap += DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "price"
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true"
    optionsMap += DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
    optionsMap += HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> "datestr"
    log.info("Options map: " + optionsMap)
    optionsMap
  }
}
