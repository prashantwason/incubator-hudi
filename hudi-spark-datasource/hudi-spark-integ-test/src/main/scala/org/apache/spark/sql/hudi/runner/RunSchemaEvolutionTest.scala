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
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Schema-evolution integration tests. Each test case returns an ordered `Seq[Step]`, so the
 * runner can run one step at a time (each in its own Spark app against a chosen Hudi runtime)
 * or the whole method in one app. State passes between steps only via the Hudi table on HDFS +
 * HMS. Version-divergent APIs go through [[VersionCompat]] so one jar runs on both 0.14 and 1.x.
 */
class RunSchemaEvolutionTest extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------
  // testAddColumns
  // --------------------------------------------------------------------------
  def testAddColumns(): Seq[Step] = {
    val tableName = "hudi_trips_add_cloumns_test"
    Seq(

      Step("initial load") { spark =>                                  // writer
        val basePath = getBasePath(tableName)
        cleanup(tableName, basePath)
        writeToHudiTable(generateSampleDf(), getDatabase(), tableName,
          SaveMode.Append, basePath, writeConfigs())
      },

      Step("verify initial state") { spark =>                         // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        val schema = tableAvroSchema(getBasePath(tableName))
        log.info("Schema after initial load: " + schema.toString(true))
        assert(schema.getFields.size() == 11,
          s"expected 11 schema fields after initial load, got ${schema.getFields.size()}")
      },

      Step("reject missing-column write") { spark =>                  // writer
        val opts = writeConfigs()
        opts += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
        // AssertionError (Error, not Exception) signals "should have failed" so
        // it is NOT swallowed by the catch below.
        try {
          writeToHudiTable(withPhoneAndCountry(generateSampleDf()), getDatabase(), tableName,
            SaveMode.Append, getBasePath(tableName), opts)
          throw new AssertionError("Adding new column with set-null=false should have failed")
        } catch {
          case _: Exception => log.info("Adding new column with set-null=false failed as expected")
        }
      },

      Step("add columns") { spark =>                                  // writer
        val opts = writeConfigs()
        opts += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true"
        writeToHudiTable(withPhoneAndCountry(generateSampleDf()), getDatabase(), tableName,
          SaveMode.Append, getBasePath(tableName), opts)
      },

      Step("verify evolved state") { spark =>                         // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        val schema = tableAvroSchema(getBasePath(tableName))
        log.info("Schema after evolution: " + schema.toString(true))
        assert(schema.getFields.size() == 12,
          s"expected 12 schema fields after adding column, got ${schema.getFields.size()}")
      }
    )
  }

  // --------------------------------------------------------------------------
  // testChangingRequiredColumnAsNullable
  // --------------------------------------------------------------------------
  def testChangingRequiredColumnAsNullable(): Seq[Step] = {
    val tableName = "hudi_trips_columns_nullability_test"
    Seq(

      Step("write non-nullable schema") { spark =>                    // writer
        val basePath = getBasePath(tableName)
        cleanup(tableName, basePath)
        val opts = writeConfigs()
        opts += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
        writeToHudiTable(generateSampleDf(), getDatabase(), tableName, SaveMode.Append, basePath, opts)
      },

      Step("verify has non-nullable fields") { spark =>               // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        val schema = tableAvroSchema(getBasePath(tableName))
        assert(schema.getFields.size() == 11,
          s"expected 11 schema fields, got ${schema.getFields.size()}")
        assert(nonNullableFieldCount(schema) > 0, "at least one column should be non-nullable")
      },

      Step("write nullable schema") { spark =>                        // writer
        val opts = writeConfigs()
        opts += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "false"
        writeToHudiTable(createNullableDataframe(generateSampleDf()), getDatabase(), tableName,
          SaveMode.Append, getBasePath(tableName), opts)
      },

      Step("verify all nullable") { spark =>                          // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        val schema = tableAvroSchema(getBasePath(tableName))
        assert(schema.getFields.size() == 11,
          s"expected 11 schema fields, got ${schema.getFields.size()}")
        assert(nonNullableFieldCount(schema) == 0, "all columns should be nullable")
      }
    )
  }

  // --------------------------------------------------------------------------
  // testBlockingColumnDeletionUsingReconcile
  //
  // Matrix of SET_NULL_FOR_MISSING_COLUMNS x RECONCILE_SCHEMA when a column is
  // dropped from the incoming batch (post HUDI-7826):
  //   set_null | reconcile | result
  //   false    | false     | FAIL  (missing column blocks write)
  //   true     | false     | PASS  (missing column filled with null)
  //   false    | true      | PASS  (reconciliation keeps all columns)
  //   true     | true      | PASS
  // --------------------------------------------------------------------------
  def testBlockingColumnDeletionUsingReconcile(): Seq[Step] = {
    val tableName = "hudi_trips_block_column_deletion_test"
    def dtNullCount(): Long =
      spark.sql(s"select * from ${getDatabase()}.$tableName where dt is null").count()
    def writeDroppingDt(setNull: String, reconcile: String): Unit = {
      val opts = writeConfigs()
      opts += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> setNull
      opts += HoodieCommonConfig.RECONCILE_SCHEMA.key() -> reconcile
      writeToHudiTable(createNullableDataframe(generateSampleDf()).drop("dt"), getDatabase(), tableName,
        SaveMode.Append, getBasePath(tableName), opts)
    }
    Seq(

      Step("write with dt column") { spark =>                         // writer
        val basePath = getBasePath(tableName)
        cleanup(tableName, basePath)
        writeToHudiTable(createNullableDataframe(generateSampleDf()), getDatabase(), tableName,
          SaveMode.Append, basePath, writeConfigs())
      },

      Step("verify dt not null") { spark =>                           // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        assert(dtNullCount() == 0, "row count should be 0 as dt is not null")
      },

      Step("case1 reject drop (set_null=false, reconcile=false)") { spark =>   // writer
        try {
          writeDroppingDt(setNull = "false", reconcile = "false")
          throw new AssertionError("Case 1: column drop should have failed")
        } catch {
          case _: Exception => log.info("Case 1 PASSED: column deletion correctly blocked")
        }
      },

      Step("case2 fill null (set_null=true, reconcile=false)") { spark =>      // writer
        writeDroppingDt(setNull = "true", reconcile = "false")
      },

      Step("verify case2 dt filled null") { spark =>                  // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        assert(dtNullCount() == 2, "Case 2: row count should be 2 as dt is filled with null")
      },

      Step("reset table") { spark =>                                  // writer
        val basePath = getBasePath(tableName)
        cleanup(tableName, basePath)
        writeToHudiTable(createNullableDataframe(generateSampleDf()), getDatabase(), tableName,
          SaveMode.Append, basePath, writeConfigs())
      },

      Step("case3 reconcile (set_null=false, reconcile=true)") { spark =>      // writer
        writeDroppingDt(setNull = "false", reconcile = "true")
      },

      Step("case4 both (set_null=true, reconcile=true)") { spark =>            // writer
        writeDroppingDt(setNull = "true", reconcile = "true")
      },

      Step("verify case4 dt filled null") { spark =>                  // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        assert(dtNullCount() == 2, "Case 4: row count should be 2 as dt is filled with null")
      }
    )
  }

  // --------------------------------------------------------------------------
  // testSparkSqlProviderConfigInHMS
  // --------------------------------------------------------------------------
  def testSparkSqlProviderConfigInHMS(): Seq[Step] = {
    val tableName = "hudi_trips_spark_sql_provider_test"
    def phonePresent(): Boolean =
      spark.sql(s"select * from ${getDatabase()}.$tableName").schema.fieldNames.contains("phone")
    Seq(

      Step("write as data source table") { spark =>                   // writer
        val basePath = getBasePath(tableName)
        cleanup(tableName, basePath)
        val opts = writeConfigs()
        opts += HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> "true"
        writeToHudiTable(generateSampleDf(), getDatabase(), tableName, SaveMode.Append, basePath, opts)
      },

      Step("verify initial state") { spark =>                         // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
      },

      Step("write non-datasource table with phone") { spark =>        // writer
        val opts = writeConfigs()
        opts += HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> "false"
        val updateDf = addNullablePhone(generateSampleDf(), "111-111-1111", "222-222-2222", "000-000-0000")
        writeToHudiTable(updateDf, getDatabase(), tableName, SaveMode.Append, getBasePath(tableName), opts)
      },

      Step("verify phone visibility (informational)") { spark =>      // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        // HMS schema is always synced regardless of HIVE_SYNC_AS_DATA_SOURCE_TABLE,
        // so this is informational, not asserted.
        log.info("phone present after non-datasource write? " + phonePresent())
      },

      Step("write data source table with phone") { spark =>           // writer
        val opts = writeConfigs()
        opts += HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> "true"
        val updateDf = addNullablePhone(generateSampleDf(), "111-111-1112", "222-222-2223", "000-000-0001")
        writeToHudiTable(updateDf, getDatabase(), tableName, SaveMode.Append, getBasePath(tableName), opts)
      },

      Step("verify phone synced as data source table") { spark =>     // operate
        runSqlQueryWithAsserts(getDatabase(), tableName, fullScan = true, expectedVal = 2)
        assert(phonePresent(),
          "'phone' should be part of the schema via spark.sql.sources.schema.part.* config")
      }
    )
  }

  // --------------------------------------------------------------------------
  // Helpers (Spark-dependent — only called inside step bodies)
  // --------------------------------------------------------------------------

  private def tableAvroSchema(basePath: String): Schema = {
    val metaClient = VersionCompat.buildMetaClient(basePath, spark.sparkContext.hadoopConfiguration)
    VersionCompat.tableAvroSchema(metaClient)
  }

  private def nonNullableFieldCount(schema: Schema): Long =
    schema.getFields.stream().filter(f => !AvroSchemaUtils.isNullable(f.schema())).count()

  /** Adds a top-level `phone` column and a nested `address.country` field. */
  private def withPhoneAndCountry(df: DataFrame): DataFrame =
    df.withColumn("phone",
      when(col("name") === "Surya", "111-111-1111")
        .when(col("name") === "Prasanna", "222-222-2222")
        .otherwise("000-000-0000"))
      .withColumn("address",
        struct(
          col("address.house_number"),
          col("address.city"),
          col("address.state"),
          col("address.zipcode"),
          when(col("address.state") === "CA", "USA").otherwise("Unknown").as("country")))

  private def addNullablePhone(df: DataFrame, surya: String, prasanna: String, other: String): DataFrame = {
    val withPhone = df.withColumn("phone",
      when(col("name") === "Surya", surya)
        .when(col("name") === "Prasanna", prasanna)
        .otherwise(other))
    spark.createDataFrame(withPhone.rdd, withPhone.schema.asNullable)
  }

  private def createNullableDataframe(df: DataFrame): DataFrame =
    spark.createDataFrame(df.rdd, df.schema.asNullable)

  private def generateSampleDf(): DataFrame = spark.sql(
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

  private def writeConfigs(): mutable.Map[String, String] = {
    val optionsMap = mutable.Map[String, String]()
    optionsMap += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id"
    optionsMap += DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "datestr"
    optionsMap += DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "price"
    optionsMap += HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() -> "true"
    optionsMap += DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
    optionsMap += HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> "datestr"
    optionsMap
  }
}
