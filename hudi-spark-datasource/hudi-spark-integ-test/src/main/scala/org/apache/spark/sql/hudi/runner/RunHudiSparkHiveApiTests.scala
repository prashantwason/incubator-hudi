/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.runner

import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, ExternalCatalog, HoodieCatalogTable}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class RunHudiSparkHiveApiTests extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  private def listTablePartitions(externalCatalog: ExternalCatalog, dbName: String, tableName: String):
  Seq[CatalogTablePartition] = {
    externalCatalog.listPartitions(dbName, tableName)
  }

  /**
   * CatalogTablePartition API examples
   * Ex: 1
   * tablePartition.spec example entry
   * Map(datestr -> 2016-09-06)
   *
   * To print use
   * log.info(s"CatalogTablePartition: ${catalogTablePartition.spec}")
   *
   * Ex: 2
   * catalogTablePartition.parameters example entry
   * Map(
   * rawDataSize -> 2760562229279,
   * abb_source_cluster -> PHXCLD_PROD,
   * numFiles -> 521,
   * transient_lastDdlTime -> 1568231350,
   * last_modified_time -> 1568231350,
   * last_modified_by -> hoover,
   * totalSize -> 2389339538028,
   * lastAnalyzedTime -> 1606974220,
   * COLUMN_STATS_ACCURATE -> {"BASIC_STATS":"true"},
   * numRows -> 17520133
   * )
   *
   * To print use
   * log.info(s"CatalogTablePartition parameters: ${catalogTablePartition.parameters}")
   *
   * Ex: 3
   * catalogTablePartition.storage example entry
   * Storage(
   * Location: hdfs://ns-router-dca1/hivesync/uber-data/tables/rawdata/kafka/hp-motion-driver_app/2016/09/06_s,
   * Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,
   * InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat,
   * OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat,
   * Storage Properties: [serialization.format=1]
   * )
   *
   * To print use
   * log.info(s"CatalogTablePartition storage: ${catalogTablePartition.storage}")
   *
   * catalogTablePartition.stats Example entry
   * Some(CatalogStatistics(2389339538028,Some(17520133),Map()))
   *
   * To print use
   * log.info(s"CatalogTablePartition stats: ${catalogTablePartition.stats}")
   */
  def testHiveExternalCatalog(): Unit = {
    val externalCatalog = spark.sessionState.catalog.externalCatalog

    // Verify rawdata dataset's partition spec
    val dbName = "rawdata"
    val tableName = "kafka_hp_scheduled_ride_job_state_changes_nodedup"
    val rawdataPartitions: Seq[CatalogTablePartition] =
      listTablePartitions(externalCatalog, dbName, tableName).take(5)
    rawdataPartitions.foreach(tablePartition => {
      val tablePartitionSpec = tablePartition.spec
      assert(tablePartitionSpec.size == 1, "Rawdata dataset should have 1 partition key")
    })

    // Verify derived dataset's partition spec for non partitioned datasets
    val etlDatabase = "dwh"
    val nonPartitionedTable = "dim_bliss_ticket"
    try {
      listTablePartitions(externalCatalog, etlDatabase, nonPartitionedTable)
      throw new AssertionError("Expected exception is not thrown for non-partitioned table")
    } catch {
      case e: Exception =>
        log.info(s"Expected ${e.getClass}, is throw for non-partitioned table")
    }

    // Verify derived dataset's partition spec for partitioned datasets
    val partitionedTableName = "fact_trip_payment"
    val derivedDatasetPartitions: Seq[CatalogTablePartition] =
      listTablePartitions(externalCatalog, etlDatabase, partitionedTableName)
        .take(5)
    derivedDatasetPartitions.foreach(tablePartition => {
      val tablePartitionSpec = tablePartition.spec
      assert(tablePartitionSpec.size == 1, "Partitioned dataset should have 1 partition key")
    })

    // Verify derived dataset's partition spec for multi-key partitioned datasets
    // This dataset is partitioned by two keys, datestr and event_name so tablePartition.spec.size should be 2
    val moneyDatabase = "money"
    val multiPartitionedTableName = "fact_mirador_user_events"
    val multiPartitionedDatasetPartitions: Seq[CatalogTablePartition] =
      listTablePartitions(externalCatalog, moneyDatabase, multiPartitionedTableName)
      .take(5)
    multiPartitionedDatasetPartitions.foreach(tablePartition => {
      val tablePartitionSpec = tablePartition.spec
      assert(tablePartitionSpec.size == 2, "Multi-key partitioned dataset should have 2 partition keys")
      val expectedPartitionKeys = Array("datestr", "event_name")
      val result = tablePartition.spec.keys.toArray
      assert(result sameElements expectedPartitionKeys, s"Partition keys should be "
        + s"${expectedPartitionKeys.mkString("Array(", ", ", ")")} found ${result.mkString("Array(", ", ", ")")}")
    })
  }

  def testCreateOrReplaceView(): Unit = {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("hudi_integ_temp_view")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("hudi_integ_temp_view2")

    val df3 = spark.sql("SHOW TABLES FROM default")
    df3.show(100000, false)
  }

  def testShowPartitionCommandOnRawdataDataset(): Unit = {
    val rawdataDataset = "rawdata.kafka_hp_scheduled_ride_job_state_changes_nodedup"
    val dbName = "rawdata"
    val tableName = "kafka_hp_scheduled_ride_job_state_changes_nodedup"

    // 1. Dump HMS table metadata
    val catalog = spark.sessionState.catalog
    val catalogTable = catalog.externalCatalog.getTable(dbName, tableName)
    log.error(s"[DEBUG] HMS table provider: ${catalogTable.provider}")
    log.error(s"[DEBUG] HMS table storage.inputFormat: ${catalogTable.storage.inputFormat}")
    log.error(s"[DEBUG] HMS table storage.locationUri: ${catalogTable.storage.locationUri}")
    log.error(s"[DEBUG] HMS table partitionColumnNames: ${catalogTable.partitionColumnNames}")
    log.error(s"[DEBUG] HMS table schema fields: ${catalogTable.schema.fieldNames.mkString(", ")}")
    log.error(s"[DEBUG] HMS table properties keys: ${catalogTable.properties.keys.mkString(", ")}")
    catalogTable.properties.foreach { case (k, v) =>
      if (k.contains("hoodie") || k.contains("partition") || k.contains("schema")) {
        log.error(s"[DEBUG] HMS property: $k = ${v.take(500)}")
      }
    }

    // 2. Load hoodie.properties from basepath
    val basePath = catalogTable.storage.locationUri.map(_.toString).getOrElse("")
    log.error(s"[DEBUG] Table basePath: $basePath")
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sessionState.newHadoopConf()))
      .build()
    val tableConfig = metaClient.getTableConfig
    log.error(s"[DEBUG] hoodie.properties tableName: ${tableConfig.getTableName}")
    log.error(s"[DEBUG] hoodie.properties databaseName: ${tableConfig.getDatabaseName}")
    log.error(s"[DEBUG] hoodie.properties tableType: ${tableConfig.getTableType}")
    log.error(s"[DEBUG] hoodie.properties partitionFields.isPresent: ${tableConfig.getPartitionFields.isPresent}")
    if (tableConfig.getPartitionFields.isPresent) {
      log.error(s"[DEBUG] hoodie.properties partitionFields: ${tableConfig.getPartitionFields.get().mkString(", ")}")
    }
    log.error(s"[DEBUG] hoodie.properties recordKeyFields: ${tableConfig.getRecordKeyFields}")
    log.error(s"[DEBUG] hoodie.properties orderingField: ${tableConfig.getOrderingFields}")
    log.error(s"[DEBUG] hoodie.properties keyGeneratorClassName: ${tableConfig.getKeyGeneratorClassName}")
    log.error(s"[DEBUG] hoodie.properties slashSeparatedDatePartitioning: ${tableConfig.getSlashSeparatedDatePartitioning}")

    // 3. Dump raw hoodie.properties key-value pairs
    val props = tableConfig.getProps
    log.error(s"[DEBUG] Raw hoodie.properties count: ${props.size()}")
    props.asScala.foreach { case (k, v) =>
      log.error(s"[DEBUG] hoodie.prop: $k = $v")
    }

    // 4. Dump Avro schema from TableSchemaResolver
    try {
      val schemaResolver = new TableSchemaResolver(metaClient)
      val avroSchema = schemaResolver.getTableSchema.getAvroSchema
      log.error(s"[DEBUG] Avro schema field count: ${avroSchema.getFields.size()}")
      avroSchema.getFields.asScala.foreach { field =>
        log.error(s"[DEBUG] Avro field: name=${field.name()}, type=${field.schema().getType}, " +
          s"fullType=${field.schema().toString.take(200)}")
      }
    } catch {
      case e: Exception =>
        log.error(s"[DEBUG] Failed to load Avro schema: ${e.getMessage}", e)
    }

    // 5. Try HoodieCatalogTable and dump its state
    try {
      val hoodieCatalogTable = HoodieCatalogTable(spark,
        org.apache.spark.sql.catalyst.TableIdentifier(tableName, Some(dbName)))
      log.error(s"[DEBUG] HoodieCatalogTable.partitionFields: ${hoodieCatalogTable.partitionFields.mkString(", ")}")

      try {
        val schema = hoodieCatalogTable.tableSchema
        log.error(s"[DEBUG] HoodieCatalogTable.tableSchema fieldNames: ${schema.fieldNames.mkString(", ")}")
        log.error(s"[DEBUG] HoodieCatalogTable.partitionSchema fieldNames: ${hoodieCatalogTable.partitionSchema.fieldNames.mkString(", ")}")
        log.error(s"[DEBUG] HoodieCatalogTable.dataSchema fieldNames: ${hoodieCatalogTable.dataSchema.fieldNames.mkString(", ")}")
      } catch {
        case e: Exception =>
          log.error(s"[DEBUG] HoodieCatalogTable.tableSchema FAILED: ${e.getMessage}", e)
      }

      try {
        val partPaths = hoodieCatalogTable.getPartitionPaths
        log.error(s"[DEBUG] HoodieCatalogTable.getPartitionPaths (first 5): ${partPaths.take(5).mkString(", ")}")
      } catch {
        case e: Exception =>
          log.error(s"[DEBUG] HoodieCatalogTable.getPartitionPaths FAILED: ${e.getMessage}", e)
      }
    } catch {
      case e: Exception =>
        log.error(s"[DEBUG] HoodieCatalogTable construction FAILED: ${e.getMessage}", e)
    }

    // 6. Now run the actual SHOW PARTITIONS and inspect results
    try {
      val showPartDf = spark.sql(s"SHOW PARTITIONS $rawdataDataset")
      val topFiveRows = showPartDf.take(5)
      log.error(s"[DEBUG] SHOW PARTITIONS returned ${topFiveRows.length} rows (of first 5)")
      topFiveRows.foreach(row => log.error(s"[DEBUG] SHOW PARTITIONS row: '${row.getString(0)}'"))
      assert(topFiveRows.length == 5)
      topFiveRows.foreach(row => assert(row.getString(0).startsWith("datestr="),
        s"Rawdata dataset's partition should start with datestr= but got '${row.getString(0)}'"))
    } catch {
      case e: Exception =>
        log.error(s"[DEBUG] SHOW PARTITIONS FAILED: ${e.getMessage}", e)
        throw e
    }
  }
}
