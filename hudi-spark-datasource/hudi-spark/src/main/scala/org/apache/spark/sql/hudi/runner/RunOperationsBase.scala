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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkSQLUtils, QuickstartUtils}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.HoodieTableVersion
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, HiveSyncConfigHolder}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

trait RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)
  private val BASE_PATH_CONST: String = "/user/hudi/integration_tests/"
  protected var spark: SparkSession = _
  protected var dataGen: DataGenerator = _
  protected var database: String = _
  private val ENABLE_HIVE_SYNC: Boolean = true
  protected val TEST_PARTITIONS = Array("2025-01-01", "2025-01-02", "2025-01-03")

  def initialize(sparkSession: SparkSession, db: String): Unit = {
    spark = sparkSession
    database = db
    cleanup()
  }

  def getDatabase(): String = database

  def cleanup() : Unit = {
    // Reset data generator
    dataGen = new DataGenerator(TEST_PARTITIONS)
    // Unpersist RDDs
    spark.sparkContext.getPersistentRDDs.values.foreach(_.unpersist())
    spark.catalog.clearCache()
  }

  def getBasePath(tableName: String): String = {
    val dc = spark.sparkContext.conf.get("spark.drogon.dc", "phx")
    val routerPath = getPathPrefixForDatacenter(dc)
    routerPath + BASE_PATH_CONST + tableName
  }

  private def getPathPrefixForDatacenter(datacenter: String): String = {
    val dc = datacenter.toLowerCase
    if (dc.contains("cld") || dc.contains("cloud") || dc.contains("cloudlake")) {
      "cfs://ns-cloudlake"
    } else if (dc.contains("phx")) {
      "hdfs://ns-router-prod-phx"
    } else if (dc.contains("dca")) {
      "hdfs://ns-router-dca1"
    } else {
      throw new HoodieException(s"Invalid datacenter: $datacenter. Expected: phx, dca, or cloudlake")
    }
  }

  def cleanup(tableName: String, basePath: String): Unit = {
    cleanup(tableName, basePath, false)
  }

  def cleanup(tableName: String, basePath: String, isMor: Boolean): Unit = {
    val tablePathObj = new Path(basePath)
    val fs = tablePathObj.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val metaFolder = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    try {
      if (fs.exists(metaFolder)) {
        assertHoodieTableConfig(database, tableName, basePath)
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $database.$tableName")
      fs.delete(tablePathObj, true)
      log.info(s"Cleaned up table: $database.$tableName at path $basePath")
      if (isMor) {
        spark.sql(s"DROP TABLE IF EXISTS $database.${tableName}_rt")
        spark.sql(s"DROP TABLE IF EXISTS $database.${tableName}_ro")
      }
    }
  }

  def createInserts(database: String, tableName: String, saveMode: SaveMode, isHudiTable: Boolean): Unit = {
    createInserts(database, tableName, saveMode, isHudiTable, mutable.Map[String, String]())
  }

  def createInserts(database: String, tableName: String, saveMode: SaveMode, isHudiTable: Boolean, optionsMap : mutable.Map[String, String]): Unit = {
    val basePath = getBasePath(tableName)
    /**
     * Generic record structure for reference
     * GenericRecord rec = new GenericData.Record(avroSchema);
     * rec.put("uuid", rowKey);
     * rec.put("ts", timestamp);
     * rec.put("rider", riderName);
     * rec.put("driver", driverName);
     * rec.put("begin_lat", rand.nextDouble());
     * rec.put("begin_lon", rand.nextDouble());
     * rec.put("end_lat", rand.nextDouble());
     * rec.put("end_lon", rand.nextDouble());
     * rec.put("fare", rand.nextDouble() * 100);
     * return rec;
     */
    val records = QuickstartUtils.convertToStringList(dataGen.generateInserts(20))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(spark.createDataset(recordsRDD)(Encoders.STRING))
    if (isHudiTable) {
      // Create inserts into Hudi dataset
      writeToHudiTable(inputDF, database, tableName, saveMode, basePath, optionsMap)
    } else {
      // Create inserts into Hive dataset
      writeToHiveTable(inputDF, database, tableName, saveMode, basePath)
    }
  }

  def writeToHudiTable(inputDF: DataFrame, database: String, tableName: String, saveMode: SaveMode, basePath: String,
                       optionsMap :mutable.Map[String, String]): Unit = {
    inputDF.write.format("hudi").
      options(QuickstartUtils.getQuickstartWriteConfigs).
      option(HoodieTableConfig.NAME.key(), tableName).
      option(HoodieTableConfig.DATABASE_NAME.key(), database).
      option(RECORDKEY_FIELD.key(), "uuid").
      option(PARTITIONPATH_FIELD.key(), "partitionpath").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL).
      options(hiveSyncOptionsMap(ENABLE_HIVE_SYNC, database, tableName)).
      option(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true").
      option(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), "true").
      options(optionsMap).
      mode(saveMode).
      save(basePath)
    assertHoodieTableConfig(database, tableName, basePath)
  }

  /**
   * Reads the on-disk hoodie.properties (HoodieTableConfig) for a freshly written Hudi table and
   * asserts the core identity/key fields were persisted as expected. Called from the common write
   * path so every Hudi table created via writeToHudiTable is validated.
   */
  def assertHoodieTableConfig(database: String, tableName: String, basePath: String): Unit = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
    val tableConfig = metaClient.getTableConfig

    assert(tableConfig.getTableName == tableName,
      s"hoodie.table.name expected '$tableName' but got '${tableConfig.getTableName}'")
    assert(tableConfig.getDatabaseName == database,
      s"hoodie.database.name expected '$database' but got '${tableConfig.getDatabaseName}'")
    assert(tableConfig.getTableVersion.versionCode() == HoodieTableVersion.SIX.versionCode(),
      s"hoodie.table.version expected ${HoodieTableVersion.SIX.versionCode()} but got ${tableConfig.getTableVersion.versionCode()}")
    val layoutVersion = tableConfig.getTimelineLayoutVersion
    assert(layoutVersion.isPresent && layoutVersion.get().getVersion == TimelineLayoutVersion.VERSION_1,
      s"hoodie.timeline.layout.version expected ${TimelineLayoutVersion.VERSION_1} but got ${if (layoutVersion.isPresent) layoutVersion.get().getVersion else "absent"}")
    log.info(s"Validated hoodie.properties for $database.$tableName at $basePath")
  }

  def writeToHiveTable(inputDF: DataFrame, database: String, tableName: String, saveMode: SaveMode, basePath: String): Unit = {
    // Here creating an external table to insert data for easy handling.
    if (saveMode.equals(SaveMode.Overwrite)) {
      spark.sql(s"DROP TABLE IF EXISTS $database.$tableName")
      createHiveTestTable(database, tableName, basePath, true, false)
    }
    inputDF.write.
      partitionBy("partitionpath").
      mode(saveMode).
      saveAsTable(database + "." + tableName)
    spark.sql(s"REFRESH TABLE $database.$tableName")
  }

  def createHiveTestTable(database: String, tableName: String, basePath: String,
                          isPartitionedDataset: Boolean, includeHoodieMetafields: Boolean): Unit = {
    val hoodieMetafields = if (includeHoodieMetafields) {
      "_hoodie_commit_time STRING, _hoodie_commit_seqno STRING, _hoodie_record_key STRING, _hoodie_partition_path STRING, _hoodie_file_name STRING, "
    } else {
      ""
    }
    if (isPartitionedDataset) {
      spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS $database.$tableName ("
        + s"$hoodieMetafields uuid STRING, ts TIMESTAMP, rider STRING, driver STRING, begin_lat DOUBLE, begin_lon DOUBLE,"
        + s" end_lat DOUBLE, end_lon DOUBLE, fare DOUBLE) PARTITIONED BY (partitionpath String) LOCATION '$basePath'")
    } else {
      spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS $database.$tableName ("
        + s"$hoodieMetafields uuid STRING, ts TIMESTAMP, rider STRING, driver STRING, begin_lat DOUBLE, begin_lon DOUBLE,"
        + s" end_lat DOUBLE, end_lon DOUBLE, fare DOUBLE, partitionpath String) LOCATION '$basePath'")
    }
  }

  def createUpserts(database: String, tableName: String, saveMode: SaveMode, isHudiTable: Boolean): Unit = {
    val basePath = getBasePath(tableName)
    // Create inserts into Hudi dataset
    val records = QuickstartUtils.convertToStringList(dataGen.generateUpdates(5))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(spark.createDataset(recordsRDD)(Encoders.STRING))
    if (isHudiTable) {
      inputDF.write.format("hudi").
        options(QuickstartUtils.getQuickstartWriteConfigs).
        option(HoodieTableConfig.NAME.key(), database + "." + tableName).
        option(RECORDKEY_FIELD.key(), "uuid").
        option(PARTITIONPATH_FIELD.key(), "partitionpath").
        option(PRECOMBINE_FIELD.key(), "ts").
        option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true").
        option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL).
        options(hiveSyncOptionsMap(ENABLE_HIVE_SYNC, database, tableName)).
        mode(saveMode).
        save(basePath)
    } else {
      inputDF.write.
        mode(saveMode).
        insertInto(database + "." + tableName)
    }
  }

  private def hiveSyncOptionsMap(enableSync: Boolean, database: String, tableName: String): Map[String, String] = {
    Map[String, String](
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key() -> String.valueOf(enableSync),
      HiveSyncConfigHolder.HIVE_SYNC_MODE.key() -> HiveSyncMode.HMS.name(),
      HoodieSyncConfig.META_SYNC_DATABASE_NAME.key() -> database,
      HoodieSyncConfig.META_SYNC_TABLE_NAME.key() -> tableName,
      HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> "partitionpath",
      HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key() -> classOf[HiveStylePartitionValueExtractor].getName
    )
    // Non partitioned configs
    // hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor
  }

  def runSqlQueryWithAsserts(database: String, tableName: String, fullScan: Boolean, expectedVal: Int): Unit = {
    val groupIdName = "SQL query"
    spark.sparkContext.setJobGroup(groupIdName, s"SQL count query on $tableName")
    var sqlDF = spark.sql(s"SELECT * from $database.$tableName")
    if (!fullScan) {
      sqlDF = sqlDF.limit(10)
    }
    sqlDF.show()
    val size = sqlDF.count()
    log.info(s"(Is Full scan enabled? $fullScan. Records returned by the sql query on the table $database.$tableName: $size")
    sqlDF = spark.sql(s"SELECT count(*) from $database.$tableName")
    val sqlCount = sqlDF.collectAsList().get(0).getLong(0)
    assert(sqlCount == expectedVal, s"Expected $expectedVal records, but got $sqlCount")
  }

  def runDataFrameReaderWithAsserts(database: String, tableName: String, expectedVal: Int): Unit = {
    val groupIdName = "Dataframe query test"
    spark.sparkContext.setJobGroup(groupIdName, s"Fetch basepath for $tableName")
    val basePath = HoodieSparkSQLUtils.getBasePathFromTableName(spark, database + "." + tableName)
    spark.sparkContext.setJobGroup(groupIdName, s"DF count on $tableName")
    val df = spark.read.format("hudi").load(basePath)
    log.info(s"DF query was executed on $tableName")
    val dfCount = df.count()
    assert(dfCount == expectedVal, s"Expected $expectedVal records, but got $dfCount")
    log.info(s"Total records in table for df query $tableName: $dfCount")
  }

  def tableExists(database: String, tableName: String): Boolean = {
    val df = spark.sql(s"SHOW TABLES IN $database")
      .filter(row => row.getString(1).equals(tableName))
    val tableExists = df.count() == 1
    log.info(s"Table exists? $tableExists")
    tableExists
  }

  object SQL_JOIN extends Enumeration {
    type Status = Value
    val INNER_JOIN = Value(1, "JOIN")
    val LEFT_JOIN = Value(2, "LEFT JOIN")
    val RIGHT_JOIN = Value(3, "RIGHT JOIN")
    val FULL_OUTER_JOIN = Value(4, "FULL OUTER JOIN")
    val CROSS_JOIN = Value(5, "CROSS JOIN")
  }

  def readSqlFromResource(resourceName: String): String = {
    val resourceURL = Source.fromResource(resourceName)
    try resourceURL.mkString // Read the entire file content as a string
    finally resourceURL.close()
  }

  def startAndGetHoodieTimer(): HoodieTimer = {
    HoodieTimer.start()
  }
}
