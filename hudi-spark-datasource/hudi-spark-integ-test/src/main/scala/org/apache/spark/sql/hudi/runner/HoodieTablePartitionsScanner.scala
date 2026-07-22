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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import scala.collection.immutable
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Try

object HoodieTablePartitionsScanner {
  private val log = LoggerFactory.getLogger(getClass)
  val NON_HOODIE_PARTITIONS_TABLE_NAME = "rawdatatmp.hudi_partition_scan_results"

  def main(args: Array[String]): Unit = {
    log.info(s"Args: ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.set("spark.hadoop.hive.cbo.enable", "false")
    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
    run(sparkSession)
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

  def run(sparkSession: SparkSession): Unit = {
    // Log hudi version used.
    val databases = List("rawdata", "secure_rawdata", "rawdata_user", "secure_rawdata_user")
    val tableNamePrefix = "kafka_"
    val tableNameExclusionPrefix = "kafka_logging_"
    val excludedTableNames = immutable.HashSet("")
    scanPartitionsAndStoreNew(sparkSession, databases, tableNamePrefix, tableNameExclusionPrefix, excludedTableNames)
  }

  def isHoodieTable(inputFormat: String): Boolean = {
    inputFormat match {
      case input if input != null && input.contains("Hoodie") => true
      case _ => false
    }
  }
  private def scanPartitionsAndStoreNew(sparkSession: SparkSession, databases: List[String],
                                        tableNamePrefix: String, tableNameExclusionPrefix: String,
                                        excludedTableNames: immutable.HashSet[String]): Unit = {
    val externalCatalog = sparkSession.sessionState.catalog.externalCatalog
    var allRDDs: Seq[RDD[HoodiePartitionScannerInfo]] = Seq()
    databases.foreach { database =>
      log.info(s"Scanning tables for database: $database")
      val tablesSeq: Seq[String] = externalCatalog.listTables(database)
        .filter(tableName => tableName.startsWith(tableNamePrefix)
          && !tableName.startsWith(tableNameExclusionPrefix)
          && !excludedTableNames.contains(tableName))
      log.info(s"Found ${tablesSeq.size} tables in database: $database")
      val serializableConfiguration = new SerializableConfiguration(sparkSession.sparkContext.hadoopConfiguration)
      if (tablesSeq.nonEmpty) {
        val databaseScanRdd: RDD[HoodiePartitionScannerInfo] = sparkSession.sparkContext.parallelize(tablesSeq, tablesSeq.size)
          .flatMap(tableName => {
            Try {
              val client: IMetaStoreClient = fetchMetastoreClient(serializableConfiguration)
              val tableStorageDesc = client.getTable(database, tableName).getSd
              val isHoodie = isHoodieTable(tableStorageDesc.getInputFormat)
              val basepath = tableStorageDesc.getLocation
              var metaClient: HoodieTableMetaClient = null
              Try {
                if (isHoodie) {
                  metaClient = HoodieTableMetaClient.builder()
                    .setBasePath(basepath).setConf(HadoopFSUtils.getStorageConfWithCopy(serializableConfiguration.value))
                    .build()
                } else {
                  null
                }
              } match {
                case util.Success(null) =>
                  log.warn(s"Table $database.$tableName is not a Hoodie table, skipping partition scan.")
                  Seq(HoodiePartitionScannerInfo(
                    tableName = s"$database.$tableName",
                    partitionValue = Seq.empty,
                    partitionPath = "",
                    isHoodiePartitionInHMS = false,
                    errorMessage = "Not a Hoodie table",
                    status = true
                  ))
                case util.Success(_) =>
                  val partitionsList = client.listPartitions(database, tableName, -1)
                  if (partitionsList != null && !partitionsList.isEmpty) {
                    val partitions: Seq[HoodiePartitionScannerInfo] = partitionsList.asScala
                      .map(p => {
                        val isHoodiePartitionInHMS = p.getSd.getInputFormat match {
                          case input if input != null && input.contains("Hoodie") => true
                          case _ => false
                        }
                        HoodiePartitionScannerInfo(
                          tableName = s"$database.$tableName",
                          partitionValue = p.getValues.asScala.toSeq,
                          partitionPath = p.getSd.getLocation,
                          isHoodiePartitionInHMS = isHoodiePartitionInHMS
                        )
                      }).toSeq
                    checkForHoodiePartitions(metaClient, database, tableName, partitions)
                  } else {
                    val failedInfo = HoodiePartitionScannerInfo(
                      tableName = s"$database.$tableName",
                      partitionValue = Seq.empty,
                      partitionPath = "",
                      isHoodiePartitionInHMS = false,
                      errorMessage = "No partitions found in HMS",
                      status = false
                    )
                    Seq(failedInfo)
                  }
                case util.Failure(exception) =>
                  val failedInfo = HoodiePartitionScannerInfo(
                    tableName = s"$database.$tableName",
                    partitionValue = Seq.empty,
                    partitionPath = "",
                    isHoodiePartitionInHMS = false,
                    errorMessage = s"Unable to create meta-client: ${exception.getMessage}",
                    status = false
                  )
                  Seq(failedInfo)
              }
            } match {
              case util.Success(result) => result
              case util.Failure(exception) =>
                val failedInfo = HoodiePartitionScannerInfo(
                  tableName = s"$database.$tableName",
                  partitionValue = Seq.empty,
                  partitionPath = "",
                  isHoodiePartitionInHMS = false,
                  errorMessage = s"Unable to scan table: ${exception.getMessage}",
                  status = false
                )
                Seq(failedInfo)
            }
          })
        allRDDs = allRDDs :+ databaseScanRdd
      }
    }
    // Convert all RDDs to Datasets and union them efficiently
    import sparkSession.implicits._
    val allPartitionResults = if (allRDDs.nonEmpty) {
      allRDDs
        .map(rdd => rdd.toDS())
        .reduce(_.union(_))
    } else {
      sparkSession.emptyDataset[HoodiePartitionScannerInfo]
    }

    // Store all partition scan results in a single table
    val outputTableName = getOutputTableName
    storePartitionScanResults(sparkSession, allPartitionResults, outputTableName)

    sparkSession.sql(s"select * from $outputTableName").show(100, truncate = false)
  }

  def fetchMetastoreClient(serializableConfiguration: SerializableConfiguration): IMetaStoreClient = {
    val conf = serializableConfiguration.value
    val hiveSyncConfig = new HiveSyncConfig(new Properties(), conf)
    Hive.get(hiveSyncConfig.getHiveConf).getMSC()
  }

  private case class HoodiePartitionScannerInfo(
                                                 tableName: String,
                                                 partitionValue: Seq[String],
                                                 partitionPath: String,
                                                 isHoodiePartitionInHMS: Boolean,
                                                 var isHoodiePartitionInFS: Boolean = false,
                                                 var isPartitionEmpty: Boolean = false,
                                                 var hoodieFormatFiles: Seq[String] = Seq.empty,
                                                 var nonHoodieFormatFiles: Seq[String] = Seq.empty,
                                                 var errorMessage: String = "",
                                                 var status: Boolean = true
                                               ) extends Serializable

  private def storePartitionScanResults(sparkSession: SparkSession, partitionScanResults: Dataset[HoodiePartitionScannerInfo], outputTableName: String): Unit = {
    import org.apache.spark.sql.functions.col
    val renamedDF = partitionScanResults.select(
      col("tableName").alias("table_name"),
      col("partitionValue").alias("partition_value"),
      col("partitionPath").alias("partition_path"),
      col("isHoodiePartitionInHMS").alias("is_hoodie_partition_in_hms"),
      col("isHoodiePartitionInFS").alias("is_hoodie_partition_in_file_system"),
      col("isPartitionEmpty").alias("is_partition_empty"),
      col("hoodieFormatFiles").alias("top_3_hoodie_format_files"),
      col("nonHoodieFormatFiles").alias("top_3_non_hoodie_format_files"),
      col("errorMessage").alias("error_message"),
      col("status")
    )
    renamedDF.write
      .mode("overwrite")
      .option("path", s"${getPathPrefix(sparkSession)}/non_hoodie/$outputTableName")
      .saveAsTable(outputTableName)
  }

  private def getOutputTableName: String = {
    val dateFormat = new SimpleDateFormat("_yyyy_MM_dd_HHmmss")
    NON_HOODIE_PARTITIONS_TABLE_NAME + dateFormat.format(new Date())
  }

  private def getPathPrefix(sparkSession: SparkSession) = {
    val datacenter = sparkSession.sparkContext.conf.get("spark.drogon.dc", "phx")
    val routerPrefix = getPathPrefixForDatacenter(datacenter)
    routerPrefix + "/user/hudi"
  }

  private def checkForHoodiePartitions(metaClient: HoodieTableMetaClient, database: String,
                                          tableName: String, partitionsSeq: Seq[HoodiePartitionScannerInfo]):
  Seq[HoodiePartitionScannerInfo] = {
    if (partitionsSeq.isEmpty) {
      Seq(HoodiePartitionScannerInfo(
        tableName = s"$database.$tableName",
        partitionValue = Seq.empty,
        partitionPath = "",
        isHoodiePartitionInHMS = false,
        errorMessage = "Empty partitions provided.",
        status = false
      ))
    } else {
      val storage = metaClient.getStorage
      var count = 0
      partitionsSeq.map(partitionInfo => {
        Try {
          count = count + 1
          if (count % 10 == 1) {
            log.info(s"Scanning partition: $count and value: ${partitionInfo.partitionValue}")
          }
          val partitionStoragePath = new StoragePath(partitionInfo.partitionPath)
          val partitionPath = new Path(partitionInfo.partitionPath)

          // 1. Check if it's a Hoodie partition in filesystem using partition metadata
          partitionInfo.isHoodiePartitionInFS = HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionStoragePath)

          // 2. If no _hoodie_partition_metadata, check if directory is empty and look for Hoodie files
          if (!partitionInfo.isHoodiePartitionInFS) {
            val fs = partitionPath.getFileSystem(metaClient.getStorageConf.unwrapAs(classOf[org.apache.hadoop.conf.Configuration]))
            if (fs.exists(partitionPath) && fs.isDirectory(partitionPath)) {
              val fileIterator = fs.listFiles(partitionPath, false)
              var isEmpty = true
              var hoodieFormatFiles: Seq[String] = Seq.empty
              var nonHoodieFormatFiles: Seq[String] = Seq.empty

              while (fileIterator.hasNext && (isEmpty || hoodieFormatFiles.size < 3)) {
                val fileStatus = fileIterator.next()
                val fileName = fileStatus.getPath.getName

                if (isEmpty) {
                  isEmpty = false
                }

                // Check if file follows Hoodie naming convention
                val storagePath = new StoragePath(fileStatus.getPath.toString)
                if (FSUtils.isBaseFile(storagePath) || FSUtils.isLogFile(storagePath)) {
                  hoodieFormatFiles = hoodieFormatFiles :+ fileName
                } else if (fileName.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)) {
                  hoodieFormatFiles = hoodieFormatFiles :+ fileName
                } else {
                  nonHoodieFormatFiles = nonHoodieFormatFiles :+ fileName
                }
              }

              partitionInfo.isPartitionEmpty = isEmpty
              partitionInfo.hoodieFormatFiles = hoodieFormatFiles
              partitionInfo.nonHoodieFormatFiles = nonHoodieFormatFiles
            } else {
              partitionInfo.isPartitionEmpty = true
              partitionInfo.hoodieFormatFiles = Seq.empty
            }
          } else {
            partitionInfo.isPartitionEmpty = false
            val fs = partitionPath.getFileSystem(metaClient.getStorageConf.unwrapAs(classOf[org.apache.hadoop.conf.Configuration]))
            val fileIterator = fs.listFiles(partitionPath, false)
            var hoodieFiles: Seq[String] = Seq.empty
            var fileCount = 0
            while (fileIterator.hasNext && fileCount < 3) {
              val fileName = fileIterator.next().getPath.getName
              hoodieFiles = hoodieFiles :+ fileName
              fileCount += 1
            }
            partitionInfo.hoodieFormatFiles = hoodieFiles
            partitionInfo.nonHoodieFormatFiles = Seq.empty
          }

          partitionInfo
        } match {
          case util.Success(result) => result
          case util.Failure(e) =>
            partitionInfo.copy(errorMessage = s"${e.getMessage}: ${e.getStackTrace.take(5).mkString("\n")}", status = false)
        }
      })
    }
  }
}
