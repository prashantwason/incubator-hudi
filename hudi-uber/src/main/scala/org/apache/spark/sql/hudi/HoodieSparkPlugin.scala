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
package org.apache.spark.sql.hudi

import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.HoodieVersion
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hudi.catalog.HoodieCatalog

import java.util

class HoodieSparkPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new HoodieSparkDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new HoodieSparkExecutorPlugin
  }
}

private class HoodieSparkDriverPlugin extends DriverPlugin with Logging{

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    // Initialize the plugin and set any required configurations
    // Do not remove the below logInfo statement, it is required to debug the spark plugin and to
    // log the configuration set during plugin initialization.
    logInfo("Initializing HoodieSparkDriverPlugin")
    val conf = pluginContext.conf()

    val sparkCatalogKey = "spark.sql.catalog.spark_catalog"
    if (!conf.contains(sparkCatalogKey)) {
      val sparkCatalogVal = s"${classOf[HoodieCatalog].getName}"
      logInfo(s"Injecting configuration: $sparkCatalogKey = $sparkCatalogVal")
      conf.set(sparkCatalogKey, sparkCatalogVal)
    }

    // Relying on the Spark's Flipr config to inject "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension" config.

    // TODO: Port HybridHoodieSparkSqlFileIndex to v1.2 and re-enable this config injection
    // val fileIndexConfigKey = "spark.hoodie.datasource.read.file.index.implementation"
    // if (!conf.contains(fileIndexConfigKey)) {
    //   conf.set(fileIndexConfigKey, "org.apache.hudi.HybridHoodieSparkSqlFileIndex")
    // }

    val hmsBasedPartitionListingKey = s"spark.${DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key}"
    if (!conf.contains(hmsBasedPartitionListingKey)) {
      logInfo(s"HMS based partition listing config is not provided so injecting configuration: $hmsBasedPartitionListingKey = true")
      conf.set(hmsBasedPartitionListingKey, "true")
    }

    val useROPathFilterClassForFilteringKey = s"spark.${DataSourceReadOptions.FILE_INDEX_LIST_FILE_STATUSES_USING_RO_PATH_FILTER.key}"
    if (!conf.contains(useROPathFilterClassForFilteringKey)) {
      logInfo(s"Use RO path filter class for filtering config is not provided so injecting configuration: $useROPathFilterClassForFilteringKey = true")
      conf.set(useROPathFilterClassForFilteringKey, "true")
    }

    val partitionValueExtractorOnReadKey = s"spark.${DataSourceReadOptions.USE_PARTITION_VALUE_EXTRACTOR_ON_READ.key}"
    if (!conf.contains(partitionValueExtractorOnReadKey)) {
      logInfo(s"PartitionValueExtractor on read config is not provided so injecting configuration: $partitionValueExtractorOnReadKey = true")
      conf.set(partitionValueExtractorOnReadKey, "true")
    }

    val reconcileSchemaKey = s"spark.${HoodieCommonConfig.RECONCILE_SCHEMA.key()}"
    if (!conf.contains(reconcileSchemaKey)) {
      val reconcileSchemaVal = "true"
      logInfo(s"Reconcile schema config is not provided so injecting configuration: $reconcileSchemaKey = $reconcileSchemaVal")
      conf.set(reconcileSchemaKey, reconcileSchemaVal)
    }

    // MAKE_NEW_COLUMNS_NULLABLE does not exist in v1.2 HoodieCommonConfig
    // val newColumnsNullableKey = s"spark.${HoodieCommonConfig.MAKE_NEW_COLUMNS_NULLABLE.key()}"
    // if (!conf.contains(newColumnsNullableKey)) {
    //   conf.set(newColumnsNullableKey, "true")
    // }

    val sparkSerializer = "spark.serializer"
    if (!conf.contains(sparkSerializer)) {
      val sparkSerializerNullableVal = "org.apache.spark.serializer.KryoSerializer"
      logInfo(s"Spark serializer config is not present so injecting configuration: $sparkSerializer = $sparkSerializerNullableVal")
      conf.set(sparkSerializer, sparkSerializerNullableVal)
    }

    val hiveCboEnableKey = "spark.hadoop.hive.cbo.enable"
    if (!conf.contains(hiveCboEnableKey)) {
      val hiveCboEnableVal = "false"
      logInfo(s"Hive CBO config is not provided so injecting configuration: $hiveCboEnableKey = $hiveCboEnableVal")
      conf.set(hiveCboEnableKey, hiveCboEnableVal)
    }

    // Setting hoodie version as part of spark configuration to better track hudi releases.
    val hudiVersion = HoodieVersion.get()
    logInfo(s"Injecting configuration: spark.hoodie.version = $hudiVersion")
    conf.set("spark.hoodie.version", hudiVersion)

    java.util.Collections.emptyMap()
  }
}

class HoodieSparkExecutorPlugin extends ExecutorPlugin with Logging {
  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    // Initialize the plugin and set any required configurations
    logInfo("Initializing HoodieSparkExecutorPlugin")
  }
}
