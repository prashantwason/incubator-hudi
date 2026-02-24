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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.config.HoodieUberConfigStore

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL procedure to display config store configurations.
 *
 * Usage:
 * {{{
 *   CALL show_config_store(path => 'hdfs://ns-router-prod-phx/uber-data/tables/my_table')
 *   CALL show_config_store(table => 'my_database.my_table')
 * }}}
 *
 * Output columns:
 * - config_type: "enforced" or "fallback"
 * - config_key: The configuration key
 * - config_value: The configuration value
 */
class ShowConfigStoreProcedure extends BaseProcedure with ProcedureBuilder {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("config_type", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("config_key", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("config_value", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))

    val basePath: String = getBasePath(tableName, tablePath)

    // Create config store by auto-detecting datacenter from path
    val configStore = HoodieUberConfigStore.fromBasePath(jsc.hadoopConfiguration(), basePath)

    val rows = new util.ArrayList[Row]

    // Add enforced configs
    val enforcedConfigs = configStore.getEnforcedConfigs
    enforcedConfigs.stringPropertyNames().asScala.toSeq.sorted.foreach { key =>
      rows.add(Row("enforced", key, enforcedConfigs.getProperty(key)))
    }

    // Add fallback configs
    val fallbackConfigs = configStore.getFallbackDefaults
    fallbackConfigs.stringPropertyNames().asScala.toSeq.sorted.foreach { key =>
      rows.add(Row("fallback", key, fallbackConfigs.getProperty(key)))
    }

    rows.asScala.toSeq
  }

  override def build: Procedure = new ShowConfigStoreProcedure()
}

object ShowConfigStoreProcedure {
  val NAME = "show_config_store"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowConfigStoreProcedure()
  }
}
