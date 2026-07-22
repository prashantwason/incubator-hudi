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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.avro.Schema

/**
 * Reflection shim so one compiled runner jar runs against both Hudi 0.14 and 1.x
 * (0.16.x) runtimes. The 1.x storage layer (org.apache.hudi.storage.*) is absent in
 * 0.14, so divergent APIs are reached via reflection and raw string config keys —
 * never by importing a 1.x-only type.
 */
object VersionCompat {

  // 1.x has HadoopFSUtils (absent in 0.14).
  private lazy val is1x: Boolean = classExists("org.apache.hudi.hadoop.fs.HadoopFSUtils")

  private def classExists(name: String): Boolean =
    try { Class.forName(name); true } catch { case _: Throwable => false }

  /** Only the setConf overload differs (StorageConfiguration in 1.x, Configuration in 0.14). */
  def buildMetaClient(basePath: String, hadoopConf: Configuration): HoodieTableMetaClient = {
    val builder = HoodieTableMetaClient.builder().setBasePath(basePath)
    if (is1x) {
      val storageConf = Class.forName("org.apache.hudi.hadoop.fs.HadoopFSUtils")
        .getMethod("getStorageConf", classOf[Configuration])
        .invoke(null, hadoopConf)
      val storageConfClass = Class.forName("org.apache.hudi.storage.StorageConfiguration")
      builder.getClass.getMethod("setConf", storageConfClass).invoke(builder, storageConf)
    } else {
      builder.getClass.getMethod("setConf", classOf[Configuration]).invoke(builder, hadoopConf)
    }
    builder.build()
  }

  /** 1.x: getTableSchema().getAvroSchema(); 0.14: getTableAvroSchema(). Returns Avro Schema. */
  def tableAvroSchema(metaClient: HoodieTableMetaClient): Schema = {
    val resolverClass = Class.forName("org.apache.hudi.common.table.TableSchemaResolver")
    val resolver = resolverClass.getConstructor(classOf[HoodieTableMetaClient]).newInstance(metaClient)
    if (is1x) {
      val hoodieSchema = resolverClass.getMethod("getTableSchema").invoke(resolver)
      hoodieSchema.getClass.getMethod("getAvroSchema").invoke(hoodieSchema).asInstanceOf[Schema]
    } else {
      resolverClass.getMethod("getTableAvroSchema").invoke(resolver).asInstanceOf[Schema]
    }
  }
}
