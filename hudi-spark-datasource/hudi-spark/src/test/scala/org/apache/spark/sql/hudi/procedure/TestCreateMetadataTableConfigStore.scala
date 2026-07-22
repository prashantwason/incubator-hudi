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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.config.HoodieUberConfigStore
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieTableMetadata

import java.io.{File, FileWriter}

class TestCreateMetadataTableConfigStore extends HoodieSparkProcedureTestBase {

  /**
   * Verifies that the metadata table inherits its parent data table's on-disk version when the
   * UberConfigStore explicitly enforces the table version. This exercises the
   * BaseProcedure.applyConfigStore path end-to-end with the version enforced by the config store.
   */
  test("create_metadata_table should create MDT matching data table version (config store enforces version)") {
    withTempConfigStore("hoodie.write.table.version=6\nhoodie.write.auto.upgrade=false\n") {
      assertCreateMetadataTablePreservesVersion()
    }
  }

  private def assertCreateMetadataTablePreservesVersion(): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  'hoodie.write.table.version' = '6'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Verify data table is version 6
      val dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(tableLocation)
        .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
        .build()
      assert(dataMetaClient.getTableConfig.getTableVersion.versionCode() == 6,
        "Data table should be version 6")

      // Delete existing MDT then re-create it via the stored procedure
      spark.sql(s"""call delete_metadata_table(table => '$tableName')""")
      spark.sql(s"""call create_metadata_table(table => '$tableName')""")

      // Read the MDT's hoodie.properties and verify it matches the data table version
      val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(tableLocation)
      val mdtMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(mdtBasePath)
        .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
        .build()

      assert(mdtMetaClient.getTableConfig.getTableVersion.versionCode() == 6,
        s"MDT table version should match data table (6), but was ${mdtMetaClient.getTableConfig.getTableVersion.versionCode()}")
      assert(mdtMetaClient.getTimelineLayoutVersion.getVersion == TimelineLayoutVersion.VERSION_1,
        s"MDT timeline layout version should be 1, but was ${mdtMetaClient.getTimelineLayoutVersion.getVersion}")
    }
  }

  /**
   * Runs the given body with a temporary UberConfigStore configured at a local temp directory.
   * In test mode the config store is only consulted when a test path is explicitly set, so this
   * helper makes the enforced configs take effect for the duration of the body and cleans up after.
   */
  private def withTempConfigStore(enforcedConfigs: String)(body: => Unit): Unit = {
    val configStoreDir = new File(System.getProperty("java.io.tmpdir"), "test-config-store-" + System.nanoTime())
    configStoreDir.mkdirs()
    val configStorePath = configStoreDir.getAbsolutePath
    writeFile(new File(configStorePath, "hudi_config_enforced.conf"), enforcedConfigs)
    writeFile(new File(configStorePath, "hudi_config_fallback.conf"), "")
    HoodieUberConfigStore.setTestConfigStorePath(configStorePath)
    try {
      body
    } finally {
      HoodieUberConfigStore.clearTestConfigStorePath()
      Option(configStoreDir.listFiles()).foreach(_.foreach(_.delete()))
      configStoreDir.delete()
    }
  }

  private def writeFile(file: File, content: String): Unit = {
    val fw = new FileWriter(file)
    try {
      fw.write(content)
    } finally {
      fw.close()
    }
  }
}
