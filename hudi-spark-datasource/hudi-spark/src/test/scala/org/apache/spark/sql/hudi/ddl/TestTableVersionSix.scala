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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableVersion
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.assertEquals

/**
 * Unit tests guarding table version 6 behaviour for Hudi tables.
 *
 * Two things are verified:
 *  1. When a table is written as table version 6, common operations keep it on v6 and never
 *     auto-upgrade it to v9 — for both the data table and its metadata table (MDT), which both
 *     carry a table version.
 *  2. A Hudi config provided with the `spark.` prefix (e.g. `spark.hoodie.write.table.version`,
 *     `spark.hoodie.metadata.*`) is normalized to `hoodie.*` and honored. Every scenario requests
 *     the write table version via the spark-prefixed session conf, and the last test proves the
 *     override path explicitly with `spark.hoodie.metadata.enable=false`.
 *
 * The production defaults are intentionally NOT modified.
 */
class TestTableVersionSix extends HoodieSparkSqlTestBase {

  // HoodieUberConfigStore skips its datacenter/scheme check only when a surefire system property is
  // present (IS_TESTING). Scalatest does not set one, so a local file:// base path otherwise throws
  // "Unsupported scheme 'file'". Set a benign value here — in the class body, before any Hudi write
  // loads HoodieUberConfigStore — so this unit test can run against the temp file:// directory.
  System.setProperty("surefire.real.class.path",
    Option(System.getProperty("surefire.real.class.path")).getOrElse("hudi-scalatest"))

  private val sparkPrefixedWriteTableVersion = "spark." + HoodieWriteConfig.WRITE_TABLE_VERSION.key

  /** Runs `f` with `spark.hoodie.write.table.version=6` set on the session conf (spark.* prefixed). */
  private def withSparkPrefixedTableVersionSix(f: => Unit): Unit =
    withSQLConf(sparkPrefixedWriteTableVersion -> HoodieTableVersion.SIX.versionCode().toString)(f)

  /** Case 1: a single insert into a freshly created v6 table must keep it on v6. */
  test("Single insert keeps the table on version 6") {
    withSparkPrefixedTableVersionSix {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        createCowTable(tableName, tablePath)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")
        assertDataAndMetadataTableVersionIsSix(tablePath, requireMetadataTable = false)
      }
    }
  }

  /**
   * Case 2: insert followed by an update with the record-level index enabled must not upgrade the
   * table. The record index lives in the MDT, so both the data table and the MDT must remain on v6.
   * The record index is enabled via spark.* prefixed session confs to also exercise the
   * spark.hoodie.* -> hoodie.* normalization on the write path.
   */
  test("Insert and update with record index keep data and metadata table on version 6") {
    withSparkPrefixedTableVersionSix {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        withSQLConf(
          ("spark." + HoodieMetadataConfig.ENABLE.key) -> "true",
          ("spark." + HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key) -> "true") {
          createCowTable(tableName, tablePath)
          spark.sql(
            s"""insert into $tableName values
               |(1, 'a1', 10.0, 1000),
               |(2, 'a2', 20.0, 2000),
               |(3, 'a3', 30.0, 3000)""".stripMargin)
          spark.sql(s"update $tableName set price = 99.0 where id = 2")
          assertDataAndMetadataTableVersionIsSix(tablePath, requireMetadataTable = true)
        }
      }
    }
  }

  /** Case 3: creating a v6 table with the new Spark SQL syntax must not auto-upgrade it to v9. */
  test("Create table using new syntax stays on version 6") {
    withSparkPrefixedTableVersionSix {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        createCowTable(tableName, tablePath)
        // Version is set at CREATE time.
        assertTableVersionIsSix(tablePath, "data-table (after create)")
        // First write must not trigger an auto-upgrade either.
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")
        assertDataAndMetadataTableVersionIsSix(tablePath, requireMetadataTable = false)
      }
    }
  }

  /**
   * Case 3b: CREATE TABLE must honor the spark-prefixed session conf for the table version even when
   * it is NOT pinned in TBLPROPERTIES. This guards the catalog-path change (HoodieCatalogTable reads
   * spark.hoodie.* session confs): with `spark.hoodie.write.table.version=6` on the session and no
   * version in TBLPROPERTIES, the created table must still be v6 (it would otherwise default to 9).
   */
  test("Create table honors spark.hoodie.write.table.version without a tblproperties pin") {
    withSparkPrefixedTableVersionSix {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Intentionally NO hoodie.write.table.version in TBLPROPERTIES; the session conf must drive it.
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             |location '$tablePath'
             |tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |""".stripMargin)
        assertTableVersionIsSix(tablePath, "data-table (create via spark session conf, no pin)")
      }
    }
  }

  /**
   * Case 4: delete then re-create the metadata table and (re)build the record index. After this MDT
   * lifecycle, both the data table and the recreated MDT must still be on v6.
   *
   * The recreate path (`create_metadata_table` / `CREATE INDEX`) drives a write client, so the test
   * pins `hoodie.write.table.version=6` for its duration (alongside the lock provider that
   * single-writer `CREATE INDEX` requires) so the rebuilt MDT is created at v6 rather than the
   * newest default.
   */
  test("Delete and recreate metadata table with record index stays on version 6") {
    withSparkPrefixedTableVersionSix {
      // CREATE INDEX drives a write client; in single-writer mode it requires a lock provider.
      // hoodie.write.table.version=6 (unprefixed) is also set so the recreated MDT / record index
      // is built at v6 rather than falling back to the default.
      withSQLConf(
        "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        HoodieWriteConfig.WRITE_TABLE_VERSION.key -> HoodieTableVersion.SIX.versionCode().toString) {
        withTempDir { tmp =>
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          createCowTable(tableName, tablePath)
          // At CREATE time only the data table exists (the MDT is built on first write).
          assertTableVersionIsSix(tablePath, "data-table (after create)")

          // First write creates the data table and the MDT (metadata table on by default).
          spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")
          // A simple additional insert; deliberately do NOT clear the MDT here.
          spark.sql(s"insert into $tableName values (2, 'a2', 20.0, 2000)")
          assertDataAndMetadataTableVersionIsSix(tablePath, requireMetadataTable = true)

          // Delete the metadata table using the delete procedure.
          spark.sql(s"call delete_metadata_table(table => '$tableName')").collect()
          val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(tablePath)
          assert(!existsPath(mdtBasePath),
            s"Metadata table should be absent after delete_metadata_table at $mdtBasePath")

          // Re-create the metadata table using the create procedure, then build the record index.
          spark.sql(s"call create_metadata_table(table => '$tableName')").collect()
          spark.sql(s"create index record_index on $tableName (id)")

          assertDataAndMetadataTableVersionIsSix(tablePath, requireMetadataTable = true)
        }
      }
    }
  }

  /**
   * Proves the `spark.` prefix is normalized to `hoodie.*` and overrides the default on the write
   * path: with `spark.hoodie.metadata.enable=false`, the MDT must not be created.
   */
  test("spark.hoodie prefixed config is normalized and overrides the hoodie default") {
    withSparkPrefixedTableVersionSix {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        withSQLConf(("spark." + HoodieMetadataConfig.ENABLE.key) -> "false") {
          createCowTable(tableName, tablePath)
          spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")
          val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(tablePath)
          assert(!existsPath(mdtBasePath),
            s"spark.hoodie.metadata.enable=false should have disabled the metadata table at $mdtBasePath")
        }
        assertTableVersionIsSix(tablePath, "data-table")
      }
    }
  }

  // -------- helpers --------

  /**
   * Creates a COW table on table version 6, pinned explicitly via TBLPROPERTIES for determinism.
   * CREATE also honors the spark-prefixed session conf for the table version (covered by the
   * "honors spark.hoodie.write.table.version without a tblproperties pin" case); pinning here keeps
   * this helper independent of that path. The spark-prefixed conf is still set by
   * [[withSparkPrefixedTableVersionSix]] and is honored on the read / DML-write paths.
   */
  private def createCowTable(tableName: String, tablePath: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         |location '$tablePath'
         |tblproperties (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  ${HoodieWriteConfig.WRITE_TABLE_VERSION.key} = '${HoodieTableVersion.SIX.versionCode()}'
         |)
         |""".stripMargin)
  }

  /**
   * Common utility: assert that the Hudi table rooted at `tablePath` (data table or MDT) is on v6,
   * including its timeline layout version. Table version 6 uses timeline layout version 1, while
   * the 1.x table versions (8/9) use layout version 2 — so asserting the layout is 1 catches an
   * upgrade even if the table-version code lagged behind.
   */
  private def assertTableVersionIsSix(tablePath: String, label: String): Unit = {
    val metaClient = createMetaClient(spark, tablePath)
    val version = metaClient.getTableConfig.getTableVersion
    assertEquals(HoodieTableVersion.SIX.versionCode(), version.versionCode(),
      s"[$label] expected table version 6 but got ${version.versionCode()} at $tablePath")
    val layoutVersion: Int = metaClient.getTimelineLayoutVersion.getVersion
    assertEquals(TimelineLayoutVersion.VERSION_1.intValue(), layoutVersion,
      s"[$label] expected timeline layout version 1 but got $layoutVersion at $tablePath")
  }

  /**
   * Common utility: assert the data table at `tablePath` is on v6 and, when present (or required),
   * that its metadata table is also on v6.
   */
  private def assertDataAndMetadataTableVersionIsSix(tablePath: String, requireMetadataTable: Boolean): Unit = {
    assertTableVersionIsSix(tablePath, "data-table")
    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(tablePath)
    if (existsPath(mdtBasePath)) {
      assertTableVersionIsSix(mdtBasePath, "metadata-table")
    } else if (requireMetadataTable) {
      fail(s"Expected metadata table to exist at $mdtBasePath but it was not found")
    }
  }
}
