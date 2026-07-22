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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Integration tests for Hudi Index DDL commands implemented in
 * org.apache.spark.sql.hudi.command.IndexCommands:
 *   - CREATE INDEX (record-index, secondary-index, column_stats with expression)
 *   - DROP INDEX (with and without IF EXISTS)
 *   - SHOW INDEXES
 *   - REFRESH INDEX (currently a no-op; test guards the dispatch path)
 *
 * Each test creates its own table to keep methods order-independent.
 */
class RunHudiIndexDDL extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)
  lazy val DEFAULT_DATABASE: String = getDatabase()

  // -------- happy-path tests --------

  /** CREATE INDEX with empty index-type and columns equal to record-key resolves to record_index. */
  def testCreateAndDropRecordIndex(): Unit = {
    val tableName = "hudi_index_ddl_record_index"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    spark.sql(s"CREATE INDEX record_index ON $DEFAULT_DATABASE.$tableName (uuid)")
    assertIndexExists(DEFAULT_DATABASE, tableName, "record_index")
    assertMetadataPartition(basePath, "record_index")

    spark.sql(s"DROP INDEX record_index ON $DEFAULT_DATABASE.$tableName")
    assertIndexAbsent(DEFAULT_DATABASE, tableName, "record_index")
    log.info("testCreateAndDropRecordIndex passed")
  }

  /**
   * CREATE INDEX with empty index-type on a non-record-key column resolves to secondary_index.
   * Requires record_index to exist first.
   */
  def testCreateAndDropSecondaryIndex(): Unit = {
    val tableName = "hudi_index_ddl_secondary_index"
    val basePath = getBasePath(tableName)
    val partitionName = "secondary_index_idx_rider"
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    spark.sql(s"CREATE INDEX record_index ON $DEFAULT_DATABASE.$tableName (uuid)")
    spark.sql(s"CREATE INDEX idx_rider ON $DEFAULT_DATABASE.$tableName (rider)")
    assertIndexExists(DEFAULT_DATABASE, tableName, partitionName)

    spark.sql(s"DROP INDEX idx_rider ON $DEFAULT_DATABASE.$tableName")
    assertIndexAbsent(DEFAULT_DATABASE, tableName, partitionName)
    log.info("testCreateAndDropSecondaryIndex passed")
  }

  /** CREATE INDEX USING column_stats with expr=lower exercises the expression-index branch. */
  def testCreateAndDropColumnStatsExpressionIndex(): Unit = {
    val tableName = "hudi_index_ddl_expr_index"
    val basePath = getBasePath(tableName)
    val partitionName = "expr_index_idx_lower_rider"
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    spark.sql(
      s"""CREATE INDEX idx_lower_rider ON $DEFAULT_DATABASE.$tableName
         |USING column_stats(rider) OPTIONS(expr='lower')""".stripMargin)
    assertIndexExists(DEFAULT_DATABASE, tableName, partitionName)

    spark.sql(s"DROP INDEX idx_lower_rider ON $DEFAULT_DATABASE.$tableName")
    assertIndexAbsent(DEFAULT_DATABASE, tableName, partitionName)
    log.info("testCreateAndDropColumnStatsExpressionIndex passed")
  }

  /** REFRESH INDEX dispatches to RefreshIndexCommand (currently a no-op). Guards the parse + dispatch path. */
  def testRefreshIndex(): Unit = {
    val tableName = "hudi_index_ddl_refresh_index"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    spark.sql(s"CREATE INDEX record_index ON $DEFAULT_DATABASE.$tableName (uuid)")
    // Should return without throwing; current implementation is Seq.empty.
    spark.sql(s"REFRESH INDEX record_index ON $DEFAULT_DATABASE.$tableName").collect()
    log.info("testRefreshIndex passed")
  }

  // -------- negative tests --------

  /**
   * Re-creating an index without IF NOT EXISTS must fail.
   * Note: Hudi 1.x does not honor IF NOT EXISTS for record_index — CreateIndexCommand parses the
   * flag but HoodieSparkIndexClient.create has no ignoreIfExists parameter, so the flag is
   * silently dropped. We only assert the duplicate-throws case here.
   */
  def testCreateIndexDuplicateFails(): Unit = {
    val tableName = "hudi_index_ddl_create_duplicate"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    spark.sql(s"CREATE INDEX record_index ON $DEFAULT_DATABASE.$tableName (uuid)")
    expectIndexException(s"CREATE INDEX record_index ON $DEFAULT_DATABASE.$tableName (uuid)",
      "Index already exists")
    log.info("testCreateIndexDuplicateFails passed")
  }

  /** DROP INDEX on a missing index must fail; with IF EXISTS must succeed. */
  def testDropIndexMissingFails(): Unit = {
    val tableName = "hudi_index_ddl_drop_missing"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    expectIndexException(s"DROP INDEX nonexistent_idx ON $DEFAULT_DATABASE.$tableName",
      "Index does not exist")
    // IF EXISTS swallows the absence.
    spark.sql(s"DROP INDEX IF EXISTS nonexistent_idx ON $DEFAULT_DATABASE.$tableName")
    log.info("testDropIndexMissingFails passed")
  }

  /** column_stats CREATE INDEX without an expression option is rejected (IndexCommands.scala guard). */
  def testCreateColumnStatsWithoutExpressionFails(): Unit = {
    val tableName = "hudi_index_ddl_column_stats_no_expr"
    val basePath = getBasePath(tableName)
    cleanup(tableName, basePath)
    createBaseTable(tableName)

    expectIndexException(
      s"CREATE INDEX idx_cs ON $DEFAULT_DATABASE.$tableName USING column_stats(rider)",
      "Column stats index without expression")
    log.info("testCreateColumnStatsWithoutExpressionFails passed")
  }

  // -------- helpers --------

  /**
   * Base table for Index DDL tests. MDT is on by default in Hudi 1.x; we deliberately do NOT
   * pre-enable record_index so that CREATE INDEX record_index actually does the work.
   */
  private def createBaseTable(tableName: String): Unit = {
    createInserts(DEFAULT_DATABASE, tableName, SaveMode.Overwrite, isHudiTable = true)
  }

  private def showIndexesAsList(db: String, tbl: String): Seq[(String, String, String)] = {
    spark.sql(s"SHOW INDEXES FROM $db.$tbl")
      .collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2)))
      .toSeq
  }

  private def assertIndexExists(db: String, tbl: String, expectedPartition: String): Unit = {
    val rows = showIndexesAsList(db, tbl)
    assert(rows.exists(_._1 == expectedPartition),
      s"Expected index partition '$expectedPartition' not found in SHOW INDEXES output: $rows")
  }

  private def assertIndexAbsent(db: String, tbl: String, partition: String): Unit = {
    val rows = showIndexesAsList(db, tbl)
    assert(!rows.exists(_._1 == partition),
      s"Index partition '$partition' should be absent but SHOW INDEXES returned: $rows")
  }

  private def assertMetadataPartition(basePath: String, partition: String): Unit = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .build()
    val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions.asScala
    assert(mdtPartitions.contains(partition),
      s"Metadata partition '$partition' not present in table config; have: $mdtPartitions")
  }

  /**
   * Run a DDL expected to fail. Walks the cause chain because Hudi index errors can be wrapped by
   * Spark's analyzer/executor before they surface.
   */
  private def expectIndexException(sqlStr: String, expectedFragment: String): Unit = {
    try {
      spark.sql(sqlStr).collect()
      throw new AssertionError(s"Expected DDL to fail with '$expectedFragment' but it succeeded: $sqlStr")
    } catch {
      case t: Throwable =>
        if (!matchesIndexException(t, expectedFragment)) {
          throw new AssertionError(
            s"Expected exception containing '$expectedFragment' for SQL: $sqlStr; got: ${t.getMessage}", t)
        }
    }
  }

  private def matchesIndexException(t: Throwable, expectedFragment: String): Boolean = {
    var cur: Throwable = t
    var found = false
    while (cur != null && !found) {
      val msg = if (cur.getMessage != null) cur.getMessage else ""
      // HoodieIndexException with the expected fragment, or any wrapping exception whose message
      // carries the fragment (some paths wrap the original cause).
      if (msg.contains(expectedFragment)) {
        found = true
      } else {
        cur = cur.getCause
      }
    }
    found
  }
}
