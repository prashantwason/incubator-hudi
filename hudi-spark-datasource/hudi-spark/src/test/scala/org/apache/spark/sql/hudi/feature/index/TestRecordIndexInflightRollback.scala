/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.feature.index

import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.WriteClientTestUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient
import org.apache.hudi.common.model.WriteConcurrencyMode
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

/**
 * Tests for the opt-in `rollbackStaleInflightCommits` CREATE INDEX option. The rollback itself is
 * the heartbeat-respecting `rollbackFailedWrites` (stale/expired inflight only; active-heartbeat
 * inflight preserved). These tests verify the option gating and end-to-end behavior.
 */
class TestRecordIndexInflightRollback extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
  }

  test("Opt-in rolls back stale (expired-heartbeat) inflight before record index creation") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTableAndInsert(tableName, basePath)
      val metaClient = buildMetaClient(basePath)

      // Inflight commit whose heartbeat is stopped/expired.
      val inflightTime = startInflightCommit(basePath, stopHeartbeat = true)
      assertTrue(isInflightPresent(metaClient, inflightTime),
        "Inflight commit should be present before CREATE INDEX")
      assertFalse(HoodieHeartbeatClient.heartbeatExists(metaClient.getStorage, basePath, inflightTime),
        "Heartbeat file should not exist for expired commit")

      spark.sql(s"create index record_index on $tableName (id) options (rollbackStaleInflightCommits = 'true')")

      assertFalse(isInflightPresent(buildMetaClient(basePath), inflightTime),
        "Stale inflight commit should have been rolled back during CREATE INDEX")
      assertRecordIndexCreated(tableName)
    }
  }

  test("Default (option absent) does not roll back inflight before record index creation") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTableAndInsert(tableName, basePath)
      val metaClient = buildMetaClient(basePath)

      val inflightTime = startInflightCommit(basePath, stopHeartbeat = true)
      assertTrue(isInflightPresent(metaClient, inflightTime),
        "Inflight commit should be present before CREATE INDEX")

      // No option -> opt-in rollback does not run.
      spark.sql(s"create index record_index on $tableName (id)")

      assertTrue(isInflightPresent(buildMetaClient(basePath), inflightTime),
        "Inflight commit should NOT be rolled back when the option is absent")
      assertRecordIndexCreated(tableName)
    }
  }

  test("Active-heartbeat inflight is preserved even when option is set") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTableAndInsert(tableName, basePath)
      val metaClient = buildMetaClient(basePath)

      // Inflight commit whose heartbeat is left active (only timers stopped, file kept).
      val inflightTime = startInflightCommit(basePath, stopHeartbeat = false)
      assertTrue(HoodieHeartbeatClient.heartbeatExists(metaClient.getStorage, basePath, inflightTime),
        "Heartbeat file should exist for active commit")

      spark.sql(s"create index record_index on $tableName (id) options (rollbackStaleInflightCommits = 'true')")

      assertTrue(isInflightPresent(buildMetaClient(basePath), inflightTime),
        "Inflight commit with active heartbeat should NOT be rolled back (heartbeat respected)")
      assertRecordIndexCreated(tableName)
    }
  }

  test("Option is ignored for non-record (secondary) index via the derived branch") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      // Record index + secondary index enabled so a secondary index can be created.
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey = 'id',
           |  type = 'cow',
           |  preCombineField = 'ts',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.metadata.index.secondary.enable = 'true'
           | )
           | partitioned by(ts)
           | location '$basePath'
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 1001)")

      val metaClient = buildMetaClient(basePath)
      val inflightTime = startInflightCommit(basePath, stopHeartbeat = true)
      assertTrue(isInflightPresent(metaClient, inflightTime),
        "Inflight commit should be present before CREATE INDEX")

      // 'name' is not a record key -> derived branch resolves to secondary_index; the option must
      // be ignored so the stale inflight is left in place.
      spark.sql(s"create index idx_name on $tableName (name) options (rollbackStaleInflightCommits = 'true')")

      assertTrue(isInflightPresent(buildMetaClient(basePath), inflightTime),
        "Inflight commit should NOT be rolled back for a (derived) secondary index")
      assertTrue(spark.sql(s"show indexes from default.$tableName").collect()
        .exists(r => r.getString(0).equals("secondary_index_idx_name")),
        "Secondary index should have been created")
    }
  }

  private def createTableAndInsert(tableName: String, basePath: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | options (
         |  primaryKey = 'id',
         |  type = 'cow',
         |  preCombineField = 'ts',
         |  hoodie.metadata.enable = 'true'
         | )
         | partitioned by(ts)
         | location '$basePath'
       """.stripMargin)
    spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
    spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 1001)")
  }

  private def buildMetaClient(basePath: String): HoodieTableMetaClient =
    HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf))
      .setBasePath(basePath)
      .build()

  /**
   * Starts an inflight commit (no completion). When stopHeartbeat is true the heartbeat file is
   * deleted (simulating an expired/crashed writer); otherwise only the heartbeat timers are stopped
   * so the heartbeat file remains, simulating a live concurrent writer.
   */
  private def startInflightCommit(basePath: String, stopHeartbeat: Boolean): String = {
    val inflightTime = WriteClientTestUtils.createNewInstantTime()
    val jsc = new JavaSparkContext(spark.sparkContext)
    val engineContext = new HoodieSparkEngineContext(jsc)
    // OCC mode auto-sets LAZY cleaning policy via autoAdjustConfigsForConcurrencyMode.
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .withEmbeddedTimelineServerEnabled(false)
      .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
      .build()
    val writeClient = new SparkRDDWriteClient(engineContext, writeConfig)
    try {
      WriteClientTestUtils.startCommitWithTime(writeClient, inflightTime)
      if (stopHeartbeat) {
        writeClient.getHeartbeatClient().stop(inflightTime)
      } else {
        writeClient.getHeartbeatClient().stopHeartbeatTimers()
      }
    } finally {
      writeClient.close()
    }
    inflightTime
  }

  private def isInflightPresent(metaClient: HoodieTableMetaClient, inflightTime: String): Boolean =
    metaClient.reloadActiveTimeline().filterInflightsAndRequested()
      .filter(i => !i.getAction.equals("indexing"))
      .getInstantsAsStream
      .anyMatch(i => i.requestedTime().equals(inflightTime))
  private def assertRecordIndexCreated(tableName: String): Unit =
    checkAnswer(s"show indexes from default.$tableName")(
      Seq("column_stats", "column_stats", ""),
      Seq("partition_stats", "partition_stats", ""),
      Seq("record_index", "record_index", "")
    )
}
