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

import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

class RunHiveBasicOperations extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  def testHiveTableWrites(): Unit = {
    val database = getDatabase()
    val tableName = "hive_trips_cow_test_df_write"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = false)
    log.info("First insert done")

    // Append to dataset and check the count.
    createInserts(database, tableName, SaveMode.Append, isHudiTable = false)
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 40)

    // Overwrite and check the count.
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = false)
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 20)
  }

  def testHiveTableRead(): Unit = {
    val database = getDatabase()
    val tableName = "hive_trips_cow_test_df_read"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = false)
    log.info("First insert done, now trying DataFrame queries on Hive table")
    runSqlQueryWithAsserts(database, tableName, fullScan = true, 20)
  }

  def testDropHiveTable(): Unit = {
    val database = getDatabase()
    val tableName = "hive_trips_cow_test_drop_table"
    cleanup(tableName, getBasePath(tableName))
    createInserts(database, tableName, SaveMode.Overwrite, isHudiTable = false)
    log.info("First insert done, now trying to drop the hive table.")
    assert(tableExists(database, tableName), s"Table $database.$tableName should exist")
    cleanup(tableName, getBasePath(tableName))
    assert(!tableExists(database, tableName), s"Table $database.$tableName should not exist")
  }
}
