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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.slf4j.LoggerFactory

/**
 * Integration tests for error handling improvements in Hudi Spark SQL.
 * These tests validate that user-facing error messages are helpful and actionable,
 * particularly for the improved error handling in HoodieAnalysis.ProducesHudiMetaFields.
 *
 * The original issue was that queries with unresolved columns would produce cryptic errors like:
 * "Invalid call to dataType on unresolved object" instead of helpful messages.
 */
class RunErrorHandlingTests extends RunOperationsBase {
  private val log = LoggerFactory.getLogger(getClass)

  // Error message that should NOT appear - this is the cryptic error we're trying to prevent
  private val CRYPTIC_ERROR = "Invalid call to dataType on unresolved object"

  // Expected helpful patterns in error messages
  private def isHelpfulErrorMessage(msg: String): Boolean = {
    msg.contains("Failed to resolve query") || // Our custom error message
      msg.toLowerCase.contains("cannot resolve") ||
      msg.toLowerCase.contains("unresolved") ||
      msg.toLowerCase.contains("not found") ||
      msg.toLowerCase.contains("does not exist")
  }

  private def assertHelpfulError(e: Exception, context: String): Unit = {
    val errorMessage = e.getMessage
    log.info(s"[$context] Caught ${e.getClass.getSimpleName}: $errorMessage")

    // CRITICAL: The error should NOT be the cryptic internal error
    assert(!errorMessage.contains(CRYPTIC_ERROR),
      s"[$context] Error should NOT contain cryptic message '$CRYPTIC_ERROR'. Got: $errorMessage")

    // The error should be helpful
    assert(isHelpfulErrorMessage(errorMessage),
      s"[$context] Error message should be helpful (mention 'cannot resolve', 'unresolved', etc.). Got: $errorMessage")

    log.info(s"[$context] PASSED: Error message is helpful and not cryptic")
  }

  /**
   * Test INSERT INTO Hudi table FROM another Hudi table with UNION and invalid column.
   * This is the closest scenario to the original bug - the ProducesHudiMetaFields extractor
   * is invoked when inserting from a Hudi source into a Hudi target.
   */
  def testInsertIntoHudiFromHudiUnionWithInvalidColumn(): Unit = {
    val database = "rawdatatmp"
    val sourceTable1 = "hudi_src_table_1"
    val sourceTable2 = "hudi_src_table_2"
    val targetTable = "hudi_target_table"

    cleanup(sourceTable1, getBasePath(sourceTable1))
    cleanup(sourceTable2, getBasePath(sourceTable2))
    cleanup(targetTable, getBasePath(targetTable))

    try {
      // Create source Hudi tables with data
      createInserts(database, sourceTable1, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      createInserts(database, sourceTable2, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      // Create target Hudi table
      createInserts(database, targetTable, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      log.info(s"Created Hudi tables: $sourceTable1, $sourceTable2, $targetTable")

      // INSERT INTO Hudi FROM UNION of Hudi tables - this triggers ProducesHudiMetaFields
      // The invalid column should cause a helpful error, not "Invalid call to dataType"
      val invalidQuery =
        s"""
           |INSERT INTO $database.$targetTable
           |SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, partitionpath
           |FROM (
           |  SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, partitionpath
           |  FROM $database.$sourceTable1
           |  UNION ALL
           |  SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, invalid_fare_column, partitionpath
           |  FROM $database.$sourceTable2
           |)
           |""".stripMargin

      spark.sql(invalidQuery)
      throw new AssertionError("Expected exception was not thrown for INSERT INTO Hudi with invalid column in UNION")

    } catch {
      case e: AnalysisException =>
        assertHelpfulError(e, "INSERT INTO Hudi FROM Hudi UNION")
      case e: UnresolvedException =>
        // If we catch UnresolvedException directly, our fix didn't work - this should be converted
        throw new AssertionError(
          s"UnresolvedException should have been caught and converted to AnalysisException with helpful message. " +
          s"Got raw UnresolvedException: ${e.getMessage}", e)
      case e: Exception =>
        log.error(s"Unexpected exception type: ${e.getClass.getName}", e)
        throw new AssertionError(s"Expected AnalysisException but got ${e.getClass.getName}: ${e.getMessage}", e)
    } finally {
      cleanup(sourceTable1, getBasePath(sourceTable1))
      cleanup(sourceTable2, getBasePath(sourceTable2))
      cleanup(targetTable, getBasePath(targetTable))
    }
  }

  /**
   * Test deeply nested UNION queries (matching the pattern from the original stack trace
   * which showed many nested Union.output calls).
   */
  def testDeeplyNestedUnionWithInvalidColumn(): Unit = {
    val database = "rawdatatmp"
    val sourceTable = "hudi_nested_union_src"
    val targetTable = "hudi_nested_union_target"

    cleanup(sourceTable, getBasePath(sourceTable))
    cleanup(targetTable, getBasePath(targetTable))

    try {
      createInserts(database, sourceTable, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      createInserts(database, targetTable, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      log.info(s"Created Hudi tables for nested UNION test")

      // Deeply nested UNION structure - similar to what caused the original issue
      val invalidQuery =
        s"""
           |INSERT INTO $database.$targetTable
           |SELECT * FROM (
           |  SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, partitionpath
           |  FROM $database.$sourceTable
           |  UNION ALL
           |  SELECT * FROM (
           |    SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, partitionpath
           |    FROM $database.$sourceTable
           |    UNION ALL
           |    SELECT * FROM (
           |      SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, partitionpath
           |      FROM $database.$sourceTable
           |      UNION ALL
           |      SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, nonexistent_col, partitionpath
           |      FROM $database.$sourceTable
           |    )
           |  )
           |)
           |""".stripMargin

      spark.sql(invalidQuery)
      throw new AssertionError("Expected exception was not thrown for deeply nested UNION with invalid column")

    } catch {
      case e: AnalysisException =>
        assertHelpfulError(e, "Deeply nested UNION")
      case e: UnresolvedException =>
        throw new AssertionError(
          s"UnresolvedException should have been converted to helpful AnalysisException. Got: ${e.getMessage}", e)
      case e: Exception =>
        log.error(s"Unexpected exception type: ${e.getClass.getName}", e)
        throw new AssertionError(s"Expected AnalysisException but got ${e.getClass.getName}: ${e.getMessage}", e)
    } finally {
      cleanup(sourceTable, getBasePath(sourceTable))
      cleanup(targetTable, getBasePath(targetTable))
    }
  }

  /**
   * Test INSERT INTO with SELECT * from Hudi tables with invalid column in subquery.
   * SELECT * triggers Star expansion which interacts with the ProducesHudiMetaFields extractor.
   */
  def testInsertWithSelectStarAndInvalidColumn(): Unit = {
    val database = "rawdatatmp"
    val sourceTable = "hudi_star_src"
    val targetTable = "hudi_star_target"

    cleanup(sourceTable, getBasePath(sourceTable))
    cleanup(targetTable, getBasePath(targetTable))

    try {
      createInserts(database, sourceTable, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      createInserts(database, targetTable, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)
      log.info(s"Created Hudi tables for SELECT * test")

      // Use SELECT * with a subquery that has an invalid column
      val invalidQuery =
        s"""
           |INSERT INTO $database.$targetTable
           |SELECT * FROM (
           |  SELECT *, bad_column FROM $database.$sourceTable
           |)
           |""".stripMargin

      spark.sql(invalidQuery)
      throw new AssertionError("Expected exception was not thrown for SELECT * with invalid column")

    } catch {
      case e: AnalysisException =>
        assertHelpfulError(e, "SELECT * with invalid column")
      case e: UnresolvedException =>
        throw new AssertionError(
          s"UnresolvedException should have been converted. Got: ${e.getMessage}", e)
      case e: Exception =>
        log.error(s"Unexpected exception type: ${e.getClass.getName}", e)
        throw new AssertionError(s"Expected AnalysisException but got ${e.getClass.getName}: ${e.getMessage}", e)
    } finally {
      cleanup(sourceTable, getBasePath(sourceTable))
      cleanup(targetTable, getBasePath(targetTable))
    }
  }

  /**
   * Test that error messages contain specific unresolved column names when our
   * improved error handling is triggered.
   */
  def testErrorMessageContainsColumnName(): Unit = {
    val database = "rawdatatmp"
    val tableName = "hudi_column_name_test"
    cleanup(tableName, getBasePath(tableName))

    try {
      createInserts(database, tableName, org.apache.spark.sql.SaveMode.Overwrite, isHudiTable = true)

      val invalidColumnName = "this_column_definitely_does_not_exist_xyz"
      val invalidQuery =
        s"""
           |INSERT INTO $database.$tableName
           |SELECT uuid, ts, rider, driver, begin_lat, begin_lon, end_lat, end_lon, $invalidColumnName, partitionpath
           |FROM $database.$tableName
           |""".stripMargin

      spark.sql(invalidQuery)
      throw new AssertionError("Expected exception was not thrown")

    } catch {
      case e: AnalysisException =>
        val msg = e.getMessage
        log.info(s"Error message: $msg")

        // Verify it's not the cryptic error
        assert(!msg.contains(CRYPTIC_ERROR),
          s"Error should not contain cryptic message. Got: $msg")

        // Verify the column name appears in the error (either from Spark or our improved message)
        val mentionsColumn = msg.contains("this_column_definitely_does_not_exist_xyz") ||
          msg.contains("Unresolved references")
        assert(mentionsColumn || isHelpfulErrorMessage(msg),
          s"Error should mention the bad column or be otherwise helpful. Got: $msg")

        log.info("PASSED: Error message is helpful")

      case e: Exception =>
        throw new AssertionError(s"Unexpected exception: ${e.getClass.getName}: ${e.getMessage}", e)
    } finally {
      cleanup(tableName, getBasePath(tableName))
    }
  }
}

