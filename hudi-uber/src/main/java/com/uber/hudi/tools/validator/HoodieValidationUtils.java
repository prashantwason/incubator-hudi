/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hudi.tools.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper functions for Hoodie validation tool.
 */
public class HoodieValidationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieValidationUtils.class);

  /**
   * Returns true if the given exception to type AccessControlException.
   */
  public static boolean isAccessControlException(Throwable e) {
    while (e != null) {
      if (e instanceof AccessControlException || e.getMessage() != null) {
        String msg = e.getMessage();
        if (msg.contains("Permission denied") || msg.contains("Client cannot authenticate")) {
          return true;
        }
      }
      // Check inner exceptions
      e = e.getCause();
    }

    return false;
  }

  /**
   * Exports a CSV file to a Hive Table.
   *
   * @param csvFilePath The path to a local CSV file
   * @param hiveTableName The name of the hive table
   * @param structType Csv file schema, contains A list of column name and column type pairs.
   * @throws IOException for any sql errors
   */
  public static void exportCsvToHiveTable(SparkSession sparkSession, Path csvFilePath, String hiveTableName, StructType structType, Configuration conf, boolean overwrite) throws IOException {
    if (hiveTableName.isEmpty()) {
      LOG.info("Empty hive table name. Skip exporting validation result to hive table");
      return;
    }

    Path destPath = new Path(csvFilePath + ".temp");
    FileSystem srcFs = csvFilePath.getFileSystem(conf);
    FileSystem destFs = destPath.getFileSystem(conf);
    FileUtil.copy(srcFs, csvFilePath, destFs, destPath, false, conf);
    csvFilePath = destPath;

    try {
      if (overwrite) {
        String dropSQL = "DROP TABLE IF EXISTS " + hiveTableName;
        LOG.info("Executing SQL: " + dropSQL);
        sparkSession.sql(dropSQL);
      }

      // Load csv file and use append mode to write into hive table.
      LOG.info("Load CSV file " + csvFilePath + ", and write the data into " + hiveTableName);
      sparkSession.read()
          .schema(structType)
          .option("maxColumns", "54000")
          .csv(csvFilePath.toString())
          .write()
          .mode(SaveMode.Append)
          .saveAsTable(hiveTableName);

    } catch (Exception e) {
      LOG.error("Exception while exporting csv file " + csvFilePath + " to hive table " + hiveTableName, e);
      throw new RuntimeException(e);
    } finally {
      destFs.delete(destPath, false);
    }
  }

  /**
   * Format the number of seconds provided into HH:MM format.
   * @param totalSeconds Total seconds
   */
  static String formatSeconds(long totalSeconds) {
    long mins = totalSeconds / 60;
    long secondsLeft = totalSeconds - (mins * 60);
    return String.format("%02d:%02d", mins, secondsLeft);
  }

  /**
   * Check if the provided basePath (or tableName) is supposed to be skipped
   * @param skipDatasetsMatchingRegex comma separated regex pattern
   * @param basePath base path
   * @param tableName table name
   * @return true if it should be skipped false otherwise
   */
  static boolean isTableSkipped(String skipDatasetsMatchingRegex, String basePath, String tableName) {
    List<Pattern> skipDatasetsMatchingRegexPatterns = new ArrayList<>();
    for (String regex : skipDatasetsMatchingRegex.split(",")) {
      if (regex.isEmpty()) {
        continue;
      }
      skipDatasetsMatchingRegexPatterns.add(Pattern.compile(regex));
    }

    for (Pattern pattern : skipDatasetsMatchingRegexPatterns) {
      if (pattern.matcher(basePath).find() || pattern.matcher(tableName).find()) {
        return true;
      }
    }

    return false;
  }
}
