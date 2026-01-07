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

package com.uber.hudi.tools.metrics;

import java.io.Serializable;

/**
 * Holds all metrics collected for a single Hudi table.
 */
public class TableMetrics implements Serializable {

  // Basic table information
  public String datacenter;
  public String database;
  public String tableName;
  public String basePath;
  public String tableType;  // COW or MOR
  public String isMetadataTable;  // Y or N
  public String metadataTableEnabled;  // Y or N
  public String recordIndexEnabled;  // Y or N
  public String replicationEnabled;  // Y or N
  public int recordIndexShardCount;
  public String isReplicatedPrimary;  // Y or N

  // Instant metrics for each type
  public InstantTypeMetrics commitMetrics;
  public InstantTypeMetrics cleanMetrics;
  public InstantTypeMetrics rollbackMetrics;
  public InstantTypeMetrics deltaCommitMetrics;
  public InstantTypeMetrics compactionMetrics;
  public InstantTypeMetrics replaceCommitMetrics;

  // Aggregate metrics from completed commits
  public WriteMetrics commitAggregateMetrics;

  // Aggregate metrics from completed delta commits
  public WriteMetrics deltaCommitAggregateMetrics;

  // Aggregate metrics from completed cleans
  public CleanAggregateMetrics cleanAggregateMetrics;

  // Aggregate metrics from completed replace commits
  public WriteMetrics replaceCommitAggregateMetrics;

  // Aggregate metrics from completed compactions
  public CompactionAggregateMetrics compactionAggregateMetrics;

  /**
   * Metrics for a specific instant type (commit, clean, etc).
   */
  public static class InstantTypeMetrics implements Serializable {
    public String latestTimestamp;
    public int totalCompletedCount;
    public double avgCompletionTimeMinutes;
    public double avgTimeBetweenInstantsMinutes;

    public InstantTypeMetrics() {
      this.latestTimestamp = "N/A";
      this.totalCompletedCount = 0;
      this.avgCompletionTimeMinutes = 0.0;
      this.avgTimeBetweenInstantsMinutes = 0.0;
    }
  }

  /**
   * Aggregate write metrics from commits, delta commits, or replace commits.
   */
  public static class WriteMetrics implements Serializable {
    public int totalPartitionsModified;
    public long totalRecordsInserted;
    public long totalRecordsUpdated;
    public long totalRecordsWritten;
    public int totalFilesAdded;
    public int totalFilesModified;
    public long totalDataWrittenBytes;
    public double avgRecordSizeBytes;

    // For delta commits specifically
    public int totalLogFilesAdded;
    public int totalLogFilesModified;

    public WriteMetrics() {
      this.totalPartitionsModified = 0;
      this.totalRecordsInserted = 0;
      this.totalRecordsUpdated = 0;
      this.totalRecordsWritten = 0;
      this.totalFilesAdded = 0;
      this.totalFilesModified = 0;
      this.totalDataWrittenBytes = 0;
      this.avgRecordSizeBytes = 0.0;
      this.totalLogFilesAdded = 0;
      this.totalLogFilesModified = 0;
    }
  }

  /**
   * Aggregate metrics from clean operations.
   */
  public static class CleanAggregateMetrics implements Serializable {
    public int totalPartitionsCleaned;
    public int totalFilesDeleted;

    public CleanAggregateMetrics() {
      this.totalPartitionsCleaned = 0;
      this.totalFilesDeleted = 0;
    }
  }

  /**
   * Aggregate metrics from compaction operations.
   */
  public static class CompactionAggregateMetrics implements Serializable {
    public long totalLogFilesCompacted;
    public long totalLogBlocksCompacted;
    public int totalBaseFilesCreated;
    public long totalRecordsWritten;
    public long totalDataWrittenBytes;
    public double avgRecordSizeBytes;

    public CompactionAggregateMetrics() {
      this.totalLogFilesCompacted = 0;
      this.totalLogBlocksCompacted = 0;
      this.totalBaseFilesCreated = 0;
      this.totalRecordsWritten = 0;
      this.totalDataWrittenBytes = 0;
      this.avgRecordSizeBytes = 0.0;
    }
  }

  public TableMetrics() {
    this.commitMetrics = new InstantTypeMetrics();
    this.cleanMetrics = new InstantTypeMetrics();
    this.rollbackMetrics = new InstantTypeMetrics();
    this.deltaCommitMetrics = new InstantTypeMetrics();
    this.compactionMetrics = new InstantTypeMetrics();
    this.replaceCommitMetrics = new InstantTypeMetrics();
    this.commitAggregateMetrics = new WriteMetrics();
    this.deltaCommitAggregateMetrics = new WriteMetrics();
    this.cleanAggregateMetrics = new CleanAggregateMetrics();
    this.replaceCommitAggregateMetrics = new WriteMetrics();
    this.compactionAggregateMetrics = new CompactionAggregateMetrics();
  }
}
