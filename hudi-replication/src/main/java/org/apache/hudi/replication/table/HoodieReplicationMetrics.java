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

package org.apache.hudi.replication.table;

import com.uber.m3.tally.m3.M3Reporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;
import org.apache.hudi.common.metrics.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class HoodieReplicationMetrics implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationMetrics.class);

  private static final String M3_HOST = "localhost";
  private static final int M3_PORT = 9052;
  private static M3Reporter m3Reporter;
  private static ImmutableMap<String, String> m3CommonTags;

  // Metric names
  public static final String REPLICATED = "replicated";
  public static final String REPLICATION_FAILED = "replication.failed";
  public static final String ARCHIVED_REPLICATED = "archivedReplicated";
  public static final String ARCHIVED_V2_REPLICATED = "archivedV2Replicated";
  @Deprecated
  public static final String ARCHIVED_REPLICATED_FAILED = "archivedReplicated.failed";
  public static final String REPLICATE_ARCHIVED_FAILED = "replicate.archived.failed";
  public static final String ALREADY_ARCHIVED_REPLICATED = "alreadyArchivedReplicated";
  public static final String NUMBER_OF_ARCHIVED_REPLICATED = "numberOfArchivedReplicated";
  public static final String REVERT = "revert";
  public static final String REVERT_INSTANT_NOT_FOUND = "revert.instantNotFound";
  public static final String REVERT_PENDING = "revertPending";
  public static final String REVERT_PENDING_NOT_FOUND = "revert.pendingNotFound";
  public static final String SET_CROSS_REGION_REPLICATION_ENABLED_SUCCESS = "setCrossRegionReplicationEnabled.secondary.success";
  public static final String SET_CROSS_REGION_REPLICATION_ENABLED_FAILED = "setCrossRegionReplicationEnabled.secondary.failed";
  public static final String SET_LAST_REPLICATED_CLUSTERING_COMMIT = "setLastReplicatedClustering.secondary.commit";
  public static final String SET_LAST_REPLICATED_COMMIT = "setLastReplicated.secondary.commit.success";
  public static final String SET_LAST_REPLICATED_COMMIT_INVALID = "setLastReplicated.secondary.commit.invalid";
  public static final String SET_LAST_REPLICATED_COMMIT_FAILED = "setLastReplicated.secondary.commit.failed";
  public static final String SET_LAST_REPLICATED_ARCHIVED_COMMIT = "setLastReplicatedArchived.secondary.commit.success";
  public static final String SET_LAST_REPLICATED_ARCHIVED_COMMIT_INVALID = "setLastReplicatedArchived.secondary.commit.invalid";
  public static final String SET_LAST_REPLICATED_ARCHIVED_COMMIT_FAILED = "setLastReplicatedArchived.secondary.commit.failed";

  /* Replication to third region related */
  public static final String SET_CROSS_REGION_REPLICATION_ENABLED_TERTIARY_SUCCESS = "setCrossRegionReplicationEnabled.tertiary.success";
  public static final String SET_CROSS_REGION_REPLICATION_ENABLED_TERTIARY_FAILED = "setCrossRegionReplicationEnabled.tertiary.failed";
  public static final String SET_LAST_REPLICATED_TERTIARY_CLUSTERING_COMMIT = "setLastReplicatedClustering.tertiary.commit";
  public static final String SET_LAST_REPLICATED_TERTIARY_COMMIT = "setLastReplicated.tertiary.commit.success";
  public static final String SET_LAST_REPLICATED_TERTIARY_COMMIT_INVALID = "setLastReplicated.tertiary.commit.invalid";
  public static final String SET_LAST_REPLICATED_TERTIARY_COMMIT_FAILED = "setLastReplicated.tertiary.commit.failed";
  public static final String SET_LAST_REPLICATED_TERTIARY_ARCHIVED_COMMIT = "setLastReplicatedArchived.tertiary.commit.success";
  public static final String SET_LAST_REPLICATED_TERTIARY_ARCHIVED_COMMIT_INVALID = "setLastReplicatedArchived.tertiary.commit.invalid";
  public static final String SET_LAST_REPLICATED_TERTIARY_ARCHIVED_COMMIT_FAILED = "setLastReplicatedArchived.tertiary.commit.failed";

  /* Replication to fourth region related */
  public static final String SET_CROSS_REGION_REPLICATION_ENABLED_QUATERNARY_SUCCESS = "setCrossRegionReplicationEnabled.quaternary.success";
  public static final String SET_CROSS_REGION_REPLICATION_ENABLED_QUATERNARY_FAILED = "setCrossRegionReplicationEnabled.quaternary.failed";
  public static final String SET_LAST_REPLICATED_QUATERNARY_CLUSTERING_COMMIT = "setLastReplicatedClustering.quaternary.commit";
  public static final String SET_LAST_REPLICATED_QUATERNARY_COMMIT = "setLastReplicated.quaternary.commit.success";
  public static final String SET_LAST_REPLICATED_QUATERNARY_COMMIT_INVALID = "setLastReplicated.quaternary.commit.invalid";
  public static final String SET_LAST_REPLICATED_QUATERNARY_COMMIT_FAILED = "setLastReplicated.quaternary.commit.failed";
  public static final String SET_LAST_REPLICATED_QUATERNARY_ARCHIVED_COMMIT = "setLastReplicatedArchived.quaternary.commit.success";
  public static final String SET_LAST_REPLICATED_QUATERNARY_ARCHIVED_COMMIT_INVALID = "setLastReplicatedArchived.quaternary.commit.invalid";
  public static final String SET_LAST_REPLICATED_QUATERNARY_ARCHIVED_COMMIT_FAILED = "setLastReplicatedArchived.quaternary.commit.failed";

  /* Metadata related */
  public static final String METADATA_REPLICATED = "metadataReplicated";
  public static final String METADATA_REPLICATION_FAILED = "metadataReplication.failed";
  public static final String METADATA_ARCHIVED_REPLICATED = "metadataArchivedReplicated";
  public static final String METADATA_ARCHIVED_REPLICATION_FAILED = "metadataArchivedReplication.failed";
  public static final String METADATA_REVERTED = "metadataReverted";
  public static final String METADATA_REVERT_FAILED = "metadataRevert.failed";
  public static final String COMMIT_REPLICATED = "commitReplicated";
  public static final String COMMIT_REPLICATION_FAILED = "commitReplication.failed";
  public static final String REPLACE_COMMIT_REPLICATED = "replaceCommitReplicated";
  public static final String REPLACE_COMMIT_REPLICATION_FAILED = "replaceCommitReplication.failed";
  public static final String CLEAN_REPLICATED = "cleanReplicated";
  public static final String CLEAN_REPLICATION_FAILED = "cleanReplication.failed";
  public static final String ROLLBACK_REPLICATED = "rollbackReplicated";
  public static final String ROLLBACK_REPLICATION_FAILED = "rollbackReplication.failed";
  public static final String RESTORE_REPLICATED = "restoreReplicated";
  public static final String RESTORE_REPLICATION_FAILED = "restoreReplication.failed";
  public static final String INSTANT_NOT_PRESENT = "getInstants.invalidTimestamp";
  public static final String GET_ON_COMPLETION_TIME = "getInstants.onCompletionTime";
  public static final String GET_ON_TIMESTAMP = "getInstants.onTimeStamp";
  public static final String GET_PENDING_TIMESTAMP = "getInstants.pending";
  public static final String GET_ARCHIVED_COMMITS = "getInstants.archived";

  /* Fetching replication enabled flag */
  public static final String FETCH_REPLICATION_FLAG_FROM_EXTERNAL_SOURCE_SUCCESS = "fetchReplicationFlagFromExternalSource.success";
  public static final String FETCH_REPLICATION_FLAG_FROM_EXTERNAL_SOURCE_FAILED = "fetchReplicationFlagFromExternalSource.failed";
  public static final String USE_REPLICATION_PROPERTIES = "useReplicationProperties";

  /* Checkpoint source attribution -- which path produced the checkpoint value */
  public static final String CHECKPOINT_SOURCE_PROPERTIES = "checkpoint.source.properties";

  /* Checkpoint dual-write consistency (Phase 1-2 hybrid mode) */
  public static final String CHECKPOINT_DUALWRITE_BOTH_SUCCESS = "checkpoint.dualwrite.both_success";
  public static final String CHECKPOINT_DUALWRITE_PARTIAL_FAILURE = "checkpoint.dualwrite.partial_failure";

  /* Defensive: primary checkpoint is NONE at read time (should not happen) */
  public static final String CHECKPOINT_PRIMARY_STORAGE_TYPE_NONE = "checkpoint.primary.storage_type.none";

  /* Replication config persistence to hoodie.properties */
  public static final String CHECKPOINT_CONFIG_PERSISTED = "checkpoint.config.persisted";
  public static final String CHECKPOINT_CONFIG_PERSIST_FAILURE = "checkpoint.config.persist.failure";

  private final transient Registry metricsRegistry;
  private final String tableName;
  private transient Map<String, Long> lastReportedCounts;

  public HoodieReplicationMetrics(Registry metricsRegistry, String tableName) {
    this.metricsRegistry = metricsRegistry;
    this.tableName = tableName;
    this.lastReportedCounts = new HashMap<>();
    ensureM3Reporter();
  }

  private static synchronized void ensureM3Reporter() {
    if (m3Reporter != null) {
      return;
    }
    try {
      String env = System.getenv("UBER_ENVIRONMENT");
      String datacenter = System.getenv("UBER_DATACENTER");
      ImmutableMap.Builder<String, String> tagBuilder = new ImmutableMap.Builder<>();
      tagBuilder.put("service", "hoodie");
      tagBuilder.put("component", "CrossRegionReplication");
      if (env == null || datacenter == null) {
        LOG.warn("UBER_ENVIRONMENT={}, UBER_DATACENTER={} -- metrics may be mis-tagged", env, datacenter);
      }
      tagBuilder.put("env", env != null ? env : "unknown");
      tagBuilder.put("datacenter", datacenter != null ? datacenter : "unknown");
      m3CommonTags = tagBuilder.build();

      m3Reporter = new M3Reporter.Builder(new InetSocketAddress(M3_HOST, M3_PORT))
          .includeHost(false)
          .commonTags(m3CommonTags)
          .build();
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        closeM3Reporter();
      }, "hudi-m3-reporter-shutdown"));
      LOG.info("Initialized M3Reporter for CrossRegionReplication metrics with tags: {}", m3CommonTags);
    } catch (Exception e) {
      LOG.warn("Failed to initialize M3Reporter for replication metrics; metrics will only be logged", e);
    }
  }

  public static synchronized void closeM3Reporter() {
    if (m3Reporter != null) {
      try {
        m3Reporter.close();
      } catch (Exception e) {
        LOG.warn("Error closing M3Reporter", e);
      }
      m3Reporter = null;
      m3CommonTags = null;
      LOG.info("Closed M3Reporter for CrossRegionReplication metrics");
    }
  }

  public void updateMetrics(final String action) {
    if (metricsRegistry == null) {
      return;
    }

    // Update total for count
    String countKey = action + ".count";
    metricsRegistry.add(countKey, 1);

    LOG.info(String.format("Updating replication metrics (%s=1) in %s", countKey, metricsRegistry));
  }

  public void updateMetrics(String action, long duration) {
    if (metricsRegistry == null) {
      return;
    }

    // Update total for count
    String countKey = action + ".count";
    metricsRegistry.add(countKey, 1);

    // Update duration
    String durationKey = action + ".duration";
    metricsRegistry.add(durationKey, duration);

    LOG.info(String.format("Updating replication metrics (%s=1, %s=%d) in %s",
        countKey, durationKey, duration, metricsRegistry));
  }

  public Registry registry() {
    return metricsRegistry;
  }

  public void publishMetrics() {
    if (metricsRegistry == null) {
      return;
    }
    metricsRegistry.getAllCounts().forEach((k, v) -> LOG.debug(k + " -> " + v.longValue()));
  }

  public void publishMetrics(int pendingCommits) {
    if (metricsRegistry == null) {
      return;
    }
    // transient field is null after deserialization
    if (lastReportedCounts == null) {
      lastReportedCounts = new HashMap<>();
    }
    metricsRegistry.add("tableCount", 1);
    metricsRegistry.add("pendingReplicationCount", pendingCommits);

    Map<String, Long> snapshot = registry().getAllCounts();
    snapshot.forEach((k, v) -> LOG.info(k + " -> " + v.longValue()));

    M3Reporter reporter;
    synchronized (HoodieReplicationMetrics.class) {
      reporter = m3Reporter;
    }
    if (reporter == null) {
      return;
    }

    ImmutableMap<String, String> tags = new ImmutableMap.Builder<String, String>()
        .put("table", tableName != null ? tableName : "unknown")
        .build();

    Map<String, Long> reportedValues = new HashMap<>();
    int reported = 0;
    try {
      for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
        String key = entry.getKey();
        long currentValue = entry.getValue();
        long lastValue = lastReportedCounts.getOrDefault(key, 0L);
        long delta = currentValue - lastValue;
        if (delta <= 0) {
          continue;
        }
        if (key.endsWith(".duration")) {
          reporter.reportTimer(key, tags, Duration.ofMillis(delta));
        } else {
          reporter.reportCounter(key, tags, delta);
        }
        reportedValues.put(key, currentValue);
        reported++;
      }
      reporter.flush();
      lastReportedCounts.putAll(reportedValues);
      LOG.info("Published {} replication metric deltas to M3 for table {}", reported, tableName);
    } catch (Exception e) {
      LOG.warn("Failed to publish replication metrics to M3", e);
    }
  }

  /* Package-private accessors for testing only */

  static void setM3ReporterForTest(M3Reporter reporter, ImmutableMap<String, String> tags) {
    synchronized (HoodieReplicationMetrics.class) {
      m3Reporter = reporter;
      m3CommonTags = tags;
    }
  }

  static ImmutableMap<String, String> getM3CommonTags() {
    synchronized (HoodieReplicationMetrics.class) {
      return m3CommonTags;
    }
  }

}
