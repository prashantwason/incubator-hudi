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

package org.apache.hudi.table;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.replication.util.ReplicationStatusUtils;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.replication.util.ReplicationPropertiesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.INIT_INSTANT_TS;

public class HoodieReplicationUtilities {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationUtilities.class);

  /**
   * Calculate the lag between two timestamps, last replicated commit and most recent commit on the local timeline.
   * @param oldestNonReplicated
   * @param mostRecentInstantTs
   * @return  seconds elapsed between last replicated and most recent commit timestamp.
   */
  public static long secondsElapsedSinceOldestNonReplicatedCommit(String oldestNonReplicated, String mostRecentInstantTs) {
    try {
      // parse the instant time and calculate the difference in seconds
      return (HoodieInstantTimeGenerator.parseDateFromInstantTime(mostRecentInstantTs).getTime()
          - HoodieInstantTimeGenerator.parseDateFromInstantTime(oldestNonReplicated).getTime()) / 1000;
    } catch (Exception e) {
      LOG.warn("Failed to parse the instant time for last replicated commit " + oldestNonReplicated
          + " or most recent commit " + mostRecentInstantTs);
      return 0;
    }
  }

  /**
   * Check if the replication lag is within the SLA.
   * @param lagInSeconds - the lag which is measured between last replicated commit and most recent commit (commit start times).
   * @param slaInHrs - SLA in hours.
   *
   * @return  true if the replication lag is within the SLA, false otherwise.
   */
  public static boolean isReplicationLagWithinSLA(long lagInSeconds, int slaInHrs) {
    return lagInSeconds < (slaInHrs * 3600);
  }

  /**
   * Check if the replication lag is within the SLA.
   * @param oldestNonReplicated - last replicated commit timestamp (most recent common commit between source and destination).
   * @param mostRecentCommitTimestamp - most recent commit timestamp on the local timeline.
   *
   * @return  true if the replication lag is within the SLA, false otherwise.
   */
  public static long getReplicationLag(Option<String> oldestNonReplicated,
                                       Option<String> mostRecentCommitTimestamp) {
    if (oldestNonReplicated.isPresent() && mostRecentCommitTimestamp.isPresent()
        && !oldestNonReplicated.get().equals(INIT_INSTANT_TS)) {
      long lagInSeconds = HoodieReplicationUtilities.secondsElapsedSinceOldestNonReplicatedCommit(
          oldestNonReplicated.get(), mostRecentCommitTimestamp.get());
      LOG.warn(String.format("Replication lag between oldestNonReplicated %s and mostRecent %s in seconds: %d",
          oldestNonReplicated, mostRecentCommitTimestamp, lagInSeconds));
      return lagInSeconds;
    }
    return 0L;
  }

  public static boolean markReplicationDisabled(HoodieTableMetaClient metaClient, ReplicationDestination regionId) {
    try {
      ReplicationPropertiesManager propertiesManager = new ReplicationPropertiesManager(metaClient);
      String key = ReplicationStatusUtils.getReplicationOperationalStatusConfigKey(regionId);
      // mark dataset as operationally disabled if it is not already
      if (ReplicationStatusUtils.getCrossRegionOperationStatus(metaClient, regionId)) {
        propertiesManager.setProperty(key, "false");
        return true;
      }
    } catch (Exception e) {
      LOG.error("Failed to mark replication as disabled for region " + regionId.name(), e);
    }
    return false;
  }

  public static Map<String, Long> invalidateLaggingReplicationsIfRequired(HoodieTableMetaClient tableMetaClient,
                                                                         HoodieWriteConfig writeConfig) {
    Map<String, Long> metrics = new HashMap<>();
    Option<String> mostRecentCommitTimestamp = tableMetaClient.reloadActiveTimeline()
        .lastInstant().map(HoodieInstant::requestedTime);
    for (ReplicationDestination regionId : ReplicationDestination.values()) {
      // Replication lag is measured between last replicated commit and most recent commit (commit start times).
      Option<String> lastReplicatedTimestamp = ReplicationStatusUtils.getDatasetLastReplicatedTimestamp(tableMetaClient, regionId);
      boolean replicationEnabled = ReplicationStatusUtils.getCrossRegionReplicationEnabled(tableMetaClient, regionId,
              writeConfig.isCrossRegionReplicationEnabled(regionId.label)).get();
      if (replicationEnabled && lastReplicatedTimestamp.isPresent() && !lastReplicatedTimestamp.get().equals(INIT_INSTANT_TS)) {
        Option<String> oldestNonReplicated = tableMetaClient.getActiveTimeline().getAllCommitsTimeline()
            .filterCompletedInstants().findInstantsModifiedAfterByCompletionTime(lastReplicatedTimestamp.get())
            .firstInstant().map(HoodieInstant::requestedTime);

        long lagInSeconds = getReplicationLag(oldestNonReplicated, mostRecentCommitTimestamp);

        // emit a lag metric to use for alerting purposes
        int alertSlaInHours = writeConfig.getReplicationMaxLagInHoursBeforeAlert();
        if (!isReplicationLagWithinSLA(lagInSeconds, alertSlaInHours)) {
          LOG.warn(String.format("Replication lag for region %s has exceeded alert SLA threshold %d hours.",
                  regionId, alertSlaInHours));
          metrics.put(String.format("replication.%s.lag", regionId.name()), lagInSeconds);
        }

        // mark as operationally disabled
        int slaInHours = writeConfig.getReplicationMaxLagInHours();
        if (!isReplicationLagWithinSLA(lagInSeconds, slaInHours)) {
          LOG.warn(String.format("Replication lag for region %s has exceeded SLA threshold %d hours. Marking the "
              + "destination as operationally disabled.", regionId, slaInHours));
          boolean isMarkedDatasetAsOperationallyDisabled = markReplicationDisabled(tableMetaClient, regionId);
          if (isMarkedDatasetAsOperationallyDisabled) {
            metrics.put(String.format("replication.%s.disabled", regionId.name()), 1L);
          }
        }
      } else if (replicationEnabled) {
        LOG.debug("Skipping replication SLA evaluation for region {}: last replicated timestamp is {}",
            regionId, lastReplicatedTimestamp);
      }
    }
    return metrics;
  }
}