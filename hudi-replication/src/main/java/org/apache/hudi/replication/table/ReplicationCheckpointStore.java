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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.replication.ReplicationConfigKeys;
import org.apache.hudi.replication.util.ReplicationPropertiesManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.INIT_INSTANT_TS;

/**
 * Reads and writes replication checkpoints to the dataset's {@code replication.properties} file.
 */
public class ReplicationCheckpointStore {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationCheckpointStore.class);

  private final String tableName;
  private final ReplicationPropertiesManager propertiesManager;

  public ReplicationCheckpointStore(HoodieTableMetaClient metaClient) {
    this.tableName = metaClient.getFullTableName();
    this.propertiesManager = new ReplicationPropertiesManager(metaClient);
  }

  // ==================== Static Key Resolution ====================

  public static String getCheckpointKeyForLastReplicatedTimestamp(ReplicationDestination destination) {
    if (destination.equals(ReplicationDestination.SECONDARY_REGION)) {
      return ReplicationConfigKeys.LAST_CROSS_REGION_REPLICATED_COMMIT;
    }
    return String.format("hoodie.crossregion.replication.%s.timestamp", destination.label.toLowerCase());
  }

  public static String getCheckpointKeyForLastReplicatedClusteringTimestamp(ReplicationDestination destination) {
    if (destination.equals(ReplicationDestination.SECONDARY_REGION)) {
      return ReplicationConfigKeys.LAST_CROSS_REGION_REPLICATED_CLUSTERING_COMMIT;
    }
    return String.format("hoodie.crossregion.replication.%s.clustering.timestamp", destination.label.toLowerCase());
  }

  public static String getCheckpointKeyForLastReplicatedArchivedTimestamp(ReplicationDestination destination) {
    if (destination.equals(ReplicationDestination.SECONDARY_REGION)) {
      return ReplicationConfigKeys.LAST_CROSS_REGION_ARCHIVED_COMMIT;
    }
    return String.format("hoodie.crossregion.replication.archived.%s.timestamp", destination.label.toLowerCase());
  }

  // ==================== Read Path ====================

  public Option<String> readLastReplicatedTimestamp(ReplicationDestination destination) {
    return getDatasetCheckpoint(getCheckpointKeyForLastReplicatedTimestamp(destination));
  }

  public Option<String> readLastReplicatedClusteringTimestamp(ReplicationDestination destination) {
    return getDatasetCheckpoint(getCheckpointKeyForLastReplicatedClusteringTimestamp(destination));
  }

  public Option<String> readLastReplicatedArchivedTimestamp(ReplicationDestination destination) {
    return getDatasetCheckpoint(getCheckpointKeyForLastReplicatedArchivedTimestamp(destination));
  }

  private Option<String> getDatasetCheckpoint(String checkpointKey) {
    try {
      String checkpoint = propertiesManager.readProperties().getProperty(checkpointKey);
      return (checkpoint == null || checkpoint.isEmpty()) ? Option.of(INIT_INSTANT_TS) : Option.of(checkpoint);
    } catch (Exception e) {
      LOG.warn("Failed to read checkpoint from replication.properties for key: {}, table: {}", checkpointKey, tableName, e);
      return Option.of(INIT_INSTANT_TS);
    }
  }

  // ==================== Write Path ====================

  public void updateReplicatedCommitCheckpoint(String instantTime, ReplicationDestination regionId) {
    writeToReplicationProperties(getCheckpointKeyForLastReplicatedTimestamp(regionId), instantTime, "replicated commit");
  }

  public void updateClusteringCheckpoint(String instantTime, ReplicationDestination regionId) {
    writeToReplicationProperties(getCheckpointKeyForLastReplicatedClusteringTimestamp(regionId), instantTime, "clustering");
  }

  public void updateArchivedCheckpoint(String instantTime, ReplicationDestination regionId) {
    writeToReplicationProperties(getCheckpointKeyForLastReplicatedArchivedTimestamp(regionId), instantTime, "archival");
  }

  private void writeToReplicationProperties(String propertiesKey, String instantTime, String checkpointType) {
    try {
      this.propertiesManager.setProperty(propertiesKey, instantTime);
    } catch (Exception e) {
      throw new HoodieException("Failed to write " + checkpointType
          + " checkpoint to properties file. Table: " + tableName, e);
    }
    LOG.debug("Successfully updated {} checkpoint in properties file: {}", checkpointType, instantTime);
  }
}
