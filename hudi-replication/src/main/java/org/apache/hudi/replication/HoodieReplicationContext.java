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

package org.apache.hudi.replication;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.replication.table.HoodieReplicationMetrics;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.replication.table.ReplicationCheckpointStore;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.replication.client.HoodieReplicationMetadata;
import org.apache.hudi.replication.client.HoodieReplicationMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.replication.util.ReplicationPropertiesManager;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Facade for replication-related reads, replacing the replication API surface
 * that was previously bolted onto {@link HoodieTableMetaClient}.
 *
 * <h3>Read-path API (stateless)</h3>
 * Every static method reads replication state from disk (replication.properties or
 * hoodie.properties) on each call. No caching, no reload propagation needed.
 *
 * <h3>Write-path API</h3>
 * Write-path operations are owned by {@code HoodieReplicationMetadataClient}, which
 * constructs its own private MetaClient and {@link ReplicationCheckpointStore}. This class is
 * not involved in write-path lifecycle.
 */
public final class HoodieReplicationContext {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationContext.class);

  private HoodieReplicationContext() {
  }

  // ==================== Config Key Helpers ====================

  public static String getCrossRegionReplicationEnabledConfigKey(ReplicationDestination destination) {
    if (destination.equals(ReplicationDestination.SECONDARY_REGION)) {
      return ReplicationConfigKeys.CROSS_REGION_REPLICATION_ENABLED;
    }
    return String.format("hoodie.crossregion.replication.%s.enabled", destination.label.toLowerCase());
  }

  public static String getReplicationOperationalStatusConfigKey(ReplicationDestination regionId) {
    return String.format("hoodie.crossregion.replication.%s.operational.status", regionId.label);
  }

  public static String getCheckpointKeyForLastReplicatedTimestamp(ReplicationDestination destination) {
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedTimestamp(destination);
  }

  public static String getCheckpointKeyForLastReplicatedClusteringTimestamp(ReplicationDestination destination) {
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedClusteringTimestamp(destination);
  }

  public static String getCheckpointKeyForLastReplicatedArchivedTimestamp(ReplicationDestination destination) {
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedArchivedTimestamp(destination);
  }

  // ==================== Read-Path API (stateless) ====================

  /**
   * Reads a replication property from disk. Returns empty string if not found.
   */
  public static String getReplicationProperty(HoodieTableMetaClient metaClient, String key) {
    return getReplicationProperty(metaClient, key, metaClient.getMetaPath(), "");
  }

  /**
   * Checks if replication is enabled (2-arg: no external metadata fetch).
   */
  public static Option<Boolean> getCrossRegionReplicationEnabled(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId, boolean runtimeConfig) {
    return getCrossRegionReplicationEnabled(metaClient, regionId, runtimeConfig, false);
  }

  /**
   * Checks if replication is enabled (3-arg: optionally fetches from external metadata source).
   */
  public static Option<Boolean> getCrossRegionReplicationEnabled(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId,
      boolean runtimeConfig, boolean readMetadataFromExternalSource) {
    final boolean configured = getCrossRegionReplicationConfigured(
        metaClient, regionId, runtimeConfig, readMetadataFromExternalSource);
    final boolean operStatus = getCrossRegionOperationStatus(metaClient, regionId);
    return Option.of(operStatus && configured);
  }

  public static boolean getCrossRegionOperationStatus(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId) {
    final String operStatusKey = getReplicationOperationalStatusConfigKey(regionId);
    return Boolean.parseBoolean(getReplicationProperty(metaClient, operStatusKey, getBaseTableMetaPath(metaClient), "true"));
  }

  public static boolean getCrossRegionReplicationConfigured(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId,
      boolean runtimeConfig, boolean readMetadataFromExternalSource) {
    Option<HoodieReplicationMetrics> replicationMetrics = getReplicationMetrics(metaClient);

    if (readMetadataFromExternalSource) {
      try {
        boolean isReplicationEnabled = fetchReplicationMetadata(metaClient).isReplicationConfigured(regionId);
        replicationMetrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.FETCH_REPLICATION_FLAG_FROM_EXTERNAL_SOURCE_SUCCESS));
        LOG.info("Fetched {} replication flag for {} from external metadata source: {}",
            regionId, metaClient.getFullTableName(), isReplicationEnabled);
        return isReplicationEnabled;
      } catch (Exception e) {
        LOG.warn("Failed to fetch replication flag from external metadata source, falling back to replication.properties", e);
        replicationMetrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.FETCH_REPLICATION_FLAG_FROM_EXTERNAL_SOURCE_FAILED));
      }
    }

    boolean isReplicationEnabled = getCrossRegionReplicationConfigured(metaClient, regionId, runtimeConfig);
    replicationMetrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.USE_REPLICATION_PROPERTIES));
    LOG.info("Fetched {} replication flag for {} from replication.properties: {}",
        regionId, metaClient.getFullTableName(), isReplicationEnabled);
    return isReplicationEnabled;
  }

  public static boolean getCrossRegionReplicationConfigured(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId, boolean runtimeConfig) {
    final String key = getCrossRegionReplicationEnabledConfigKey(regionId);
    if (regionId.equals(ReplicationDestination.SECONDARY_REGION)) {
      return Boolean.parseBoolean(getReplicationProperty(metaClient, key, getBaseTableMetaPath(metaClient), String.valueOf(runtimeConfig)));
    }
    return Boolean.parseBoolean(getReplicationProperty(metaClient, key, getBaseTableMetaPath(metaClient), String.valueOf(false)));
  }

  public static Option<Boolean> getDatasetReplicationOperationalStatus(
      HoodieTableMetaClient metaClient, ReplicationDestination destination) {
    final String key = getReplicationOperationalStatusConfigKey(destination);
    final String enabled = getReplicationProperty(metaClient, key);
    return enabled.isEmpty() ? Option.of(true) : Option.of(Boolean.parseBoolean(enabled));
  }

  public static Option<String> getDatasetReplicationOperationalStatusReason(
      HoodieTableMetaClient metaClient, ReplicationDestination destination) {
    final String key = getReplicationOperationalStatusConfigKey(destination) + ".reason";
    final String reason = getReplicationProperty(metaClient, key);
    return reason.isEmpty() ? Option.of("all good") : Option.of(reason);
  }

  public static Option<String> getDatasetLastReplicatedTimestamp(
      HoodieTableMetaClient metaClient, ReplicationDestination destination) {
    return checkpointStoreFor(metaClient).readLastReplicatedTimestamp(destination);
  }

  public static Option<String> getDatasetLastReplicatedClusteringTimestamp(
      HoodieTableMetaClient metaClient, ReplicationDestination destination) {
    return checkpointStoreFor(metaClient).readLastReplicatedClusteringTimestamp(destination);
  }

  public static Option<String> getDatasetLastReplicatedArchivedTimestamp(
      HoodieTableMetaClient metaClient, ReplicationDestination destination) {
    return checkpointStoreFor(metaClient).readLastReplicatedArchivedTimestamp(destination);
  }

  // ==================== Internal Helpers ====================

  private static String getReplicationProperty(
      HoodieTableMetaClient metaClient, String key, StoragePath replicationMetaPath, String defaultValue) {
    String property = defaultValue;
    try {
      Properties props = readReplicationProperties(metaClient, replicationMetaPath);
      if (props.containsKey(key)) {
        property = props.getProperty(key);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read replication property '{}', returning default", key, e);
    }
    return property;
  }

  private static Properties readReplicationProperties(
      HoodieTableMetaClient metaClient, StoragePath replicationMetaPath) throws IOException {
    TypedProperties props = new TypedProperties();
    try {
      StoragePath replicationPropsPath = new StoragePath(replicationMetaPath, ReplicationPropertiesManager.REPLICATION_PROPERTIES_FILE);
      StoragePath replicationPropsBackupPath = new StoragePath(replicationMetaPath, ReplicationPropertiesManager.REPLICATION_PROPERTIES_FILE_BACKUP);
      if (metaClient.getStorage().exists(replicationPropsPath) || metaClient.getStorage().exists(replicationPropsBackupPath)) {
        props = ConfigUtils.fetchConfigs(
            metaClient.getStorage(), replicationMetaPath,
            ReplicationPropertiesManager.REPLICATION_PROPERTIES_FILE,
            ReplicationPropertiesManager.REPLICATION_PROPERTIES_FILE_BACKUP,
            1, 0);
      }
    } catch (HoodieIOException e) {
      LOG.warn("replication.properties file is empty");
    }
    return props;
  }

  private static StoragePath getBaseTableMetaPath(HoodieTableMetaClient metaClient) {
    String basePathStr = metaClient.getBasePath().toString();
    if (basePathStr.endsWith(StoragePath.SEPARATOR)) {
      basePathStr = basePathStr.substring(0, basePathStr.length() - 1);
    }
    if (basePathStr.endsWith(HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH)) {
      String metaPathStr = metaClient.getMetaPath().toString();
      return new StoragePath(metaPathStr.substring(0, metaPathStr.length()
          - HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH.length() - 1));
    }
    return metaClient.getMetaPath();
  }

  private static Option<HoodieReplicationMetrics> getReplicationMetrics(HoodieTableMetaClient metaClient) {
    String tableName = metaClient.getFullTableName();
    Registry registry = Registry.getRegistry(
        HoodieReplicationContext.class.getSimpleName() + "_Replication_" + tableName);
    return Option.of(new HoodieReplicationMetrics(registry, tableName));
  }

  private static HoodieReplicationMetadata fetchReplicationMetadata(HoodieTableMetaClient metaClient) throws Exception {
    return HoodieReplicationMetadataUtils.getReplicationMetadata(
        metaClient.getDbName(), metaClient.getMainTableName());
  }

  /**
   * Creates a read-only {@link ReplicationCheckpointStore} for the given metaClient.
   * Constructed per call (stateless); callers may cache if performance requires.
   */
  static ReplicationCheckpointStore checkpointStoreFor(HoodieTableMetaClient metaClient) {
    return new ReplicationCheckpointStore(metaClient);
  }
}
