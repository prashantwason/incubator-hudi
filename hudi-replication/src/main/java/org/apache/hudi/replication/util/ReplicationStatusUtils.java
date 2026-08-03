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

package org.apache.hudi.replication.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.replication.config.HoodieReplicationConfig;
import org.apache.hudi.replication.table.ReplicationCheckpointStore;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Read-only helpers for cross-region replication status, backed solely by {@code replication.properties}.
 *
 * <p>This intentionally does not support fetching the replication-enabled flag from an external metadata
 * source (TAS), since that requires {@code HoodieTableMetaClient#getDbName()}/{@code #getMainTableName()},
 * which are not yet available on this branch. Callers needing that capability should route through
 * {@code HoodieReplicationContext} once it is ported (tracked separately as UFlash-coupled work).
 */
public class ReplicationStatusUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationStatusUtils.class);

  private ReplicationStatusUtils() {
  }

  public static String getCrossRegionReplicationEnabledConfigKey(ReplicationDestination destination) {
    if (destination.equals(ReplicationDestination.SECONDARY_REGION)) {
      return HoodieReplicationConfig.CROSS_REGION_REPLICATION_ENABLED.key();
    }
    return String.format("hoodie.crossregion.replication.%s.enabled", destination.label.toLowerCase());
  }

  public static String getReplicationOperationalStatusConfigKey(ReplicationDestination regionId) {
    return String.format("hoodie.crossregion.replication.%s.operational.status", regionId.label);
  }

  public static String getCheckpointKeyForLastReplicatedTimestamp(ReplicationDestination destination) {
    // Delegate to the checkpoint store so the read key always matches the written key
    // (SECONDARY_REGION uses the legacy region-less key).
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedTimestamp(destination);
  }

  public static boolean getCrossRegionOperationStatus(HoodieTableMetaClient metaClient, ReplicationDestination regionId) {
    final String operStatusKey = getReplicationOperationalStatusConfigKey(regionId);
    return Boolean.parseBoolean(getReplicationProperty(metaClient, operStatusKey, getBaseTableMetaPath(metaClient), "true"));
  }

  /**
   * Checks if replication is configured and operationally enabled, reading only from replication.properties.
   */
  public static Option<Boolean> getCrossRegionReplicationEnabled(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId, boolean runtimeConfig) {
    final boolean configured = getCrossRegionReplicationConfigured(metaClient, regionId, runtimeConfig);
    final boolean operStatus = getCrossRegionOperationStatus(metaClient, regionId);
    return Option.of(operStatus && configured);
  }

  public static boolean getCrossRegionReplicationConfigured(
      HoodieTableMetaClient metaClient, ReplicationDestination regionId, boolean runtimeConfig) {
    final String key = getCrossRegionReplicationEnabledConfigKey(regionId);
    if (regionId.equals(ReplicationDestination.SECONDARY_REGION)) {
      return Boolean.parseBoolean(getReplicationProperty(metaClient, key, getBaseTableMetaPath(metaClient), String.valueOf(runtimeConfig)));
    }
    return Boolean.parseBoolean(getReplicationProperty(metaClient, key, getBaseTableMetaPath(metaClient), String.valueOf(false)));
  }

  public static Option<String> getDatasetLastReplicatedTimestamp(
      HoodieTableMetaClient metaClient, ReplicationDestination destination) {
    final String checkpoint = getReplicationProperty(metaClient, getCheckpointKeyForLastReplicatedTimestamp(destination));
    return checkpoint.isEmpty() ? Option.of(HoodieTimeline.INIT_INSTANT_TS) : Option.of(checkpoint);
  }

  private static String getReplicationProperty(HoodieTableMetaClient metaClient, String key) {
    return getReplicationProperty(metaClient, key, metaClient.getMetaPath(), "");
  }

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
    Properties props = new Properties();
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
}
