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

package org.apache.hudi.replication.client;

import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.replication.table.ReplicationDirection;
import org.apache.hudi.replication.ReplicationRegionUtils;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieReplicationMetadata implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationMetadata.class);
  private final String tableName;
  private final Map<ReplicationDestination, Option<HoodieRegionReplicationMetadata>> forwardReplicationMetadataMap;
  private final Map<ReplicationDestination, Option<HoodieRegionReplicationMetadata>> reverseReplicationMetadataMap;

  public HoodieReplicationMetadata(String tableName, List<HoodieRegionReplicationMetadata> replicationMetadataList) {
    this.tableName = tableName;
    forwardReplicationMetadataMap = new HashMap<>();
    reverseReplicationMetadataMap = new HashMap<>();

    ReplicationRegionUtils.getAllRegions().forEach(region -> {
      forwardReplicationMetadataMap.put(region, Option.fromJavaOptional(replicationMetadataList.stream()
          .filter(metadata -> metadata.getSource().equals(ReplicationRegionUtils.getSourceDC(region, ReplicationDirection.FORWARD))
                && metadata.getDestination().equals(ReplicationRegionUtils.getTargetDC(region, ReplicationDirection.FORWARD)))
          .findFirst()));

      reverseReplicationMetadataMap.put(region, Option.fromJavaOptional(replicationMetadataList.stream()
          .filter(metadata -> metadata.getSource().equals(ReplicationRegionUtils.getSourceDC(region, ReplicationDirection.REVERSE))
              && metadata.getDestination().equals(ReplicationRegionUtils.getTargetDC(region, ReplicationDirection.REVERSE)))
          .findFirst()));
    });
  }

  /**
   * Get the table's replication metadata for the specified region and replication direction
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @param direction the replication direction (forward or reverse)
   * @return option of ReplicationRegionMetadata for the table and region. If metadata was not found for
   * that region, return an empty option
   */
  private Option<HoodieRegionReplicationMetadata> getReplicationResult(ReplicationDestination region, ReplicationDirection direction) {
    if (direction.equals(ReplicationDirection.REVERSE)) {
      return reverseReplicationMetadataMap.get(region);
    } else {
      return forwardReplicationMetadataMap.get(region);
    }
  }

  /**
   * Get whether replication is enabled for the specified region and replication direction. If no metadata is found
   * for the given replication region, return false.
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @param direction the replication direction (forward or reverse)
   * @return true if replication is enabled for the region and direction, otherwise returns false
   */
  private Boolean isReplicationEnabled(ReplicationDestination region, ReplicationDirection direction) {
    try {
      Option<HoodieRegionReplicationMetadata> replicationMetadata = getReplicationResult(region, direction);
      return replicationMetadata.isPresent() && replicationMetadata.get().getReplicationEnabled();
    } catch (Exception e) {
      LOG.info("Could not find replication metadata for " + region);
      return false;
    }
  }

  /**
   * Get whether replication is enabled for the specified region in any direction (reverse or forward)
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @return true if replication is ongoing for the region in any direction, otherwise return false
   */
  public Boolean isReplicationEnabled(ReplicationDestination region) {
    return isReplicationEnabled(region, ReplicationDirection.FORWARD) || isReplicationEnabled(region, ReplicationDirection.REVERSE);
  }

  /**
   * Get whether OTRS is ongoing for the specified region and replication direction. If no metadata is found for the
   * given replication region, return false.
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @param direction the replication direction (forward or reverse)
   * @return true if OTRS is ongoing for the region and direction, otherwise returns false
   */
  private Boolean isOtrsOngoing(ReplicationDestination region, ReplicationDirection direction) {
    try {
      Option<HoodieRegionReplicationMetadata> replicationMetadata = getReplicationResult(region, direction);
      return replicationMetadata.isPresent() && replicationMetadata.get().isOtrsOngoing();
    } catch (Exception e) {
      LOG.info("Could not find replication metadata for " + region);
      return false;
    }
  }

  /**
   * Get whether OTRS is ongoing for the specified region in any direction (reverse or forward)
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @return true if OTRS is ongoing for the region in any direction, otherwise return false
   */
  public Boolean isOtrsOngoing(ReplicationDestination region) {
    return isOtrsOngoing(region, ReplicationDirection.FORWARD) || isOtrsOngoing(region, ReplicationDirection.REVERSE);
  }

  /**
   * Gets the OTRS (One-Time Replication Sync) duration for a specific region and direction.
   * Returns 0 if no metadata exists for the given region or if OTRS is not ongoing.
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @param direction the replication direction (forward or reverse)
   * @return the duration of ongoing OTRS in seconds, or 0 if not applicable
   */
  private long getOtrsDuration(ReplicationDestination region, ReplicationDirection direction) {
    try {
      Option<HoodieRegionReplicationMetadata> replicationMetadata = getReplicationResult(region, direction);
      if (replicationMetadata.isPresent()) {
        return replicationMetadata.get().getOtrsDuration();
      }
    } catch (Exception e) {
      LOG.info("Could not find replication metadata for " + region);
    }
    return 0L;
  }

  /**
   * Get the OTRS duration in seconds
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @return the duration of ongoing OTRS in seconds, or 0 if not applicable
   */
  public long getOtrsDuration(ReplicationDestination region) {
    if (isOtrsOngoing(region, ReplicationDirection.FORWARD)) {
      return getOtrsDuration(region, ReplicationDirection.FORWARD);
    }

    if (isOtrsOngoing(region, ReplicationDirection.REVERSE)) {
      return getOtrsDuration(region, ReplicationDirection.REVERSE);
    }

    return 0L;
  }

  /**
   * Get whether replication is configured (otrs is ongoing or incremental replication is enabled) in the
   * given direction.
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @param direction the replication direction (forward or reverse)
   * @return true if otrs is ongoing or incremental replication is enabled in the given direction
   */
  public Boolean isReplicationConfigured(ReplicationDestination region, ReplicationDirection direction) {
    return isReplicationEnabled(region, direction) || isOtrsOngoing(region, direction);
  }

  /**
   * Get whether replication is configured (otrs is ongoing or incremental replication is enabled)
   *
   * @param region the replication region (secondary, tertiary, quaternary, etc.)
   * @return true if otrs is ongoing or incremental replication is enabled
   */
  public Boolean isReplicationConfigured(ReplicationDestination region) {
    return isReplicationEnabled(region) || isOtrsOngoing(region);
  }

  /**
   * Given a region, return true if this is the primary region. Return false otherwise.
   *
   * @param region the region (PHX, DCA, etc.)
   * @return true if this is the primary region, false otherwise.
   */
  public Boolean isPrimaryRegion(Region region) {
    // if the given datacenter is the target of a configured replication, return false.
    // primary regions cannot be receiving replication by definition.
    if (isRegionATargetOfConfiguredReplication(region)) {
      return false;
    }

    return true;
  }


  /**
   * Get the primary regions for the dataset
   *
   * @return set of primary regions
   */
  public Set<Region> getPrimaryRegions() {
    Set<Region> primaryRegions = new HashSet<>();
    for (Region region : Region.values()) {
      if (isPrimaryRegion(region)) {
        primaryRegions.add(region);
      }
    }

    return primaryRegions;
  }

  /**
   * Given a region and replication direction, check if the given region is the replication target of all
   * types of configured replications (i.e. secondary replication, tertiary replication, etc.)
   *
   * @param region the region (PHX, DCA, etc.)
   * @param direction direction the replication direction (forward or reverse)
   * @return true if region is target of any configured replication in the given direction
   */
  private boolean isRegionATargetOfConfiguredReplication(Region region, ReplicationDirection direction) {
    for (ReplicationDestination replicationDestination : ReplicationDestination.values()) {
      if (isReplicationConfigured(replicationDestination, direction)) {
        if (region.equals(ReplicationRegionUtils.getTargetDC(replicationDestination, direction))) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Given a region, check if the given region is the replication target of any configured replication.
   *
   * @param region the region (PHX, DCA, etc.)
   * @return true if region is target of any configured replication
   */
  private boolean isRegionATargetOfConfiguredReplication(Region region) {
    return isRegionATargetOfConfiguredReplication(region, ReplicationDirection.FORWARD)
        || isRegionATargetOfConfiguredReplication(region, ReplicationDirection.REVERSE);
  }

  public String getTableName() {
    return tableName;
  }
}