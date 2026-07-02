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

import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.replication.table.ReplicationDirection;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReplicationRegionUtils {
  /**
   * Gets map of replication to a pair of DCs where this replication takes place. For example, quaternary
   * replication refers to replication which happens across PHX and PHXCLD. So, quaternary replication would
   * map to (PHX, PHXCLD).
   *
   * @return replication region -> pair of DCs
   */
  public static Map<ReplicationDestination, Pair<Region, Region>> getDCPairsForAllRegions() {
    Map<ReplicationDestination, Pair<Region, Region>> regionInfo = new HashMap<>();
    regionInfo.put(ReplicationDestination.SEPTENARY_REGION, Pair.of(Region.DCA, Region.DCACLD));
    regionInfo.put(ReplicationDestination.SENARY_REGION, Pair.of(Region.PHXCLD, Region.DCACLD));
    regionInfo.put(ReplicationDestination.QUINARY_REGION, Pair.of(Region.PHXCLD, Region.DCA));
    regionInfo.put(ReplicationDestination.QUATERNARY_REGION, Pair.of(Region.PHX, Region.PHXCLD));
    regionInfo.put(ReplicationDestination.TERTIARY_REGION, Pair.of(Region.PHX, Region.DCACLD));
    regionInfo.put(ReplicationDestination.SECONDARY_REGION, Pair.of(Region.PHX, Region.DCA));

    return regionInfo;
  }

  /**
   * Gets the DC pair for a given region. For example, given the quaternary region, this method would
   * return (PHX, PHXCLD)
   *
   * @param region replication region such as quaternary, tertiary, etc.
   * @return the replication pair (region1, region2) for the given replication type
   */
  public static Pair<Region, Region> getDCPair(ReplicationDestination region) {
    return getDCPairsForAllRegions().get(region);
  }

  /**
   * Gets the DC which is the source of replication for a given region and replication type.
   * For example, (quaternary, false) would return PHX
   *
   * @param region replication region such as quaternary, tertiary, etc.
   * @param direction the replication direction (forward or reverse)
   * @return the DC which is the source of replication
   */
  public static Region getSourceDC(ReplicationDestination region, ReplicationDirection direction) {
    Pair<Region, Region> dcPair = getDCPair(region);
    if (direction.equals(ReplicationDirection.REVERSE)) {
      return dcPair.getRight();
    } else {
      return dcPair.getLeft();
    }
  }

  /**
   * Gets the DC which is the target of replication for a given region and replication type.
   * For example, (quaternary, true) would return PHXCLD
   *
   * @param region replication region such as quaternary, tertiary, etc.
   * @param direction the replication direction (forward or reverse)
   * @return the DC which is the target of replication
   */
  public static Region getTargetDC(ReplicationDestination region, ReplicationDirection direction) {
    Pair<Region, Region> dcPair = getDCPair(region);
    if (direction.equals(ReplicationDirection.REVERSE)) {
      return dcPair.getLeft();
    } else {
      return dcPair.getRight();
    }
  }

  /**
   * Gets all valid replication regions.
   *
   * @return a set of replication regions
   */
  public static Set<ReplicationDestination> getAllRegions() {
    return getDCPairsForAllRegions().keySet();
  }

  /**
   * Gets a set of replication regions where the given dc is either a source or target.
   *
   * @param dc a datacenter
   * @return set of replication regions
   */
  public static Set<ReplicationDestination> getValidReplicationRegionsForDC(String dc) throws HoodieException {
    Region region = Region.getRegionFromString(dc);
    Set<ReplicationDestination> validRegions = new HashSet<>();
    for (ReplicationDestination replicationDestination : getAllRegions()) {
      if (region.equals(getTargetDC(replicationDestination, ReplicationDirection.FORWARD))) {
        validRegions.add(replicationDestination);
      } else if (region.equals(getSourceDC(replicationDestination, ReplicationDirection.FORWARD))) {
        validRegions.add(replicationDestination);
      }
    }

    return validRegions;
  }
}
