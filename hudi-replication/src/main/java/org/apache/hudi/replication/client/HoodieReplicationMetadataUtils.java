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

import org.apache.hudi.replication.client.tas.utils.TASPrimaryRegionApiType;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.exception.HoodieBlockWritesException;

public class HoodieReplicationMetadataUtils {
  /**
   * Create HiveSync Client and call metadata API. Get replication metadata for the given db and table.
   *
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @return replication metadata for given table
   * @throws Exception in case of any error when getting info from HiveSync metadata API
   */
  public static HoodieReplicationMetadata getReplicationMetadata(String dbName, String tableName)
          throws Exception {
    try (HoodieTASClient tasClient = new HoodieTASClient()) {
      return tasClient.getReplicationTopology(dbName + "." + tableName);
    }
  }

  /**
   * Throw a HoodieException if the region where the job is running based on provided job context is not
   * the primary region based on HiveSync replication metadata
   *
   * @param tableName the table name
   * @param datacenter the datacenter string identifying the current region (e.g. from UBER_DATACENTER env var)
   * @param apiType the TAS API type to use for determining primary regions
   */
  public static void verifyThatJobIsRunningInPrimaryRegion(String tableName, String datacenter, TASPrimaryRegionApiType apiType) throws HoodieBlockWritesException {
    Region region;
    try {
      region = Region.getRegionFromString(datacenter);
    } catch (Exception e) {
      throw new HoodieBlockWritesException("Could not fetch region from datacenter string: " + datacenter,
          e, "failed.to.fetch.region.from.spark");
    }

    // check if region from engine context is the primary region based on the replication metadata
    boolean isPrimaryRegion;
    try {
      // Use the provided TAS API type - construct TAS client directly for easier testing
      try (HoodieTASClient tasClient = new HoodieTASClient()) {
        isPrimaryRegion = tasClient.isRegionPrimary(tableName, region, apiType);
      }
    } catch (Exception e) {
      throw new HoodieBlockWritesException(
          String.format("Failed to check if %s is primary region for %s", region, tableName),
          e, "failed.to.fetch.primary.region");
    }

    if (!isPrimaryRegion) {
      throw new HoodieBlockWritesException(region + " is not primary region.", "region.not.primary");
    }
  }
}
