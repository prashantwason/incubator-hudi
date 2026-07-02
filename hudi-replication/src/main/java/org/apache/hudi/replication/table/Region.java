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

import org.apache.hudi.exception.HoodieException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum Region {
  PHX,
  PHXCLD,
  DCA,
  DCACLD;

  /**
   * Gets the valid region which is defined in this enum which corresponds to the provided
   * unformatted datacenter string.
   *
   * @param dc a datacenter
   * @return valid region
   * @throws HoodieException if the given string cannot be mapped to a valid region
   */
  public static Region getRegionFromString(String dc) throws HoodieException {
    dc = dc.toLowerCase();
    if (dc.contains("cld") || dc.contains("cloud")) {
      if (dc.contains("phx")) {
        return Region.PHXCLD;
      } else if (dc.contains("dca")) {
        return Region.DCACLD;
      }
    }

    if (dc.contains("phx")) {
      return Region.PHX;
    } else if (dc.contains("dca")) {
      return Region.DCA;
    }

    throw new HoodieException("Invalid dc string, dc: " + dc);
  }

  /**
   * Gets all available regions.
   *
   * @return Set of all regions
   */
  public static Set<Region> getAllRegions() {
    return new HashSet<>(Arrays.asList(Region.values()));
  }
}
