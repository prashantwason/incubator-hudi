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


/**
 * Interface representing replication metadata for a dataset.
 * This class provides common methods to access replication information
 * across different replication services (e.g. HiveSync, TAS).
 */
public interface HoodieRegionReplicationMetadata {

  /**
   * Get the source region for this replication metadata.
   *
   * @return The source region
   */
  Region getSource();

  /**
   * Get the destination region for this replication metadata.
   *
   * @return The destination region
   */
  Region getDestination();

  /**
   * Get whether replication is enabled for this metadata.
   *
   * @return true if replication is enabled, false otherwise
   */
  boolean getReplicationEnabled();

  /**
   * Get whether OTRS (One-Time Replication Service) is ongoing for this metadata.
   *
   * @return true if OTRS is ongoing, false otherwise
   */
  boolean isOtrsOngoing();

  /**
   * Get the duration of OTRS (One-Time Replication Service) for this metadata.
   *
   * @return The duration of OTRS in seconds. returns 0 if OTRS is not ongoing.
   */
  long getOtrsDuration();
} 