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

/**
 * Cross-region replication configuration key constants.
 * Extracted from {@code HoodieTableMetaClient} to decouple replication surface.
 */
public final class ReplicationConfigKeys {
  private ReplicationConfigKeys() {
  }

  public static final String CROSS_REGION_REPLICATION_ENABLED = "hoodie.crossregion.replication.enabled";
  public static final String LAST_CROSS_REGION_REPLICATED_COMMIT = "hoodie.crossregion.replication.timestamp";
  public static final String LAST_CROSS_REGION_REPLICATED_CLUSTERING_COMMIT = "hoodie.crossregion.replication.clustering.timestamp";
  public static final String LAST_CROSS_REGION_ARCHIVED_COMMIT = "hoodie.crossregion.replication.archived.timestamp";

  public static final String CROSS_REGION_REPLICATION_TERTIARY_ENABLED = "hoodie.crossregion.replication.tertiary.enabled";
  public static final String LAST_CROSS_REGION_REPLICATED_TERTIARY_COMMIT = "hoodie.crossregion.replication.tertiary.timestamp";
  public static final String LAST_CROSS_REGION_REPLICATED_TERTIARY_CLUSTERING_COMMIT = "hoodie.crossregion.replication.tertiary.clustering.timestamp";
  public static final String LAST_CROSS_REGION_ARCHIVED_TERTIARY_COMMIT = "hoodie.crossregion.replication.archived.tertiary.timestamp";

  public static final String CROSS_REGION_REPLICATION_QUATERNARY_ENABLED = "hoodie.crossregion.replication.quaternary.enabled";
  public static final String LAST_CROSS_REGION_REPLICATED_QUATERNARY_COMMIT = "hoodie.crossregion.replication.quaternary.timestamp";
  public static final String LAST_CROSS_REGION_REPLICATED_QUATERNARY_CLUSTERING_COMMIT = "hoodie.crossregion.replication.quaternary.clustering.timestamp";
  public static final String LAST_CROSS_REGION_ARCHIVED_QUATERNARY_COMMIT = "hoodie.crossregion.replication.archived.quaternary.timestamp";
}
