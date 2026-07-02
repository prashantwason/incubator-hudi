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

package org.apache.hudi.replication.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.replication.client.tas.utils.TASPrimaryRegionApiType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Canonical source for replication configuration, available to both Hudi ingestion/table services
 * and external replication systems (e.g., ReAir).
 *
 * <p>Contains:
 * <ul>
 *   <li>Cross-region replication flags (enabled, tertiary, quaternary, lag, TAS)</li>
 * </ul>
 *
 * <p>Checkpoint storage types (primary/secondary) are NOT defined here; they are owned solely by
 * {@code HoodieTableConfig} and persisted in {@code hoodie.properties} as the single source of truth.
 */
@ConfigClassProperty(name = "Replication Configs",
    groupName = ConfigGroups.Names.REPLICATION,
    description = "Configurations that control the Replication table service in hudi, which replicates the dataset "
        + "to multiple remote locations for Redundancy and Disaster Recovery.")
public class HoodieReplicationConfig extends HoodieConfig {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationConfig.class);

  /**
   * Hudi cross region replication related flags.
   */
  public static final ConfigProperty<String> CROSS_REGION_REPLICATION_ENABLED = ConfigProperty
      .key("hoodie.crossregion.replication.enabled")
      .defaultValue("false")
      .sinceVersion("0.8.0")
      .withDocumentation("Support for cross region replication of Hudi commits");

  public static final ConfigProperty<String> CROSS_REGION_REPLICATION_TERTIARY_ENABLED = ConfigProperty
      .key("hoodie.crossregion.replication.tertiary.enabled")
      .defaultValue("false")
      .sinceVersion("0.10.0")
      .withDocumentation("Support for cross region replication of Hudi commits to a third region");

  public static final ConfigProperty<String> CROSS_REGION_REPLICATION_QUATERNARY_ENABLED = ConfigProperty
      .key("hoodie.crossregion.replication.quaternary.enabled")
      .defaultValue("false")
      .sinceVersion("0.10.0")
      .withDocumentation("Support for cross region replication of Hudi commits to a fourth region");

  public static final ConfigProperty<Integer> CROSS_REGION_REPLICATION_MAX_LAG_HOURS = ConfigProperty
      .key("hoodie.crossregion.replication.max.lag.hours")
      .defaultValue(48)
      .sinceVersion("0.10.0")
      .withDocumentation("Table services like clean, archival will preserve the commits on the timeline for this"
          + " duration. If the replication lag exceeds this threshold, then table services will continue marking"
          + " the region as operationally down.");

  public static final ConfigProperty<Integer> CROSS_REGION_REPLICATION_MAX_LAG_HOURS_BEFORE_ALERT = ConfigProperty
      .key("hoodie.crossregion.replication.max.lag.hours.to.alert")
      .defaultValue(48)
      .sinceVersion("0.10.0")
      .withDocumentation("If the replication lag exceeds this threshold, a metric will be emitted which will "
          + "be used for alerting purposes.");

  public static final ConfigProperty<Boolean> CROSS_REGION_REPLICATION_FETCH_REPLICATION_FLAG_FROM_EXTERNAL_SOURCE = ConfigProperty
      .key("hoodie.crossregion.replication.fetch.replication.flag.from.external.source")
      .defaultValue(false)
      .sinceVersion("0.14.1")
      .withDocumentation("Use an external source (TAS service) to infer whether replication flag is enabled");

  public static final ConfigProperty<Boolean> CROSS_REGION_REPLICATION_CHECK_WRITES_ON_TARGETS = ConfigProperty
      .key("hoodie.crossregion.replication.check.writes.on.targets")
      .defaultValue(false)
      .sinceVersion("0.14.1")
      .withDocumentation("When enabled, will check if the region is primary and emit metrics when there is a "
          + "failure to check if the region is primary or if the region where the job is running is not the "
          + "primary region.");

  public static final ConfigProperty<Boolean> CROSS_REGION_REPLICATION_FAIL_WRITES_ON_TARGETS = ConfigProperty
      .key("hoodie.crossregion.replication.fail.writes.on.targets")
      .defaultValue(false)
      .sinceVersion("0.14.1")
      .withDocumentation("When enabled, all write jobs on replication targets will fail to prevent cross "
          + "region inconsistencies. This requires CROSS_REGION_REPLICATION_CHECK_WRITES_ON_TARGETS to be enabled.");

  public static final ConfigProperty<String> TAS_API_TYPE = ConfigProperty
      .key("hoodie.crossregion.replication.tas.api.type")
      .defaultValue(TASPrimaryRegionApiType.UPDATE_CONSTRAINTS.name())
      .markAdvanced()
      .sinceVersion("0.14.1")
      .withDocumentation("TAS API type to use for determining if a region is primary. "
          + "Options: 'PRIMARY_REGION' (uses GetPrimaryRegionForDatasets API) or "
          + "'UPDATE_CONSTRAINTS' (uses GetDatasetsRegionalUpdateConstraints API). "
          + "Default: 'UPDATE_CONSTRAINTS'.");

  public HoodieReplicationConfig() {
    super();
  }

  public TASPrimaryRegionApiType getTASPrimaryRegionApiType() {
    String tasApiType = getStringOrDefault(TAS_API_TYPE);
    try {
      return TASPrimaryRegionApiType.valueOf(tasApiType.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOG.info("TAS API type {} is invalid, falling back to default {}", tasApiType, TAS_API_TYPE.defaultValue());
      return TASPrimaryRegionApiType.valueOf(TAS_API_TYPE.defaultValue());
    }
  }

  public static HoodieReplicationConfig from(TypedProperties props) {
    return HoodieReplicationConfig.newBuilder().fromProperties(props).build();
  }

  public static HoodieReplicationConfig.Builder newBuilder() {
    return new HoodieReplicationConfig.Builder();
  }

  public static class Builder {
    private final HoodieReplicationConfig replicationConfig = new HoodieReplicationConfig();

    public HoodieReplicationConfig.Builder withCrossRegionReplicationEnabled(boolean enabled) {
      replicationConfig.setValue(CROSS_REGION_REPLICATION_ENABLED, Boolean.toString(enabled));
      return this;
    }

    public HoodieReplicationConfig.Builder withTertiaryCrossRegionReplicationEnabled(boolean enabled) {
      replicationConfig.setValue(CROSS_REGION_REPLICATION_TERTIARY_ENABLED, Boolean.toString(enabled));
      return this;
    }

    public HoodieReplicationConfig.Builder withQuaternaryCrossRegionReplicationEnabled(boolean enabled) {
      replicationConfig.setValue(CROSS_REGION_REPLICATION_QUATERNARY_ENABLED, Boolean.toString(enabled));
      return this;
    }

    public HoodieReplicationConfig.Builder withFetchReplicationFlagFromExternalSource(boolean shouldFetchReplicationFlagFromExternalSource) {
      replicationConfig.setValue(CROSS_REGION_REPLICATION_FETCH_REPLICATION_FLAG_FROM_EXTERNAL_SOURCE, Boolean.toString(shouldFetchReplicationFlagFromExternalSource));
      return this;
    }

    public HoodieReplicationConfig.Builder withCheckWritesOnTargets(boolean checkWritesOnTargets) {
      replicationConfig.setValue(CROSS_REGION_REPLICATION_CHECK_WRITES_ON_TARGETS, Boolean.toString(checkWritesOnTargets));
      return this;
    }

    public HoodieReplicationConfig.Builder withFailWritesOnTargets(boolean failWritesOnTargets) {
      replicationConfig.setValue(CROSS_REGION_REPLICATION_FAIL_WRITES_ON_TARGETS, Boolean.toString(failWritesOnTargets));
      return this;
    }

    public HoodieReplicationConfig.Builder withTasApiType(TASPrimaryRegionApiType apiType) {
      replicationConfig.setValue(TAS_API_TYPE, apiType.name());
      return this;
    }

    public HoodieReplicationConfig.Builder fromProperties(Properties props) {
      this.replicationConfig.getProps().putAll(props);
      return this;
    }

    public HoodieReplicationConfig build() {
      replicationConfig.setDefaults(HoodieReplicationConfig.class.getName());
      return replicationConfig;
    }
  }
}
