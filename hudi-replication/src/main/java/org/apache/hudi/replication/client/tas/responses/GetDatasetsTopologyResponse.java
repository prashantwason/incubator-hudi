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

package org.apache.hudi.replication.client.tas.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.client.HoodieRegionReplicationMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Response model for dataset topology operations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetDatasetsTopologyResponse {
  @JsonProperty("datasetsTopology")
  private Map<String, DatasetTopology> datasetsTopology;

  public Map<String, DatasetTopology> getDatasetsTopology() {
    return datasetsTopology;
  }

  /**
   * Response model for dataset topology information.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DatasetTopology {
    @JsonProperty("replicationTopology")
    private List<ReplicationTopology> replicationTopology;

    public List<ReplicationTopology> getReplicationTopology() {
      return replicationTopology;
    }
  }

  /**
   * Response model for replication topology information.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReplicationTopology implements HoodieRegionReplicationMetadata {
    @JsonProperty("source")
    private String source;

    @JsonProperty("destination")
    private String destination;

    @JsonProperty("replicationChangedAtEpoch")
    private Long replicationChangedAtEpoch;

    @JsonProperty("status")
    private ReplicationStatus status;

    @Override
    public Region getSource() {
      return Region.getRegionFromString(source);
    }

    @Override
    public Region getDestination() {
      return Region.getRegionFromString(destination);
    }

    @Override
    public boolean getReplicationEnabled() {
      return status.equals(ReplicationStatus.REPLICATION_STATE_ENABLED);
    }

    @Override
    public boolean isOtrsOngoing() {
      return status.equals(ReplicationStatus.REPLICATION_STATE_TRANSITIONING);
    }

    @Override
    public long getOtrsDuration() {
      if (isOtrsOngoing()) {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - replicationChangedAtEpoch;
      }
      return 0L;
    }
  }

  public enum ReplicationStatus {
    REPLICATION_STATE_ENABLED,
    REPLICATION_STATE_TRANSITIONING
  }
} 