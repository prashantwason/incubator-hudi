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

package org.apache.hudi.replication.client.tas.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Request model for dataset operations in the Table Availability Service (TAS).
 * This class represents the common request structure used for various TAS operations
 * such as fetching primary regions, regional constraints, and topology information.
 */
public class DatasetRequest {
  @JsonProperty("datasets")
  private List<SingleDataset> datasets;

  @JsonProperty("client_id")
  private String clientId;

  @JsonProperty("trace_id")
  private String traceId;

  public DatasetRequest(List<SingleDataset> datasets, String clientId, String traceId) {
    this.datasets = datasets;
    this.clientId = clientId;
    this.traceId = traceId;
  }

  /**
   * Request model for a single dataset operation.
   */
  public static class SingleDataset {
    @JsonProperty("dataset_name")
    private String datasetName;

    public SingleDataset(String datasetName) {
      this.datasetName = datasetName;
    }
  }
} 