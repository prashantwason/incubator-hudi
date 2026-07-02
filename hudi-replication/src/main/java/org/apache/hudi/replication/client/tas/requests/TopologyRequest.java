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
 * Request model for fetching dataset topology information from the Table Availability Service (TAS).
 * This class corresponds to the GetDatasetsTopologyRequest proto message.
 */
public class TopologyRequest {
  @JsonProperty("dataset_names")
  private List<String> datasetNames;

  @JsonProperty("client_id")
  private String clientId;

  @JsonProperty("trace_id")
  private String traceId;

  public TopologyRequest(List<String> datasetNames, String clientId, String traceId) {
    this.datasetNames = datasetNames;
    this.clientId = clientId;
    this.traceId = traceId;
  }
} 