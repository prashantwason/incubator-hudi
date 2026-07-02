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
import java.util.List;
import java.util.Map;

/**
 * Response model for primary region dataset operations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetPrimaryRegionForDatasetsResponse {
  
  /**
   * Error code indicating that primary regions for datasets are not known.
   */
  public static final String PRIMARY_REGIONS_NOT_KNOWN_ERROR_CODE = 
      "GET_PRIMARY_REGION_FOR_DATASETS_ERROR_CODE_PRIMARY_REGIONS_FOR_DATASETS_NOT_KNOWN";
  @JsonProperty("datasetPrimaryRegionMap")
  private Map<String, DatasetResponse> datasetPrimaryRegionMap;

  @JsonProperty("error")
  private ErrorResponse error;

  public Map<String, DatasetResponse> getDatasetPrimaryRegionMap() {
    return datasetPrimaryRegionMap;
  }

  public ErrorResponse getError() {
    return error;
  }

  /**
   * Response model for dataset primary regions.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DatasetResponse {
    @JsonProperty("datasetPrimaryRegions")
    private List<String> datasetPrimaryRegions;

    public List<String> getDatasetPrimaryRegions() {
      return datasetPrimaryRegions;
    }
  }

  /**
   * Response model for primary region errors.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ErrorResponse {
    @JsonProperty("code")
    private String code;

    @JsonProperty("errorMessage")
    private String errorMessage;

    public String getCode() {
      return code;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }
}
