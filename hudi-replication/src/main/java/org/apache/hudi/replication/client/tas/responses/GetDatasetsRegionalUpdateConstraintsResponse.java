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
import java.util.Map;

/**
 * Response model for regional update constraints.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetDatasetsRegionalUpdateConstraintsResponse {
  
  /**
   * Error code indicating that valid update regions for datasets are not known.
   */
  public static final String VALID_UPDATE_REGIONS_NOT_KNOWN_ERROR_CODE = 
      "GET_DATASETS_REGIONAL_UPDATE_CONSTRAINTS_ERROR_CODE_VALID_UPDATE_REGIONS_FOR_DATASETS_NOT_KNOWN";
  @JsonProperty("updateConstraints")
  private Map<String, DatasetConstraints> updateConstraints;

  @JsonProperty("error")
  private ErrorResponse error;

  public Map<String, DatasetConstraints> getUpdateConstraints() {
    return updateConstraints;
  }

  public ErrorResponse getError() {
    return error;
  }

  /**
   * Response model for dataset constraints.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DatasetConstraints {
    @JsonProperty("allowedRegions")
    private Map<String, String> allowedRegions;

    public Map<String, String> getAllowedRegions() {
      return allowedRegions;
    }
  }

  /**
   * Response model for regional update constraints errors.
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