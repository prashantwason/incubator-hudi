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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.client.HoodieHttpClient;
import org.apache.hudi.replication.client.HoodieHttpClientMetricTags;
import org.apache.hudi.replication.client.tas.utils.TASPrimaryRegionApiType;
import org.apache.hudi.replication.client.tas.requests.DatasetRequest;
import org.apache.hudi.replication.client.tas.requests.TopologyRequest;
import org.apache.hudi.replication.client.tas.responses.GetDatasetsRegionalUpdateConstraintsResponse;
import org.apache.hudi.replication.client.tas.responses.GetPrimaryRegionForDatasetsResponse;
import org.apache.hudi.replication.client.tas.responses.GetDatasetsTopologyResponse;
import org.apache.hudi.exception.HoodieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Client for interacting with the Table Availability Service (TAS).
 * This client provides methods to fetch replication metadata for Hudi datasets
 * across different regions.
 */
public class HoodieTASClient extends HoodieHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTASClient.class);
  private static final String TAS_SERVICE = "cloudlake-migrator";
  private static final String CLIENT_ID = "hudi";
  private static final String TRACE_ID = "hudi-tas-client";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

  public HoodieTASClient() {
    super(TAS_SERVICE);
  }

  /**
   * Fetches the primary regions for a given dataset.
   *
   * @param dataset The dataset name
   * @return Set of primary regions
   * @throws HoodieException if the request fails or response is invalid
   */
  public Set<Region> getPrimaryRegions(String dataset) throws HoodieException {
    validateDataset(dataset);

    HoodieHttpClientMetricTags metricTags = new HoodieHttpClientMetricTags()
        .withDatasetName(dataset)
        .withApiName(TASProcedure.GET_PRIMARY_REGION.name());

    try (CloseableHttpResponse response = execute(
        createDatasetRequest(dataset),
        TASProcedure.GET_PRIMARY_REGION.getProcedure(),
        metricTags)) {

      validateResponse(response);
      GetPrimaryRegionForDatasetsResponse primaryRegionResponse = parseResponse(
          response, GetPrimaryRegionForDatasetsResponse.class);

      // Check if there's an error indicating table does not exist, so primary regions is all regions
      if (primaryRegionResponse.getError() != null) {
        String errorCode = primaryRegionResponse.getError().getCode();
        if (GetPrimaryRegionForDatasetsResponse.PRIMARY_REGIONS_NOT_KNOWN_ERROR_CODE.equals(errorCode)) {
          LOG.warn("TAS returned error code {} for dataset {}, returning all regions", errorCode, dataset);
          return Region.getAllRegions();
        } else {
          throw new HoodieException(primaryRegionResponse.getError().getErrorMessage());
        }
      }

      List<String> regions = primaryRegionResponse.getDatasetPrimaryRegionMap().get(dataset).getDatasetPrimaryRegions();
      return extractRegions(regions);
    } catch (Exception e) {
      throw new HoodieException("Failed to get primary regions for dataset: " + dataset, e);
    }
  }

  /**
   * Fetches the primary regions for a given set of datasets with default retry logic.
   * Uses a default of 5 retries for failed datasets.
   *
   * @param datasets The set of dataset names
   * @return Map of dataset names to their primary regions
   * @throws HoodieException if the request fails or response is invalid
   */
  public Map<String, Set<Region>> getPrimaryRegions(Set<String> datasets) throws HoodieException {
    return getPrimaryRegions(datasets, 5);
  }

  /**
   * Fetches the primary regions for a given set of datasets with retry logic.
   *
   * @param datasets The set of dataset names
   * @param maxRetries The maximum number of retries for failed datasets
   * @return Map of dataset names to their primary regions
   * @throws HoodieException if the request fails or response is invalid
   */
  public Map<String, Set<Region>> getPrimaryRegions(Set<String> datasets, int maxRetries) throws HoodieException {
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be non-negative");
    }

    Set<String> remainingDatasets = new HashSet<>(datasets);
    Map<String, Set<Region>> result = new HashMap<>();
    int retryCount = 0;

    while (!remainingDatasets.isEmpty() && retryCount <= maxRetries) {
      if (retryCount > 0) {
        LOG.info("Retrying failed datasets (attempt {}/{}): {}", retryCount, maxRetries, remainingDatasets);
      }

      HoodieHttpClientMetricTags metricTags = new HoodieHttpClientMetricTags()
          .withApiName(TASProcedure.GET_PRIMARY_REGION.name());

      try (CloseableHttpResponse response = execute(
          createDatasetRequest(new ArrayList<>(remainingDatasets)),
          TASProcedure.GET_PRIMARY_REGION.getProcedure(),
          metricTags)) {

        validateResponse(response);
        GetPrimaryRegionForDatasetsResponse primaryRegionResponse = parseResponse(
            response, GetPrimaryRegionForDatasetsResponse.class);

        Map<String, GetPrimaryRegionForDatasetsResponse.DatasetResponse> datasetPrimaryRegionMap =
            primaryRegionResponse.getDatasetPrimaryRegionMap();

        Set<String> failedDatasets = new HashSet<>();

        for (String dataset : remainingDatasets) {
          if (datasetPrimaryRegionMap.containsKey(dataset)) {
            try {
              List<String> regions = datasetPrimaryRegionMap.get(dataset).getDatasetPrimaryRegions();
              result.put(dataset, extractRegions(regions));
            } catch (Exception e) {
              // If there's an error processing a specific dataset (e.g., invalid region),
              // track it for potential retry
              LOG.warn("Failed to process primary regions for dataset {}: {}", dataset, e.getMessage());
              failedDatasets.add(dataset);
            }
          } else {
            // Dataset not found in response, add to failed datasets for retry
            LOG.warn("Dataset {} not found in response, will retry", dataset);
            failedDatasets.add(dataset);
          }
        }

        // Update remaining datasets to only include failed ones
        remainingDatasets = failedDatasets;
        retryCount++;

      } catch (Exception e) {
        LOG.error("Failed to get primary regions for datasets (attempt {}/{}): {}", 
            retryCount, maxRetries, remainingDatasets, e);
        
        if (retryCount >= maxRetries) {
          throw new HoodieException("Failed to get primary regions for datasets after " 
            +  maxRetries + " retries: " + remainingDatasets, e);
        }
        retryCount++;
      }
    }

    if (!remainingDatasets.isEmpty()) {
      LOG.warn("Failed to process primary regions for datasets after {} retries: {}", 
          maxRetries, remainingDatasets);
    }

    return result;
  }

  /**
   * Check if the given region is primary for the specified table using the specified API type.
   *
   * @param dataset The dataset name
   * @param region The region to check
   * @param apiType The TAS API type to use for determining primary regions
   * @return true if the region is primary, false otherwise
   * @throws HoodieException if there's an error checking the region
   */
  public boolean isRegionPrimary(String dataset, Region region, TASPrimaryRegionApiType apiType) throws HoodieException {
    Set<Region> primaryRegions;
    
    if (apiType == TASPrimaryRegionApiType.UPDATE_CONSTRAINTS) {
      // Use update constraints API - if region is in allowed update regions, it's primary
      primaryRegions = getAllowedUpdateRegions(dataset);
    } else {
      // Use primary region API (default)
      primaryRegions = getPrimaryRegions(dataset);
    }
    
    return primaryRegions.contains(region);
  }

  /**
   * Fetches the allowed update regions for a given dataset.
   *
   * @param dataset The dataset name
   * @return Set of allowed update regions
   * @throws HoodieException if the request fails or response is invalid
   */
  public Set<Region> getAllowedUpdateRegions(String dataset) throws HoodieException {
    validateDataset(dataset);

    HoodieHttpClientMetricTags metricTags = new HoodieHttpClientMetricTags()
        .withDatasetName(dataset)
        .withApiName(TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.name());

    try (CloseableHttpResponse response = execute(
        createDatasetRequest(dataset),
        TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure(),
        metricTags)) {

      validateResponse(response);
      GetDatasetsRegionalUpdateConstraintsResponse constraintsResponse = parseResponse(
          response, GetDatasetsRegionalUpdateConstraintsResponse.class);

      // Check if there's an error indicating table does not exist, so allowed regions is all regions
      if (constraintsResponse.getError() != null) {
        String errorCode = constraintsResponse.getError().getCode();
        if (GetDatasetsRegionalUpdateConstraintsResponse.VALID_UPDATE_REGIONS_NOT_KNOWN_ERROR_CODE.equals(errorCode)) {
          LOG.warn("TAS returned error code {} for dataset {}, returning all regions", errorCode, dataset);
          return Region.getAllRegions();
        } else {
          throw new HoodieException(constraintsResponse.getError().getErrorMessage());
        }
      }

      List<String> regions = new ArrayList<>(constraintsResponse.getUpdateConstraints().get(dataset).getAllowedRegions().keySet());
      return extractRegions(regions);
    } catch (Exception e) {
      throw new HoodieException("Failed to get allowed update regions for dataset: " + dataset, e);
    }
  }

  /**
   * Fetches the replication topology information for a given dataset.
   * This returns the list of replication directions configured for the dataset.
   *
   * @param dataset The dataset name
   * @return List of replication topology information containing source and destination regions
   * @throws HoodieException if the request fails or response is invalid
   */
  public HoodieReplicationMetadata getReplicationTopology(String dataset) throws HoodieException {
    LOG.info("Getting replication topology for {}", dataset);
    HoodieHttpClientMetricTags metricTags = new HoodieHttpClientMetricTags()
        .withDatasetName(dataset)
        .withApiName(TASProcedure.GET_TOPOLOGY.name());

    try (CloseableHttpResponse response = execute(
        createTopologyRequest(dataset),
        TASProcedure.GET_TOPOLOGY.getProcedure(),
        metricTags)) {

      validateResponse(response);
      GetDatasetsTopologyResponse topologyResponse = parseResponse(
          response, GetDatasetsTopologyResponse.class);

      Map<String, GetDatasetsTopologyResponse.DatasetTopology> topologies = topologyResponse.getDatasetsTopology();
      if (!topologies.containsKey(dataset)) {
        throw new HoodieException("No replication topology information found for dataset: " + dataset);
      }

      List<HoodieRegionReplicationMetadata> replicationMetadataList = topologies.get(dataset).getReplicationTopology()
          .stream()
          .map(metadata -> (HoodieRegionReplicationMetadata) metadata)
          .collect(Collectors.toList());
      return new HoodieReplicationMetadata(dataset, replicationMetadataList);
    } catch (Exception e) {
      throw new HoodieException("Failed to get replication topology for dataset: " + dataset, e);
    }
  }

  private void validateDataset(String dataset) {
    if (dataset == null || dataset.trim().isEmpty()) {
      throw new IllegalArgumentException("Dataset name cannot be null or empty");
    }
  }

  private void validateResponse(CloseableHttpResponse response) throws IOException {
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != 200) {
      String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
      throw new HoodieException(String.format("Request failed with status %d: %s", statusCode, responseBody));
    }
  }

  private <T> T parseResponse(CloseableHttpResponse response, Class<T> responseType) throws IOException {
    String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
    return OBJECT_MAPPER.readValue(responseBody, responseType);
  }

  private Set<Region> extractRegions(List<String> regions) {
    Set<Region> allowedRegions = new HashSet<>();
    regions.forEach(region -> allowedRegions.add(Region.getRegionFromString(region)));
    return allowedRegions;
  }

  private HttpPost buildHttpPost(Object requestBodyPayload) throws IOException {
    String requestJson = OBJECT_MAPPER.writeValueAsString(requestBodyPayload);
    LOG.debug("Sending request: {}", requestJson);

    HttpPost httpPost = new HttpPost(getURL());
    httpPost.setEntity(new StringEntity(requestJson));
    return httpPost;
  }

  protected HttpPost createDatasetRequest(String dataset) throws IOException {
    return createDatasetRequest(Collections.singletonList(dataset));
  }

  protected HttpPost createDatasetRequest(List<String> datasets) throws IOException {
    List<DatasetRequest.SingleDataset> datasetReqList = datasets.stream().map(DatasetRequest.SingleDataset::new).collect(Collectors.toList());
    DatasetRequest requestBody = new DatasetRequest(datasetReqList, CLIENT_ID, TRACE_ID);
    return buildHttpPost(requestBody);
  }

  private HttpPost createTopologyRequest(String dataset) throws IOException {
    List<String> datasetNames = Collections.singletonList(dataset);
    TopologyRequest requestBody = new TopologyRequest(datasetNames, CLIENT_ID, TRACE_ID);
    return buildHttpPost(requestBody);
  }

  /* Enum used for fetching RPC procedures for all TAS endpoints */
  public enum TASProcedure {
    GET_PRIMARY_REGION("TableAvailabilityService", "GetPrimaryRegionForDatasets"),
    GET_REGIONAL_UPDATE_CONSTRAINTS("TableAvailabilityService", "GetDatasetsRegionalUpdateConstraints"),
    GET_TOPOLOGY("DatasetsTopologyService", "GetDatasetsTopology");

    private static final String TAS_PROCEDURE_BASE = "uber.data.cloudlake.migrator.v2.protos.";

    final String protoName;
    final String rpcProcedure;

    TASProcedure(String protoName, String procedure) {
      this.protoName = protoName;
      this.rpcProcedure = procedure;
    }

    public String getProcedure() {
      return TAS_PROCEDURE_BASE + protoName + "::" + rpcProcedure;
    }
  }
}
