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

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.replication.client.HoodieHttpClientMetricTags;
import org.apache.hudi.replication.client.tas.utils.TASPrimaryRegionApiType;
import org.apache.hudi.exception.HoodieException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestHoodieTASClient {
  private CloseableHttpResponse mockResponse;
  private HoodieTASClient tasClient;
  private static final String PRIMARY_REGION_RESPONSE = "tas-responses/primary-region-response.json";
  private static final String ALLOWED_UPDATE_REGIONS_RESPONSE = "tas-responses/allowed-update-regions-response.json";
  private static final String MULTIPLE_PRIMARY_REGIONS_RESPONSE = "tas-responses/multiple-primary-regions-response.json";
  private static final String MULTIPLE_ALLOWED_REGIONS_RESPONSE = "tas-responses/multiple-allowed-regions-response.json";
  private static final String EMPTY_PRIMARY_REGIONS_RESPONSE = "tas-responses/empty-primary-regions-response.json";
  private static final String EMPTY_ALLOWED_REGIONS_RESPONSE = "tas-responses/empty-allowed-regions-response.json";
  private static final String PRIMARY_REGION_ERROR_RESPONSE = "tas-responses/primary-region-error-response.json";
  private static final String ALLOWED_REGIONS_ERROR_RESPONSE = "tas-responses/allowed-regions-error-response.json";
  private static final String TOPOLOGY_RESPONSE = "tas-responses/topology-response.json";
  private static final String MULTIPLE_DATASETS_SUCCESS_RESPONSE = "tas-responses/multiple-datasets-success-response.json";
  private static final String MULTIPLE_DATASETS_WITH_ERROR_RESPONSE = "tas-responses/multiple-datasets-primary-regions-response.json";

  @BeforeEach
  void setUp() {
    this.mockResponse = Mockito.mock(CloseableHttpResponse.class);
    this.tasClient = Mockito.spy(new HoodieTASClient());
  }

  @AfterEach
  public void teardown() throws Exception {
    Mockito.reset(mockResponse);
    Mockito.reset(tasClient);
  }

  private void mockResponse(String resourceName, String procedure) throws Exception {
    // mock the response, always return above string
    HttpEntity entity = new StringEntity(TestClientUtils.getResponseBodyFromResource(resourceName),
        ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, always return 200
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(200);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // mock response returned by client
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(procedure),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));
  }

  @Test
  public void testGetPrimaryRegion() throws Exception {
    mockResponse(PRIMARY_REGION_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    Set<Region> primaryRegions = tasClient.getPrimaryRegions("test_db.test_table");

    assertEquals(primaryRegions.size(), 1);
    assertTrue(primaryRegions.contains(Region.PHX));
  }

  @Test
  public void testGetPrimaryRegionHttpFailure() throws Exception {
    // mock the response body, return empty string
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 400 for failure
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 400 status code and empty response body
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    assertThrows(HoodieException.class, () ->  tasClient.getPrimaryRegions("test_db.test_table"));
  }

  @Test
  public void testGetPrimaryRegionResponseParsingFailure() throws Exception {
    // mock the response body, return bad response
    HttpEntity entity = new StringEntity("{\"test\":\"foo\"}", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 200
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(200);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    assertThrows(HoodieException.class, () -> tasClient.getPrimaryRegions("test_db.test_table"));
  }

  @Test
  public void testGetPrimaryRegionServiceError() throws Exception {
    mockResponse(PRIMARY_REGION_ERROR_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());
    Exception exception = assertThrows(HoodieException.class, () -> {
      tasClient.getPrimaryRegions("test_db.test_table");
    });
    assertTrue(exception.getCause().getMessage().contains("Service unavailable for dataset: test_db.test_table"));
  }

  @Test
  public void testGetAllowedUpdateRegions() throws Exception {
    mockResponse(ALLOWED_UPDATE_REGIONS_RESPONSE, HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure());

    Set<Region> allowedUpdateRegions = tasClient.getAllowedUpdateRegions("test_db.test_table");

    assertEquals(allowedUpdateRegions.size(), 1);
    assertTrue(allowedUpdateRegions.contains(Region.PHX));
  }

  @Test
  public void testGetAllowedUpdateRegionsFailure() throws Exception {
    // mock the response body, return empty string
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 400 for failure
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 400 status code and empty response body
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    // assert that allowed update regions wasn't successfully fetched
    assertThrows(HoodieException.class, () ->  tasClient.getAllowedUpdateRegions("test_db.test_table"));
  }

  @Test
  public void testGetAllowedUpdateRegionsResponseParsingFailure() throws Exception {
    // mock the response body, return bad response
    HttpEntity entity = new StringEntity("{\"test\":\"foo\"}", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 200
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(200);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 200 status code, but the response body is bad which will cause a failure
    // when parsing the response body
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    // assert that allowed update regions wasn't successfully fetched
    assertThrows(HoodieException.class, () -> tasClient.getAllowedUpdateRegions("test_db.test_table"));
  }

  @Test
  public void testGetReplicationTopology() throws Exception {
    mockResponse(TOPOLOGY_RESPONSE, HoodieTASClient.TASProcedure.GET_TOPOLOGY.getProcedure());
    String datasetName = "test_db.test_table";
    HoodieReplicationMetadata replicationMetadata = tasClient.getReplicationTopology(datasetName);

    assertEquals(datasetName, replicationMetadata.getTableName());
    assertTrue(replicationMetadata.isReplicationEnabled(ReplicationDestination.QUATERNARY_REGION));
    assertFalse(replicationMetadata.isOtrsOngoing(ReplicationDestination.QUATERNARY_REGION));
  }

  @Test
  public void testGetReplicationTopologyDatasetNotFound() throws Exception {
    mockResponse(TOPOLOGY_RESPONSE, HoodieTASClient.TASProcedure.GET_TOPOLOGY.getProcedure());
    String datasetName = "non_existent_dataset";
    Exception exception = assertThrows(HoodieException.class, () -> {
      tasClient.getReplicationTopology(datasetName);
    });
    assertTrue(exception.getMessage().contains("Failed to get replication topology for dataset: " + datasetName));
  }

  @Test
  public void testGetReplicationTopologyHttpFailure() throws Exception {
    // mock the response body, return empty string
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 400 for failure
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 400 status code and empty response body
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_TOPOLOGY.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    assertThrows(HoodieException.class, () ->  tasClient.getReplicationTopology("test_db.test_table"));
  }

  @Test
  public void testGetReplicationTopologyResponseParsingFailure() throws Exception {
    // mock the response body, return bad response
    HttpEntity entity = new StringEntity("{\"test\":\"foo\"}", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 200
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(200);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_TOPOLOGY.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    assertThrows(HoodieException.class, () -> tasClient.getReplicationTopology("test_db.test_table"));
  }

  @Test
  public void testGetPrimaryRegionsFailure() throws Exception {
    // mock the response body, return empty string
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 400 for failure
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 400 status code and empty response body
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    // assert that primary regions wasn't successfully fetched
    assertThrows(HoodieException.class, () -> tasClient.getPrimaryRegions("test_db.test_table"));
  }

  @Test
  public void testGetPrimaryRegionsResponseParsingFailure() throws Exception {
    // mock the response body, return bad response
    HttpEntity entity = new StringEntity("{\"test\":\"foo\"}", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 200
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(200);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 200 status code, but the response body is bad which will cause a failure
    // when parsing the response body
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    // assert that primary regions wasn't successfully fetched
    assertThrows(HoodieException.class, () -> tasClient.getPrimaryRegions("test_db.test_table"));
  }

  @Test
  public void testGetPrimaryRegionsWithErrorResponse() throws Exception {
    mockResponse(PRIMARY_REGION_ERROR_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    // assert that primary regions wasn't successfully fetched due to error in response
    HoodieException exception = assertThrows(HoodieException.class,
        () -> tasClient.getPrimaryRegions("test_db.test_table"));
    assertTrue(exception.getCause().getMessage().contains("Service unavailable for dataset"));
  }

  @Test
  public void testGetPrimaryRegionsWithMultipleRegions() throws Exception {
    mockResponse(MULTIPLE_PRIMARY_REGIONS_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    Set<Region> primaryRegions = tasClient.getPrimaryRegions("test_db.test_table");
    assertEquals(2, primaryRegions.size());
    assertTrue(primaryRegions.contains(Region.PHX));
    assertTrue(primaryRegions.contains(Region.DCA));
  }

  @Test
  public void testGetAllowedUpdateRegionsWithMultipleRegions() throws Exception {
    mockResponse(MULTIPLE_ALLOWED_REGIONS_RESPONSE, HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure());

    Set<Region> allowedRegions = tasClient.getAllowedUpdateRegions("test_db.test_table");
    assertEquals(2, allowedRegions.size());
    assertTrue(allowedRegions.contains(Region.PHX));
    assertTrue(allowedRegions.contains(Region.DCA));
  }

  @Test
  public void testGetPrimaryRegionsWithEmptyRegions() throws Exception {
    mockResponse(EMPTY_PRIMARY_REGIONS_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    Set<Region> primaryRegions = tasClient.getPrimaryRegions("test_db.test_table");
    assertTrue(primaryRegions.isEmpty());
  }

  @Test
  public void testGetAllowedUpdateRegionsWithEmptyRegions() throws Exception {
    mockResponse(EMPTY_ALLOWED_REGIONS_RESPONSE, HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure());

    Set<Region> allowedRegions = tasClient.getAllowedUpdateRegions("test_db.test_table");
    assertTrue(allowedRegions.isEmpty());
  }

  @Test
  public void testGetAllowedUpdateRegionsWithErrorResponse() throws Exception {
    mockResponse(ALLOWED_REGIONS_ERROR_RESPONSE, HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure());

    // assert that allowed update regions wasn't successfully fetched due to error in response
    HoodieException exception = assertThrows(HoodieException.class,
        () -> tasClient.getAllowedUpdateRegions("test_db.test_table"));
    assertTrue(exception.getMessage().contains("Failed to get allowed update regions for dataset"));
  }

  @Test
  public void testClientInitialization() {
    HoodieTASClient client = new HoodieTASClient();
    assertNotNull(client);
  }

  @Test
  public void testRequestConstruction() throws Exception {
    String dataset = "test_db.test_table";
    HttpPost request = tasClient.createDatasetRequest(dataset);

    assertNotNull(request);
    assertEquals("POST", request.getMethod());
    assertNotNull(request.getEntity());

    String requestBody = EntityUtils.toString(request.getEntity());
    assertTrue(requestBody.contains(dataset));
    assertTrue(requestBody.contains("hudi")); // client_id
    assertTrue(requestBody.contains("hudi-tas-client")); // trace_id
  }

  // Tests for getPrimaryRegions(Set<String> datasets) method

  @Test
  public void testGetPrimaryRegionsForMultipleDatasets() throws Exception {
    mockResponse(MULTIPLE_DATASETS_SUCCESS_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    Set<String> datasets = new HashSet<>(Arrays.asList(
        "test_db.test_table1",
        "test_db.test_table2",
        "test_db.test_table3",
        "test_db.test_table4"
    ));

    Map<String, Set<Region>> result = tasClient.getPrimaryRegions(datasets);

    // Verify all datasets are returned
    assertEquals(4, result.size());
    assertTrue(result.containsKey("test_db.test_table1"));
    assertTrue(result.containsKey("test_db.test_table2"));
    assertTrue(result.containsKey("test_db.test_table3"));

    // Verify specific region mappings
    Set<Region> table1Regions = result.get("test_db.test_table1");
    assertEquals(2, table1Regions.size());
    assertTrue(table1Regions.contains(Region.PHX));
    assertTrue(table1Regions.contains(Region.DCA));

    Set<Region> table2Regions = result.get("test_db.test_table2");
    assertEquals(1, table2Regions.size());
    assertTrue(table2Regions.contains(Region.PHX));

    Set<Region> table3Regions = result.get("test_db.test_table3");
    assertEquals(1, table3Regions.size());
    assertTrue(table3Regions.contains(Region.DCA));

    Set<Region> table4Regions = result.get("test_db.test_table4");
    assertEquals(2, table4Regions.size());
    assertTrue(table4Regions.contains(Region.DCA));
    assertTrue(table4Regions.contains(Region.PHX));
  }

  @Test
  public void testGetPrimaryRegionsForMultipleDatasetsWithError() throws Exception {
    mockResponse(MULTIPLE_DATASETS_WITH_ERROR_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    Set<String> datasets = new HashSet<>(Arrays.asList(
        "test_db.test_table1",
        "test_db.test_table2",
        "test_db.test_table3",
        "test_db.test_table4"
    ));

    Map<String, Set<Region>> result = tasClient.getPrimaryRegions(datasets);

    // Verify that all datasets are returned
    assertEquals(3, result.size());
    assertTrue(result.containsKey("test_db.test_table1"));
    assertTrue(result.containsKey("test_db.test_table2"));
    assertTrue(result.containsKey("test_db.test_table4"));

    // Verify specific region mappings
    Set<Region> table1Regions = result.get("test_db.test_table1");
    assertEquals(2, table1Regions.size());
    assertTrue(table1Regions.contains(Region.PHX));
    assertTrue(table1Regions.contains(Region.DCA));

    Set<Region> table2Regions = result.get("test_db.test_table2");
    assertEquals(1, table2Regions.size());
    assertTrue(table2Regions.contains(Region.PHX));

    Set<Region> table4Regions = result.get("test_db.test_table4");
    assertEquals(1, table4Regions.size());
    assertTrue(table4Regions.contains(Region.DCA));

    assertNull(result.get("test_db.test_table3"));
  }

  // Tests for isRegionPrimary method

  @Test
  public void testIsRegionPrimaryWithPrimaryRegionApi() throws Exception {
    mockResponse(PRIMARY_REGION_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    // Test with PRIMARY_REGION API type - PHX should be primary, DCA should not
    assertTrue(tasClient.isRegionPrimary("test_db.test_table", Region.PHX, TASPrimaryRegionApiType.PRIMARY_REGION));
    assertFalse(tasClient.isRegionPrimary("test_db.test_table", Region.DCA, TASPrimaryRegionApiType.PRIMARY_REGION));
  }

  @Test
  public void testIsRegionPrimaryWithUpdateConstraintsApi() throws Exception {
    mockResponse(ALLOWED_UPDATE_REGIONS_RESPONSE, HoodieTASClient.TASProcedure.GET_REGIONAL_UPDATE_CONSTRAINTS.getProcedure());

    // Test with UPDATE_CONSTRAINTS API type - PHX should be allowed, DCA should not
    assertTrue(tasClient.isRegionPrimary("test_db.test_table", Region.PHX, TASPrimaryRegionApiType.UPDATE_CONSTRAINTS));
    assertFalse(tasClient.isRegionPrimary("test_db.test_table", Region.DCA, TASPrimaryRegionApiType.UPDATE_CONSTRAINTS));
  }

  @Test
  public void testIsRegionPrimaryWithException() throws Exception {
    // Mock HTTP failure
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    // Should throw exception when API fails
    assertThrows(HoodieException.class, () -> 
        tasClient.isRegionPrimary("test_db.test_table", Region.PHX, TASPrimaryRegionApiType.PRIMARY_REGION));
  }

  // Tests for retry functionality

  @Test
  public void testGetPrimaryRegionsWithRetrySuccess() throws Exception {
    mockResponse(MULTIPLE_DATASETS_SUCCESS_RESPONSE, HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure());

    Set<String> datasets = new HashSet<>(Arrays.asList(
        "test_db.test_table1",
        "test_db.test_table2"
    ));

    Map<String, Set<Region>> result = tasClient.getPrimaryRegions(datasets, 2);

    // Verify all datasets are returned
    assertEquals(2, result.size());
    assertTrue(result.containsKey("test_db.test_table1"));
    assertTrue(result.containsKey("test_db.test_table2"));
  }

  @Test
  public void testGetPrimaryRegionsWithRetryFailure() throws Exception {
    // Mock HTTP failure
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);
    Mockito.doReturn(mockResponse)
        .when(tasClient)
        .execute(ArgumentMatchers.any(HttpPost.class),
            ArgumentMatchers.eq(HoodieTASClient.TASProcedure.GET_PRIMARY_REGION.getProcedure()),
            ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    Set<String> datasets = new HashSet<>(Arrays.asList("test_db.test_table1"));

    // Should throw exception after max retries
    HoodieException exception = assertThrows(HoodieException.class, () -> 
        tasClient.getPrimaryRegions(datasets, 1));
    assertTrue(exception.getMessage().contains("Failed to get primary regions for datasets after 1 retries"));
  }

  @Test
  public void testGetPrimaryRegionsWithRetryInvalidMaxRetries() {
    Set<String> datasets = new HashSet<>(Arrays.asList("test_db.test_table1"));

    // Should throw exception for negative maxRetries
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
        tasClient.getPrimaryRegions(datasets, -1));
    assertEquals("maxRetries must be non-negative", exception.getMessage());
  }

}
