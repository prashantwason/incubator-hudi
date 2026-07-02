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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.hudi.replication.client.HoodieHttpClientMetricTags;
import org.apache.hudi.exception.HoodieException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestHoodieHiveSyncClient {
  private CloseableHttpResponse mockResponse;
  private HoodieHiveSyncClient hoodieHiveSyncClient;
  private static final String RESPONSE1_RESOURCE_NAME = "hivesync-responses/response1.json";
  private static final String NON_HMS_REGISTERED_RESPONSE = "hivesync-responses/non-hms-registered-table-response.json";

  @BeforeEach
  void setUp() throws IOException {
    this.mockResponse = Mockito.mock(CloseableHttpResponse.class);
    this.hoodieHiveSyncClient = Mockito.spy(new HoodieHiveSyncClient());
  }

  @AfterEach
  public void teardown() throws Exception {
    Mockito.reset(mockResponse);
    Mockito.reset(hoodieHiveSyncClient);
  }

  private HoodieReplicationMetadata initializeClientAndGetMetadata(String resourceName) throws Exception {
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
            .when(hoodieHiveSyncClient)
            .execute(ArgumentMatchers.any(HttpGet.class), ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    return hoodieHiveSyncClient.getReplicationMetadata("test_db", "test_table");
  }

  @Test
  public void testGetReplicationMetadataAPISuccess() throws Exception {
    HoodieReplicationMetadata hoodieHiveSyncMetadata = initializeClientAndGetMetadata(RESPONSE1_RESOURCE_NAME);
    assertNotNull(hoodieHiveSyncMetadata);
  }

  @Test
  public void testGetReplicationMetadataAPIFailure() throws Exception {
    // mock the response body, return empty string
    HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);

    // mock the status code, return 400 for failure
    StatusLine statusLineMock = Mockito.mock(StatusLine.class);
    Mockito.when(statusLineMock.getStatusCode()).thenReturn(400);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(statusLineMock);

    // client response will be 400 status code and empty response body
    Mockito.doReturn(mockResponse)
        .when(hoodieHiveSyncClient)
        .execute(ArgumentMatchers.any(HttpGet.class), ArgumentMatchers.any(HoodieHttpClientMetricTags.class));

    // assert that HoodieHiveSyncMetadata wasn't successfully fetched
    assertThrows(HoodieException.class, () ->  hoodieHiveSyncClient
        .getReplicationMetadata("test_db", "test_table"));
  }

  @Test
  public void testIsReplicationEnabled() throws Exception {
    HoodieReplicationMetadata hoodieHiveSyncMetadata = initializeClientAndGetMetadata(RESPONSE1_RESOURCE_NAME);

    // assert that quaternary replication is enabled, tertiary and secondary are disabled
    assertTrue(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.QUATERNARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.TERTIARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.SECONDARY_REGION));

    // assert that undefined region replication is not enabled
    assertFalse(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.OCTONARY_REGION));
  }

  @Test
  public void testIsOTRSOngoing() throws Exception {
    HoodieReplicationMetadata hoodieHiveSyncMetadata = initializeClientAndGetMetadata(RESPONSE1_RESOURCE_NAME);
    // assert that HoodieHiveSyncMetadata was successfully fetched
    assertNotNull(hoodieHiveSyncMetadata);

    // assert that otrs is ongoing for secondary replication, otrs is not ongoing for tertiary or quaternary
    assertTrue(hoodieHiveSyncMetadata.isOtrsOngoing(ReplicationDestination.SECONDARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isOtrsOngoing(ReplicationDestination.TERTIARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isOtrsOngoing(ReplicationDestination.QUATERNARY_REGION));

    // assert that expected start time is returned
    assertTrue(hoodieHiveSyncMetadata.getOtrsDuration(ReplicationDestination.SECONDARY_REGION) > 0);
  }

  @Test
  public void testIsReplicationConfigured() throws Exception {
    HoodieReplicationMetadata hoodieHiveSyncMetadata = initializeClientAndGetMetadata(RESPONSE1_RESOURCE_NAME);
    // assert that HoodieHiveSyncMetadata was successfully fetched
    assertNotNull(hoodieHiveSyncMetadata);

    // assert that replication is configured even though incremental replication is not enabled (due to ongoing otrs)
    assertTrue(hoodieHiveSyncMetadata.isReplicationConfigured(ReplicationDestination.SECONDARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.SECONDARY_REGION));
    assertTrue(hoodieHiveSyncMetadata.isOtrsOngoing(ReplicationDestination.SECONDARY_REGION));

    // assert that replication is not configured if incremental replication is not enabled and otrs is not ongoing
    assertFalse(hoodieHiveSyncMetadata.isReplicationConfigured(ReplicationDestination.TERTIARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.TERTIARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isOtrsOngoing(ReplicationDestination.TERTIARY_REGION));

    // assert that replication is configured when incremental replication is enabled
    assertTrue(hoodieHiveSyncMetadata.isReplicationConfigured(ReplicationDestination.QUATERNARY_REGION));
    assertTrue(hoodieHiveSyncMetadata.isReplicationEnabled(ReplicationDestination.QUATERNARY_REGION));
    assertFalse(hoodieHiveSyncMetadata.isOtrsOngoing(ReplicationDestination.QUATERNARY_REGION));

  }

  @Test
  public void testIsPrimaryRegion() throws Exception {
    HoodieReplicationMetadata hoodieHiveSyncMetadata = initializeClientAndGetMetadata(RESPONSE1_RESOURCE_NAME);
    // assert that HoodieHiveSyncMetadata was successfully fetched
    assertNotNull(hoodieHiveSyncMetadata);

    // there is chained replication PHX -> PHXCLD -> DCA
    // PHX is primary. all other regions are not.
    assertTrue(hoodieHiveSyncMetadata.isPrimaryRegion(Region.PHX));
    assertFalse(hoodieHiveSyncMetadata.isPrimaryRegion(Region.PHXCLD));
    assertFalse(hoodieHiveSyncMetadata.isPrimaryRegion(Region.DCA));

    // DCACLD is not part of any active replication pair
    // therefore it should be treated as a primary region
    assertTrue(hoodieHiveSyncMetadata.isPrimaryRegion(Region.DCACLD));
  }

  @Test
  public void testIsPrimaryRegionOnNonHMSRegisteredTable() throws Exception {
    HoodieReplicationMetadata hoodieHiveSyncMetadata = initializeClientAndGetMetadata(NON_HMS_REGISTERED_RESPONSE);
    // assert that HoodieHiveSyncMetadata was successfully fetched
    assertNotNull(hoodieHiveSyncMetadata);

    // all regions should be treated as primary since the table is not onboarded to any replication
    assertTrue(hoodieHiveSyncMetadata.isPrimaryRegion(Region.PHX));
    assertTrue(hoodieHiveSyncMetadata.isPrimaryRegion(Region.PHXCLD));
    assertTrue(hoodieHiveSyncMetadata.isPrimaryRegion(Region.DCA));
    assertTrue(hoodieHiveSyncMetadata.isPrimaryRegion(Region.DCACLD));
  }
}
