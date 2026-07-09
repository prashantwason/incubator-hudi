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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.client.tas.utils.TASPrimaryRegionApiType;
import org.apache.hudi.exception.HoodieBlockWritesException;
import org.apache.hudi.exception.HoodieException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieReplicationMetadataUtils {

  private static final String TEST_TABLE_NAME = "test_table";

  @Test
  public void testGetReplicationMetadataSuccess() throws Exception {
    String dbName = "test_db";
    String tableName = "test_table";
    HoodieReplicationMetadata expectedMetadata = Mockito.mock(HoodieReplicationMetadata.class);

    try (MockedConstruction<HoodieTASClient> tasClientMock = Mockito.mockConstruction(HoodieTASClient.class, (mock, context) -> {
      Mockito.when(mock.getReplicationTopology(dbName + "." + tableName)).thenReturn(expectedMetadata);
    })) {
      HoodieReplicationMetadata result = HoodieReplicationMetadataUtils.getReplicationMetadata(dbName, tableName);
      assertEquals(expectedMetadata, result);
    }
  }

  @Test
  public void testGetReplicationMetadataException() throws Exception {
    String dbName = "test_db";
    String tableName = "test_table";
    Exception expectedException = new HoodieException("Test exception");

    try (MockedConstruction<HoodieTASClient> tasClientMock = Mockito.mockConstruction(HoodieTASClient.class, (mock, context) -> {
      Mockito.when(mock.getReplicationTopology(dbName + "." + tableName)).thenThrow(expectedException);
    })) {
      Exception thrownException = assertThrows(Exception.class,
          () -> HoodieReplicationMetadataUtils.getReplicationMetadata(dbName, tableName));
      assertEquals(expectedException, thrownException);
    }
  }

  @Test
  public void testVerifyThatJobIsRunningInPrimaryRegionSuccessAndFailure() throws Exception {
    HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);
    Set<Region> primaryRegions = new HashSet<>(Arrays.asList(Region.PHX, Region.DCACLD));

    try (MockedConstruction<HoodieTASClient> tasClientMock = createTasClientMock(primaryRegions)) {
      // Test primary region should succeed
      setDatacenter(context, "PHX");
      assertDoesNotThrow(() -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
          TEST_TABLE_NAME, context, TASPrimaryRegionApiType.PRIMARY_REGION));

      // Test non-primary region should fail
      setDatacenter(context, "DCA");
      assertThrows(HoodieBlockWritesException.class,
          () -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
              TEST_TABLE_NAME, context, TASPrimaryRegionApiType.PRIMARY_REGION));
    }
  }

  @ParameterizedTest
  @EnumSource(TASPrimaryRegionApiType.class)
  public void testVerifyThatJobIsRunningInPrimaryRegionWithDifferentApiTypes(TASPrimaryRegionApiType apiType) throws Exception {
    HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);
    Set<Region> primaryRegions = new HashSet<>(Arrays.asList(Region.PHX));

    try (MockedConstruction<HoodieTASClient> tasClientMock = createTasClientMock(primaryRegions)) {
      setDatacenter(context, "PHX");
      assertDoesNotThrow(() -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
          TEST_TABLE_NAME, context, apiType));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "invalid"})
  public void testVerifyThatJobIsRunningInPrimaryRegionInvalidDatacenter(String datacenter) throws Exception {
    HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);

    if (datacenter.isEmpty() || datacenter.trim().isEmpty()) {
      Mockito.when(context.getDatacenter()).thenReturn(Option.empty());
    } else {
      setDatacenter(context, datacenter);
    }

    HoodieBlockWritesException exception = assertThrows(HoodieBlockWritesException.class,
        () -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
            TEST_TABLE_NAME, context, TASPrimaryRegionApiType.PRIMARY_REGION));

    assertEquals("Could not fetch region from the job context", exception.getMessage());
  }

  @Test
  public void testVerifyThatJobIsRunningInPrimaryRegionContextException() throws Exception {
    HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);
    Mockito.when(context.getDatacenter()).thenThrow(new RuntimeException("Context error"));

    HoodieBlockWritesException exception = assertThrows(HoodieBlockWritesException.class,
        () -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
            TEST_TABLE_NAME, context, TASPrimaryRegionApiType.PRIMARY_REGION));

    assertEquals("Could not fetch region from the job context", exception.getMessage());
  }

  @Test
  public void testVerifyThatJobIsRunningInPrimaryRegionTasException() throws Exception {
    HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);
    setDatacenter(context, "PHX");

    try (MockedConstruction<HoodieTASClient> tasClientMock = Mockito.mockConstruction(HoodieTASClient.class, (mock, mockContext) -> {
      Mockito.when(mock.isRegionPrimary(Mockito.anyString(), Mockito.any(Region.class), Mockito.any(TASPrimaryRegionApiType.class)))
          .thenThrow(new HoodieException("TAS API error"));
    })) {
      HoodieBlockWritesException exception = assertThrows(HoodieBlockWritesException.class,
          () -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
              TEST_TABLE_NAME, context, TASPrimaryRegionApiType.PRIMARY_REGION));

      assertTrue(exception.getMessage().contains("Failed to check if PHX is primary region for " + TEST_TABLE_NAME));
    }
  }

  @Test
  public void testVerifyThatJobIsRunningInPrimaryRegionNullContext() throws Exception {
    HoodieBlockWritesException exception = assertThrows(HoodieBlockWritesException.class,
        () -> HoodieReplicationMetadataUtils.verifyThatJobIsRunningInPrimaryRegion(
            TEST_TABLE_NAME, null, TASPrimaryRegionApiType.PRIMARY_REGION));

    assertEquals("Could not fetch region from the job context", exception.getMessage());
  }

  private MockedConstruction<HoodieTASClient> createTasClientMock(Set<Region> primaryRegions) {
    return Mockito.mockConstruction(HoodieTASClient.class, (mock, context) -> {
      Mockito.when(mock.getPrimaryRegions(TEST_TABLE_NAME)).thenReturn(primaryRegions);
      // Mock the new isRegionPrimary method that takes API type
      Mockito.when(mock.isRegionPrimary(Mockito.anyString(), Mockito.any(Region.class), Mockito.any(TASPrimaryRegionApiType.class)))
          .thenAnswer(invocation -> {
            Region region = invocation.getArgument(1);
            return primaryRegions.contains(region);
          });
    });
  }

  private void setDatacenter(HoodieEngineContext context, String datacenter) {
    Mockito.when(context.getDatacenter()).thenReturn(Option.of(datacenter));
  }
}
