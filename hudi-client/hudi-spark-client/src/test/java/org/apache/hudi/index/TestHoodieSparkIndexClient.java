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

package org.apache.hudi.index;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestHoodieSparkIndexClient {

  @Test
  public void testDropOverrideConfigsColumnStats() {
    Map<String, String> overrides = HoodieSparkIndexClient.getDropOverrideConfigs(PARTITION_NAME_COLUMN_STATS);
    assertEquals("false", overrides.get(ENABLE_METADATA_INDEX_COLUMN_STATS.key()),
        "Dropping column_stats should disable ENABLE_METADATA_INDEX_COLUMN_STATS");
  }

  @Test
  public void testDropOverrideConfigsBloomFilters() {
    Map<String, String> overrides = HoodieSparkIndexClient.getDropOverrideConfigs(PARTITION_NAME_BLOOM_FILTERS);
    assertEquals("false", overrides.get(ENABLE_METADATA_INDEX_BLOOM_FILTER.key()),
        "Dropping bloom_filters should disable ENABLE_METADATA_INDEX_BLOOM_FILTER");
  }

  @Test
  public void testDropOverrideConfigsRecordIndex() {
    Map<String, String> overrides = HoodieSparkIndexClient.getDropOverrideConfigs(PARTITION_NAME_RECORD_INDEX);
    assertEquals("false", overrides.get(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key()),
        "Dropping record_index should disable GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP");
  }

  @Test
  public void testDropOverrideConfigsExpressionIndex() {
    Map<String, String> overrides = HoodieSparkIndexClient.getDropOverrideConfigs(PARTITION_NAME_EXPRESSION_INDEX_PREFIX + "my_idx");
    assertTrue(overrides.isEmpty(),
        "Dropping an expression index should have no override configs");
  }

  /**
   * Fail-fast: if rolling back stale inflight writes fails, rollbackInflightWrites must propagate
   * the exception (so CREATE INDEX aborts) rather than swallowing it. Uses a spy over the
   * package-private getWriteClient() seam to inject a failing write client deterministically.
   */
  @Test
  public void testRollbackInflightWritesPropagatesFailure() {
    HoodieSparkIndexClient indexClient =
        spy(new HoodieSparkIndexClient(Option.empty(), Option.empty(), Option.empty()));

    SparkRDDWriteClient mockWriteClient = mock(SparkRDDWriteClient.class);
    doThrow(new HoodieRollbackException("injected rollback failure"))
        .when(mockWriteClient).rollbackFailedWrites(any());
    doReturn(mockWriteClient).when(indexClient).getWriteClient(any(), any(), any(), any());

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("file:///tmp/test-table"));

    assertThrows(HoodieRollbackException.class, () -> indexClient.rollbackInflightWrites(metaClient));
  }
}
