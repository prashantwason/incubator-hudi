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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
