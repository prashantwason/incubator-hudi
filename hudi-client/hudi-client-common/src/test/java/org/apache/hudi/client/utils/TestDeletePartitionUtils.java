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

package org.apache.hudi.client.utils;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieDeletePartitionException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hudi.storage.HoodieInstantWriter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDeletePartitionUtils {

  private static final String PARTITION_IN_PENDING_SERVICE_ACTION = "partition_with_pending_table_service_action";
  private static final String HARDCODED_INSTANT_TIME = "0";
  private static final String TARGETED_FOR_DELETION = "delete.partition.targeted";

  private final HoodieTable table = Mockito.mock(HoodieTable.class);

  private final SyncableFileSystemView fileSystemView = Mockito.mock(SyncableFileSystemView.class);

  public static Stream<Arguments> generateTruthValues() {
    int noOfVariables = 3;
    int noOfRows = 1 << noOfVariables;
    Object[][] truthValues = new Object[noOfRows][noOfVariables];
    for (int i = 0; i < noOfRows; i++) {
      for (int j = noOfVariables - 1; j >= 0; j--) {
        boolean out = (i / (1 << j)) % 2 != 0;
        truthValues[i][j] = out;
      }
    }
    return Stream.of(truthValues).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("generateTruthValues")
  public void testDeletePartitionUtils(
      boolean hasPendingCompactionOperations,
      boolean hasPendingLogCompactionOperations,
      boolean hasFileGroupsInPendingClustering) {
    Mockito.when(table.getSliceView()).thenReturn(fileSystemView);
    Mockito.when(fileSystemView.getPendingCompactionOperations()).thenReturn(createPendingCompactionOperations(hasPendingCompactionOperations));
    Mockito.when(fileSystemView.getPendingLogCompactionOperations()).thenReturn(createPendingCompactionOperations(hasPendingLogCompactionOperations));
    Mockito.when(fileSystemView.getFileGroupsInPendingClustering()).thenReturn(createFileGroupsInPendingClustering(hasFileGroupsInPendingClustering));

    boolean shouldThrowException = hasPendingCompactionOperations || hasPendingLogCompactionOperations || hasFileGroupsInPendingClustering;

    if (shouldThrowException) {
      assertThrows(HoodieDeletePartitionException.class,
          () -> DeletePartitionUtils.checkForPendingTableServiceActions(table,
              Collections.singletonList(PARTITION_IN_PENDING_SERVICE_ACTION)));
    } else {
      assertDoesNotThrow(() -> DeletePartitionUtils.checkForPendingTableServiceActions(table,
          Collections.singletonList(PARTITION_IN_PENDING_SERVICE_ACTION)));
    }
  }

  private static Stream<Pair<String, CompactionOperation>> createPendingCompactionOperations(boolean hasPendingCompactionOperations) {
    return Stream.of(Pair.of(HARDCODED_INSTANT_TIME, getCompactionOperation(hasPendingCompactionOperations)));
  }

  private static CompactionOperation getCompactionOperation(boolean hasPendingJobInPartition) {
    return new CompactionOperation(
        "fileId", getPartitionName(hasPendingJobInPartition), HARDCODED_INSTANT_TIME, Option.empty(),
        new ArrayList<>(), Option.empty(), Option.empty(), new HashMap<>());
  }

  private static Stream<Pair<HoodieFileGroupId, HoodieInstant>> createFileGroupsInPendingClustering(boolean hasFileGroupsInPendingClustering) {
    HoodieFileGroupId hoodieFileGroupId = new HoodieFileGroupId(getPartitionName(hasFileGroupsInPendingClustering), "fileId");
    HoodieInstant hoodieInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, "replacecommit", HARDCODED_INSTANT_TIME);
    return Stream.of(Pair.of(hoodieFileGroupId, hoodieInstant));
  }

  private static String getPartitionName(boolean hasPendingTableServiceAction) {
    return hasPendingTableServiceAction ? PARTITION_IN_PENDING_SERVICE_ACTION : "unaffected_partition";
  }

  private static HoodieRequestedReplaceMetadata createClusteringMetadata(String... partitions) {
    List<HoodieClusteringGroup> groups = Arrays.stream(partitions).map(partition -> {
      HoodieSliceInfo slice = HoodieSliceInfo.newBuilder()
          .setPartitionPath(partition)
          .setFileId("fileId-" + partition)
          .setDeltaFilePaths(Collections.emptyList())
          .build();
      return HoodieClusteringGroup.newBuilder()
          .setSlices(Collections.singletonList(slice))
          .setMetrics(Collections.emptyMap())
          .setNumOutputFileGroups(1)
          .build();
    }).collect(Collectors.toList());

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName("strategyClass")
        .setStrategyParams(Collections.emptyMap())
        .build();
    HoodieClusteringPlan plan = HoodieClusteringPlan.newBuilder()
        .setInputGroups(groups)
        .setExtraMetadata(Collections.emptyMap())
        .setStrategy(strategy)
        .setPreserveHoodieMetadata(true)
        .build();
    return HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setClusteringPlan(plan)
        .build();
  }

  private static HoodieReplaceCommitMetadata createCompletedDeletePartitionMetadata(String... partitions) {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    metadata.setOperationType(WriteOperationType.DELETE_PARTITION);
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    for (String partition : partitions) {
      partitionToReplaceFileIds.put(partition, Collections.singletonList("fileId-" + partition));
    }
    metadata.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
    return metadata;
  }

  private static HoodieReplaceCommitMetadata createCompletedClusterMetadata() {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    metadata.setOperationType(WriteOperationType.CLUSTER);
    return metadata;
  }

  private static byte[] createInflightDeletePartitionBytes(String... partitions) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setOperationType(WriteOperationType.DELETE_PARTITION);
    metadata.addMetadata(TARGETED_FOR_DELETION, JsonUtils.getObjectMapper().writeValueAsString(Arrays.asList(partitions)));
    return serializeCommitMetadata(metadata);
  }

  private static byte[] serializeCommitMetadata(HoodieCommitMetadata metadata) throws IOException {
    Option<HoodieInstantWriter> writerOption = COMMIT_METADATA_SER_DE.getInstantWriter(metadata);
    if (!writerOption.isPresent()) {
      throw new IOException("Failed to serialize commit metadata");
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    writerOption.get().writeToStream(outputStream);
    return outputStream.toByteArray();
  }

  private static void setupMetaClientMocks(
      HoodieTable mockTable,
      HoodieTableMetaClient mockMetaClient,
      HoodieActiveTimeline mockTimeline) {
    when(mockTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockMetaClient.reloadActiveTimeline()).thenReturn(mockTimeline);
    when(mockMetaClient.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(mockMetaClient.getCommitMetadataSerDe()).thenReturn(COMMIT_METADATA_SER_DE);
  }

  private static void setupTimelineMocks(
      HoodieActiveTimeline timeline,
      List<HoodieInstant> completedInstants,
      List<HoodieInstant> inflightInstants) {
    HoodieTimeline replaceTimeline = mock(HoodieTimeline.class);
    when(timeline.getTimelineOfActions(any())).thenReturn(replaceTimeline);

    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(replaceTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    HoodieTimeline filteredTimeline = mock(HoodieTimeline.class);
    when(completedTimeline.findInstantsModifiedAfterByCompletionTime(anyString())).thenReturn(filteredTimeline);
    when(filteredTimeline.getInstantsAsStream()).thenReturn(completedInstants.stream());

    HoodieTimeline inflightTimeline = mock(HoodieTimeline.class);
    when(replaceTimeline.filterInflights()).thenReturn(inflightTimeline);
    when(inflightTimeline.getInstantsAsStream()).thenReturn(inflightInstants.stream());
  }

  @Test
  public void testNoDeletePartitionConflictsWithClustering() {
    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);

    setupMetaClientMocks(mockTable, mockMetaClient, mockTimeline);
    setupTimelineMocks(mockTimeline, Collections.emptyList(), Collections.emptyList());

    HoodieRequestedReplaceMetadata clusteringMeta = createClusteringMetadata("partition1");
    assertDoesNotThrow(() -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, clusteringMeta, "001"));
  }

  @Test
  public void testCompletedDeletePartitionConflictWithClustering() throws Exception {
    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);

    setupMetaClientMocks(mockTable, mockMetaClient, mockTimeline);

    HoodieInstant completedInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, REPLACE_COMMIT_ACTION, "002");
    setupTimelineMocks(mockTimeline, Collections.singletonList(completedInstant), Collections.emptyList());
    when(mockTimeline.readReplaceCommitMetadata(completedInstant))
        .thenReturn(createCompletedDeletePartitionMetadata("partition1"));

    HoodieRequestedReplaceMetadata clusteringMeta = createClusteringMetadata("partition1");
    assertThrows(HoodieException.class, () -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, clusteringMeta, "001"));
  }

  @Test
  public void testInflightDeletePartitionConflictWithClustering() throws Exception {
    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);

    setupMetaClientMocks(mockTable, mockMetaClient, mockTimeline);

    HoodieInstant inflightInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, "002");
    setupTimelineMocks(mockTimeline, Collections.emptyList(), Collections.singletonList(inflightInstant));
    when(mockTimeline.getInstantDetails(inflightInstant))
        .thenReturn(Option.of(createInflightDeletePartitionBytes("partition1")));

    HoodieRequestedReplaceMetadata clusteringMeta = createClusteringMetadata("partition1");
    assertThrows(HoodieException.class, () -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, clusteringMeta, "001"));
  }

  @Test
  public void testInflightDeletePartitionNoConflictDifferentPartitions() throws Exception {
    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);

    setupMetaClientMocks(mockTable, mockMetaClient, mockTimeline);

    HoodieInstant inflightInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, "002");
    setupTimelineMocks(mockTimeline, Collections.emptyList(), Collections.singletonList(inflightInstant));
    when(mockTimeline.getInstantDetails(inflightInstant))
        .thenReturn(Option.of(createInflightDeletePartitionBytes("partitionX")));

    HoodieRequestedReplaceMetadata clusteringMeta = createClusteringMetadata("partition1");
    assertDoesNotThrow(() -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, clusteringMeta, "001"));
  }

  @Test
  public void testCompletedClusteringReplaceCommitNoConflict() throws Exception {
    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);

    setupMetaClientMocks(mockTable, mockMetaClient, mockTimeline);

    HoodieInstant completedInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, REPLACE_COMMIT_ACTION, "002");
    setupTimelineMocks(mockTimeline, Collections.singletonList(completedInstant), Collections.emptyList());
    when(mockTimeline.readReplaceCommitMetadata(completedInstant))
        .thenReturn(createCompletedClusterMetadata());

    HoodieRequestedReplaceMetadata clusteringMeta = createClusteringMetadata("partition1");
    assertDoesNotThrow(() -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, clusteringMeta, "001"));
  }

  @Test
  public void testInflightReadFailureInstantGoneAfterReload() {
    HoodieTable mockTable = mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline firstTimeline = mock(HoodieActiveTimeline.class);
    HoodieActiveTimeline secondTimeline = mock(HoodieActiveTimeline.class);

    when(mockTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockMetaClient.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(mockMetaClient.getCommitMetadataSerDe()).thenReturn(COMMIT_METADATA_SER_DE);
    when(mockMetaClient.reloadActiveTimeline())
        .thenReturn(firstTimeline)
        .thenReturn(firstTimeline)
        .thenReturn(secondTimeline);

    HoodieInstant inflightInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, "002");
    setupTimelineMocks(firstTimeline, Collections.emptyList(), Collections.singletonList(inflightInstant));
    when(firstTimeline.getInstantDetails(inflightInstant))
        .thenThrow(new HoodieIOException("read failure", new IOException("disk error")));

    when(secondTimeline.containsInstant(inflightInstant)).thenReturn(false);

    HoodieRequestedReplaceMetadata clusteringMeta = createClusteringMetadata("partition1");
    assertDoesNotThrow(() -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, clusteringMeta, "001"));
  }

  @Test
  public void testEmptyClusteringPartitionsSkipsCheck() {
    HoodieTable mockTable = mock(HoodieTable.class);

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName("strategyClass")
        .setStrategyParams(Collections.emptyMap())
        .build();
    HoodieClusteringPlan emptyPlan = HoodieClusteringPlan.newBuilder()
        .setInputGroups(Collections.emptyList())
        .setExtraMetadata(Collections.emptyMap())
        .setStrategy(strategy)
        .setPreserveHoodieMetadata(true)
        .build();
    HoodieRequestedReplaceMetadata emptyMeta = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setClusteringPlan(emptyPlan)
        .build();

    assertDoesNotThrow(() -> DeletePartitionUtils.checkForDeletePartitionConflictsWithClustering(
        mockTable, emptyMeta, "001"));
  }
}
