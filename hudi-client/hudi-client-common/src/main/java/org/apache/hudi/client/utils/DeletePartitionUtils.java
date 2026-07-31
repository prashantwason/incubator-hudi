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

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDeletePartitionPendingTableServiceException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility class for helper functions when performing a delete partition operation.
 */
public class DeletePartitionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DeletePartitionUtils.class);

  // [UBER INTERNAL BEGIN] Temporary support for Uber 0.14 deletePartitions stash operations.
  // Uber 0.14 clients using deletePartitions(partitions, instant, stashPath) write TARGETED_FOR_DELETION
  // metadata to .replacecommit.inflight before stashing files. Remove once DLM stash deletePartitions is
  // ported to HUDI 1.2.
  // Matches DeletePartitionCommitMetadata.TARGETED_FOR_DELETION on the Uber internal release branch.
  public static final String STASH_DELETE_PARTITION_TARGETED_FOR_DELETION_KEY = "delete.partition.targeted";
  // [UBER INTERNAL END]

  /**
   * Check if there are any pending table service actions (requested + inflight) on a table affecting the partitions to
   * be dropped.
   * <p>
   * This check is to prevent a drop-partition from proceeding should a partition have a table service action in
   * the pending stage. If this is allowed to happen, the filegroup that is an input for a table service action, might
   * also be a candidate for being replaced. As such, when the table service action and drop-partition commits are
   * committed, there will be two commits replacing a single filegroup.
   * <p>
   * For example, a timeline might have an execution order as such:
   * 000.replacecommit.requested (clustering filegroup_1 + filegroup_2 -> filegroup_3)
   * 001.replacecommit.requested, 001.replacecommit.inflight, 0001.replacecommit (drop_partition to replace filegroup_1)
   * 000.replacecommit.inflight (clustering is executed now)
   * 000.replacecommit (clustering completed)
   * For an execution order as shown above, 000.replacecommit and 001.replacecommit will both flag filegroup_1 to be replaced.
   * This will cause  downstream duplicate key errors when a map is being constructed.
   *
   * @param table Table to perform validation on
   * @param partitionsToDrop List of partitions to drop
   */
  public static void checkForPendingTableServiceActions(HoodieTable table, List<String> partitionsToDrop) {
    List<String> instantsOfOffendingPendingTableServiceAction = new ArrayList<>();
    // ensure that there are no pending inflight clustering/compaction operations involving this partition
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();

    // separating the iteration of pending compaction operations from clustering as they return different stream types
    Stream.concat(fileSystemView.getPendingCompactionOperations(), fileSystemView.getPendingLogCompactionOperations())
        .filter(op -> partitionsToDrop.contains(op.getRight().getPartitionPath()))
        .forEach(op -> instantsOfOffendingPendingTableServiceAction.add(op.getLeft()));

    fileSystemView.getFileGroupsInPendingClustering()
        .filter(fgIdInstantPair -> partitionsToDrop.contains(fgIdInstantPair.getLeft().getPartitionPath()))
        .forEach(x -> instantsOfOffendingPendingTableServiceAction.add(x.getRight().requestedTime()));

    if (instantsOfOffendingPendingTableServiceAction.size() > 0) {
      throw new HoodieDeletePartitionPendingTableServiceException("Failed to drop partitions. "
          + "Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: " + partitionsToDrop + ". "
          + "Instant(s) of offending pending table service action: "
          + instantsOfOffendingPendingTableServiceAction.stream().distinct().collect(Collectors.toList()));
    }
  }

  /**
   * Check if a clustering plan conflicts with any completed or inflight DELETE_PARTITION replacecommit
   * that targets any of the same partitions as the clustering plan.
   *
   * @param table the Hudi table
   * @param clusteringMetadata the requested replace metadata containing the clustering plan
   * @param clusteringInstantTime the instant time of the clustering plan
   * @throws HoodieException if a conflicting DELETE_PARTITION replacecommit is found
   */
  public static void checkForDeletePartitionConflictsWithClustering(
      HoodieTable table,
      HoodieRequestedReplaceMetadata clusteringMetadata,
      String clusteringInstantTime) {
    Set<String> clusteringPartitions = ClusteringUtils.getFileGroupsFromClusteringPlan(
        clusteringMetadata.getClusteringPlan())
        .map(HoodieFileGroupId::getPartitionPath)
        .collect(Collectors.toSet());

    if (clusteringPartitions.isEmpty()) {
      return;
    }

    LOG.info("Checking for DELETE_PARTITION conflicts with clustering plan {} targeting partitions {}",
        clusteringInstantTime, clusteringPartitions);

    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();

    checkCompletedDeletePartitionCommits(activeTimeline, clusteringPartitions, clusteringInstantTime);
    checkInflightDeletePartitionCommits(metaClient, activeTimeline, clusteringPartitions, clusteringInstantTime);
  }

  private static void checkCompletedDeletePartitionCommits(
      HoodieActiveTimeline activeTimeline,
      Set<String> clusteringPartitions,
      String clusteringInstantTime) {
    List<HoodieInstant> completedReplaceCommits = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.REPLACE_COMMIT_ACTION))
        .filterCompletedInstants()
        .findInstantsModifiedAfterByCompletionTime(clusteringInstantTime)
        .getInstantsAsStream()
        .collect(Collectors.toList());

    for (HoodieInstant instant : completedReplaceCommits) {
      try {
        HoodieReplaceCommitMetadata replaceMetadata = activeTimeline.readReplaceCommitMetadata(instant);
        if (WriteOperationType.DELETE_PARTITION.equals(replaceMetadata.getOperationType())) {
          Set<String> deletedPartitions = replaceMetadata.getPartitionToReplaceFileIds().keySet();
          if (!Collections.disjoint(clusteringPartitions, deletedPartitions)) {
            Set<String> overlapping = new HashSet<>(clusteringPartitions);
            overlapping.retainAll(deletedPartitions);
            throw new HoodieException("Cannot create clustering plan " + clusteringInstantTime
                + ". Completed DELETE_PARTITION replacecommit " + instant
                + " targets overlapping partitions: " + overlapping);
          }
        }
      } catch (IOException io) {
        throw new HoodieIOException("Error reading completed replacecommit " + instant, io);
      }
    }
  }

  private static void checkInflightDeletePartitionCommits(
      HoodieTableMetaClient metaClient,
      HoodieActiveTimeline activeTimeline,
      Set<String> clusteringPartitions,
      String clusteringInstantTime) {
    List<HoodieInstant> inflightReplaceCommits = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.REPLACE_COMMIT_ACTION))
        .filterInflights()
        .getInstantsAsStream()
        .collect(Collectors.toList());

    for (HoodieInstant instant : inflightReplaceCommits) {
      Option<String> conflictMessage = Option.empty();
      try {
        Option<HoodieCommitMetadata> inflightMetadataOption =
            readInflightReplaceCommitMetadata(metaClient, instant.requestedTime());
        if (!inflightMetadataOption.isPresent()) {
          continue;
        }
        HoodieCommitMetadata inflightMetadata = inflightMetadataOption.get();
        if (WriteOperationType.DELETE_PARTITION.equals(inflightMetadata.getOperationType())) {
          String targetedValue = inflightMetadata.getMetadata(STASH_DELETE_PARTITION_TARGETED_FOR_DELETION_KEY);
          Set<String> deletePartitions = parseTargetedPartitions(targetedValue);
          if (!Collections.disjoint(clusteringPartitions, deletePartitions)) {
            Set<String> overlapping = new HashSet<>(clusteringPartitions);
            overlapping.retainAll(deletePartitions);
            conflictMessage = Option.of("Cannot create clustering plan " + clusteringInstantTime
                + ". Inflight DELETE_PARTITION replacecommit " + instant
                + " targets overlapping partitions: " + overlapping);
          }
        }
      } catch (Exception e) {
        activeTimeline = metaClient.reloadActiveTimeline();
        if (!activeTimeline.containsInstant(instant)) {
          LOG.warn("Error reading inflight replacecommit {} which is no longer in the timeline after reload. "
              + "Likely rolled back. Ignoring.", instant, e);
          continue;
        } else {
          throw new HoodieException("Error reading inflight replacecommit " + instant
              + " which still exists in the timeline after reload", e);
        }
      }
      if (conflictMessage.isPresent()) {
        throw new HoodieException(conflictMessage.get());
      }
    }
  }

  /**
   * Parse the list of partitions targeted for deletion from the TARGETED_FOR_DELETION metadata value,
   * which is stored as a JSON array (e.g., '["part1","part2","part3"]').
   */
  public static Set<String> parseTargetedPartitions(String targeted) {
    if (targeted == null || targeted.isEmpty()) {
      return Collections.emptySet();
    }
    try {
      List<String> partitions = JsonUtils.getObjectMapper().readValue(targeted,
          JsonUtils.getObjectMapper().getTypeFactory().constructCollectionType(List.class, String.class));
      return new HashSet<>(partitions);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to parse " + STASH_DELETE_PARTITION_TARGETED_FOR_DELETION_KEY
          + " value: " + targeted, e);
    }
  }

  /**
   * Reads inflight replacecommit metadata for the given instant timestamp.
   */
  public static Option<HoodieCommitMetadata> readInflightReplaceCommitMetadata(
      HoodieTableMetaClient metaClient, String instantTimestamp) {
    HoodieInstant instant = metaClient.getInstantGenerator().createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTimestamp);
    try {
      Option<byte[]> content = metaClient.reloadActiveTimeline().getInstantDetails(instant);
      if (!content.isPresent() || content.get().length == 0) {
        return Option.empty();
      }
      return Option.of(metaClient.getCommitMetadataSerDe().deserialize(instant,
          new ByteArrayInputStream(content.get()), () -> content.get().length == 0, HoodieCommitMetadata.class));
    } catch (Exception e) {
      HoodieActiveTimeline reloadedTimeline = metaClient.reloadActiveTimeline();
      if (!reloadedTimeline.containsInstant(instant)) {
        LOG.warn("Error reading inflight replacecommit {} which is no longer in the timeline after reload. "
            + "Likely rolled back. Ignoring.", instant, e);
        return Option.empty();
      }
      throw new HoodieException("Error reading inflight replacecommit " + instant
          + " which still exists in the timeline after reload", e);
    }
  }

}
