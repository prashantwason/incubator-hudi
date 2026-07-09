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

package org.apache.hudi.replication.table;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.Collectors;

public class HoodieReplicationTimelineComparator {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HoodieReplicationTimelineComparator.class);
  HoodieTableMetaClient localMetaClient;
  HoodieTableMetaClient remoteMetaClient;

  HoodieReplicationTimelineComparator(HoodieTableMetaClient localClient, HoodieTableMetaClient remoteClient) {
    this.localMetaClient = localClient;
    this.remoteMetaClient = remoteClient;
  }

  /**
   * Compares the local and remote timelines to return the archived commits that are still present in the
   * remote timeline, but archived on localTimeline and are older than the oldest common parent.  Check can be performed
   * on write timeline and non-write timeline (clean, rollback etc) separately.
   * @return stream of commit timestamps
   */
  public List<String> getRemoteCommitsForArchivalInWriteTimeline() {
    return filterArchivedCommitsGreaterThanECTR(getRemoteCommitsForArchival(localMetaClient.getActiveTimeline().getWriteTimeline(),
            remoteMetaClient.getActiveTimeline().getWriteTimeline())).collect(Collectors.toList());
  }

  /***
   * Compares the local and remote write timelines to return the list of archived instants that are
   * still present in remote timeline, but archived on local timeline and are older than the oldest common parent.
   *
   * @return List of hoodie instants
   */
  public List<HoodieInstant> getRemoteInstantsForArchivalInWriteTimeline() {
    List<String> archivalCommits = filterArchivedCommitsGreaterThanECTR(getRemoteCommitsForArchival(localMetaClient.getActiveTimeline().getWriteTimeline(),
            remoteMetaClient.getActiveTimeline().getWriteTimeline())).collect(Collectors.toList());
    return remoteMetaClient
            .getRawActiveTimeline()
            .getInstantsAsStream()
            .filter(instant -> archivalCommits.contains(instant.requestedTime()))
            .collect(Collectors.toList());
  }

  public List<String> getRemoteCommitsForArchivalInNonWriteTimeline() {
    List<String> remoteCommitsForArchival = new ArrayList<>();
    remoteCommitsForArchival.addAll(getRemoteCommitsForArchival(localMetaClient.getActiveTimeline().getCleanerTimeline(),
            remoteMetaClient.getActiveTimeline().getCleanerTimeline()).collect(Collectors.toList()));
    remoteCommitsForArchival.addAll(getRemoteCommitsForArchival(localMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline(),
            remoteMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline()).collect(Collectors.toList()));
    return remoteCommitsForArchival;
  }

  /***
   * Compares the local and remote non write timelines to return the list of archived instants that are
   * still present in remote timeline, but archived on local timeline and are older than the oldest common parent.
   *
   * @return List of hoodie instants
   */
  public List<HoodieInstant> getRemoteInstantsForArchivalInNonWriteTimeline() {
    List<String> remoteCommitsForArchival = new ArrayList<>();
    remoteCommitsForArchival.addAll(getRemoteCommitsForArchival(localMetaClient.getActiveTimeline().getCleanerTimeline(),
            remoteMetaClient.getActiveTimeline().getCleanerTimeline()).collect(Collectors.toList()));
    remoteCommitsForArchival.addAll(getRemoteCommitsForArchival(localMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline(),
            remoteMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline()).collect(Collectors.toList()));
    return remoteMetaClient
            .getRawActiveTimeline()
            .getInstantsAsStream()
            .filter(instant -> remoteCommitsForArchival.contains(instant.requestedTime()))
            .collect(Collectors.toList());
  }

  /**
   * Get the diverged commits in the remote timeline that are not present in the local timeline, and newer than
   * the latestCommonParent.
   * @return diverged commits in remote timeline
   */
  public Set<String> getDivergedRemoteCommits() {
    return getDivergedRemoteCommits(
            localMetaClient.getActiveTimeline().getAllCommitsTimeline(),
            remoteMetaClient.getActiveTimeline().getAllCommitsTimeline()).collect(Collectors.toSet());
  }

  /**
   * find the oldest common parent between the local and remote write timelines.
   * @return oldest common parent in write timeline
   */
  public Option<String> findOldestCommonParentInActiveWriteTimeline() {
    return findOldestCommonParent(localMetaClient.getActiveTimeline().getWriteTimeline(),
            remoteMetaClient.getActiveTimeline().getWriteTimeline());
  }

  /**
   * Find the latest common parent between the local and remote write timelines.
   * This is used to determine the last replicated commit (LRT) when checkpoint is unavailable or not stored.
   * Walks through the target timeline in reverse chronological order and finds the first
   * commit that exists on the source timeline.
   * @return latest common parent in write timeline
   */
  public Option<String> findLatestCommonParentInActiveWriteTimeline() {
    return findLatestCommonParent(localMetaClient.getActiveTimeline().getWriteTimeline(),
            remoteMetaClient.getActiveTimeline().getWriteTimeline());
  }

  /**
   * Find the latest common parent between the local and remote replace commit timelines.
   * This is used to determine the last replicated clustering commit when checkpoint is unavailable or not stored.
   * Walks through the target replacecommit timeline in reverse chronological order and finds
   * the first commit that exists on the source timeline.
   * @return latest common parent in replace commit timeline
   */
  public Option<String> findLatestCommonParentInActiveReplaceTimeline() {
    return findLatestCommonParent(localMetaClient.getActiveTimeline().getCompletedReplaceTimeline(),
            remoteMetaClient.getActiveTimeline().getCompletedReplaceTimeline());
  }

  /**
   * find the oldest common parent between the local and remote non-write timelines.
   * @return oldest common parent
   */
  public Option<String> findOldestCommonParentInActiveNonWriteTimeline() {
    Option<String> oldestClean = findOldestCommonParent(localMetaClient.getActiveTimeline().getCleanerTimeline(),
            remoteMetaClient.getActiveTimeline().getCleanerTimeline());
    Option<String> oldestRollback = findOldestCommonParent(localMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline(),
            remoteMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline());
    if (!oldestClean.isPresent()) {
      return oldestRollback;
    }
    if (!oldestRollback.isPresent()) {
      return oldestClean;
    }
    return (oldestClean.get().compareTo(oldestRollback.get()) <= 0 ? oldestClean : oldestRollback);
  }

  private Set<String> getDivergedRemoteWriteCommits() {
    return getDivergedRemoteCommits(localMetaClient.getActiveTimeline().getWriteTimeline(),
            remoteMetaClient.getActiveTimeline().getWriteTimeline()).collect(Collectors.toSet());
  }

  private Set<String> getDivergedRemoteNonWriteCommits() {
    Set<String> divergedRemoteNonWriteCommits = new HashSet<>();
    divergedRemoteNonWriteCommits.addAll(getDivergedRemoteCommits(
            localMetaClient.getActiveTimeline().getCleanerTimeline(),
            remoteMetaClient.getActiveTimeline().getCleanerTimeline()).collect(Collectors.toSet()));
    divergedRemoteNonWriteCommits.addAll(getDivergedRemoteCommits(
            localMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline(),
            remoteMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline()).collect(Collectors.toSet()));
    return divergedRemoteNonWriteCommits;
  }

  /**
   * Commits that are present only on the remote timeline and newer than the latest common parent.
   * @param localTimeline
   * @param remoteTimeline
   * @return
   */
  public Stream<String> getDivergedRemoteCommits(HoodieTimeline localTimeline, HoodieTimeline remoteTimeline) {
    Option<String> latestCommonCommit = findLatestCommonParent(localTimeline, remoteTimeline);
    if (latestCommonCommit.isPresent()) {
      return getAllRemoteOnlyCommits(localTimeline, remoteTimeline)
              .filter(dc -> latestCommonCommit.get().compareTo(dc) < 0);
    }
    return Stream.empty();
  }

  /**
   * Get all commits that are present on remoteTimeline but absent on localTimeline.
   * @param localTimeline
   * @param remoteTimeline
   * @return stream of commits present only on remote timeline.
   */
  private Stream<String> getAllRemoteOnlyCommits(HoodieTimeline localTimeline, HoodieTimeline remoteTimeline) {
    Set<String> localCommits = getCommitTs(localTimeline);
    return getCommitTs(remoteTimeline).stream().filter(i -> !localCommits.contains(i));
  }

  /**
   * find the oldest commit from the list of common commits between local, remote timelines.
   * @param localTimeline
   * @param remoteTimeline
   * @return
   */
  public Option<String> findOldestCommonParent(HoodieTimeline localTimeline, HoodieTimeline remoteTimeline) {
    try {
      List<String> commits = getCommonCommits(localTimeline, remoteTimeline).sorted().collect(Collectors.toList());
      return commits.isEmpty() ? Option.empty() : Option.of(commits.get(0));
    } catch (NoSuchElementException e) {
      return Option.empty();
    }
  }

  /**
   * find latest commit from the list of common commits between the local, remote timelines.
   * @param localTimeline
   * @param remoteTimeline
   * @return
   */
  public Option<String> findLatestCommonParent(HoodieTimeline localTimeline, HoodieTimeline remoteTimeline) {
    try {
      List<String> commits = getCommonCommits(localTimeline, remoteTimeline).sorted().collect(Collectors.toList());
      return commits.isEmpty() ? Option.empty() : Option.of(commits.get(commits.size() - 1));
    } catch (NoSuchElementException e) {
      return Option.empty();
    }
  }

  /**
   * Get the common commits between the local and remote timeline.
   * @return common commits
   */
  public Stream<String> getCommonCommits(HoodieTimeline localTimeline, HoodieTimeline remoteTimeline) {
    Set<String> remoteCommits = getCommitTs(remoteTimeline);
    return getCommitTs(localTimeline).stream().filter(remoteCommits::contains);
  }

  /**
   * Returns a stream of commit timestamps that have been archived on the localTimeline, but are still present
   * on the remoteTimeline.
   * @param localTimeline
   * @param remoteTimeline
   * @return
   */
  public Stream<String> getRemoteCommitsForArchival(HoodieTimeline localTimeline, HoodieTimeline remoteTimeline) {
    Set<String> divergedRemoteCommits =  getAllRemoteOnlyCommits(localTimeline, remoteTimeline).collect(Collectors.toSet());
    Option<String> oldestCommonParent = findOldestCommonParent(localTimeline, remoteTimeline);
    if (oldestCommonParent.isPresent()) {
      LOG.info("Oldest common parent for table {} : {}", localMetaClient.getTableName(), oldestCommonParent.get());
      return divergedRemoteCommits.stream().filter(ts -> ts.compareTo(oldestCommonParent.get()) < 0).sorted();
    }
    if (!localTimeline.getInstants().isEmpty()) {
      LOG.info("Oldest common parent didn't find for table {}, using the earliest commit in localTimeline for comparison", localMetaClient.getTableName());
      String oldestCommitInLocalTimeline = localTimeline.getInstants().get(0).requestedTime();
      return divergedRemoteCommits.stream().filter(ts -> ts.compareTo(oldestCommitInLocalTimeline) < 0).sorted();
    }
    return Stream.empty();
  }

  private Stream<String> filterArchivedCommitsGreaterThanECTR(Stream<String> commitsForArchival) {
    HoodieActiveTimeline remoteActiveTimeline = remoteMetaClient.getActiveTimeline();
    HoodieActiveTimeline localActiveTimeline = localMetaClient.getActiveTimeline();
    Option<HoodieInstant> latestCleanInstant = remoteActiveTimeline.getCleanerTimeline().filterCompletedInstants().lastInstant();

    if (!latestCleanInstant.isPresent()) {
      // Check if there are any replace commits in the commitsForArchival
      List<String> commitsForArchivalList = commitsForArchival.collect(Collectors.toList());
      HoodieTimeline remoteReplaceTimeline = remoteActiveTimeline.getCompletedReplaceTimeline();
      Set<String> remoteReplaceTimestamps = remoteReplaceTimeline.getInstants().stream()
              .map(HoodieInstant::requestedTime).collect(Collectors.toSet());
      if (commitsForArchivalList.stream().noneMatch(remoteReplaceTimestamps::contains)) {
        // No replace commit found in commits eligible for archival, returning the given stream
        LOG.warn("No replace commits eligible for archival found; remote timeline has no clean instant. Returning commits for archival as-is.");
        return commitsForArchivalList.stream();
      }
      // If there is a replace-commit in the commits eligible for archival, and in remote timeline there is no
      // clean instant found, i.e. we can expect there is a clean instant in primary which is yet to be replicated
      // to the remote timeline. But for now keeping the exists check in place so that to make sure that
      // assumed invariant is true
      Option<String> latestCommonParent = findLatestCommonParent(localActiveTimeline, remoteActiveTimeline);
      if (!latestCommonParent.isPresent()) {
        LOG.warn("Common parent not found while trying to filter commits for archival");
        // This can happen when it is first time of bootstrap of the remote client
        return commitsForArchivalList.stream();
      }
      if (localActiveTimeline.getCleanerTimeline().getInstants().stream().anyMatch(instant -> instant.requestedTime().compareTo(latestCommonParent.get()) > 0)) {
        LOG.warn("Pausing the archival as localTimeline has un-replicated clean instants and no clean instant found in secondary");
        return Stream.empty();
      } else {
        // returning instants as it is as no clean instants were present / un-replicated
        LOG.warn("Remote timeline has no clean instant. Returning commits for archival as-is.");
        return commitsForArchivalList.stream();
      }
    }

    Option<byte[]> data = remoteActiveTimeline.getInstantDetails(latestCleanInstant.get());
    if (data.isPresent() && data.get().length > 0) {
      HoodieCleanMetadata cleanMetadata = null;
      try {
        cleanMetadata = TimelineMetadataUtils.deserializeAvroMetadata(new ByteArrayInputStream(data.get()), HoodieCleanMetadata.class);
      } catch (IOException e) {
        LOG.error("Failed to deserialize HoodieCleanMetadata for the instant {}", latestCleanInstant.get().requestedTime());
        throw new HoodieIOException(
                "Error reading earliest commit to retain from latest completed clean", e);
      }
      if (cleanMetadata != null) {
        String ectrInstant = cleanMetadata.getEarliestCommitToRetain();
        if (ectrInstant != null && !ectrInstant.isEmpty()) {
          return commitsForArchival.filter(instant -> instant.compareTo(ectrInstant) < 0);
        }
      }
    }

    return commitsForArchival;
  }

  private Set<String> getCommitTs(HoodieTimeline timeline) {
    return timeline.getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
  }
}
