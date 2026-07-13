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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.hudi.exception.HoodieException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link HoodieReplicationTimelineComparator}.
 */
public class TestHoodieReplicationTimelineComparator {
  @TempDir
  public java.nio.file.Path tempFolder;

  private HoodieTableMetaClient localMetaClient;
  private HoodieTableMetaClient remoteMetaClient;

  @BeforeEach
  public void setUp() throws IOException {
    localMetaClient = createMetaClient("local");
    remoteMetaClient = createMetaClient("remote");
  }

  @AfterEach
  public void tearDown() {
    if (localMetaClient != null) {
      localMetaClient = null;
    }
    if (remoteMetaClient != null) {
      remoteMetaClient = null;
    }
  }

  private HoodieTableMetaClient createMetaClient(String name) throws IOException {
    java.nio.file.Path basePath = tempFolder.resolve(name);
    Files.createDirectories(basePath);
    return HoodieTestUtils.init(basePath.toAbsolutePath().toString());
  }

  /**
   * Creates a completed commit on the given meta client's timeline.
   * Creates: ts.action.requested, ts.action.inflight, ts.action (completed).
   */
  private void createCompletedInstant(HoodieTableMetaClient mc, String ts, String action) throws IOException {
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, action, ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    mc.getActiveTimeline().createNewInstant(requested);
    HoodieInstant inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, action, ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    mc.getActiveTimeline().transitionRequestedToInflight(requested, org.apache.hudi.common.util.Option.empty());
    mc.getActiveTimeline().saveAsComplete(inflight, org.apache.hudi.common.util.Option.empty());
    mc.reloadActiveTimeline();
  }

  /**
   * Scenario:
   * Local  (primary, archived older commits): C4, C5, C6 active
   * Remote (secondary):                       C1, C2, C3, C4, C5, C6 active
   * <p>
   * C1, C2, C3 are on remote but not on local → candidates for archival.
   * The oldest common parent is C4, so C1/C2/C3 (< C4) should be returned.
   * <p>
   * Verifies that getRemoteInstantsForArchivalInWriteTimeline() returns HoodieInstant
   * objects (not just timestamps), preserving state and action from the remote timeline.
   */
  @Test
  public void testGetRemoteInstantsForArchivalInWriteTimeline_returnsInstantObjects() throws Exception {
    // Remote has C1..C6; local only has C4..C6 (C1-C3 archived on local)
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String c6 = "20230101000006";

    for (String ts : new String[]{c1, c2, c3, c4, c5, c6}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    for (String ts : new String[]{c4, c5, c6}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<HoodieInstant> result = comparator.getRemoteInstantsForArchivalInWriteTimeline();

    // Only C1, C2, C3 should be returned (archived on local, older than C4 the common parent)
    Set<String> resultTs = result.stream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    assertEquals(3, resultTs.size(), "Should return 3 archived timestamps");
    assertTrue(resultTs.contains(c1));
    assertTrue(resultTs.contains(c2));
    assertTrue(resultTs.contains(c3));
    assertFalse(resultTs.contains(c4), "C4 is common, should not be in archival list");

    // Each timestamp has 3 states: REQUESTED, INFLIGHT, COMPLETED
    // Verify the returned objects are HoodieInstant with proper state (not just strings)
    assertEquals(9, result.size(), "Should return 9 instants (3 commits × 3 states)");
    Set<HoodieInstant.State> states = result.stream().map(HoodieInstant::getState).collect(Collectors.toSet());
    assertTrue(states.contains(HoodieInstant.State.REQUESTED));
    assertTrue(states.contains(HoodieInstant.State.INFLIGHT));
    assertTrue(states.contains(HoodieInstant.State.COMPLETED));
  }

  /**
   * Verifies that when local and remote write timelines are identical, no instants are
   * returned (nothing to archive).
   */
  @Test
  public void testGetRemoteInstantsForArchivalInWriteTimeline_whenNoArchival_returnsEmpty() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    for (String ts : new String[]{c1, c2}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);
    List<HoodieInstant> result = comparator.getRemoteInstantsForArchivalInWriteTimeline();

    assertTrue(result.isEmpty(), "No instants should be returned when timelines are in sync");
  }

  /**
   * Scenario:
   * Local  clean timeline:    CL4, CL5, CL6
   * Remote clean timeline:    CL1, CL2, CL3, CL4, CL5, CL6
   * <p>
   * CL1, CL2, CL3 are on remote but archived on local → should be returned.
   * Verifies HoodieInstant objects are returned (not strings), with correct states.
   */
  @Test
  public void testGetRemoteInstantsForArchivalInNonWriteTimeline_cleanerTimeline() throws Exception {
    String cl1 = "20230101000001";
    String cl2 = "20230101000002";
    String cl3 = "20230101000003";
    String cl4 = "20230101000004";
    String cl5 = "20230101000005";
    String cl6 = "20230101000006";

    for (String ts : new String[]{cl1, cl2, cl3, cl4, cl5, cl6}) {
      createCompletedInstant(remoteMetaClient, ts, CLEAN_ACTION);
    }
    for (String ts : new String[]{cl4, cl5, cl6}) {
      createCompletedInstant(localMetaClient, ts, CLEAN_ACTION);
    }

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<HoodieInstant> result = comparator.getRemoteInstantsForArchivalInNonWriteTimeline();

    Set<String> resultTs = result.stream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    assertEquals(3, resultTs.size());
    assertTrue(resultTs.contains(cl1));
    assertTrue(resultTs.contains(cl2));
    assertTrue(resultTs.contains(cl3));
    assertFalse(resultTs.contains(cl4));

    // Each commit has 3 states
    assertEquals(9, result.size());
  }

  /**
   * Scenario: rollback commits on remote that are archived on local.
   * Verifies that getRemoteInstantsForArchivalInNonWriteTimeline handles rollback timeline.
   */
  @Test
  public void testGetRemoteInstantsForArchivalInNonWriteTimeline_rollbackTimeline() throws Exception {
    String rb1 = "20230101000001";
    String rb2 = "20230101000002";
    String rb3 = "20230101000003";
    String rb4 = "20230101000004";

    for (String ts : new String[]{rb1, rb2, rb3, rb4}) {
      createCompletedInstant(remoteMetaClient, ts, ROLLBACK_ACTION);
    }
    for (String ts : new String[]{rb3, rb4}) {
      createCompletedInstant(localMetaClient, ts, ROLLBACK_ACTION);
    }

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<HoodieInstant> result = comparator.getRemoteInstantsForArchivalInNonWriteTimeline();

    Set<String> resultTs = result.stream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    assertEquals(2, resultTs.size());
    assertTrue(resultTs.contains(rb1));
    assertTrue(resultTs.contains(rb2));
    assertFalse(resultTs.contains(rb3));
    assertFalse(resultTs.contains(rb4));
  }

  /**
   * Tests the new fallback behavior in getRemoteCommitsForArchival when there is no
   * common parent between local and remote timelines.
   * <p>
   * Scenario :
   * Local  timeline: C5.clean, C6, C7
   * Remote timeline: C1, C2, C3, C4, C5.clean
   * <p>
   * No common parent for write timeline exists. The new logic uses C5 (oldest in local) as the boundary.
   * All remote commits older than C5 should be returned → C1, C2, C3, C4.
   */
  @Test
  public void testGetRemoteCommitsForArchival_whenNoCommonParent_usesOldestLocalCommitAsFallback() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String c6 = "20230101000006";
    String c7 = "20230101000007";

    // Remote only has C1-C4
    for (String ts : new String[]{c1, c2, c3, c4}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    // Create C5 as common parent but as a clean instant
    createCompletedInstant(localMetaClient, c5, CLEAN_ACTION);
    createCompletedInstant(remoteMetaClient, c5, CLEAN_ACTION);

    // Local only has C6-C7 (no overlap)
    for (String ts : new String[]{c6, c7}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    HoodieTimeline localWriteTimeline = localMetaClient.getActiveTimeline().getWriteTimeline();
    HoodieTimeline remoteWriteTimeline = remoteMetaClient.getActiveTimeline().getWriteTimeline();
    List<String> result = comparator.getRemoteCommitsForArchival(localWriteTimeline, remoteWriteTimeline)
            .collect(Collectors.toList());

    assertEquals(4, result.size(), "All 4 remote-only commits (< C5) should be returned");
    assertTrue(result.contains(c1));
    assertTrue(result.contains(c2));
    assertTrue(result.contains(c3));
    assertTrue(result.contains(c4));
  }

  /**
   * When local timeline is empty and there is no common parent,
   * getRemoteCommitsForArchival should return an empty stream (original behavior preserved).
   */
  @Test
  public void testGetRemoteCommitsForArchival_whenNoCommonParentAndLocalEmpty_returnsEmpty() throws Exception {
    // Remote has some commits, local has none
    createCompletedInstant(remoteMetaClient, "20230101000001", COMMIT_ACTION);
    createCompletedInstant(remoteMetaClient, "20230101000002", COMMIT_ACTION);

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    HoodieTimeline localWriteTimeline = localMetaClient.getActiveTimeline().getWriteTimeline();
    HoodieTimeline remoteWriteTimeline = remoteMetaClient.getActiveTimeline().getWriteTimeline();
    Stream<String> result = comparator.getRemoteCommitsForArchival(localWriteTimeline, remoteWriteTimeline);

    assertEquals(0, result.count(), "Should return empty when local timeline is empty");
  }

  /**
   * Helper: creates a completed clean instant with serialized HoodieCleanMetadata containing
   * the given earliestCommitToRetain (ECTR) value.
   */
  private void createCompletedCleanWithECTR(HoodieTableMetaClient mc, String ts, String ectrInstant) throws IOException {
    HoodieCleanMetadata cleanMetadata = HoodieCleanMetadata.newBuilder()
            .setVersion(1)
            .setTimeTakenInMillis(100)
            .setTotalFilesDeleted(0)
            .setStartCleanTime(ts)
            .setEarliestCommitToRetain(ectrInstant)
            .setLastCompletedCommitTimestamp("")
            .setPartitionMetadata(new java.util.HashMap<>())
            .build();
    Option<byte[]> content = TimelineMetadataUtils.serializeAvroMetadata(cleanMetadata, HoodieCleanMetadata.class);
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, CLEAN_ACTION, ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    mc.getStorage().create(new StoragePath(mc.getMetaPath(), mc.getInstantFileNameGenerator().getFileName(requested)), false).close();
    mc.getActiveTimeline().transitionRequestedToInflight(requested, Option.empty());
    HoodieInstant inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, CLEAN_ACTION, ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    mc.getActiveTimeline().saveAsComplete(inflight, content);
    mc.reloadActiveTimeline();
  }

  /**
   * Verifies that filterArchivedCommitsGreaterThanECTR (via getRemoteCommitsForArchivalInWriteTimeline)
   * filters out commits at or above the ECTR from the latest remote clean.
   * <p>
   * Scenario:
   * Remote write timeline: C1, C2, C3, C4, C5, C6
   * Local write timeline:  C4, C5, C6  (C1-C3 archived on local)
   * Remote clean with ECTR = C3
   * <p>
   * Expected: only C1, C2 are returned — C3 is at ECTR so it is filtered out.
   */
  @Test
  public void testGetRemoteCommitsForArchivalInWriteTimeline_ectrFiltersCommitsAtOrAboveEctr() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String c6 = "20230101000006";
    String cleanTs = "20230101000007";

    for (String ts : new String[]{c1, c2, c3, c4, c5, c6}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    for (String ts : new String[]{c4, c5, c6}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }
    createCompletedCleanWithECTR(remoteMetaClient, cleanTs, c3);

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<String> result = comparator.getRemoteCommitsForArchivalInWriteTimeline();

    assertEquals(2, result.size(), "Only commits strictly older than ECTR should be returned");
    assertTrue(result.contains(c1));
    assertTrue(result.contains(c2));
    assertFalse(result.contains(c3), "C3 equals ECTR, should be filtered out");
  }

  /**
   * When the latest remote clean exists but has an empty ECTR, the filter should be a no-op
   * and all archived commits should be returned.
   */
  @Test
  public void testGetRemoteCommitsForArchivalInWriteTimeline_emptyEctr_returnsAllArchived() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String cleanTs = "20230101000006";

    for (String ts : new String[]{c1, c2, c3, c4, c5}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    for (String ts : new String[]{c3, c4, c5}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }
    // Clean exists but ECTR is empty — filter should not apply
    createCompletedCleanWithECTR(remoteMetaClient, cleanTs, "");

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<String> result = comparator.getRemoteCommitsForArchivalInWriteTimeline();

    assertEquals(2, result.size(), "All archived commits should be returned when ECTR is empty");
    assertTrue(result.contains(c1));
    assertTrue(result.contains(c2));
  }

  /**
   * Creates a completed clean instant with corrupted (non-Avro) bytes so that
   * deserialization will throw an IOException.
   */
  private void createCompletedCleanWithCorruptedData(HoodieTableMetaClient mc, String ts) throws IOException {
    byte[] corruptedBytes = new byte[]{1, 2, 3, 4, 5};
    Option<byte[]> content = Option.of(corruptedBytes);
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, CLEAN_ACTION, ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    mc.getStorage().create(new StoragePath(mc.getMetaPath(), mc.getInstantFileNameGenerator().getFileName(requested)), false).close();
    mc.getActiveTimeline().transitionRequestedToInflight(requested, Option.empty());
    HoodieInstant inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, CLEAN_ACTION, ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    mc.getActiveTimeline().saveAsComplete(inflight, content);
    mc.reloadActiveTimeline();
  }

  /**
   * Verifies that archival is paused (empty stream returned) when:
   * - Remote has no completed clean instant
   * - Local has at least one clean instant whose timestamp is newer than the latest common parent
   *
   * Scenario:
   * Remote write timeline: C1, C2, C3, C4, C5  (C1-C3 will be archived on local)
   * Local  write timeline: C4, C5
   * Local  clean timeline: CL6  (unreplicated — remote has no clean at all)
   *
   * Latest common parent (across all timelines) = C5.
   * CL6 > C5 → archival should be paused → getRemoteCommitsForArchivalInWriteTimeline() returns empty.
   */
  @Test
  public void testFilterArchival_whenNoCleanOnRemote_andLocalHasUnreplicatedClean_withNoReplaceCommitInCommitsForArchival() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String cl6 = "20230101000006";

    for (String ts : new String[]{c1, c2, c3, c4, c5}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    for (String ts : new String[]{c4, c5}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }
    // Local has a clean at cl6 that has not been replicated (remote has no clean)
    createCompletedInstant(localMetaClient, cl6, CLEAN_ACTION);

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<String> result = comparator.getRemoteCommitsForArchivalInWriteTimeline();

    assertEquals(3, result.size(), "All archived commits should be returned as-is");
    assertTrue(result.contains(c1));
    assertTrue(result.contains(c2));
    assertTrue(result.contains(c3));
  }

  /**
   * Verifies that archival is paused (empty stream returned) when:
   * - Remote has no completed clean instant
   * - Local has at least one clean instant whose timestamp is newer than the latest common parent
   * - Commits for archival has the replace instant
   *
   * Scenario:
   * Remote write timeline: C1, C2, C3, C4, C5  (C1-C3 will be archived on local, C3 will be the replace instant)
   * Local  write timeline: C4, C5
   * Local  clean timeline: CL6  (unreplicated — remote has no clean at all)
   *
   * Latest common parent (across all timelines) = C5.
   * CL6 > C5 → archival should be paused → getRemoteCommitsForArchivalInWriteTimeline() returns empty.
   */
  @Test
  public void testFilterArchival_whenNoCleanOnRemote_andLocalHasUnreplicatedClean_withReplaceCommitInCommitsForArchival() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String cl6 = "20230101000006";

    for (String ts : new String[]{c1, c2, c4, c5}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    createCompletedInstant(remoteMetaClient, c3, REPLACE_COMMIT_ACTION);
    for (String ts : new String[]{c4, c5}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }
    // Local has a clean at cl6 that has not been replicated (remote has no clean)
    createCompletedInstant(localMetaClient, cl6, CLEAN_ACTION);

    HoodieReplicationTimelineComparator comparator =
        new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    List<String> result = comparator.getRemoteCommitsForArchivalInWriteTimeline();

    assertTrue(result.isEmpty(),
        "Archival should be paused when local has an unreplicated clean and remote has no clean");
  }

  /**
   * Verifies that we return none when there is no clean on the remote
   * timeline AND no common parent can be found between the local and remote timelines.
   *
   * Scenario:
   * Remote write timeline: {}   (completely empty)
   * Local  write timeline: C1, C2, C3, C4, C5, C6, C7      (completely disjoint from remote)
   * Remote: no clean instants
   *
   * Since there is no common commit on either timeline, findLatestCommonParent returns empty,
   * which we return empty set since there is no common parent
   */
  @Test
  public void testFilterArchival_whenNoCleanOnRemote_andNoCommonParent() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String c6 = "20230101000006";
    String c7 = "20230101000007";

    for (String ts : new String[]{c1, c2, c3, c4, c5, c6, c7}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }

    HoodieReplicationTimelineComparator comparator =
        new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    assertEquals(0, comparator.getRemoteInstantsForArchivalInWriteTimeline().size());
  }

  /**
   * Verifies that when the latest remote clean instant has corrupted (non-deserializable) metadata,
   * throwing an exception
   *
   * Scenario:
   * Remote write timeline: C1, C2, C3, C4, C5
   * Local  write timeline: C4, C5  (C1-C3 archived on local)
   * Remote clean timeline: CL6 with corrupted bytes (undeserializable)
   */
  @Test
  public void testFilterArchival_whenCleanDeserializationFails_throwsException() throws Exception {
    String c1 = "20230101000001";
    String c2 = "20230101000002";
    String c3 = "20230101000003";
    String c4 = "20230101000004";
    String c5 = "20230101000005";
    String cl6 = "20230101000006";

    for (String ts : new String[]{c1, c2, c3, c4, c5}) {
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    for (String ts : new String[]{c4, c5}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
    }
    // Remote has a clean with corrupted data — deserialization will throw IOException
    createCompletedCleanWithCorruptedData(remoteMetaClient, cl6);

    HoodieReplicationTimelineComparator comparator =
        new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);
    try {
      List<String> result = comparator.getRemoteCommitsForArchivalInWriteTimeline();
      fail("Expected exception when trying to read the malformed clean metadata");
    } catch (HoodieException e) {
      assertEquals("Error reading earliest commit to retain from latest completed clean",
              e.getMessage());
    }
  }

  /**
   * Verifies that findOldestCommonParent correctly returns the chronologically
   * smallest timestamp
   */
  @Test
  public void testFindOldestCommonParent_returnsSortedOldest() throws Exception {
    // Both timelines share C1, C3, C5 as common commits
    String c1 = "20230101000001";
    String c3 = "20230101000003";
    String c5 = "20230101000005";
    // Local also has C7 (not on remote)
    String c7 = "20230101000007";
    // Remote also has C0 (not on local, and older than C1)
    String c0 = "20230101000000";

    for (String ts : new String[]{c1, c3, c5}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    createCompletedInstant(localMetaClient, c7, COMMIT_ACTION);
    createCompletedInstant(remoteMetaClient, c0, COMMIT_ACTION);

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    HoodieTimeline localTimeline = localMetaClient.getActiveTimeline().getWriteTimeline();
    HoodieTimeline remoteTimeline = remoteMetaClient.getActiveTimeline().getWriteTimeline();
    Option<String> oldest = comparator.findOldestCommonParent(localTimeline, remoteTimeline);

    assertTrue(oldest.isPresent());
    assertEquals(c1, oldest.get(), "Oldest common parent should be C1 (the chronologically first common commit)");
  }

  @Test
  public void testFindLatestCommonParent() throws Exception {
    // Both timelines share C1, C3, C5 as common commits
    String c1 = "20230101000001";
    String c3 = "20230101000003";
    String c5 = "20230101000005";
    // Local also has C7 (not on remote)
    String c7 = "20230101000007";
    // Remote also has C0 (not on local, and older than C1)
    String c0 = "20230101000000";

    for (String ts : new String[]{c1, c3, c5}) {
      createCompletedInstant(localMetaClient, ts, COMMIT_ACTION);
      createCompletedInstant(remoteMetaClient, ts, COMMIT_ACTION);
    }
    createCompletedInstant(localMetaClient, c7, COMMIT_ACTION);
    createCompletedInstant(remoteMetaClient, c0, COMMIT_ACTION);

    HoodieReplicationTimelineComparator comparator =
            new HoodieReplicationTimelineComparator(localMetaClient, remoteMetaClient);

    HoodieTimeline localTimeline = localMetaClient.getActiveTimeline().getWriteTimeline();
    HoodieTimeline remoteTimeline = remoteMetaClient.getActiveTimeline().getWriteTimeline();
    Option<String> latest = comparator.findLatestCommonParent(localTimeline, remoteTimeline);

    assertTrue(latest.isPresent());
    assertEquals(c5, latest.get(), "Latest common parent should be C5 (the chronologically latest common commit)");
  }
}
