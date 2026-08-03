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

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.replication.commitmetadata.DeletePartitionCommitMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.LocalRegistry;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.ArchivedTimelineV1;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.replication.util.ReplicationPropertiesManager;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.replication.config.HoodieReplicationConfig;
import org.apache.hudi.replication.HoodieReplicationContext;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.INIT_INSTANT_TS;
import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
import static org.apache.hudi.common.util.MarkerUtils.readMarkersFromFile;
import static org.apache.hudi.common.util.MarkerUtils.readMarkerType;

/**
 * Provides an API for reading commit metadata for replicating a given commit.
 */
public class HoodieReplicationMetadataClient extends HoodieSnapshotMetadataClient {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationMetadataClient.class);
  private static final String CROSS_REGION_REPLICATION_REGISTRY = "CrossRegionReplication";
  @Deprecated
  private static final Integer MIN_ARCHIVED_INSTANTS_TO_LOAD = 32;
  private static final Boolean REPLICATE_HOODIE_PROPERTIES = true;
  private static final Integer MAX_ARCHIVED_COMMITS_TO_REPLICATE_PER_RUN = 128;
  // Cap on archived instants loaded per archival replication pass (0.x fork value).
  private static final Integer MAX_ARCHIVED_INSTANTS_TO_LOAD = 1024;

  // Default number of archived instants to load for archival replication.
  private volatile int maxArchivedInstantsToLoad = 1024;

  @VisibleForTesting
  public static String createCompactionTimestamp(String commitTime) {
    return commitTime + "001";
  }

  @VisibleForTesting
  public static String createCleanTimestamp(String commitTime) {
    return commitTime + "002";
  }

  // M3 metrics reporting is now handled by HoodieReplicationMetrics via M3Reporter.

  public enum ReplicationAction {
    CREATE_FILES,
    REPLICATE_FILES,
    DELETE_FILES,
    DELETE_DIRS
  }

  public enum ReplicationStep {
    START_MARKERS,
    DATA_MARKERS,
    DATA_ADD_FILES,
    DATA_DEL_FILES,
    META_START_MARKERS,
    META_DATA_MARKERS,
    META_ADD_FILES,
    META_DEL_FILES,
    META_FINISH_MARKERS,
    META_CLEANUP_MARKERS,
    FINISH_MARKERS,
    CLEANUP_MARKERS,
    HAS_MORE_COMMITS,
    NEW_LAST_REPLICATION_TIMESTAMP
  }

  public enum ReplicationStatus {
    SUCCESS("success"),
    INVALID("invalid"),
    FAILED("failed");

    private final String label;
    ReplicationStatus(String label) {
      this.label = label;
    }
  }

  /**
   * Replication client API returns a map of ReplicationInfo structures for each step of the replication.
   * Each replicationInfo contains the action to be applied on a list of files present under relative partition path.
   */
  public class ReplicationInfo {
    public ReplicationAction action;
    public String relativePath;
    public List<String> files;

    public ReplicationInfo(ReplicationAction action, String relativePath, List<String> files) {
      this.action = action;
      this.relativePath = relativePath;
      this.files = files;
    }

    @Override
    public String toString() {
      return "ReplicationInfo{action=" + action + ", relativePath='" + relativePath + '\'' + ", files=" + files + '}';
    }
  }

  protected Option<HoodieReplicationMetrics> metrics;

  // Cache last replicated commit, last replicated clustering commit and last archived commit,
  // to avoid going to the disk.
  protected String lastReplicatedCommit;
  protected String lastArchivedCommit;
  protected String lastReplicatedClusteringCommit;
  protected Comparator<HoodieInstant> instantComparator;
  protected ReplicationDestination regionId;
  protected boolean hoodiePropertiesReplication = REPLICATE_HOODIE_PROPERTIES;
  protected HoodieTableMetaClient destinationMetaClient;
  protected boolean useArchivalReplicationV2 = false;
  protected boolean validateLogFilesForArchivalReplicationV2 = false;
  protected boolean replicateLogFilesForArchivalReplicationV2 = true;

  // BiFunction for computing START_MARKERS in V2 API
  private final BiFunction<String, String, Stream<StoragePath>> startMarkersV2 =
      (i1, i2) -> getStartReplicationMarkersV2(i1, i2);

  private HoodieReplicationConfig replicationConfig;
  private ReplicationCheckpointStore checkpointManager;
  // Memoized result of the on-storage MDT existence check; see isMetadataTableConfigured().
  private volatile Boolean metadataTablePresent;

  /**
   * Constructor with source/destination paths and config override.
   */
  public HoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath, String secondaryBasePath,
                                         ReplicationDestination destination, HoodieReplicationConfig replicationConfig) {
    this(conf, basePath, destination, replicationConfig);
    try {
      this.destinationMetaClient = HoodieTableMetaClient.builder().setConf(conf)
              .setBasePath(secondaryBasePath).setLoadActiveTimelineOnLoad(true).build();
    } catch (TableNotFoundException e) {
      this.destinationMetaClient = HoodieReplicationMetadataClient.bootstrapSecondaryTable(this, conf, secondaryBasePath).getMetaClient();
    }
    this.useArchivalReplicationV2 = true;
    this.validateLogFilesForArchivalReplicationV2 = true;
    this.replicateLogFilesForArchivalReplicationV2 = true;
  }

  /**
   * Constructor with source/destination paths (default config).
   */
  @Deprecated
  public HoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath, String secondaryBasePath,
                                         ReplicationDestination destination) {
    this(conf, basePath, secondaryBasePath, destination, null);
  }

  /**
   * @deprecated Use {@link #HoodieReplicationMetadataClient(StorageConfiguration, String, ReplicationDestination, HoodieReplicationConfig)} instead.
   */
  @Deprecated
  public HoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath) {
    this(conf, basePath, ReplicationDestination.SECONDARY_REGION, null);
  }

  /**
   * Primary constructor for creating a replication metadata client.
   *
   * @param conf storage configuration bag.
   * @param basePath Table location absolute path.
   * @param destination replication destination region.
   * @param replicationConfig optional config overrides (may be null).
   */
  public HoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath,
                                         ReplicationDestination destination, HoodieReplicationConfig replicationConfig) {
    super(buildMetaClient(conf, basePath));
    this.replicationConfig = replicationConfig;
    regionId = destination;
    initRegistry();

    instantComparator = new Comparator<HoodieInstant>() {
      public int compare(HoodieInstant h1, HoodieInstant h2) {
        String c1 = h1.getCompletionTime();
        String c2 = h2.getCompletionTime();
        if (c1 != null && c2 != null) {
          int completionMatch = c1.compareTo(c2);
          return completionMatch != 0 ? completionMatch : h1.requestedTime().compareTo(h2.requestedTime());
        }
        return h1.requestedTime().compareTo(h2.requestedTime());
      }
    };

    this.checkpointManager = new ReplicationCheckpointStore(getMetaClient());
  }

  /**
   * @deprecated Use {@link #HoodieReplicationMetadataClient(StorageConfiguration, String, ReplicationDestination, HoodieReplicationConfig)} instead.
   */
  @Deprecated
  public HoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath, ReplicationDestination destination) {
    this(conf, basePath, destination, null);
  }

  private static HoodieTableMetaClient buildMetaClient(StorageConfiguration<?> conf, String basePath) {
    return HoodieTableMetaClient.builder()
        .setConf(conf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
  }

  public ReplicationCheckpointStore getCheckpointManager() {
    return checkpointManager;
  }

  /**
   * Set maximum archived instants to load (> 0).
   */
  public void setMaxArchivedInstantsToLoad(int value) {
    if (value <= 0) {
      LOG.warn("maxArchivedInstantsToLoad must be > 0; retaining existing value {}", this.maxArchivedInstantsToLoad);
      return;
    }
    this.maxArchivedInstantsToLoad = value;
  }

  public int getMaxArchivedInstantsToLoad() {
    return this.maxArchivedInstantsToLoad;
  }


  /**
   * getDestinationMetaClient - returns the metadata client for the destination region.
   */
  public HoodieTableMetaClient getDestinationMetaClient() {
    return destinationMetaClient;
  }

  /**
   * isTertiaryClient - client for third region replication?
   *
   * @return returns boolean based on whether replication client is being used for third region replication.
   */
  @Deprecated
  public boolean isTertiaryClient() {
    return regionId == ReplicationDestination.TERTIARY_REGION;
  }

  public ReplicationDestination getReplicationDestination() {
    return regionId;
  }

  public boolean isReplicateHoodieProperties() {
    return hoodiePropertiesReplication;
  }

  /**
   * Fall back to archival replication V1, by overriding the archival replication V2 (default).
   * @return
   */
  public void setArchivalReplicationV1() {
    this.useArchivalReplicationV2 = false;
    this.validateLogFilesForArchivalReplicationV2 = false;
    this.replicateLogFilesForArchivalReplicationV2 = false;
  }

  public boolean getValidateLogFilesForArchivalReplicationV2() {
    return this.validateLogFilesForArchivalReplicationV2;
  }

  public boolean getReplicateLogFilesForArchivalReplicationV2() {
    return this.replicateLogFilesForArchivalReplicationV2;
  }

  public void setValidateLogFilesForArchivalReplicationV2(boolean value) {
    ValidationUtils.checkArgument(!value || useArchivalReplicationV2,
              "Cannot enable validateLogFilesForArchivalReplicationV2 when Archival Replication V2 is not enabled");
    this.validateLogFilesForArchivalReplicationV2 = value;
  }

  public void setUseArchivalReplicationV2() {
    ValidationUtils.checkArgument(getDestinationMetaClient() != null, "Destination meta client is not set");
    this.useArchivalReplicationV2 = true;
    this.validateLogFilesForArchivalReplicationV2 = true;
    this.replicateLogFilesForArchivalReplicationV2 = true;
  }

  public void setReplicateLogFilesForArchivalReplicationV2(boolean value) {
    ValidationUtils.checkArgument(!value || useArchivalReplicationV2,
            "Cannot enable replicateLogFilesForArchivalReplicationV2 when Archival Replication V2 is not enabled");
    this.replicateLogFilesForArchivalReplicationV2 = value;
  }

  /**
   * Bootstraps the Hudi table at the secondary region, using the Hudi properties configured in the primary region.
   * Called for fresh tables onboarded, without initial replication using One time replication service (OTRS).
   *
   * @param srcReplicationClient - Replication client for the primary region Hudi table. (source client)
   * @param dstStorageConf - Storage configuration for the secondary region (destination config)
   * @param dstBasePath - Secondary region Hudi table's base path (destination base path)
   *
   * @return HoodieReplicationMetadataClient for the secondary region Hudi table (destination table).
   */
  public static HoodieReplicationMetadataClient bootstrapSecondaryTable(
      HoodieReplicationMetadataClient srcReplicationClient, StorageConfiguration<?> dstStorageConf, String dstBasePath) {
    try {
      HoodieTableMetaClient srcMetaClient = srcReplicationClient.getMetaClient();
      // bootstrap the primary region dataset table, with properties from source region dataset.
      HoodieTableMetaClient dstMetaClient = initTableIdempotent(dstStorageConf, dstBasePath,
          srcMetaClient.getTableConfig().getProps());
      if (isMetadataTableConfigured(srcMetaClient)) {
        // bootstrap the secondary region metadata table, with properties from source region metadata table.
        HoodieReplicationMetadataClient internalReplicationClient = srcReplicationClient.getInternalClient();
        HoodieTableMetaClient srcMetadataMetaClient = internalReplicationClient.getMetaClient();
        initTableIdempotent(dstStorageConf,
            HoodieTableMetadata.getMetadataTableBasePath(dstBasePath),
            srcMetadataMetaClient.getTableConfig().getProps());
      }
      return new HoodieReplicationMetadataClient(dstMetaClient.getStorageConf(), dstMetaClient.getBasePath().toString(),
          srcReplicationClient.getReplicationDestination());
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Failed to Bootstrap secondary table %s", srcReplicationClient.getMetaClient().getBasePath()), e);
    }
  }

  /**
   * Creates the table layout on storage and writes hoodie.properties only if the file does not
   * already exist (idempotent bootstrap). Preserves the 0.14.x overwriteProperties=false semantics
   * and passes raw properties directly to avoid lossy round-tripping through TableBuilder.
   */
  private static HoodieTableMetaClient initTableIdempotent(
      StorageConfiguration<?> storageConf, String basePath, java.util.Properties props) throws IOException {
    StoragePath baseStoragePath = new StoragePath(basePath);
    StoragePath metaFolder = new StoragePath(baseStoragePath, HoodieTableMetaClient.METAFOLDER_NAME);
    HoodieStorage storage = HoodieStorageUtils.getStorage(baseStoragePath, storageConf);
    StoragePath propsPath = new StoragePath(metaFolder, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    boolean shouldCreateConfig = !storage.exists(propsPath);
    HoodieTableMetaClient.createTableLayoutOnStorage(storageConf, baseStoragePath, props, null, shouldCreateConfig);
    return HoodieTableMetaClient.builder()
        .setConf(storageConf)
        .setBasePath(baseStoragePath)
        .build();
  }

  /**
   * Returns a stream of commit timestamps from the timeline, for instants created/modified after the last modification
   * time of the instant associated with input timestamp.  Commit timestamps returned are sorted by corresponding
   * last modification time as well.
   * Example:
   *    |----C0---------|
   *      |-C1-|
   *         |----C2--------|
   *             |-C3-|
   *    getInstantsAfter(C0) -> [C2]
   *    getInstantsAfter(C1) -> [C0, C2, C3]
   *    getInstantsAfter(C2) -> []
   *    getInstantsAfter(C3) -> [C0, C2]
   *    Note: events completed after the end time of the input instant.
   *
   * @return stream of completed commit timestamps, represented as String.
   */
  public Stream<String> getInstantsAfter(String instantTime) {
    Option<HoodieInstant> matchingInstant = getMatchingInstant(instantTime);
    if (matchingInstant.isPresent()) {
      // Unlike start times, there is no guarantee that completion times won't overlap.
      String modTime = matchingInstant.get().getCompletionTime();
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.GET_ON_COMPLETION_TIME));
      List<HoodieInstant> instants = getMetaClient().getActiveTimeline().getAllCommitsTimeline()
          .filterCompletedInstants().getInstantsAsStream()
          .filter(i -> i.getCompletionTime() != null && i.getCompletionTime().compareTo(modTime) >= 0)
          .filter(i -> !i.requestedTime().equals(instantTime)).sorted(instantComparator).collect(Collectors.toList());
      LOG.info(String.format("InstantsAfter [%s,%s]: %s",
          matchingInstant.get().requestedTime(), matchingInstant.get().getCompletionTime(),
          instants.stream().map(instant -> String.format("[%s,%s]", instant.requestedTime(), instant.getCompletionTime())).collect(Collectors.toList())));
      return instants.stream().map(HoodieInstant::requestedTime);
    }
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.INSTANT_NOT_PRESENT));
    return getInstantsModifiedAfterTs(instantTime);
  }

  /**
   * Returns a stream of completed commit timestamps from the timeline, for instants created/modified after
   * the given timestamp. Commit timestamps returned are sorted by last modification time of the associated instants.
   * (Instant corresponding to the input timestamp need not exist on the timeline).
   * Example:
   *   |----C0---------|
   *     |-C1-|
   *        |----C2--------|
   *            |-C3-|
   *
   *   getInstantsModifiedAfterTs(C0) -> C1, C2, C3
   *   getInstantsModifiedAfterTs(C1) -> C0, C2, C3
   *   getInstantsModifiedAfterTs(C2) -> C0, C1, C3
   *   getInstantsModifiedAfterTs(C3) -> C0, C2
   *   Note: output list of commit timestamps, excludes the input timestamp.
   *
   * @return stream of completed commit timestamps, represented as String.
   */
  public Stream<String> getInstantsModifiedAfterTs(String timestamp) {
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.GET_ON_TIMESTAMP));
    List<HoodieInstant> instants = getMetaClient().getActiveTimeline().getAllCommitsTimeline()
        .filterCompletedInstants().findInstantsModifiedAfterByCompletionTime(timestamp)
        .getInstantsAsStream().sorted(instantComparator)
        .collect(Collectors.toList());
    LOG.info(String.format("InstantsModifiedAfterTs %s: %s", timestamp,
        instants.stream().map(instant -> String.format("[%s,%s]", instant.requestedTime(), instant.getCompletionTime())).collect(Collectors.toList())));
    return instants.stream().sorted(Comparator.comparing(o -> o.getCompletionTime()))
        .map(HoodieInstant::requestedTime);
  }

  /*
   * Called on the primary region to get the list of commits that have been archived on the primary region and
   * to be archived on the secondary region. Returns all commits archived since lastArchivedCommit.
   */
  public Stream<String> getCommitsForArchival(String lastReplicatedArchivedCommit) {
    Option<HoodieInstant> firstCommit = getFirstCommitOnTimeline();
    if (firstCommit.isPresent()) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      // Archive-position-based load (0.x fork semantics): returns the instants appended to the
      // archive after the checkpoint instant's record, so instants archived late with older
      // requested times (e.g. rollbacks) are included and the checkpoint itself is excluded.
      HoodieArchivedTimeline archivedTimeline = ArchivedTimelineV1.loadInstantsArchivedAfter(
          getMetaClient(), lastReplicatedArchivedCommit, MAX_ARCHIVED_INSTANTS_TO_LOAD);
      Stream<String> timeStamps = archivedTimeline.filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime);

      long archivalInstantLoadDuration = timer.endTimer();
      LOG.info(String.format("[%s] ArchivedInstants between (%s, %s). Duration: %s", getMetaClient().getTableConfig().getTableName(), lastReplicatedArchivedCommit, firstCommit.get().requestedTime(),
              archivalInstantLoadDuration / 1000));
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.GET_ARCHIVED_COMMITS, archivalInstantLoadDuration));
      return timeStamps;
    }
    return Stream.empty();
  }

  public Stream<String> getCommitsForArchival() {
    return getCommitsForArchival(getLastArchivedCommit());
  }

  public Option<HoodieInstant> getMatchingInstant(String instantTime) {
    List<HoodieInstant> instants = getMetaClient().getActiveTimeline().getAllCommitsTimeline()
        .filterCompletedInstants().getInstantsAsStream().filter(i -> i.requestedTime().equals(instantTime))
        .sorted(instantComparator).collect(Collectors.toList());
    return instants.isEmpty() ? Option.empty() : Option.of(instants.get(instants.size() - 1));
  }

  /**
   * returns the timestamps of pending commits on dataset/metadata timelines.
   * @return timestamp of pending commits
   */
  public Stream<String> getPendingInstants() throws IOException {
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.GET_PENDING_TIMESTAMP));

    // pickup timestamps for any  pending commits on the dataset timeline.
    List<HoodieInstant> instants = getInflightAndRequestedInstants();

    // pickup timestamps for any pending commits on the metadata timeline.
    if (isMetadataTableConfigured()) {
      instants.addAll(getInternalClient().getInflightAndRequestedInstants());
    }

    LOG.info(String.format("PendingInstants %s", instants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList())));
    return instants.stream().map(HoodieInstant::requestedTime);
  }

  /**
   * returns an ordered list of ReplicationSteps for Replication.
   * @return List<ReplicationStep>
   */
  public List<ReplicationStep> getReplicationOrdering() {
    return new ArrayList<>(Arrays.asList(
        ReplicationStep.START_MARKERS,
        ReplicationStep.DATA_MARKERS,
        ReplicationStep.DATA_ADD_FILES,
        ReplicationStep.DATA_DEL_FILES,
        ReplicationStep.META_START_MARKERS,
        ReplicationStep.META_DATA_MARKERS,
        ReplicationStep.META_ADD_FILES,
        ReplicationStep.META_DEL_FILES,
        ReplicationStep.META_FINISH_MARKERS,
        ReplicationStep.META_CLEANUP_MARKERS,
        ReplicationStep.FINISH_MARKERS,
        ReplicationStep.CLEANUP_MARKERS));
  }

  /**
   * returns an ordered list of ReplicationSteps for Rollback.
   * @return List<ReplicationStep>
   */
  public List<ReplicationStep> getRollbackOrdering() {
    return new ArrayList<>(Arrays.asList(
        ReplicationStep.META_DATA_MARKERS,
        ReplicationStep.META_FINISH_MARKERS,
        ReplicationStep.META_DEL_FILES,    // no op
        ReplicationStep.META_ADD_FILES,
        ReplicationStep.META_START_MARKERS,
        ReplicationStep.META_CLEANUP_MARKERS,
        ReplicationStep.DATA_MARKERS,
        ReplicationStep.FINISH_MARKERS,
        ReplicationStep.DATA_DEL_FILES,
        ReplicationStep.DATA_ADD_FILES,   // no op
        ReplicationStep.START_MARKERS,
        ReplicationStep.CLEANUP_MARKERS));
  }

  /**
   * Returns a map of ReplicationStep to list of ReplicationInfo.  ReplicationSteps are to be executed in the
   * Replication order provided by the getReplicationOrdering().
   * @param instantTime- hudi commit timestamp
   * @return Map of ReplicationStep -> associated ReplicationInfo
   */
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForReplication(String instantTime) {
    // V1 API: startTime is null (not used by V1's start markers logic)
    return getOrderedFilesForReplication(null, instantTime,
        (start, end) -> getStartReplicationMarkers(end));
  }

  /**
   * Internal method that accepts a custom function for computing START_MARKERS.
   * This allows V2/V3 APIs to provide different start marker logic while reusing the rest of the replication flow.
   *
   * @param startTime - start time for the window (null for V1)
   * @param endTime - hudi commit timestamp (the commit being replicated)
   * @param startMarkers - BiFunction(startTime, endTime) to compute start marker files
   * @return Map of ReplicationStep -> associated ReplicationInfo
   */
  private Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForReplication(
      String startTime, String endTime, BiFunction<String, String, Stream<StoragePath>> startMarkers) {
    Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList = new HashMap<>();
    if (getMatchingInstant(endTime).isPresent()) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      orderedReplicationList.put(ReplicationStep.START_MARKERS,
          makeReplicationInfo(ReplicationAction.REPLICATE_FILES, startMarkers.apply(startTime, endTime)));
      orderedReplicationList.put(ReplicationStep.DATA_MARKERS,
          makeReplicationInfo(ReplicationAction.CREATE_FILES, getBaseFileMarkers(endTime)));
      orderedReplicationList.put(ReplicationStep.DATA_ADD_FILES,
          makeReplicationInfo(ReplicationAction.REPLICATE_FILES, getBaseFilesAdded(endTime, true)));
      // Files deleted as part of clean/rollback operations, if any.
      List<ReplicationInfo> deletedFilesDirs = makeReplicationInfo(ReplicationAction.DELETE_FILES,
          getBaseFilesDeleted(endTime));
      // add directories removed by delete/stash partition operation, if any.
      deletedFilesDirs.addAll(makeReplicationInfo(ReplicationAction.DELETE_DIRS, getDeletedDirectories(endTime)));
      orderedReplicationList.put(ReplicationStep.DATA_DEL_FILES, deletedFilesDirs);

      orderedReplicationList.put(ReplicationStep.FINISH_MARKERS, makeReplicationInfo(ReplicationAction.REPLICATE_FILES,
          getFinishReplicationMarkers(endTime, isReplicateHoodieProperties())));
      orderedReplicationList.put(ReplicationStep.CLEANUP_MARKERS, makeReplicationInfo(ReplicationAction.DELETE_DIRS,
          Stream.of(new StoragePath(getMetaClient().getMarkerFolderPath(endTime)))));
      // add metadata related files
      addMetadataInstantsForReplication(endTime, orderedReplicationList);
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLICATED, timer.endTimer()));
    }
    metrics.ifPresent(m -> m.publishMetrics(getInstantsAfter(getLastReplicatedCommit())
        .collect(Collectors.toList()).size()));
    return  orderedReplicationList;
  }

  /**
   * Returns the replication payload for a window between startTime and endTime.
   *
   * This method is designed to be used with getInstantsAfter() and processes one completed commit window:
   * 1. startTime is the previous LRT
   * 2. endTime is the next immediate completed commit from getInstantsAfter()
   * 3. All pending instants (.requested, .inflight) between startTime and endTime are included in START_MARKERS
   * 4. The completed commit at endTime contributes the full replication payload
   *
   * Pending instants are ordered by modification time to preserve the timeline sequence.
   *
   * @param startTime - last replicated completed commit timestamp
   * @param endTime - next immediate completed commit timestamp
   * @return Map of ReplicationStep -> associated ReplicationInfo for the window
   */
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForReplicationV2(String startTime, String endTime) {
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.GET_ON_TIMESTAMP));
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<ReplicationStep, List<ReplicationInfo>> result = new HashMap<>();

    try {
      // Input validation
      if (StringUtils.isNullOrEmpty(startTime) || StringUtils.isNullOrEmpty(endTime)) {
        LOG.warn("getOrderedFilesForReplicationV2: Invalid input - startTime={}, endTime={}", startTime, endTime);
        return result;
      }

      if (!getMatchingInstant(endTime).isPresent()) {
        LOG.warn("getOrderedFilesForReplicationV2: endTime {} not found on timeline, returning empty payload", endTime);
        return result;
      }

      // Validate that at most one completed instant exists in the window (using modification time)
      String startModTime = getModificationTimeOrFallback(startTime);
      String endModTime = getMatchingInstant(endTime).get().getCompletionTime();
      long completedCount = getMetaClient().getActiveTimeline()
          .getAllCommitsTimeline()
          .filterCompletedInstants()
          .getInstantsAsStream()
          .filter(instant -> {
            String modTime = instant.getCompletionTime();
            return modTime.compareTo(startModTime) > 0 && modTime.compareTo(endModTime) <= 0;
          })
          .count();
      ValidationUtils.checkArgument(completedCount <= 1,
          "Found more than 1 commit in the window (" + startModTime + ", " + endModTime + "]. "
              + "completedCount=" + completedCount);

      LOG.info("getOrderedFilesForReplicationV2: startTime={}, endTime={}", startTime, endTime);

      // Use pending markers from the window (already includes endTime's .requested/.inflight if they exist)
      return getOrderedFilesForReplication(startTime, endTime, startMarkersV2);
    } finally {
      long duration = timer.endTimer();
      LOG.info("getOrderedFilesForReplicationV2: completed, duration={}ms", duration);
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLICATED, duration));
      metrics.ifPresent(m -> m.publishMetrics(getInstantsAfter(getLastReplicatedCommit())
          .collect(Collectors.toList()).size()));
    }
  }

  /**
   * Computes the pending instant marker files (requested/inflight) between two instant timestamps.
   * Converts timestamps to modification times internally for proper ordering.
   * Used by V2 API to include pending instants in START_MARKERS.
   *
   * Optimization: First uses active timeline to find timestamps modified after startModTime (typically 2-3),
   * then uses raw timeline filtered by those timestamps to get all states. This reduces list status calls
   * compared to iterating over all instants on the raw timeline.
   *
   * @param startTime - start instant timestamp (exclusive)
   * @param endTime - end instant timestamp (inclusive)
   * @return Stream of pending instant marker file paths
   */
  private Stream<StoragePath> getStartReplicationMarkersV2(String startTime, String endTime) {
    // Convert timestamps to modification times
    String startModTime = getModificationTimeOrFallback(startTime);
    HoodieInstant endInstantObj = getMatchingInstant(endTime).get();
    String endCompletion = endInstantObj.getCompletionTime();
    String endModTime = StringUtils.isNullOrEmpty(endCompletion)
        ? getModificationTimeOrFallback(endTime)
        : endCompletion;

    // Step 1: Use active timeline to find timestamps modified after startModTime
    // This gives us a small list of ~2-3 timestamps due to incremental runs
    Set<String> candidateTimestamps = getMetaClient().getActiveTimeline()
        .getInstantsAsStream()
        .filter(instant -> {
          String modTime = instant.getCompletionTime();
          if (StringUtils.isNullOrEmpty(modTime)) {
            return true;
          }
          return modTime.compareTo(startModTime) > 0;
        })
        .map(HoodieInstant::requestedTime)
        .collect(Collectors.toSet());

    LOG.info("getStartReplicationMarkersV2: found {} candidate timestamps after startModTime={}",
        candidateTimestamps.size(), startModTime);

    // Step 2: Use raw timeline filtered by candidate timestamps to get all pending states
    // (.requested, .inflight) - typically 2-4 files per instant
    StoragePath metaPath = getMetaClient().getMetaPath();
    return getMetaClient().getRawActiveTimeline()
        .getInstantsAsStream()
        .filter(instant -> candidateTimestamps.contains(instant.requestedTime()))
        .filter(instant -> !instant.isCompleted())
        .filter(instant -> {
          // Verify modification time is in window
          String modTime = instant.getCompletionTime();
          // Requested/inflight instants may not have completion timestamps populated; keep them
          // when they are already in the candidate set (avoids NPE on MOR delta timelines).
          if (StringUtils.isNullOrEmpty(modTime)) {
            return true;
          }
          return modTime.compareTo(startModTime) > 0 && modTime.compareTo(endModTime) <= 0;
        })
        .sorted(instantComparator)
        .map(instant -> new StoragePath(metaPath, getInstantFileName(instant)));
  }

  /**
   * Gets the modification time for a given instant timestamp.
   * If the instant is found on the timeline, returns its modification time.
   * Otherwise, falls back to using the timestamp itself as the modification time.
   * Used by V2 and V3 APIs to convert instant timestamps to modification times.
   *
   * @param instantTimestamp - the instant timestamp to look up
   * @return the modification time or the timestamp as fallback
   */
  private String getModificationTimeOrFallback(String instantTimestamp) {
    Option<HoodieInstant> instant = getMatchingInstant(instantTimestamp);
    if (instant.isPresent()) {
      String modTime = instant.get().getCompletionTime();
      LOG.info("getModificationTimeOrFallback: timestamp={}, modTime={}", instantTimestamp, modTime);
      return modTime;
    } else {
      LOG.warn("getModificationTimeOrFallback: timestamp {} not found, using as fallback", instantTimestamp);
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.INSTANT_NOT_PRESENT));
      return instantTimestamp;
    }
  }

  /**
   * Returns the replication payload for all the instants until the next immediate completed commit after the Last Replicated Time (LRT).
   *
   * This method processes only ONE completed commit at a time. It:
   * 1. Finds the next immediate completed commit after LRT
   * 2. Delegates to V2 API for the core replication logic (START_MARKERS, DATA_*, FINISH_MARKERS, etc.)
   * 3. Adds HAS_MORE_COMMITS flag to indicate if there are more completed commits
   * 4. Adds NEW_LAST_REPLICATION_TIMESTAMP with the processed commit timestamp
   *
   * @param lastReplicatedTimestamp - the instant time (timestamp) of the last replicated completed instant (LRT)
   * @return Map of ReplicationStep to ReplicationInfo, including HAS_MORE_COMMITS and NEW_LAST_REPLICATION_TIMESTAMP
   */
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForReplicationV3(String lastReplicatedTimestamp) {
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.GET_ON_TIMESTAMP));
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<ReplicationStep, List<ReplicationInfo>> result = new HashMap<>();
    String nextLastReplicatedCommit = null;
    boolean hasMoreCommits = false;

    try {
      // Input validation
      if (StringUtils.isNullOrEmpty(lastReplicatedTimestamp)) {
        LOG.warn("getOrderedFilesForReplicationV3: Invalid input - lastReplicatedTimestamp is null or empty");
        addHasMoreCommitsFlag(result, false);
        return result;
      }

      // Find the modification time for the given LRT timestamp
      String lrtModTime = getModificationTimeOrFallback(lastReplicatedTimestamp);

      // Find next completed commit after LRT using active timeline
      Option<HoodieInstant> nextCompletedOpt = findNextCompletedCommitAfterLrt(lrtModTime);

      if (!nextCompletedOpt.isPresent()) {
        // No completed commit after LRT - return empty payload
        // Note: This case should rarely happen in production since HiveSync triggers on commit events
        LOG.info("getOrderedFilesForReplicationV3: No completed commit after LRT={}, returning empty payload",
            lastReplicatedTimestamp);
        addHasMoreCommitsFlag(result, false);
        return result;
      }

      HoodieInstant nextCompleted = nextCompletedOpt.get();
      nextLastReplicatedCommit = nextCompleted.requestedTime();

      // Check if there are more completed commits after this one
      String nextCompletedModTime = nextCompleted.getCompletionTime();
      hasMoreCommits = hasMoreCompletedCommitsAfter(nextCompletedModTime);

      LOG.info("getOrderedFilesForReplicationV3: LRT={}, nextLastReplicatedCommit={}, hasMoreCommits={}",
          lastReplicatedTimestamp, nextLastReplicatedCommit, hasMoreCommits);

      // Delegate to core replication logic
      result = getOrderedFilesForReplication(lastReplicatedTimestamp, nextLastReplicatedCommit, startMarkersV2);


      // Add V3-specific fields
      addHasMoreCommitsFlag(result, hasMoreCommits);
      addNewLastReplicationTimestamp(result, nextLastReplicatedCommit);

      return result;
    } finally {
      long duration = timer.endTimer();
      LOG.info("getOrderedFilesForReplicationV3: completed, nextLastReplicatedCommit={}, hasMoreCommits={}, duration={}ms",
          nextLastReplicatedCommit, hasMoreCommits, duration);
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLICATED, duration));
      metrics.ifPresent(m -> m.publishMetrics(getInstantsAfter(getLastReplicatedCommit())
          .collect(Collectors.toList()).size()));
    }
  }

  /**
   * Finds the next completed commit after the given modification time.
   */
  private Option<HoodieInstant> findNextCompletedCommitAfterLrt(String lrtModTime) {
    return Option.fromJavaOptional(
        getMetaClient().getActiveTimeline()
            .getAllCommitsTimeline()
            .filterCompletedInstants()
            .getInstantsAsStream()
            .filter(instant -> instant.getCompletionTime().compareTo(lrtModTime) > 0)
            .sorted(instantComparator)
            .findFirst()
    );
  }

  /**
   * Checks if there are more completed commits after the given modification time.
   */
  private boolean hasMoreCompletedCommitsAfter(String modTime) {
    return getMetaClient().getActiveTimeline()
        .getAllCommitsTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .anyMatch(instant -> instant.getCompletionTime().compareTo(modTime) > 0);
  }

  /**
   * Helper method to add the HAS_MORE_COMMITS flag to the replication payload.
   */
  private void addHasMoreCommitsFlag(Map<ReplicationStep, List<ReplicationInfo>> payload, boolean hasMoreCommits) {
    List<String> flagValue = Collections.singletonList(String.valueOf(hasMoreCommits));
    ReplicationInfo flagInfo = new ReplicationInfo(ReplicationAction.REPLICATE_FILES, "", flagValue);
    payload.put(ReplicationStep.HAS_MORE_COMMITS, Collections.singletonList(flagInfo));
  }

  /**
   * Helper method to add the NEW_LAST_REPLICATION_TIMESTAMP to the replication payload.
   * This tells the caller which commit timestamp was replicated, so they can set it as their new LRT.
   */
  private void addNewLastReplicationTimestamp(Map<ReplicationStep, List<ReplicationInfo>> payload, String commitTimestamp) {
    List<String> timestampValue = Collections.singletonList(commitTimestamp);
    ReplicationInfo timestampInfo = new ReplicationInfo(ReplicationAction.REPLICATE_FILES, "", timestampValue);
    payload.put(ReplicationStep.NEW_LAST_REPLICATION_TIMESTAMP, Collections.singletonList(timestampInfo));
  }

  /**
   * For replicating commits that have been archived on primary region, provides a list of steps
   * to be performed on the secondary region.
   *
   * Returns a map of ReplicationStep to list of ReplicationInfo.  ReplicationSteps are to be executed in the
   * Replication order provided by the getReplicationOrdering().
   * @param instantTime- hudi archived commit timestamp - must be less than the first commit on timeline
   * @return Map of ReplicationStep -> associated ReplicationInfo
   */
  @Deprecated
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForArchival(String instantTime) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList = new HashMap<>();
    if (useArchivalReplicationV2) {
      // return empty list, if archival v2 is enabled.
      LOG.warn("Archival replication V2 is enabled. Skipping archival replication V1.");
      return orderedReplicationList;
    }
    orderedReplicationList.put(ReplicationStep.DATA_DEL_FILES,
        makeReplicationInfo(ReplicationAction.DELETE_FILES, getArchivedCommitFiles(getLastArchivedCommit())));
    orderedReplicationList.put(ReplicationStep.DATA_ADD_FILES,
        makeReplicationInfo(ReplicationAction.REPLICATE_FILES, getArchivalFiles()));
    // add metadata related archived commit files
    addMetadataFilesForArchival(instantTime, orderedReplicationList);
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.ARCHIVED_REPLICATED, timer.endTimer()));
    metrics.ifPresent(m -> m.publishMetrics((int) getInstantsAfter(getLastReplicatedCommit()).count()));
    return  orderedReplicationList;
  }

  /**
   * Provides the list of instants that needs to be archived in secondary by comparing the active timelines
   * of both local and remote client.
   * 
   * Returns a map of ReplicationStep to list of ReplicationInfo.  ReplicationSteps are to be executed in the
   * Replication order provided by the getReplicationOrdering().
   */
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForArchivalV2() {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList = new HashMap<>();
    if (!useArchivalReplicationV2) {
      // return empty list, if archival v2 is not enabled.
      LOG.warn("Archival replication V2 is not enabled. Skipping archival replication V2.");
      return orderedReplicationList;
    }
    replicateArchivedCommitsIfRequired(orderedReplicationList);
    metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.ARCHIVED_V2_REPLICATED, timer.endTimer()));
    return orderedReplicationList;
  }

  /**
   * Returns a map of ReplicationStep to list of ReplicationInfo.  ReplicationSteps are to be executed in the
   * Rollback order provided by the getRollbackOrdering().
   * @param instantTime - hudi commit timestamp to be rolled back.
   * @return
   */
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForRollback(String instantTime) {
    Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList = new HashMap<>();
    if (getMatchingInstant(instantTime).isPresent()) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      addMetadataInstantsForRollback(instantTime, orderedReplicationList);
      orderedReplicationList.put(ReplicationStep.DATA_MARKERS,
          makeReplicationInfo(ReplicationAction.CREATE_FILES, getBaseFileMarkers(instantTime)));
      orderedReplicationList.put(ReplicationStep.FINISH_MARKERS,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, getFinishReplicationMarkers(instantTime, !REPLICATE_HOODIE_PROPERTIES)));
      orderedReplicationList.put(ReplicationStep.DATA_DEL_FILES,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, getBaseFilesDeleted(instantTime)));
      orderedReplicationList.put(ReplicationStep.DATA_ADD_FILES,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, getBaseFilesAdded(instantTime, false)));
      orderedReplicationList.put(ReplicationStep.START_MARKERS,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, getStartReplicationMarkers(instantTime)));
      orderedReplicationList.put(ReplicationStep.CLEANUP_MARKERS, makeReplicationInfo(ReplicationAction.DELETE_DIRS,
          Stream.of(new StoragePath(getMetaClient().getMarkerFolderPath(instantTime)))));
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REVERT, timer.endTimer()));
    } else {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REVERT_INSTANT_NOT_FOUND));
      LOG.warn(String.format("getOrderedFilesForRollback failed to find the instant %s on Hudi timeline",
          instantTime));
    }
    metrics.ifPresent(m -> m.publishMetrics(getInstantsAfter(getLastReplicatedCommit())
        .collect(Collectors.toList()).size()));
    return  orderedReplicationList;
  }

  /**
   * Returns a map of ReplicationStep to list of ReplicationInfo.  ReplicationSteps are to be executed in the
   * Rollback order provided by the getRollbackOrdering().
   * @param instantTime - hudi pending commit timestamp to be rolled back.
   * @return
   */
  public Map<ReplicationStep, List<ReplicationInfo>> getOrderedFilesForRollbackOfPendingCommit(String instantTime) throws IOException {
    Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList = new HashMap<>();
    List<String> inflightDatasetTs = getInflightAndRequestedInstants().stream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());
    List<String> inflightMetadataTs = new ArrayList<>();
    if (isMetadataTableConfigured()) {
      inflightMetadataTs = getInternalClient().getInflightAndRequestedInstants().stream()
          .map(HoodieInstant::requestedTime).collect(Collectors.toList());
    }
    if (inflightDatasetTs.contains(instantTime)) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      addMetadataInstantsForRollback(instantTime, orderedReplicationList);
      // No delete action for DATA_MARKERS as these are used for identifying data files. In case of partial failures,
      // where only data markers are deleted, the data files will be unreferenced and not picked in next attempt.
      // The cleanup of data markers is handled in CLEANUP_MARKERS step at the end.
      orderedReplicationList.put(ReplicationStep.DATA_MARKERS,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, Stream.empty()));
      orderedReplicationList.put(ReplicationStep.FINISH_MARKERS,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, Stream.empty()));
      orderedReplicationList.put(ReplicationStep.DATA_DEL_FILES,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, getPendingDataFiles(instantTime)));
      orderedReplicationList.put(ReplicationStep.DATA_ADD_FILES,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, Stream.empty()));
      orderedReplicationList.put(ReplicationStep.START_MARKERS,
          makeReplicationInfo(ReplicationAction.DELETE_FILES, getStartReplicationMarkersForPendingCommit(instantTime)));
      orderedReplicationList.put(ReplicationStep.CLEANUP_MARKERS, makeReplicationInfo(ReplicationAction.DELETE_DIRS,
          Stream.of(new StoragePath(getMetaClient().getMarkerFolderPath(instantTime)))));
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REVERT_PENDING, timer.endTimer()));
    } else if (inflightMetadataTs.contains(instantTime)) {
      // pending commit on metadata timeline, but not on dataset timeline
      addMetadataInstantsForRollback(instantTime, orderedReplicationList);
    } else {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REVERT_PENDING_NOT_FOUND));
      LOG.warn(String.format("getOrderedFilesForRollbackOfPendingCommit failed to find the instant %s on Hudi timeline",
          instantTime));
    }
    metrics.ifPresent(m -> m.publishMetrics(getInstantsAfter(getLastReplicatedCommit())
        .collect(Collectors.toList()).size()));
    return  orderedReplicationList;
  }

  public String getCheckPointKeyForClustering(ReplicationDestination regionId) {
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedClusteringTimestamp(regionId);
  }

  public String getMetricKeyForClustering(ReplicationDestination regionId) {
    return String.format("setLastReplicatedClustering.%s.commit", regionId.label.toLowerCase());
  }

  public String getCheckPointKeyForCommit(ReplicationDestination regionId) {
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedTimestamp(regionId);
  }

  public String getMetricKeyForCommit(ReplicationDestination regionId, ReplicationStatus status) {
    return String.format("setLastReplicated.%s.commit.%s", regionId.label.toLowerCase(), status.name().toLowerCase());
  }

  public boolean setLastReplicatedCommit(String instantTime) {
    HoodieTableMetaClient metaClient = getMetaClient();
    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    HoodieTimeline replaceTimeline = timeline.getCompletedReplaceTimeline();
    HoodieTimeline allCommitsTimeline = timeline.getAllCommitsTimeline();
    boolean success = true;
    try {
      if (replaceTimeline.containsInstant(instantTime)) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        checkpointManager.updateClusteringCheckpoint(instantTime, regionId);
        metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForClustering(regionId), timer.endTimer()));
        lastReplicatedClusteringCommit = instantTime;
      }
      if (allCommitsTimeline.containsInstant(instantTime)) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        checkpointManager.updateReplicatedCommitCheckpoint(instantTime, regionId);
        lastReplicatedCommit = instantTime;
        metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForCommit(regionId, ReplicationStatus.SUCCESS), timer.endTimer()));
      } else {
        metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForCommit(regionId, ReplicationStatus.INVALID)));
        LOG.warn(String.format("setLastReplicatedCommit - requested instant %s not found on Hudi timeline", instantTime));
      }

      // Update the MDT checkpoints
      if (isMetadataTableConfigured()) {
        HoodieReplicationMetadataClient internalClient = getInternalClient();
        internalClient.setLastReplicatedCommit(instantTime);
      }
    } catch (Exception e) {
      LOG.error("Failed to set last replicated commit for table: {}, instant: {}",
          getMetaClient().getFullTableName(), instantTime, e);
      metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForCommit(regionId, ReplicationStatus.FAILED)));
      success = false;
    }
    metrics.ifPresent(m -> m.publishMetrics((int) getInstantsAfter(getLastReplicatedCommit()).count()));
    return success;
  }

  public String getCheckPointKeyForArchival(ReplicationDestination regionId) {
    return ReplicationCheckpointStore.getCheckpointKeyForLastReplicatedArchivedTimestamp(regionId);
  }

  public String getMetricKeyForArchival(ReplicationDestination regionId, ReplicationStatus status) {
    return String.format("setLastReplicatedArchived.%s.commit.%s", regionId.label.toLowerCase(), status.label.toLowerCase());
  }

  /*
   * Expected to be called after archival of commits on the secondary region.
   * Since replication of archived commits could fail during hivessync, keeping
   * this as a separate API (to avoid Hudi incorrectly marking as archived commit).
   */
  public boolean setLastArchivedCommit(String instantTime) {
    HoodieTimeline allCommitsTimeline = getMetaClient().reloadActiveTimeline().getAllCommitsTimeline();
    boolean success = true;
    try {
      if (!allCommitsTimeline.containsInstant(instantTime)) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        checkpointManager.updateArchivedCheckpoint(instantTime, regionId);
        // update the cache for last archived commit
        lastArchivedCommit = instantTime;
        LOG.info(String.format("Successfully set last archived commit for %s region to %s", regionId.label, instantTime));
        metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForArchival(regionId, ReplicationStatus.SUCCESS), timer.endTimer()));
      } else {
        metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForArchival(regionId, ReplicationStatus.INVALID)));
        LOG.warn(String.format("Requested instant for archival to %s region %s is found on Hudi timeline", regionId.label, instantTime));
      }

      // Update the MDT checkpoints
      if (isMetadataTableConfigured()) {
        HoodieReplicationMetadataClient internalClient = getInternalClient();
        internalClient.setLastArchivedCommit(instantTime);
      }
    } catch (Exception e) {
      LOG.error("Failed to set last archived commit for table: {}, instant: {}",
          getMetaClient().getFullTableName(), instantTime, e);
      metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForArchival(regionId, ReplicationStatus.FAILED)));
      success = false;
    }
    return success;
  }

  public String getMetricKeyForReplicationEnable(ReplicationDestination regionId, ReplicationStatus status) {
    return String.format("setCrossRegionReplicationEnabled.%s.commit.%s", regionId.name().toLowerCase(), status.name().toLowerCase());
  }

  /**
   * set cross region replication enabled flag
   * @param destinationRegion replication destination
   * @param enabled flag to set
   * @return true if set is successful, false otherwise
   */
  public boolean setCrossRegionReplicationEnabled(ReplicationDestination destinationRegion, boolean enabled) {
    try {
      final String key = HoodieReplicationContext.getCrossRegionReplicationEnabledConfigKey(destinationRegion);
      final String operStatusKey = HoodieReplicationContext.getReplicationOperationalStatusConfigKey(destinationRegion);
      new ReplicationPropertiesManager(getMetaClient()).setProperty(key, String.valueOf(enabled));
      // propertiesManager.removeProperty(operStatusKey);
      metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForReplicationEnable(destinationRegion, ReplicationStatus.SUCCESS)));
      return true;
    } catch (Exception e) {
      LOG.error(String.format("Setting cross region replication enabled failed. region: %s, enabled: %s", destinationRegion.label, enabled));
      metrics.ifPresent(m -> m.updateMetrics(getMetricKeyForReplicationEnable(destinationRegion, ReplicationStatus.FAILED)));
      return false;
    }
  }

  public String getLastReplicatedCommit() {
    if (lastReplicatedCommit != null) {
      return lastReplicatedCommit;
    }
    lastReplicatedCommit = HoodieReplicationContext
        .getDatasetLastReplicatedTimestamp(getMetaClient(), regionId).orElse(INIT_INSTANT_TS);
    if (INIT_INSTANT_TS.equals(lastReplicatedCommit) && destinationMetaClient != null) {
      lastReplicatedCommit = computeLastReplicatedCommitFromTimelines(false);
    }
    return lastReplicatedCommit;
  }

  public String getLastReplicatedClusteringCommit() {
    if (lastReplicatedClusteringCommit != null) {
      return lastReplicatedClusteringCommit;
    }
    lastReplicatedClusteringCommit = HoodieReplicationContext
        .getDatasetLastReplicatedClusteringTimestamp(getMetaClient(), regionId).orElse(INIT_INSTANT_TS);
    if (INIT_INSTANT_TS.equals(lastReplicatedClusteringCommit) && destinationMetaClient != null) {
      lastReplicatedClusteringCommit = computeLastReplicatedCommitFromTimelines(true);
    }
    return lastReplicatedClusteringCommit;
  }

  public String getLastArchivedCommit() {
    if (lastArchivedCommit != null) {
      return lastArchivedCommit;
    }
    lastArchivedCommit = HoodieReplicationContext
        .getDatasetLastReplicatedArchivedTimestamp(getMetaClient(), regionId).orElse(INIT_INSTANT_TS);
    return lastArchivedCommit;
  }

  /**
   * Computes the last replicated commit (LRT) by comparing source and target write timelines.
   * Walks through the target timeline in reverse chronological order and finds the first
   * commit that exists on the source timeline.
   *
   * @return the timestamp of the latest common parent in write timeline, or INIT_INSTANT_TS if
   *         no common parent is found or destinationMetaClient is not available
   */
  public String computeLastReplicatedCommitFromWriteTimeline() {
    if (destinationMetaClient == null) {
      LOG.warn("Cannot compute LRT from write timeline: destinationMetaClient is not available");
      return INIT_INSTANT_TS;
    }
    return computeLastReplicatedCommitFromTimelines(false);
  }

  /**
   * Computes the last replicated clustering commit by comparing source and target replace timelines.
   * Walks through the target replace commit timeline in reverse chronological order and finds the first
   * commit that exists on the source timeline.
   *
   * @return the timestamp of the latest common parent in replace timeline, or INIT_INSTANT_TS if
   *         no common parent is found or destinationMetaClient is not available
   */
  public String computeLastReplicatedClusteringCommitFromReplaceTimeline() {
    if (destinationMetaClient == null) {
      LOG.warn("Cannot compute clustering LRT from replace timeline: destinationMetaClient is not available");
      return INIT_INSTANT_TS;
    }
    return computeLastReplicatedCommitFromTimelines(true);
  }

  /**
   * Internal method that computes the last replicated commit (LRT) by comparing source and target timelines.
   *
   * @param forClustering if true, compare replace commit timelines for clustering LRT;
   *                      if false, compare write timelines for regular LRT
   * @return the timestamp of the latest common parent, or INIT_INSTANT_TS if no common parent is found
   */
  private String computeLastReplicatedCommitFromTimelines(boolean forClustering) {
    try {
      HoodieReplicationTimelineComparator comparator =
          new HoodieReplicationTimelineComparator(getMetaClient(), destinationMetaClient);

      Option<String> latestCommonParent = forClustering
          ? comparator.findLatestCommonParentInActiveReplaceTimeline()
          : comparator.findLatestCommonParentInActiveWriteTimeline();

      if (latestCommonParent.isPresent()) {
        LOG.info("LRT computed from timeline comparison for {} region: {} (forClustering={})",
            regionId.label, latestCommonParent.get(), forClustering);
        return latestCommonParent.get();
      }
      LOG.warn("No common parent found between source and target timelines for {} region (forClustering={}). "
          + "Returning INIT_INSTANT_TS.", regionId.label, forClustering);
    } catch (Exception e) {
      LOG.error("Failed to compute LRT from timeline comparison for {} region (forClustering={}). "
          + "Returning INIT_INSTANT_TS.", regionId.label, forClustering, e);
    }
    return INIT_INSTANT_TS;
  }

  /**
   * Adds archived commit related files to the orderedReplicationList, if commits are archived on the primary timeline.
   */
  private void replicateArchivedCommitsIfRequired(Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList) {

    if (!useArchivalReplicationV2 || getDestinationMetaClient() == null) {
      LOG.warn("Archival replication V1 is enabled or secondary base path was not provided. Skipping archival replication V2.");
      return;
    }
    try {
      // replicate archived commits from main dataset timeline.
      replicateArchivedCommits(getMetaClient(), getDestinationMetaClient(), orderedReplicationList, false);
      if (isMetadataTableConfigured()) {
        HoodieReplicationMetadataClient internalClient = getInternalClient();
        ValidationUtils.checkArgument(internalClient != null, "Internal client is not initialized");
        // replicate archived commits from metadata timeline.
        replicateArchivedCommits(internalClient.getMetaClient(), internalClient.getDestinationMetaClient(),
                orderedReplicationList, true);
      }
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLICATE_ARCHIVED_FAILED));
      throw new HoodieException(
              String.format("Failed to replicate archived commit files to %s region with exception (%s)", regionId, e));
    }
  }

  /**
   * Given the HoodieTableMetaClient for the local and remote copies of the dataset, find the list of archived commits on primary
   * and updates the OrderedReplicationList.
   * @param metaClient - Local HoodieTableMetaClient
   * @param remoteMetaClient remote HoodieTableMetaClient
   * @param orderedReplicationList - OrderedReplicationList to be updated
   * @param isMDT
   */
  private void replicateArchivedCommits(HoodieTableMetaClient metaClient, HoodieTableMetaClient remoteMetaClient,
                                        Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList, boolean isMDT) {
    HoodieReplicationTimelineComparator comparator = new HoodieReplicationTimelineComparator(metaClient, remoteMetaClient);

    if (!validateLogFilesForArchivalReplicationV2) {
      // Add the archived instants from the primary region's write timeline.
      addArchivedCommitsWithoutLogFiles(metaClient, remoteMetaClient, comparator.getRemoteInstantsForArchivalInWriteTimeline(),
                          orderedReplicationList, isMDT);
      // Add the archived instants from the primary region's non-write timeline.
      addArchivedCommitsWithoutLogFiles(metaClient, remoteMetaClient, comparator.getRemoteInstantsForArchivalInNonWriteTimeline(),
                          orderedReplicationList, isMDT);
      return;
    }

    // Add the archived commits from the primary region's write timeline.
    addArchivedCommits(metaClient, comparator.getRemoteCommitsForArchivalInWriteTimeline(),
            comparator.findOldestCommonParentInActiveWriteTimeline(), orderedReplicationList, isMDT);

    // Add the archived commits from the primary region's non-write timeline.
    addArchivedCommits(metaClient, comparator.getRemoteCommitsForArchivalInNonWriteTimeline(),
            comparator.findOldestCommonParentInActiveNonWriteTimeline(), orderedReplicationList, isMDT);

  }

  private HoodieArchivedTimeline getArchivedTimeline(HoodieTableMetaClient metaClient, List<String> archivalCommits,
                                                     Option<String> firstCommonCommit) {
    // limit the number of commits to be replicated in a single run.
    archivalCommits = archivalCommits.stream().limit(MAX_ARCHIVED_COMMITS_TO_REPLICATE_PER_RUN).collect(Collectors.toList());
    String startTs = archivalCommits.get(0);
    String endTs = (archivalCommits.size() > MAX_ARCHIVED_COMMITS_TO_REPLICATE_PER_RUN)
            ? archivalCommits.get(MAX_ARCHIVED_COMMITS_TO_REPLICATE_PER_RUN) : firstCommonCommit.get();
    LOG.warn(String.format("%s: Loading archival commits from %s to %s to archived timeline.",
            metaClient.getTableConfig().getTableName(), startTs, endTs));
    // Bounded all-states load over [startTs, endTs] (v6/layout-V1 archive format): the delete
    // list must cover the requested/inflight/completed file of each archived instant, so the
    // completed-only getArchivedTimeline(startTs) cannot be used, and an unbounded full-archive
    // load is too much I/O on long-lived tables. Inclusive start: startTs is the first commit to
    // replicate. Callers restrict the instants they act on to archivalCommits.
    return ArchivedTimelineV1.loadAllStatesInClosedRange(metaClient, startTs, endTs);
  }

  private Stream<StoragePath> getUnReplicatedLogFiles(HoodieTableMetaClient metaClient, HoodieTableMetaClient remoteMetaClient) {
    try {
      List<StoragePathInfo> localPathInfos = metaClient.getStorage().globEntries(
          new StoragePath(metaClient.getArchivePath() + "/.commits_.archive*"));
      List<StoragePathInfo> remotePathInfos = remoteMetaClient.getStorage().globEntries(
          new StoragePath(remoteMetaClient.getArchivePath() + "/.commits_.archive*"));

      Map<String, Long> remoteFileMap = remotePathInfos.stream()
              .collect(Collectors.toMap(pathInfo -> pathInfo.getPath().getName(), StoragePathInfo::getLength));

      List<StoragePath> unReplicatedFiles = new ArrayList<>();
      for (StoragePathInfo primaryFile : localPathInfos) {
        String fileName = primaryFile.getPath().getName();
        if (!remoteFileMap.containsKey(fileName)) {
          LOG.warn("Archive log file present in local but missing in remote : {}", primaryFile.getPath());
          unReplicatedFiles.add(primaryFile.getPath());
        } else if (remoteFileMap.get(fileName) != primaryFile.getLength()) {
          LOG.warn("Archive log file size mismatch - file: {}, local size: {}, remote size: {}",
                  fileName, primaryFile.getLength(), remoteFileMap.get(fileName));
          unReplicatedFiles.add(primaryFile.getPath());
        }
      }
      return unReplicatedFiles.stream();
    } catch (IOException e) {
      LOG.error("Caught exception {} while fetching un-replicated log files", e.getMessage());
    }
    return Stream.empty();
  }

  private void addArchivedCommitsWithoutLogFiles(HoodieTableMetaClient metaClient,
                                                 HoodieTableMetaClient remoteMetaClient,
                                                 List<HoodieInstant> commitsForArchival,
                                                 Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList,
                                                 boolean isMDT) {
    if (commitsForArchival.isEmpty()) {
      return;
    }
    String tableName = metaClient.getTableConfig().getTableName();
    LOG.info(String.format("%s: Found %d instants archived on primary write timeline that are yet to be replicated "
            + "to the %s destination (%s)", tableName, commitsForArchival.size(), regionId, commitsForArchival));
    updateReplicationMap(orderedReplicationList, isMDT ? ReplicationStep.META_DEL_FILES : ReplicationStep.DATA_DEL_FILES,
            ReplicationAction.DELETE_FILES, commitsForArchival.stream().map(instant -> new StoragePath(metaClient.getMetaPath(), getInstantFileName(metaClient, instant))));
    if (replicateLogFilesForArchivalReplicationV2) {
      updateReplicationMap(orderedReplicationList, isMDT ? ReplicationStep.META_ADD_FILES : ReplicationStep.DATA_ADD_FILES,
              ReplicationAction.REPLICATE_FILES, getUnReplicatedLogFiles(metaClient, remoteMetaClient));
    }
  }

  private void addArchivedCommits(HoodieTableMetaClient metaClient,
                                  List<String> commitsForArchival, Option<String> oldestCommonCommit,
                                  Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList, boolean isMDT) {
    if (commitsForArchival.isEmpty() || !oldestCommonCommit.isPresent()) {
      return;
    }
    String tableName = metaClient.getTableConfig().getTableName();
    LOG.info(String.format("%s: Found %d commits archived on primary write timeline that are yet to be replicated "
            + "to the %s destination (%s)", tableName, commitsForArchival.size(), regionId, commitsForArchival));
    HoodieArchivedTimeline archivedTimeline = getArchivedTimeline(metaClient, commitsForArchival, oldestCommonCommit);
    updateReplicationMap(orderedReplicationList, isMDT ? ReplicationStep.META_DEL_FILES : ReplicationStep.DATA_DEL_FILES,
            ReplicationAction.DELETE_FILES, getArchivedCommitFiles(metaClient, archivedTimeline, commitsForArchival));
    updateReplicationMap(orderedReplicationList, isMDT ? ReplicationStep.META_ADD_FILES : ReplicationStep.DATA_ADD_FILES,
            ReplicationAction.REPLICATE_FILES, getArchivalLogFiles(metaClient, archivedTimeline));
    LOG.info(String.format("%s: Replicated %d archived commits to %s destination",
            metaClient.getTableConfig().getTableName(), commitsForArchival.size(), regionId));
  }

  private boolean isReplicationOperational() {
    Option<Boolean> crossRegionReplicationEnabled = HoodieReplicationContext.getCrossRegionReplicationEnabled(getMetaClient(), regionId, true);
    Option<Boolean> operationalStatus = HoodieReplicationContext.getDatasetReplicationOperationalStatus(getMetaClient(), regionId);
    if (!crossRegionReplicationEnabled.isPresent() || !operationalStatus.isPresent()) {
      LOG.warn(String.format("Cross region replication enabled or operational status not found for %s region", regionId));
      return false;
    }
    return crossRegionReplicationEnabled.get() && operationalStatus.get();
  }

  /**
   * Returns the last archived instant's timestamp from ArchivedTimeline, INIT_INSTANT_TS if archived timeline is empty.
   * @return last archived instant's timestamp
   */
  public String getFirstCommitForOnboarding() {
    HoodieArchivedTimeline archivedTimeline = getMetaClient().getArchivedTimeline();
    Option<HoodieInstant> lastInstant = archivedTimeline.filterCompletedInstants().lastInstant();
    if (lastInstant.isPresent()) {
      return lastInstant.get().requestedTime();
    }
    return INIT_INSTANT_TS;
  }

  /**
   * Groups files by their prefix and generates a ReplicationInfo for each group.
   *
   * @param action  - whether to create/delete file/directory
   * @param filePaths  - absolute file paths for the files to be replicated.
   * @return  List of ReplicationInfo, each replicationInfo containing the action to be performed, relative path of the
   * files and list of files.
   */
  private List<ReplicationInfo> makeReplicationInfo(ReplicationAction action, Stream<StoragePath> filePaths) {
    Map<String, List<String>> relativePathToFiles = new HashMap<>();
    if (action == ReplicationAction.DELETE_DIRS) {
      filePaths.forEach(fp -> {
        String prefix = FSUtils.getRelativePartitionPath(getMetaClient().getBasePath(), fp);
        relativePathToFiles.put(prefix, new ArrayList<>());
      });
    } else {
      // create a map of relative path to files.
      filePaths.forEach(fp -> {
        String prefix = FSUtils.getRelativePartitionPath(getMetaClient().getBasePath(), fp.getParent());
        String filename = fp.getName();
        List<String> files = relativePathToFiles.getOrDefault(prefix, new ArrayList<>());
        files.add(filename);
        relativePathToFiles.put(prefix, files);
      });
    }
    return relativePathToFiles.entrySet().stream().map(es -> new ReplicationInfo(action, es.getKey(), es.getValue()))
        .collect(Collectors.toList());
  }

  private void addMetadataInstantsForReplication(String datasetInstantTs, Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList) {
    try {
      // file listing metadata table is enabled
      if (isMetadataTableConfigured()) {
        String lastReplicatedDatasetCommit = getLastReplicatedCommit();
        HoodieReplicationMetadataClient internalClient = getInternalClient();
        /*
         * With v1 metadata implementation, metadata sync is performed with prewrite and postCommit operations.
         * As a result, the metadata sync can happen with the following commit. Replicate the dataset commit requested
         * and all commits completed between [last replicated commit & the dataset commit).
         *
         * Note: with Metadata V2, dataset commit will be performed after metadata commit. Dependency on last replicated
         * commit can be removed with Metadata v2.
         */
        Option<HoodieInstant> metadataCommit = internalClient.getMatchingInstant(lastReplicatedDatasetCommit);
        String startReplicationTs = metadataCommit.isPresent()
            ? metadataCommit.get().getCompletionTime() : lastReplicatedDatasetCommit;
        internalClient.getInstantsModifiedAfterTs(startReplicationTs).filter(i -> i.compareTo(datasetInstantTs) < 0)
            .forEach(instantTs -> addMetadataFilesForReplication(internalClient, instantTs, orderedReplicationList));

        // replicate the metadata commit associated with the dataset commit
        addMetadataFilesForReplication(internalClient, datasetInstantTs, orderedReplicationList);

        // If an associate compaction commit is present, replicate it.
        String compactionTs = createCompactionTimestamp(datasetInstantTs);
        if (internalClient.getMatchingInstant(compactionTs).isPresent()) {
          addMetadataFilesForReplication(internalClient, compactionTs, orderedReplicationList);
        }

        // If an associated clean commit is present, replicate it.
        String cleanTs = createCleanTimestamp(datasetInstantTs);
        if (internalClient.getMatchingInstant(cleanTs).isPresent()) {
          addMetadataFilesForReplication(internalClient, cleanTs, orderedReplicationList);
        }
      }
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.METADATA_REPLICATION_FAILED));
      throw new HoodieException(String.format("Failed to add metadata files for replication of %s", datasetInstantTs), e);
    }
  }

  private void updateReplicationMap(Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList,
                                    ReplicationStep step, ReplicationAction action, Stream<StoragePath> pathStream) {
    List<ReplicationInfo> replicationInfo = orderedReplicationList.getOrDefault(step, new ArrayList<>());
    replicationInfo.addAll(makeReplicationInfo(action, pathStream));
    orderedReplicationList.put(step, replicationInfo);
  }

  /*
   * Updates the orderedReplicationMap with metadata specific replication steps.
   */
  private void addMetadataFilesForReplication(HoodieReplicationMetadataClient internalClient, String instantTs,
                                              Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList) {
    if (internalClient.getMatchingInstant(instantTs).isPresent()) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      LOG.info(String.format("Adding metadata files for replicating commit %s", instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_START_MARKERS,
          ReplicationAction.REPLICATE_FILES, internalClient.getStartReplicationMarkers(instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_DATA_MARKERS,
          ReplicationAction.CREATE_FILES, internalClient.getBaseFileMarkers(instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_ADD_FILES,
          ReplicationAction.REPLICATE_FILES, internalClient.getBaseFilesAdded(instantTs, true));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_DEL_FILES,
          ReplicationAction.DELETE_FILES, internalClient.getBaseFilesDeleted(instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_FINISH_MARKERS,
          ReplicationAction.REPLICATE_FILES, internalClient.getFinishReplicationMarkers(instantTs, isReplicateHoodieProperties()));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_CLEANUP_MARKERS, ReplicationAction.DELETE_DIRS,
          Stream.of(new StoragePath(internalClient.getMetaClient().getMarkerFolderPath(instantTs))));
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.METADATA_REPLICATED, timer.endTimer()));
    }
  }

  /*
   * Updates the orderedReplicationMap with metadata specific steps for replicating archived commits.
   */
  @Deprecated
  private void addMetadataFilesForArchival(String instantTs,
                                           Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList) {
    try {
      // file listing metadata table is enabled
      if (isMetadataTableConfigured()) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        HoodieReplicationMetadataClient internalClient = getInternalClient();
        updateReplicationMap(orderedReplicationList, ReplicationStep.META_DEL_FILES, ReplicationAction.DELETE_FILES,
            internalClient.getArchivedCommitFiles(getLastArchivedCommit()));
        updateReplicationMap(orderedReplicationList, ReplicationStep.META_ADD_FILES, ReplicationAction.REPLICATE_FILES,
            internalClient.getArchivalFiles());
        metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.METADATA_ARCHIVED_REPLICATED, timer.endTimer()));
      }
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.METADATA_ARCHIVED_REPLICATION_FAILED));
      throw new HoodieException(String.format("Failed to add metadata files for replication of %s", instantTs), e);
    }
  }

  private void addMetadataInstantsForRollback(String datasetInstantTs, Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList) {
    try {
      // file listing metadata table is enabled
      if (isMetadataTableConfigured()) {
        HoodieReplicationMetadataClient internalClient = getInternalClient();

        // If an associated clean commit is present, revert it.
        String cleanTs = createCleanTimestamp(datasetInstantTs);
        if (internalClient.getMatchingInstant(cleanTs).isPresent()) {
          addMetadataFilesForRollback(internalClient, cleanTs, orderedReplicationList);
        }

        // If an associated compaction commit is present, revert it.
        String compactionTs = createCompactionTimestamp(datasetInstantTs);
        if (internalClient.getMatchingInstant(compactionTs).isPresent()) {
          addMetadataFilesForRollback(internalClient, compactionTs, orderedReplicationList);
        }

        // revert the metadata commit associated with the dataset commit
        addMetadataFilesForRollback(internalClient, datasetInstantTs, orderedReplicationList);
      }
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.METADATA_REVERT_FAILED));
      throw new HoodieException(String.format("Failed to add metadata files for rollback of %s", datasetInstantTs), e);
    }
  }

  /*
   * Updates the orderedReplicationMap with metadata specific rollback steps.
   */
  private void addMetadataFilesForRollback(HoodieReplicationMetadataClient internalClient, String instantTs, Map<ReplicationStep, List<ReplicationInfo>> orderedReplicationList) throws IOException {
    List<String> pendingCommits = internalClient
        .getInflightAndRequestedInstants()
        .stream()
        .map(HoodieInstant::requestedTime)
        .collect(Collectors.toList());
    // While a commit was ongoing, failed over to the secondary region and replication was started from
    // secondary -> primary region, and the pending commit on the primary region is being rolled back (reverted).
    // If there were completed commits that were yet to be replicated to the secondary region (before the failover),
    // then they will be rolled back (reverted) as well.
    boolean isPendingCommit = pendingCommits.contains(instantTs);
    if (internalClient.getMatchingInstant(instantTs).isPresent() || isPendingCommit) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      LOG.info(String.format("Adding metadata files for reverting commit %s", instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_DATA_MARKERS, ReplicationAction.CREATE_FILES,
          internalClient.getBaseFileMarkers(instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_FINISH_MARKERS, ReplicationAction.DELETE_FILES,
          internalClient.getFinishReplicationMarkers(instantTs, !REPLICATE_HOODIE_PROPERTIES));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_DEL_FILES, ReplicationAction.DELETE_FILES,
          isPendingCommit ? internalClient.getPendingDataFiles(instantTs)
              : internalClient.getBaseFilesDeleted(instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_ADD_FILES, ReplicationAction.DELETE_FILES,
          internalClient.getBaseFilesAdded(instantTs, false));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_START_MARKERS, ReplicationAction.DELETE_FILES,
          isPendingCommit ? internalClient.getStartReplicationMarkersForPendingCommit(instantTs)
              : internalClient.getStartReplicationMarkers(instantTs));
      updateReplicationMap(orderedReplicationList, ReplicationStep.META_CLEANUP_MARKERS, ReplicationAction.DELETE_DIRS,
          Stream.of(new StoragePath(internalClient.getMetaClient().getMarkerFolderPath(instantTs))));
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.METADATA_REVERTED, timer.endTimer()));
    }
  }

  /**
   * Returns whether .requested file is present for the supplied action or not.
   * @param action - Hoodie action
   * @return  true - if .requested file is present for the action.  false otherwise.
   */
  private boolean requestedPresent(String action) {
    // currently savepoint, restore actions don't create .requested files
    return !HoodieTimeline.SAVEPOINT_ACTION.equals(action);
  }

  /**
   * Returns a stream of files to be replicated to mark the start of a commit.
   * Typically, this includes .requested, .inflight files for an instant.
   */
  private Stream<StoragePath> getStartReplicationMarkers(String instantTime) {
    Option<HoodieInstant> optCompleted = getMatchingInstant(instantTime);
    if (!optCompleted.isPresent()) {
      return Stream.of();
    }
    return getStartReplicationMarkersForInstant(optCompleted);
  }

  private Stream<StoragePath> getStartReplicationMarkersForInstant(Option<HoodieInstant> optInstant) {
    if (getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      String action = optInstant.get().getAction();
      if (action.equals(HoodieTimeline.COMMIT_ACTION)) {
        return Stream.of(
            new StoragePath(getMetaClient().getMetaPath(),
                getInstantFileName(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION,
                    optInstant.get().requestedTime(), getInstantComparator()))),
            new StoragePath(getMetaClient().getMetaPath(),
                getInstantFileName(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION,
                    optInstant.get().requestedTime(), getInstantComparator())))
        );
      }
      if (action.equals(HoodieTimeline.LOG_COMPACTION_ACTION)) {
        return Stream.of(
            new StoragePath(getMetaClient().getMetaPath(),
                getInstantFileName(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.LOG_COMPACTION_ACTION,
                    optInstant.get().requestedTime(), getInstantComparator()))),
            new StoragePath(getMetaClient().getMetaPath(),
                getInstantFileName(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION,
                    optInstant.get().requestedTime(), getInstantComparator())))
        );
      }
    }
    List<HoodieInstant.State> startStates = requestedPresent(optInstant.get().getAction())
        ? new ArrayList<>(Arrays.asList(HoodieInstant.State.REQUESTED, HoodieInstant.State.INFLIGHT))
        : new ArrayList<>(Collections.singletonList(HoodieInstant.State.INFLIGHT));
    return optInstant.map(completed ->
        startStates.stream()
            .map(state -> new HoodieInstant(state, completed.getAction(), completed.requestedTime(), getInstantComparator()))
            .map(instant -> getInstantFileName(instant))
            .map(fileName -> new StoragePath(getMetaClient().getMetaPath(), fileName))).get();
  }

  private Stream<StoragePath> getStartReplicationMarkersForPendingCommit(String instantTime) {
    Option<HoodieInstant> optPendingInstant = getMetaClient().getActiveTimeline().getAllCommitsTimeline()
        .filterInflightsAndRequested().filter(i -> i.requestedTime().equals(instantTime)).firstInstant();
    return  getStartReplicationMarkersForInstant(optPendingInstant);
  }

  /*
   * Returns a stream of marker file paths for every data file, for a given instant time and partition.
   */
  private Stream<StoragePath> getBaseFileMarkers(String instantTime) {
    return getBaseFilesAdded(instantTime, false).map(bf -> {
      String partition = FSUtils.getRelativePartitionPath(getMetaClient().getBasePath(), bf.getParent());
      String markerPartitionPath = String.format("%s/%s", getMetaClient().getMarkerFolderPath(instantTime), partition);
      return new StoragePath(markerPartitionPath, bf.getName() + HoodieTableMetaClient.MARKER_EXTN + ".CREATE");
    });
  }

  /**
   * Extracts all file paths to replicate from write stats.
   * For HoodieWriteStat: uses getPath() (base parquet or single file).
   * For HoodieDeltaWriteStat (MOR): uses getPath(), getLogFiles(), and getBaseFile() to include
   * all log files and base files in the file group.
   * For CDC-enabled tables: uses getCdcStats() to include CDC log files (.cdc).
   * Uses a Set to deduplicate paths.
   */
  private Stream<StoragePath> getFilesToReplicateFromWriteStats(List<HoodieWriteStat> writeStats, String partition) {
    if (writeStats == null) {
      return Stream.empty();
    }
    StoragePath basePath = getMetaClient().getBasePath();
    Set<String> pathStrings = new HashSet<>();
    for (HoodieWriteStat ws : writeStats) {
      String path = ws.getPath();
      if (path != null && !path.isEmpty()) {
        pathStrings.add(new StoragePath(basePath, path).toString());
      }
      if (ws instanceof HoodieDeltaWriteStat) {
        HoodieDeltaWriteStat deltaStat = (HoodieDeltaWriteStat) ws;
        for (String logFile : deltaStat.getLogFiles()) {
          if (logFile != null && !logFile.isEmpty()) {
            pathStrings.add(new StoragePath(basePath, partition + StoragePath.SEPARATOR + logFile).toString());
          }
        }
        String baseFile = deltaStat.getBaseFile();
        if (baseFile != null && !baseFile.isEmpty()) {
          String baseFilePathStr = baseFile.contains(StoragePath.SEPARATOR)
              ? new StoragePath(basePath, baseFile).toString()
              : new StoragePath(basePath, partition + StoragePath.SEPARATOR + baseFile).toString();
          pathStrings.add(baseFilePathStr);
        }
      }
      // Include CDC log files when hoodie.table.cdc.enabled is true
      Map<String, Long> cdcStats = ws.getCdcStats();
      if (cdcStats != null && !cdcStats.isEmpty()) {
        for (String cdcPath : cdcStats.keySet()) {
          if (cdcPath != null && !cdcPath.isEmpty()) {
            pathStrings.add(new StoragePath(basePath, cdcPath).toString());
          }
        }
      }
    }
    return pathStrings.stream().map(StoragePath::new);
  }

  /*
   * Returns a stream of base data file paths, for a given instant time and partition.
   */
  private Stream<StoragePath> getBaseFilesAdded(String instantTime, boolean withPartitionMetadataFiles) {
    HoodieTimeline timeline = getMetaClient().getActiveTimeline().getCommitsTimeline();
    Option<HoodieInstant> instant = getMatchingInstant(instantTime);
    if (instant.isPresent()) {
      String action = instant.get().getAction();
      if (HoodieTimeline.COMMIT_ACTION.equals(action) || HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)
          || HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action) || HoodieTimeline.COMPACTION_ACTION.equals(action)
          || HoodieTimeline.CLUSTERING_ACTION.equals(action) || HoodieTimeline.LOG_COMPACTION_ACTION.equals(action)) {
        try {
          HoodieTimer timer = new HoodieTimer().startTimer();
          HoodieCommitMetadata commit = timeline.readCommitMetadata(instant.get());
          Stream<StoragePath> paths = commit.getPartitionToWriteStats().keySet().stream().flatMap(
              partition -> getFilesToReplicateFromWriteStats(commit.getWriteStats(partition), partition));
          if (withPartitionMetadataFiles) {
            paths = Stream.of(paths,
                commit.getWritePartitionPaths().stream()
                    .map(p -> new StoragePath(getMetaClient().getBasePath() + StoragePath.SEPARATOR + p,
                        HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX))).flatMap(i -> i);
          }
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.COMMIT_REPLICATED, timer.endTimer()));
          return paths;
        } catch (Exception e) {
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.COMMIT_REPLICATION_FAILED));
          throw new HoodieException(String.format("Failed to replicate commit %s", instantTime), e);
        }
      }
    }
    return Stream.of();
  }

  private Stream<StoragePath> getRollbackFiles(HoodieRollbackMetadata rollback) {
    Stream<StoragePath> paths = Stream.of(
        // completed commit files for the rolled back instances.
        rollback.getInstantsRollback().stream().map(rbi -> getInstantFileName(
                new HoodieInstant(HoodieInstant.State.COMPLETED, rbi.getAction(), rbi.getCommitTime(), getInstantComparator())))
            .map(fn -> new StoragePath(getMetaClient().getMetaPath(), String.valueOf(fn))),

        // deleted files in rolled back instances
        rollback.getPartitionMetadata().keySet().stream().flatMap(
            partition -> rollback.getPartitionMetadata().get(partition).getSuccessDeleteFiles().stream()
                .map(fn -> new StoragePath(getMetaClient().getBasePath() + StoragePath.SEPARATOR + partition, new StoragePath(fn).getName()))),

        // Failed deletion attempts.
        rollback.getPartitionMetadata().keySet().stream().flatMap(
            partition -> rollback.getPartitionMetadata().get(partition).getFailedDeleteFiles().stream()
                .map(fn -> new StoragePath(getMetaClient().getBasePath() + StoragePath.SEPARATOR + partition, new StoragePath(fn).getName()))),

        // .inflight commit files for all rolled back instants
        rollback.getInstantsRollback().stream().map(rbi -> getInstantFileName(
                new HoodieInstant(HoodieInstant.State.INFLIGHT, rbi.getAction(), rbi.getCommitTime(), getInstantComparator())))
            .map(fn -> new StoragePath(getMetaClient().getMetaPath(), String.valueOf(fn))),

        // .requested commit files for all rolled back instants
        rollback.getInstantsRollback().stream().map(rbi -> getInstantFileName(
                new HoodieInstant(HoodieInstant.State.REQUESTED, rbi.getAction(), rbi.getCommitTime(), getInstantComparator())))
            .map(fn -> new StoragePath(getMetaClient().getMetaPath(), String.valueOf(fn)))
    ).flatMap(i -> i);
    return paths;
  }

  /**
   * returns a stream of files deleted by commit with instantTime.
   * @param instantTime - Hudi commit timestamp
   * @return  Stream of file paths deleted by clean or rollback commit.
   */
  private Stream<StoragePath> getDeletedFiles(String instantTime) {
    Option<HoodieInstant> instant = getMatchingInstant(instantTime);
    if (instant.isPresent()) {
      String action = instant.get().getAction();
      if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
        try {
          HoodieTimer timer = new HoodieTimer().startTimer();
          HoodieCleanMetadata clean = CleanerUtils.getCleanerMetadata(getMetaClient(), instant.get());
          // collect the successful and failed deletes from the clean commit metadata.
          Stream<StoragePath> paths =  Stream.of(
              clean.getPartitionMetadata().keySet().stream().flatMap(
                  partition -> clean.getPartitionMetadata().get(partition).getSuccessDeleteFiles().stream()
                      .map(fn -> new StoragePath(getMetaClient().getBasePath() + StoragePath.SEPARATOR + partition, fn))),
              clean.getPartitionMetadata().keySet().stream().flatMap(
                  partition -> clean.getPartitionMetadata().get(partition).getFailedDeleteFiles().stream()
                      .map(fn -> new StoragePath(getMetaClient().getBasePath() + StoragePath.SEPARATOR + partition, fn)))
          ).flatMap(i -> i);
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.CLEAN_REPLICATED, timer.endTimer()));
          return paths;
        } catch (Exception e) {
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.CLEAN_REPLICATION_FAILED));
          throw new HoodieException(String.format("Failed to replicate clean instant %s", instantTime), e);
        }
      }
      if (HoodieTimeline.ROLLBACK_ACTION.equals(action)) {
        try {
          HoodieTimer timer = new HoodieTimer().startTimer();
          HoodieRollbackMetadata rollback =
              getMetaClient().getActiveTimeline().readRollbackMetadata(instant.get());
          // collect the successful and failed deletes from the rolled back commit.
          Stream<StoragePath> paths = getRollbackFiles(rollback);
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.ROLLBACK_REPLICATED, timer.endTimer()));
          return paths;
        } catch (Exception e) {
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.ROLLBACK_REPLICATION_FAILED));
          throw new HoodieException(String.format("Failed to replicate rollback instant %s", instantTime), e);
        }
      }
      if (HoodieTimeline.RESTORE_ACTION.equals(action)) {
        try {
          HoodieTimer timer = new HoodieTimer().startTimer();
          HoodieRestoreMetadata restore =
              getMetaClient().getActiveTimeline().readRestoreMetadata(instant.get());
          // collect the successful and failed deletes from the rolled back commits.
          Stream<StoragePath> paths = Stream.empty();
          for (Map.Entry<String, List<HoodieRollbackMetadata>> rbe : restore.getHoodieRestoreMetadata().entrySet()) {
            paths = Stream.of(paths, rbe.getValue().stream().flatMap(rollback -> getRollbackFiles(rollback))).flatMap(i -> i);
          }
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.RESTORE_REPLICATED, timer.endTimer()));
          return paths;
        } catch (Exception e) {
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.RESTORE_REPLICATION_FAILED));
          throw new HoodieException(String.format("Failed to replicate restore instant %s", instantTime), e);
        }
      }
    }
    return Stream.of();
  }

  /*
   * Returns a stream of base data file paths deleted, for a given instant time and partition.
   */
  private Stream<StoragePath> getBaseFilesDeleted(String instantTime) {
    Option<HoodieInstant> instant = getMatchingInstant(instantTime);
    if (instant.isPresent()) {
      return getDeletedFiles(instantTime);
    }
    return Stream.empty();
  }

  /**
   * getDeletedDirectories - With the delete partition operation, directories would be deleted. Returns a list of
   * directory path deleted/stashed by the deletePartition, stashPartition operations.
   */
  private Stream<StoragePath> getDeletedDirectories(String instantTime) {
    HoodieTimeline timeline = getMetaClient().getActiveTimeline().getAllCommitsTimeline();
    Option<HoodieInstant> instant = getMatchingInstant(instantTime);
    if (instant.isPresent()) {
      String action = instant.get().getAction();
      if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
        try {
          HoodieTimer timer = new HoodieTimer().startTimer();
          HoodieReplaceCommitMetadata replaceCommitMetadata = timeline.readReplaceCommitMetadata(instant.get());
          // If a stashed location is provided, the deletePartitions API will move the partition to the backup location.
          // The partition will no longer exist in the Hudi basepath once the replacecommit is complete.
          // Since the partition will no longer exist on the primary region, it should also be deleted on
          // the replication targets during replication of replacecommit
          if (replaceCommitMetadata.getOperationType() == WriteOperationType.DELETE_PARTITION
              && replaceCommitMetadata.getMetadata(DeletePartitionCommitMetadata.STASHED_LOCATION_KEY) != null) {
            Stream<StoragePath> paths = Stream.of(
                replaceCommitMetadata.getPartitionToReplaceFileIds().keySet().stream()
                    .map(partition -> new StoragePath(getMetaClient().getBasePath() + StoragePath.SEPARATOR + partition))
            ).flatMap(i -> i);
            metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLACE_COMMIT_REPLICATED, timer.endTimer()));
            return paths;
          }
        } catch (Exception e) {
          metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLACE_COMMIT_REPLICATION_FAILED));
          throw new HoodieException(String.format("Failed to replicate replacecommit instant %s", instantTime), e);
        }
      }
    }
    return Stream.empty();
  }

  /*
   * Returns a stream of Finish replication markers. Filename associated with the completed Hudi instant.
   */
  private Stream<StoragePath> getFinishReplicationMarkers(String instantTime, boolean withHoodieProperties) {
    Stream<StoragePath> paths = Stream.of(getMetaClient().getActiveTimeline().getAllCommitsTimeline()
        .filterCompletedInstants().findInstantsAfterOrEquals(instantTime, 1).getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(instantTime))
        .map(commit -> new StoragePath(getMetaClient().getMetaPath(), getInstantFileName(commit)))).flatMap(i -> i);
    if (withHoodieProperties) {
      paths = Stream.of(paths,
          Stream.of(new StoragePath(getMetaClient().getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE),
              new StoragePath(getMetaClient().getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP))).flatMap(i -> i);
    }
    return paths;
  }

  private Stream<StoragePath> getArchivedCommitFiles(HoodieTableMetaClient tableMetaClient,
          HoodieArchivedTimeline archivedTimeline, List<String> archivedCommits) {
    try {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.NUMBER_OF_ARCHIVED_REPLICATED));
      StoragePath instantFilePath = tableMetaClient.getTimelinePath();
      return archivedTimeline.getInstantsAsStream()
              .filter(i -> archivedCommits.contains(i.requestedTime()))
              .map(archived -> new StoragePath(instantFilePath, getInstantFileName(tableMetaClient, archived)));
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLICATE_ARCHIVED_FAILED));
      throw new HoodieException(
              String.format("Failed to get archived commit files for instant range [%s] with exception (%s)",
                      archivedCommits, e));
    }
  }

  private Stream<StoragePath> getArchivalLogFiles(HoodieTableMetaClient tableMetaClient,
                                           HoodieArchivedTimeline archivedTimeline) {
    try {
      StoragePath archivePath = tableMetaClient.getArchivePath();
      List<StoragePathInfo> archiveFiles = tableMetaClient.getStorage().globEntries(
          new StoragePath(archivePath, ".commits_.archive*"));
      if (!archiveFiles.isEmpty()) {
        return archiveFiles.stream().map(StoragePathInfo::getPath);
      }
      return tableMetaClient.getStorage().listDirectEntries(archivePath)
          .stream().map(StoragePathInfo::getPath);
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.REPLICATE_ARCHIVED_FAILED));
      throw new HoodieException(String.format("Failed to get Archival files from %s for %s  (%s)",
              tableMetaClient.getArchivePath(), regionId, e));
    }
  }

  @Deprecated
  private Stream<StoragePath> getArchivedCommitFiles(String lastArchivedTs) {
    // Archive-position-based all-states load (0.x fork semantics): the archive carries
    // requested/inflight/completed records per instant and archival deletes all three files
    // from the timeline, so the delete list must cover all three states of every instant
    // appended to the archive after the checkpoint — including late-archived instants with
    // older requested times (rollbacks) — while excluding the checkpoint instant itself.
    HoodieArchivedTimeline archivedTimeline = ArchivedTimelineV1.loadInstantsArchivedAfter(
        getMetaClient(), lastArchivedTs, MAX_ARCHIVED_INSTANTS_TO_LOAD);
    try {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.NUMBER_OF_ARCHIVED_REPLICATED));
      StoragePath instantFilePath = getMetaClient().getTimelinePath();
      return archivedTimeline.getInstantsAsStream()
          .map(archived -> new StoragePath(instantFilePath, getInstantFileName(archived)));
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.ARCHIVED_REPLICATED_FAILED));
      throw new HoodieException(
          String.format("Failed to get archived commit files for instant range %s - %s with exception (%s)",
          archivedTimeline.firstInstant().get().requestedTime(), archivedTimeline.lastInstant().get().requestedTime(), e));
    }
  }

  @Deprecated
  private Stream<StoragePath> getArchivalFiles() {
    try {
      // Replicate only the archive log files that carry content past the archived checkpoint
      // (the files the position-based load actually read), not the entire archive folder.
      return ArchivedTimelineV1.loadInstantsArchivedAfter(
          getMetaClient(), getLastArchivedCommit(), MAX_ARCHIVED_INSTANTS_TO_LOAD).getFilesLoaded().stream();
    } catch (Exception e) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieReplicationMetrics.ARCHIVED_REPLICATED_FAILED));
      throw new HoodieException(
          String.format("Failed to get Archival files at %s (%s)", getMetaClient().getArchivePath(), e));
    }
  }

  protected void initRegistry() {
    String tableName = getMetaClient().getTableConfig().getTableName();
    Registry registry = Registry.getRegistryOfClass(tableName, CROSS_REGION_REPLICATION_REGISTRY, LocalRegistry.class.getName());
    this.metrics = Option.of(new HoodieReplicationMetrics(registry, tableName));
  }

  private Comparator<HoodieInstant> getInstantComparator() {
    return getMetaClient().getTimelineLayout().getInstantComparator().requestedTimeOrderedComparator();
  }

  private String getInstantFileName(HoodieInstant instant) {
    return getMetaClient().getTimelineLayout().getInstantFileNameGenerator().getFileName(instant);
  }

  private String getInstantFileName(HoodieTableMetaClient metaClient, HoodieInstant instant) {
    return metaClient.getTimelineLayout().getInstantFileNameGenerator().getFileName(instant);
  }

  private Stream<StoragePath> getPendingDataFiles(String instantTs) throws IOException {
    StoragePath markerFolderPath = new StoragePath(getMetaClient().getMarkerFolderPath(instantTs));
    // example:
    //   <basepath>/.hoodie/.temp/20220223010045/2022/02/22/eeb6b6a1-6329-446d-9d68-a7af4686ef02-0_78-7-9542_20220223010045.parquet.marker.CREATE
    //   a) strip prefix <basepath>/.hoodie/.temp/20220223010045/
    //   b) strip suffix .marker.CREATE (or .marker.MERGE)
    // CREATE and MERGE markers both denote a brand-new file path written by this instant (small-file
    // bin-packing produces a MERGE marker for a new file, not an in-place append), so both must be
    // deleted on rollback of a pending commit -- matching MarkerBasedRollbackStrategy's treatment of
    // CREATE and MERGE identically. APPEND markers (MOR log blocks) are intentionally excluded.
    String markerCreateSuffix = HoodieTableMetaClient.MARKER_EXTN + ".CREATE";
    String markerMergeSuffix = HoodieTableMetaClient.MARKER_EXTN + ".MERGE";
    return allMarkerFilePaths(instantTs)
        .filter(path -> path.getName().endsWith(markerCreateSuffix) || path.getName().endsWith(markerMergeSuffix))
        .map(fullPath -> new StoragePath(FSUtils.getRelativePartitionPath(markerFolderPath, fullPath)))
        .map(suffix -> suffix.toString())
        .map(suffixStr -> {
          String markerSuffix = suffixStr.endsWith(markerCreateSuffix) ? markerCreateSuffix : markerMergeSuffix;
          return new StoragePath(getMetaClient().getBasePath(),
              suffixStr.substring(0, suffixStr.length() - markerSuffix.length()));
        });
  }

  private List<StoragePath> readDirectMarkers(String markerFolderStr) {
    List<StoragePath> markerFiles = new ArrayList<>();
    try {
      FSUtils.processFiles(getMetaClient().getStorage(), markerFolderStr, fileStatus -> {
        markerFiles.add(fileStatus.getPath());
        return true;
      }, false);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get marker file paths", e);
    }
    return markerFiles;
  }

  private List<StoragePath> readTimelineServerMarkers(String markerFolderStr) {
    List<StoragePath> markerFiles = new ArrayList<>();
    try {
      FSUtils.processFiles(getMetaClient().getStorage(), markerFolderStr, fileStatus -> {
        try {
          FSUtils.processFiles(getMetaClient().getStorage(), fileStatus.getPath().toString(), innerFileStatus -> {
            String innerFileName = innerFileStatus.getPath().getName();
            if (innerFileName.equals(MARKER_TYPE_FILENAME)) {
              return true;
            }
            if (innerFileName.startsWith(MARKERS_FILENAME_PREFIX)) {
              readMarkersFromFile(innerFileStatus.getPath(), getMetaClient().getStorageConf(), false)
                      .forEach(p -> markerFiles.add(new StoragePath(markerFolderStr, p)));
              return true;
            }
            return false;
          }, false);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to read marker files in " + markerFolderStr, e);
        }
        return true;
      }, false);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to process marker folder " + markerFolderStr, e);
    }
    return markerFiles;
  }

  private Stream<StoragePath> allMarkerFilePaths(String instantTs) throws IOException {
    List<StoragePath> markerFiles = new ArrayList<>();
    StoragePath markerFolderPath = new StoragePath(getMetaClient().getMarkerFolderPath(instantTs));
    if (getMetaClient().getStorage().exists(markerFolderPath)) {
      Option<MarkerType> markerTypeOption = readMarkerType(getMetaClient().getStorage(), markerFolderPath.toString());
      if (markerTypeOption.isPresent() && markerTypeOption.get() == MarkerType.TIMELINE_SERVER_BASED) {
        markerFiles.addAll(readTimelineServerMarkers(markerFolderPath.toString()));
      } else {
        markerFiles.addAll(readDirectMarkers(markerFolderPath.toString()));
      }
    }
    return markerFiles.stream();
  }

  private List<HoodieInstant> getInflightAndRequestedInstants() {
    return getMetaClient().getActiveTimeline().getAllCommitsTimeline()
        .filterInflightsAndRequested().getInstantsAsStream().collect(Collectors.toList());
  }

  @VisibleForTesting
  public HoodieReplicationMetadataClient getInternalClient() {
    if (!useArchivalReplicationV2) {
      // when hivesync code is updated to use the constructor with source/target paths, we can remove this check.
      return new HoodieReplicationMetadataClient(getMetaClient().getStorageConf(),
          HoodieTableMetadata.getMetadataTableBasePath(getMetaClient().getBasePath().toString()),
          getReplicationDestination());
    }
    // If replicationV2 is enabled, make sure destination meta client is set.
    ValidationUtils.checkArgument(getDestinationMetaClient() != null, "Destination meta client is not set");
    return new HoodieReplicationMetadataClient(getMetaClient().getStorageConf(),
        HoodieTableMetadata.getMetadataTableBasePath(getMetaClient().getBasePath().toString()),
        HoodieTableMetadata.getMetadataTableBasePath(getDestinationMetaClient().getBasePath().toString()),
        getReplicationDestination());
  }

  private Option<HoodieInstant> getFirstCommitOnTimeline() {
    return getMetaClient().reloadActiveTimeline().getAllCommitsTimeline().filterCompletedInstants().firstInstant();
  }

  /**
   * Memoized variant of {@link #isMetadataTableConfigured(HoodieTableMetaClient)} for this client's
   * data table. MDT existence is effectively monotonic (created once at bootstrap, deleted only
   * out-of-band), so a positive result is cached to avoid a live storage check on every per-commit
   * replication path; a negative result is re-checked so a late MDT bootstrap is picked up. The
   * cache is cleared on {@link #reload()}.
   */
  private boolean isMetadataTableConfigured() throws IOException {
    if (metadataTablePresent == null || !metadataTablePresent) {
      metadataTablePresent = isMetadataTableConfigured(getMetaClient());
    }
    return metadataTablePresent;
  }

  @Override
  public void reload() {
    super.reload();
    this.metadataTablePresent = null;
  }

  /**
   * Returns {@code true} if the metadata table exists on storage for the given data table meta client.
   * This is a live filesystem check, unlike {@link HoodieTableConfig#isMetadataTableAvailable()} which reads
   * a cached in-memory flag that only reflects reality as of the meta client's last reload.
   */
  private static boolean isMetadataTableConfigured(HoodieTableMetaClient dataMetaClient) throws IOException {
    StoragePath metadataTableBasePath = new StoragePath(
        HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePath().toString()));
    return dataMetaClient.getStorage().exists(metadataTableBasePath);
  }
}
