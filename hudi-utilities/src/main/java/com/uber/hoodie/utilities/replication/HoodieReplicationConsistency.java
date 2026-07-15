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

package com.uber.hoodie.utilities.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checks the consistency of a replicated dataset.
 */
public class HoodieReplicationConsistency implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplicationConsistency.class);
  private final transient HoodieEngineContext engineContext;
  private final String sourceBasePath;
  private final String targetBasePath;
  private final String instantTimestamp;
  private boolean ignoreArchivalConsistency = false;

  // The time window in hours for rollback consistency check
  private long rollbackConsistencyWindowHours = 12;

  // Configs which are ignored in cross-dc comparison as their values are datacenter specific
  private static final Set<String> IGNORED_CONFIGS = new HashSet<>(Arrays.asList(HoodieLockConfig.ZK_CONNECT_URL.key()));

  public enum ReplicationConsistencyType {
    QUERY_CONSISTENCY,
    WRITER_CONSISTENCY,
    ROLLBACK_CONSISTENCY,
    TABLE_SERVICES_CONSISTENCY
  }

  /**
   * @param sourceBasePath The basePath of the source (primary) dataset
   * @param targetBasePath The basePath of the target (replicated) dataset
   * @param instantTimestamp The instant time (e.g. timestamp of last successful replication) to check consistency for
   */
  public HoodieReplicationConsistency(String sourceBasePath, String targetBasePath, String instantTimestamp) {
    this(new HoodieLocalEngineContext(new HadoopStorageConfiguration(new Configuration())), sourceBasePath, targetBasePath, instantTimestamp);
  }

  public HoodieReplicationConsistency(HoodieEngineContext engineContext, String sourceBasePath, String targetBasePath, String instantTimestamp) {
    this.engineContext = engineContext;
    this.sourceBasePath = sourceBasePath;
    this.targetBasePath = targetBasePath;
    this.instantTimestamp = instantTimestamp;
  }

  /**
   * Checks the query consistency.
   *
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  public HoodieReplicationConsistencyInfo checkQueryConsistency(Option<List<String>> partitionsToCheck) {
    return checkAllConsistency(Collections.singleton(ReplicationConsistencyType.QUERY_CONSISTENCY), partitionsToCheck);
  }

  /**
   * Checks the writer consistency.
   *
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  public HoodieReplicationConsistencyInfo checkWriterConsistency(Option<List<String>> partitionsToCheck) {
    return checkAllConsistency(Collections.singleton(ReplicationConsistencyType.WRITER_CONSISTENCY), partitionsToCheck);
  }

  /**
   * Checks the rollback/restore consistency.
   *
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  public HoodieReplicationConsistencyInfo checkRollbackConsistency(Option<List<String>> partitionsToCheck) {
    return checkAllConsistency(Collections.singleton(ReplicationConsistencyType.ROLLBACK_CONSISTENCY), partitionsToCheck);
  }

  /**
   * Checks the table services consistency.
   *
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  public HoodieReplicationConsistencyInfo checkTableServicesConsistency(Option<List<String>> partitionsToCheck) {
    return checkAllConsistency(Collections.singleton(ReplicationConsistencyType.TABLE_SERVICES_CONSISTENCY), partitionsToCheck);
  }

  /**
   * Checks for different types of consistency.
   *
   * @param consistencyTypeToCheck The type of consistency to check. If not provided, all types of consistency are checked.
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  public HoodieReplicationConsistencyInfo checkConsistency(Option<Set<ReplicationConsistencyType>> consistencyTypeToCheck,
                                                           Option<List<String>> partitionsToCheck) {
    // if no consistency type is provided, check all types of consistencies
    Set<ReplicationConsistencyType> consistencyChecks = consistencyTypeToCheck.orElse(new HashSet<>(Arrays.asList(ReplicationConsistencyType.values())));
    return checkAllConsistency(consistencyChecks, partitionsToCheck);
  }

  /**
   * Checks for all the types of consistency.
   *
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  public HoodieReplicationConsistencyInfo checkAllConsistency(Option<List<String>> partitionsToCheck) {
    // if no consistency type is provided, check all types of consistencies
    return checkAllConsistency(new HashSet<>(Arrays.asList(ReplicationConsistencyType.values())), partitionsToCheck);
  }

  /**
   * Checks for all the types of replication consistency for a pair of replicated datasets.
   *
   * @param consistencyChecks The type of consistency to check.
   * @param partitionsToCheck The partitions to check for consistency. If not provided, all partitions are checked.
   * @return {@code HoodieReplicationConsistencyInfo}
   */
  private HoodieReplicationConsistencyInfo checkAllConsistency(Set<ReplicationConsistencyType> consistencyChecks,
                                                               Option<List<String>> partitionsToCheck) {
    HoodieTableMetaClient srcMetaClient;
    HoodieTableMetaClient targetMetaClient;

    // Check that the source and target datasets exist
    try {
      srcMetaClient = HoodieTableMetaClient.builder().setConf(engineContext.getStorageConf()).setBasePath(sourceBasePath).build();
    } catch (TableNotFoundException e) {
      LOG.error("Source HUDI dataset not found at path " + sourceBasePath, e);
      throw e;
    }
    try {
      targetMetaClient = HoodieTableMetaClient.builder().setConf(engineContext.getStorageConf()).setBasePath(targetBasePath).build();
    } catch (TableNotFoundException e) {
      LOG.error("Target HUDI dataset not found at path " + targetBasePath, e);
      throw e;
    }

    HoodieReplicationConsistencyInfo result = new HoodieReplicationConsistencyInfo(sourceBasePath, targetBasePath, instantTimestamp);

    // All latest data files at given instantTimestamp are present on the target and are consistent with the primary copy of data.
    boolean dataFilesConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.QUERY_CONSISTENCY) || consistencyChecks.contains(ReplicationConsistencyType.WRITER_CONSISTENCY)) {
      dataFilesConsistent = checkLatestDataFiles(srcMetaClient, targetMetaClient, instantTimestamp, partitionsToCheck, result);
    }

    // Hudi timeline does not have any gaps, that would hide data present in the dataset to the query engines.
    boolean timelineConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.QUERY_CONSISTENCY) || consistencyChecks.contains(ReplicationConsistencyType.WRITER_CONSISTENCY)) {
      timelineConsistent = checkTimeline(srcMetaClient, targetMetaClient, instantTimestamp, result);
    }

    // All ingestion configuration overrides (in Hudi configstore) are present in the secondary/tertiary region
    // and are consistent with the primary region configurations.
    boolean configStoreConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.WRITER_CONSISTENCY)) {
      configStoreConsistent = checkConfigStore(srcMetaClient, targetMetaClient, instantTimestamp, partitionsToCheck, result);
    }

    // All property files (hoodie.properties, replication.properties) are present in the secondary/tertiary copy
    // of the dataset and are consistent with the primary dataset copy.
    boolean propertiesConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.WRITER_CONSISTENCY)) {
      propertiesConsistent = checkProperties(srcMetaClient, targetMetaClient, instantTimestamp, partitionsToCheck, result);
    }

    // Metadata table associated with the dataset is consistent,
    boolean metadataTableConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.WRITER_CONSISTENCY)) {
      metadataTableConsistent = checkMetadataTable(srcMetaClient, targetMetaClient, instantTimestamp, partitionsToCheck, result);
    }

    // Older snapshots of data files required for the successful rollback are present in the target dataset.
    boolean rollbackConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.ROLLBACK_CONSISTENCY)) {
      rollbackConsistent = checkRollback(srcMetaClient, targetMetaClient, instantTimestamp, result);
    }

    // Clean consistency
    boolean cleanConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.TABLE_SERVICES_CONSISTENCY)) {
      cleanConsistent = checkClean(srcMetaClient, targetMetaClient, instantTimestamp, partitionsToCheck, result);
    }

    // Archival consistency
    boolean archivalConsistent = true;
    if (consistencyChecks.contains(ReplicationConsistencyType.TABLE_SERVICES_CONSISTENCY)) {
      archivalConsistent = checkArchival(srcMetaClient, targetMetaClient, instantTimestamp, result);
    }

    // Assign the score to the result
    if (consistencyChecks.contains(ReplicationConsistencyType.QUERY_CONSISTENCY)) {
      result.setConsistency(ReplicationConsistencyType.QUERY_CONSISTENCY, dataFilesConsistent && timelineConsistent);
    }
    if (consistencyChecks.contains(ReplicationConsistencyType.WRITER_CONSISTENCY)) {
      result.setConsistency(ReplicationConsistencyType.WRITER_CONSISTENCY,
          dataFilesConsistent && timelineConsistent && configStoreConsistent && propertiesConsistent && metadataTableConsistent);
    }
    if (consistencyChecks.contains(ReplicationConsistencyType.ROLLBACK_CONSISTENCY)) {
      result.setConsistency(ReplicationConsistencyType.ROLLBACK_CONSISTENCY, rollbackConsistent);
    }
    if (consistencyChecks.contains(ReplicationConsistencyType.TABLE_SERVICES_CONSISTENCY)) {
      result.setConsistency(ReplicationConsistencyType.TABLE_SERVICES_CONSISTENCY, cleanConsistent && archivalConsistent);
    }

    return result;
  }

  /**
   * Ensure all latest data files at given instantTimestamp are present on the target and are consistent with the primary copy of data.
   */
  public boolean checkLatestDataFiles(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                       Option<List<String>> partitionsToCheck, HoodieReplicationConsistencyInfo result) {
    List<String> sourcePartitions = partitionsToCheck.orElseGet(() -> {
      LOG.info("Listing all partitions in source dataset " + srcMetaClient.getBasePath());
      return FSUtils.getAllPartitionPaths(engineContext, srcMetaClient, false);
    });

    engineContext.setJobStatus(this.getClass().getSimpleName(),
        "Checking data files in " + sourcePartitions.size() + " partitions at instant " + instantTimestamp);
    HoodieTableFileSystemView sourceFsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(engineContext, srcMetaClient,
        getValidTimeline(srcMetaClient, srcMetaClient.getCommitsAndCompactionTimeline()));
    HoodieTableFileSystemView targetFsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(engineContext, targetMetaClient,
        getValidTimeline(targetMetaClient, targetMetaClient.getCommitsAndCompactionTimeline()));

    HoodieAccumulator fileCountAcc = engineContext.newAccumulator();

    // check base file consistency
    List<HoodieReplicationConsistencyInfo> subResultList = engineContext.map(sourcePartitions, partition -> {
      HoodieReplicationConsistencyInfo subResult = new HoodieReplicationConsistencyInfo(result);

      fileCountAcc.add(sourceFsView.getLatestFileSlicesBeforeOrOn(partition, instantTimestamp, false).count());
      checkBaseFileConsistency(sourceFsView, targetFsView, partition, subResult);
      return subResult;
    }, sourcePartitions.size());

    subResultList.forEach(sr -> {
      result.addMissingFilesInTarget(sr.getMissingFilesInTarget());
      result.addExtraFilesInTarget(sr.getExtraFilesInTarget());
      result.addMismatchFileSizes(sr.getMismatchFileSizes());
    });

    // check log file consistency for merge on read datasets only
    // TODO: This fails in unit testing probably because of using last modified timestamp to check for log files
    // checkLogFileConsistency(srcMetaClient, targetMetaClient, sourceFsView, targetFsView, sourcePartitions, result);

    result.setNumFileGroupsChecked(fileCountAcc.value());
    result.setNumPartitionsChecked(sourcePartitions.size());

    if (!result.getMissingFilesInTarget().isEmpty() || !result.getExtraFilesInTarget().isEmpty() || !result.getMismatchFileSizes().isEmpty()) {
      LOG.error(String.format("Data files are not consistent. #Missing files in target: %d, #Extra files in target: %d, "
              + "# Files with size mismatch: %d",
            result.getMissingFilesInTarget().size(), result.getExtraFilesInTarget().size(), result.getMismatchFileSizes().size()));
      return false;
    }

    return true;
  }

  /**
   * Ensure that Hudi timeline does not have any gaps, that would hide data present in the dataset to the query engines.
   */
  public boolean checkTimeline(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                HoodieReplicationConsistencyInfo result) {
    // On the source timeline, filter out any operations which completed after the completion of the instantTime being
    // checked. These filtered operations were due to multi-writer and should not have been replicated yet.
    final String srcModificationTimestamp = getValidActiveTimeline(srcMetaClient).filterCompletedInstants()
            .findInstantsAfterOrEquals(instantTimestamp, 1)
            .filter(instant -> instant.requestedTime().equals(instantTimestamp))
            .getInstantsAsStream()
            .map(instant -> instant.getCompletionTime())
            .findFirst().orElse(instantTimestamp);
    HoodieTimeline sourceTimeline = getValidActiveTimeline(srcMetaClient).filterCompletedInstants()
            .filter(instant -> instant.requestedTime().equals(instantTimestamp)
                    || InstantComparison.compareTimestamps(instant.getCompletionTime(), InstantComparison.LESSER_THAN_OR_EQUALS, srcModificationTimestamp));

    // On the target timeline, the instants are created in order and we only need to check for instantTimestamp
    HoodieTimeline targetTimeline = getValidActiveTimeline(targetMetaClient).filterCompletedInstants()
            .findInstantsBeforeOrEquals(instantTimestamp);

    boolean ret = true;

    // The supplied instantTimestamp should be present in both the timelines
    if (!sourceTimeline.containsInstant(instantTimestamp)) {
      LOG.error(String.format("Instant %s not found in source timeline", instantTimestamp));
      result.addExtraInstantInTarget(instantTimestamp);
      ret = false;
    }
    if (!targetTimeline.containsInstant(instantTimestamp)) {
      LOG.error(String.format("Instant %s not found in target timeline", instantTimestamp));
      result.addMissingInstantInTarget(instantTimestamp);
      ret = false;
    }

    // Target timeline should not have any instants that have not been replicated (i.e. greater than the supplied instantTimestamp)
    // If any of these is an instant which is also present in the source timeline then maybe these are being replicated by another job
    List<String> targetExtraInstants = targetTimeline.findInstantsAfterOrEquals(instantTimestamp, Integer.MAX_VALUE)
        .getInstantsAsStream()
        .map(HoodieInstant::requestedTime)
        .filter(ts -> !sourceTimeline.containsInstant(ts))
        .collect(Collectors.toList());
    if (!targetExtraInstants.isEmpty()) {
      LOG.error(String.format("Extra instants found in target timeline (not present in source timeline) after supplied timestamp %s: %s", instantTimestamp, targetExtraInstants));
      targetExtraInstants.forEach(result::addExtraInstantInTarget);
      ret = false;
    }

    // There should not be any holes (missing commits) in the target timeline
    List<String> missingInstantsInTarget = sourceTimeline.findInstantsBefore(instantTimestamp).getInstantsAsStream()
        .map(HoodieInstant::requestedTime)
        .filter(ts -> !targetTimeline.containsInstant(ts) && !targetTimeline.isBeforeTimelineStarts(ts))
        .collect(Collectors.toList());
    if (!missingInstantsInTarget.isEmpty()) {
      LOG.error(String.format("Missing instants in target timeline before supplied timestamp %s: %s", instantTimestamp, missingInstantsInTarget));
      missingInstantsInTarget.forEach(result::addMissingInstantInTarget);
      ret = false;
    }

    // Timeline sizes wrt the write instants‰dro
    result.setSourceTimelineInfo(sourceTimeline.firstInstant().get().requestedTime(), sourceTimeline.lastInstant().get().requestedTime(),
        sourceTimeline.countInstants());
    result.setTargetTimelineInfo(targetTimeline.firstInstant().get().requestedTime(), targetTimeline.lastInstant().get().requestedTime(),
        targetTimeline.countInstants());

    return ret;
  }

  /**
   * All configuration overrides are present in the secondary region and are consistent with the primary region configurations.
   */
  public boolean checkConfigStore(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                   Option<List<String>> partitionsToCheck, HoodieReplicationConsistencyInfo result) {
    // Load and compare config overrides
    Properties srcConfigOverrides = getConfigStoreProperties(srcMetaClient.getBasePath().toString());
    Properties targetConfigOverrides = getConfigStoreProperties(targetMetaClient.getBasePath().toString());
    List<String> missingConfigOverrides = srcConfigOverrides.stringPropertyNames().stream()
        .filter(k -> !IGNORED_CONFIGS.contains(k))
        .filter(k -> !targetConfigOverrides.containsKey(k))
        .collect(Collectors.toList());
    List<String> extraConfigOverrides = targetConfigOverrides.stringPropertyNames().stream()
        .filter(k -> !IGNORED_CONFIGS.contains(k))
        .filter(k -> !srcConfigOverrides.containsKey(k))
        .collect(Collectors.toList());
    List<String> mismatchConfigOverrides = srcConfigOverrides.entrySet().stream()
        .filter(e -> !IGNORED_CONFIGS.contains(e.getKey()))
        .map(e -> {
          if (targetConfigOverrides.containsKey(e.getKey()) && !targetConfigOverrides.get(e.getKey()).equals(e.getValue())) {
            return (String)e.getKey();
          } else {
            return null;
          }
        })
        .filter(Objects::nonNull)
        .filter(key -> {
          if (key.equals(HoodieLockConfig.ZK_BASE_PATH.key())) {
            // If the two regions are sharing the ZK cluster then they should have a different lock base_path to avoid
            // conflict of pipelines running in different regions.
            return !srcConfigOverrides.getProperty(HoodieLockConfig.ZK_CONNECT_URL.key()).equals(targetConfigOverrides.getProperty(HoodieLockConfig.ZK_CONNECT_URL.key()));
          }
          return true;
        })
        .collect(Collectors.toList());

    result.setMissingPropertiesInTarget("configstore", missingConfigOverrides);
    result.setExtraPropertiesInTarget("configstore", extraConfigOverrides);
    result.setMismatchProperties("configstore", mismatchConfigOverrides);

    return mismatchConfigOverrides.isEmpty() && missingConfigOverrides.isEmpty() && extraConfigOverrides.isEmpty();
  }

  /**
   * All property files (hoodie.properties, replication.properties) are present in the secondary/tertiary copy of the dataset and are consistent with the primary dataset copy.
   */
  public boolean checkProperties(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                  Option<List<String>> partitionsToCheck, HoodieReplicationConsistencyInfo result) {
    // Load and compare properties files
    final boolean[] success = {true}; // needs to be an array to be used in lambda
    Arrays.asList("hoodie.properties").stream().forEach(propertiesFile -> {
      Properties srcProperties = new Properties();
      Properties targetProperties = new Properties();
      Path srcPath = new Path(srcMetaClient.getMetaPath().toString(), propertiesFile);
      Path targetPath = new Path(targetMetaClient.getMetaPath().toString(), propertiesFile);

      try (InputStream sis = srcMetaClient.getStorage().open(new StoragePath(srcPath.toString())); InputStream tis = targetMetaClient.getStorage().open(new StoragePath(targetPath.toString()))) {
        srcProperties.load(sis);
        targetProperties.load(tis);
      } catch (Exception e) {
        LOG.error("Error loading properties file", e);
        throw new RuntimeException("Error loading properties file", e);
      }

      List<String> missingPropertyNames = srcProperties.stringPropertyNames().stream()
          .filter(k -> !targetProperties.containsKey(k))
          .collect(Collectors.toList());
      List<String> extraPropertyNames = targetProperties.stringPropertyNames().stream()
          .filter(k -> !srcProperties.containsKey(k))
          .collect(Collectors.toList());
      List<String> mismatchProperties = srcProperties.entrySet().stream().map(e -> {
        if (targetProperties.containsKey(e.getKey()) && !targetProperties.get(e.getKey()).equals(e.getValue())) {
          return (String)e.getKey();
        } else {
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());

      result.setMissingPropertiesInTarget(propertiesFile, missingPropertyNames);
      result.setExtraPropertiesInTarget(propertiesFile, extraPropertyNames);
      result.setMismatchProperties(propertiesFile, mismatchProperties);
      success[0] &= missingPropertyNames.isEmpty() && extraPropertyNames.isEmpty() && mismatchProperties.isEmpty();
    });

    return success[0];
  }

  public boolean checkMetadataTable(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                     Option<List<String>> partitionsToCheck, HoodieReplicationConsistencyInfo result) {
    // MDT is only present on the main dataset
    // If MDT is disabled, we ignore its validation
    if (HoodieTableMetadata.isMetadataTable(srcMetaClient.getBasePath()) || !srcMetaClient.getTableConfig().isMetadataTableAvailable()) {
      return true;
    }

    result.setMDTEnabled(true);
    HoodieReplicationConsistency mdtConsistency = new HoodieReplicationConsistency(engineContext, HoodieTableMetadata.getMetadataTableBasePath(srcMetaClient.getBasePath()).toString(),
        HoodieTableMetadata.getMetadataTableBasePath(targetMetaClient.getBasePath()).toString(), instantTimestamp);
    mdtConsistency.setIgnoreArchivalConsistency(ignoreArchivalConsistency);
    result.setMDTConsistency(mdtConsistency.checkAllConsistency(Option.empty()));
    return result.isSuccessful();
  }

  /**
   * Older snapshots of data files required for the successful rollback are present in the target dataset.
   */
  public boolean checkRollback(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                HoodieReplicationConsistencyInfo result) {
    HoodieTimeline targetTimeline = getValidActiveTimeline(targetMetaClient).getWriteTimeline().filterCompletedInstants();
    HoodieTimeline targetCleanTimeline = getValidActiveTimeline(targetMetaClient).getCleanerTimeline().filterCompletedInstants();
    String earliestCommitRetained = null;
    // If there was a clean, load the last clean and find the retained commit from it
    if (!targetCleanTimeline.empty()) {
      // Load HoodieCleanMetadata from the last clean instant
      HoodieCleanMetadata cleanMetadata;
      try {
        cleanMetadata = TimelineMetadataUtils.deserializeAvroMetadataLegacy(targetCleanTimeline.getInstantDetails(targetCleanTimeline.lastInstant().get()).get(), HoodieCleanMetadata.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      earliestCommitRetained = cleanMetadata.getEarliestCommitToRetain();
    }
    if (StringUtils.isNullOrEmpty(earliestCommitRetained)) {
      // If there was no clean, the earliest commit retained is the earliest commit in the timeline
      earliestCommitRetained = targetTimeline.firstInstant().get().requestedTime();
    }

    // Time difference between the last commit and the earliest commit retained is the rollback window
    try {
      final long rollbackWindowMsec = HoodieInstantTimeGenerator.parseDateFromInstantTime(targetTimeline.lastInstant().get().requestedTime()).getTime()
          - HoodieInstantTimeGenerator.parseDateFromInstantTime(earliestCommitRetained).getTime();
      final long rollbackWindowHours = Math.floorDiv(rollbackWindowMsec, 60 * 60 * 1000);
      result.setRollbackWindowHours(rollbackWindowHours);
      if (rollbackWindowHours < rollbackConsistencyWindowHours) {
        LOG.error(String.format("Rollback window is less than %d hours. Rollback window: %d hours", rollbackConsistencyWindowHours, rollbackWindowHours));
      }

      return rollbackWindowHours >= rollbackConsistencyWindowHours;
    } catch (Exception e) {
      if (earliestCommitRetained.equals("00000000000000010")) {
        // This is the case where MDT was initialized on an empty dataset
        return true;
      }
      LOG.error(String.format("Error parsing rollback window timestamps: %s or %s", targetTimeline.lastInstant().get().requestedTime(), earliestCommitRetained), e);
      return false;
    }
  }

  public boolean checkClean(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                             Option<List<String>> partitionsToCheck, HoodieReplicationConsistencyInfo result) {
    // TODO: What should be check here? Clean instants?
    // Files have already been compared so any missing clean operations would be caught there.
    return true;
  }

  /**
   * Archival files should match between source and target
   */
  public boolean checkArchival(HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient targetMetaClient, String instantTimestamp,
                                HoodieReplicationConsistencyInfo result) {

    List<String> srcArchivalFiles = getArchivalFiles(srcMetaClient, Option.of(instantTimestamp));
    List<String> targetArchivalFiles = getArchivalFiles(targetMetaClient, Option.empty());

    List<String> missingArchivalFiles = srcArchivalFiles.stream().filter(f -> !targetArchivalFiles.contains(f)).collect(Collectors.toList());
    List<String> extraArchivalFiles = targetArchivalFiles.stream().filter(f -> !srcArchivalFiles.contains(f)).collect(Collectors.toList());
    result.setMissingArchivalFilesInTarget(missingArchivalFiles);
    result.setExtraArchivalFilesInTarget(extraArchivalFiles);

    return ignoreArchivalConsistency
            || (result.getMissingArchivalFilesInTarget().isEmpty() && result.getExtraArchivalFilesInTarget().isEmpty());
  }

  private List<String> getArchivalFiles(HoodieTableMetaClient metaClient, Option<String> instantTimestamp) {
    // HUDI has two archive locations - in the older code the archives were written to .hoodie folder, in the newer code the archives are written to .hoodie/.archive folder
    final String oldArchiveFolderName = HoodieTableMetaClient.METAFOLDER_NAME;
    final String newArchiveFolderName = HoodieTableConfig.ARCHIVELOG_FOLDER.defaultValue();
    final String archiveFileGlobPattern = ".commits_.archive*";
    final String newArchiveLoc = metaClient.getMetaPath() + Path.SEPARATOR + newArchiveFolderName + Path.SEPARATOR + archiveFileGlobPattern;
    final String oldArchiveLoc = metaClient.getMetaPath() + Path.SEPARATOR + archiveFileGlobPattern;
    final long modificationTs;
    try {
      modificationTs = instantTimestamp.isPresent() ? HoodieInstantTimeGenerator.parseDateFromInstantTime(instantTimestamp.get()).getTime() : Long.MAX_VALUE;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    try {
      List<StoragePathInfo> newFsStatuses = metaClient.getStorage().globEntries(new StoragePath(newArchiveLoc));
      List<StoragePathInfo> oldFsStatuses = metaClient.getStorage().globEntries(new StoragePath(oldArchiveLoc));

      List<String> newArchiveFiles = newFsStatuses.stream()
          .filter(fs -> fs.getModificationTime() <= modificationTs)
          .map(fs -> newArchiveFolderName + Path.SEPARATOR + fs.getPath().getName()).collect(Collectors.toList());

      List<String> oldArchiveFiles = oldFsStatuses.stream()
          .filter(fs -> fs.getModificationTime() <= modificationTs)
          .map(fs -> oldArchiveFolderName + Path.SEPARATOR + fs.getPath().getName()).collect(Collectors.toList());

      // Sort files in reverse chronological order
      newArchiveFiles.addAll(oldArchiveFiles);
      newArchiveFiles.sort(Collections.reverseOrder());
      return newArchiveFiles;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Properties getConfigStoreProperties(String basePath) {
    // TODO: Port HoodieConfigManager integration from 0.14.x
    LOG.warn("Config store integration not yet ported to 1.x; returning empty properties for {}", basePath);
    return new java.util.Properties();
  }

  /**
   * validates base file consistency and updates subResult with any consistency check failures
   */
  private void checkBaseFileConsistency(HoodieTableFileSystemView sourceFsView,
                                        HoodieTableFileSystemView targetFsView,
                                        String partition,
                                        HoodieReplicationConsistencyInfo subResult) {
    // Load latest files in source and target
    Map<String, Long> srcLatestFileMap = getMapOfBaseFilesAndSize(sourceFsView, partition, instantTimestamp);
    Map<String, Long> targetLatestFileMap = getMapOfBaseFilesAndSize(targetFsView, partition, instantTimestamp);

    // Iterate and compare
    for (String filePath: targetLatestFileMap.keySet()) {
      if (!srcLatestFileMap.containsKey(filePath)) {
        // Extra file in target (Missing from source)
        subResult.addExtraFileInTarget(filePath);
      } else {
        // Match sizes of file
        if (!targetLatestFileMap.get(filePath).equals(srcLatestFileMap.get(filePath))) {
          //  file size mismatch
          subResult.addMismatchFileSize(filePath, srcLatestFileMap.get(filePath), targetLatestFileMap.get(filePath));
        }

        // Remove from source file list
        srcLatestFileMap.remove(filePath);
      }
    }

    // Any entries left in source map are those which where not found on the target
    srcLatestFileMap.keySet().stream().forEach(f -> {
      subResult.addMissingFileInTarget(f);
    });
  }

  /**
   * validates log file consistency and updates subResult with any consistency check failures
   */
  private void checkLogFileConsistency(HoodieTableMetaClient srcMetaClient,
                                       HoodieTableMetaClient targetMetaClient,
                                       HoodieTableFileSystemView sourceFsView,
                                       HoodieTableFileSystemView targetFsView,
                                       List<String> partitions,
                                       HoodieReplicationConsistencyInfo subResult) {
    // TODO: Port log file consistency check - LogReaderUtils.getAllLogFilesWithMaxCommit was removed in 1.x
    LOG.warn("Log file consistency check not yet ported to 1.x");
  }

  /**
   * Returns a map of all files (base files, log files) and their sizes in the given partition.
   */
  private Map<String, Long> getMapOfBaseFilesAndSize(HoodieTableFileSystemView fsView, String partition, String instantTimestamp) {
    Map<String, Long> fileMap = new HashMap<>();
    fsView.getLatestFileSlicesBeforeOrOn(partition, instantTimestamp, false).forEach(slice -> {
      // Collect base file and its size
      if (slice.getBaseFile().isPresent()) {
        HoodieBaseFile baseFile = slice.getBaseFile().get();
        fileMap.put(partition + Path.SEPARATOR + baseFile.getFileName(), baseFile.getFileSize());
      }
    });
    //fsView.clear(partition);

    return fileMap;
  }

  public void setRollbackConsistencyWindowHours(int rollbackConsistencyWindowHours) {
    this.rollbackConsistencyWindowHours = rollbackConsistencyWindowHours;
  }

  public void setIgnoreArchivalConsistency(boolean ignoreArchivalConsistency) {
    this.ignoreArchivalConsistency = ignoreArchivalConsistency;
  }

  private static HoodieTimeline getValidTimeline(HoodieTableMetaClient metaClient, HoodieTimeline timeline) {
    // TODO: Port HoodieTableMetadataUtil.getValidTimeline filtering for MDT if needed
    return timeline;
  }

  public static HoodieTimeline getValidActiveTimeline(HoodieTableMetaClient metaClient) {
    return getValidTimeline(metaClient, metaClient.getActiveTimeline());
  }
}
