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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HoodieReplicationConsistencyInfo implements Serializable {
  // Source and target base paths
  private final String sourceBasePath;
  private final String targetBasePath;

  // The instant time (e.g. LRT) that was used for the consistency check
  private final String instantTime;

  // Consistency scores for each type of consistency
  private final Map<HoodieReplicationConsistency.ReplicationConsistencyType, Boolean> consistencyScores = new HashMap<>();

  // List of files that were found missing or extra in the target dataset
  private final List<String> extraFilesInTarget = new LinkedList<>();
  private final List<String> missingFilesInTarget = new LinkedList<>();

  // List of files whose sizes no not match
  private final Map<String, Pair<Long, Long>> mismatchFileSizes = new HashMap<>();

  // List of instant timestamps that were found missing or extra in the target dataset
  private final List<String> extraInstantsInTarget = new LinkedList<>();
  private final List<String> missingInstantsInTarget = new LinkedList<>();
  private final Map<String, List<String>> missingPropertyNames = new HashMap<>();
  private final Map<String, List<String>> extraPropertyNames = new HashMap<>();
  private final Map<String, List<String>> mismatchPropertyNames = new HashMap<>();
  private long rollbackWindowHours = 0;
  private long numFileGroupsChecked = 0;
  private int numPartitionsChecked = 0;
  private List<String> missingArchivalFilesInTarget = Collections.emptyList();
  private List<String> extraArchivalFilesInTarget = Collections.emptyList();
  private HoodieReplicationConsistencyInfo mdtConsistency;
  private String sourceTimelineFirstInstant;
  private String sourceTimelineLastInstant;
  private int sourceTimelineInstantCount;
  private String targetTimelineFirstInstant;
  private String targetTimelineLastInstant;
  private int targetTimelineInstantCount;
  private boolean isMDTEnabled = false;
  private String replicationDestination;

  // Limit the number of reported files to prevent OOM due to large file differences
  private final int maxReportedFiles = 250;

  public HoodieReplicationConsistencyInfo(String sourceBasePath, String targetBasePath, String instantTime) {
    this.sourceBasePath = sourceBasePath;
    this.targetBasePath = targetBasePath;
    this.instantTime = instantTime;
  }

  public HoodieReplicationConsistencyInfo(HoodieReplicationConsistencyInfo result) {
    this(result.sourceBasePath, result.targetBasePath, result.instantTime);
  }

  public String getSourceBasePath() {
    return sourceBasePath;
  }

  public String getTargetBasePath() {
    return targetBasePath;
  }

  public String getInstantTime() {
    return instantTime;
  }

  /**
   * Returns the result of a specific consistency check.
   * @param type The type of consistency check
   * @return An option which has the value true if the consistency check passed, false if it failed, and empty if the check was not performed.
   */
  public Option<Boolean> isConsistent(HoodieReplicationConsistency.ReplicationConsistencyType type) {
    return Option.ofNullable(consistencyScores.getOrDefault(type, null));
  }

  public void setConsistency(HoodieReplicationConsistency.ReplicationConsistencyType type, boolean consistency) {
    consistencyScores.put(type, consistency);
  }

  public void addExtraFilesInTarget(List<String> extraFiles) {
    if (extraFilesInTarget.size() < maxReportedFiles) {
      extraFilesInTarget.addAll(extraFiles);
    }
  }

  public void addExtraFileInTarget(String extraFile) {
    if (extraFilesInTarget.size() < maxReportedFiles && !extraFilesInTarget.contains(extraFile)) {
      extraFilesInTarget.add(extraFile);
    }
  }

  public void addMissingFilesInTarget(List<String> missingFiles) {
    if (missingFilesInTarget.size() < maxReportedFiles) {
      missingFilesInTarget.addAll(missingFiles);
    }
  }

  public void addMissingFileInTarget(String missingFile) {
    if (missingFilesInTarget.size() < maxReportedFiles && !missingFilesInTarget.contains(missingFile)) {
      missingFilesInTarget.add(missingFile);
    }
  }

  public void addExtraInstantInTarget(String extraInstant) {
    if (!extraInstantsInTarget.contains(extraInstant)) {
      extraInstantsInTarget.add(extraInstant);
    }
  }

  public void addMissingInstantInTarget(String missingInstant) {
    if (!missingInstantsInTarget.contains(missingInstant)) {
      missingInstantsInTarget.add(missingInstant);
    }
  }

  public List<String> getMissingFilesInTarget() {
    return missingFilesInTarget;
  }

  public List<String> getExtraFilesInTarget() {
    return extraFilesInTarget;
  }

  public List<String> getMissingInstantsInTarget() {
    return missingInstantsInTarget;
  }

  public List<String> getExtraInstantsInTarget() {
    return extraInstantsInTarget;
  }

  public boolean isSuccessful() {
    return consistencyScores.values().stream().allMatch(Boolean::booleanValue)
        && (mdtConsistency == null || mdtConsistency.isSuccessful());
  }

  public void setMissingPropertiesInTarget(String propertiesFile, List<String> missingPropertyNames) {
    this.missingPropertyNames.put(propertiesFile, missingPropertyNames);
  }

  public void setExtraPropertiesInTarget(String propertiesFile, List<String> extraPropertyNames) {
    this.extraPropertyNames.put(propertiesFile, extraPropertyNames);
  }

  public void setMismatchProperties(String propertiesFile, List<String> mismatchPropertyNames) {
    this.mismatchPropertyNames.put(propertiesFile, mismatchPropertyNames);
  }

  public Map<String, List<String>> getMissingPropertiesInTarget() {
    return missingPropertyNames;
  }

  public Map<String, List<String>> getExtraPropertiesInTarget() {
    return extraPropertyNames;
  }

  public Map<String, List<String>> getMismatchProperties() {
    return mismatchPropertyNames;
  }

  public void setRollbackWindowHours(long rollbackWindowHours) {
    this.rollbackWindowHours = rollbackWindowHours;
  }

  public long getRollbackWindowHours() {
    return rollbackWindowHours;
  }

  public void setNumFileGroupsChecked(long count) {
    this.numFileGroupsChecked = count;
  }

  public void setNumPartitionsChecked(int count) {
    this.numPartitionsChecked = count;
  }

  public long getNumFileGroupsChecked() {
    return numFileGroupsChecked;
  }

  public int getNumPartitionsChecked() {
    return numPartitionsChecked;
  }

  public void setMissingArchivalFilesInTarget(List<String> missingArchivalFiles) {
    this.missingArchivalFilesInTarget = missingArchivalFiles;
  }

  public List<String> getMissingArchivalFilesInTarget() {
    return missingArchivalFilesInTarget;
  }

  public void setExtraArchivalFilesInTarget(List<String> extraArchivalFiles) {
    this.extraArchivalFilesInTarget = extraArchivalFiles;
  }

  public List<String> getExtraArchivalFilesInTarget() {
    return extraArchivalFilesInTarget;
  }

  public void setMDTConsistency(HoodieReplicationConsistencyInfo hoodieReplicationConsistencyInfo) {
    this.mdtConsistency = hoodieReplicationConsistencyInfo;
  }

  public HoodieReplicationConsistencyInfo getMDTConsistency() {
    return mdtConsistency;
  }

  public void setSourceTimelineInfo(String firstTimestamp, String lastTimestamp, int instantCount) {
    this.sourceTimelineFirstInstant = firstTimestamp;
    this.sourceTimelineLastInstant = lastTimestamp;
    this.sourceTimelineInstantCount = instantCount;
  }

  public void setTargetTimelineInfo(String firstTimestamp, String lastTimestamp, int instantCount) {
    this.targetTimelineFirstInstant = firstTimestamp;
    this.targetTimelineLastInstant = lastTimestamp;
    this.targetTimelineInstantCount = instantCount;
  }

  public String getSourceTimelineFirstInstant() {
    return sourceTimelineFirstInstant;
  }

  public String getSourceTimelineLastInstant() {
    return sourceTimelineLastInstant;
  }

  public int getSourceTimelineInstantCount() {
    return sourceTimelineInstantCount;
  }

  public String getTargetTimelineFirstInstant() {
    return targetTimelineFirstInstant;
  }

  public String getTargetTimelineLastInstant() {
    return targetTimelineLastInstant;
  }

  public int getTargetTimelineInstantCount() {
    return targetTimelineInstantCount;
  }

  public void setMDTEnabled(boolean enabled) {
    this.isMDTEnabled = enabled;
  }

  public boolean isMDTEnabled() {
    return isMDTEnabled;
  }

  public void addMismatchFileSize(String filePath, long sizeOnSource, long sizeOnTarget) {
    mismatchFileSizes.put(filePath, Pair.of(sizeOnSource, sizeOnTarget));
  }

  public Map<String, Pair<Long, Long>> getMismatchFileSizes() {
    return mismatchFileSizes;
  }

  public void addMismatchFileSizes(Map<String, Pair<Long, Long>> mismatchFileSizes) {
    this.mismatchFileSizes.putAll(mismatchFileSizes);
  }

  public void setReplicationDestination(String destination) {
    this.replicationDestination = destination;
  }

  public String getReplicationDestination() {
    return replicationDestination;
  }

  public LinkedHashMap<String, String> toCsv() {
    LinkedHashMap<String, String> csv = new LinkedHashMap<>();
    csv.put("src_base_path", sourceBasePath);
    csv.put("tgt_base_path", targetBasePath);
    csv.put("instant_time", instantTime);
    csv.put("destination", StringUtils.isNullOrEmpty(replicationDestination) ? "unknown" : replicationDestination);
    Arrays.stream(HoodieReplicationConsistency.ReplicationConsistencyType.values()).forEach(type -> {
      Option<Boolean> consistencyResult = isConsistent(type);
      if (consistencyResult.isPresent()) {
        csv.put(type.toString().toLowerCase(), (consistencyResult.get() ? "pass" : "fail"));
      } else {
        csv.put(type.toString().toLowerCase(), "ignore");
      }
    });
    csv.put("total_partitions", String.valueOf(numPartitionsChecked));
    csv.put("total_file_groups", String.valueOf(numFileGroupsChecked));
    csv.put("extra_files_in_target", String.join(" ", extraFilesInTarget));
    csv.put("missing_files_in_target", String.join(" ", missingFilesInTarget));
    final String[] mismatchFileSizesStr = {""};
    mismatchFileSizes.forEach((path, sizePair) -> {
      mismatchFileSizesStr[0] += String.format("%s(src=%d tgt=%d) ", path, sizePair.getLeft(), sizePair.getRight());
    });
    csv.put("mismatch_file_sizes", mismatchFileSizesStr[0]);
    csv.put("exta_instants_in_target", String.join(" ", extraInstantsInTarget));
    csv.put("missing_instants_in_target", String.join(" ", missingInstantsInTarget));

    final String[] missingPropertiesStr = {""};
    missingPropertyNames.forEach((key, value) -> {
      if (!value.isEmpty()) {
        missingPropertiesStr[0] += String.format("%s: %s ", key, String.join(" ", value));
      }
    });
    csv.put("properties_missing_in_target", missingPropertiesStr[0]);

    final String[] extraPropertiesStr = {""};
    extraPropertyNames.forEach((key, value) -> {
      if (!value.isEmpty()) {
        extraPropertiesStr[0] += String.format("%s: %s ", key, String.join(" ", value));
      }
    });
    csv.put("properties_extra_in_target", extraPropertiesStr[0]);

    final String[] mismatchPropertiesStr = {""};
    mismatchPropertyNames.forEach((key, value) -> {
      if (!value.isEmpty()) {
        mismatchPropertiesStr[0] += String.format("%s: %s ", key, String.join(" ", value));
      }
    });
    csv.put("properties_mismatch_in_target", mismatchPropertiesStr[0]);

    csv.put("rollback_window", String.valueOf(rollbackWindowHours));
    csv.put("missing_archival_files_in_target", String.join(" ", missingArchivalFilesInTarget));
    csv.put("extra_archival_files_in_target", String.join(" ", extraArchivalFilesInTarget));

    csv.put("src_timeline_start", sourceTimelineFirstInstant);
    csv.put("target_timeline_start", targetTimelineFirstInstant);
    csv.put("src_timeline_end", sourceTimelineLastInstant);
    csv.put("target_timeline_end", targetTimelineLastInstant);
    csv.put("src_timeline_instant_count", String.valueOf(sourceTimelineInstantCount));
    csv.put("target_timeline_instant_count", String.valueOf(targetTimelineInstantCount));

    csv.put("mdt_enabled", String.valueOf(isMDTEnabled));
    if (mdtConsistency == null) {
      csv.put("mdt_consistent", "ignore");
    } else {
      csv.put("mdt_consistent", mdtConsistency.isSuccessful() ? "pass" : "fail");
    }

    return csv;
  }
}
