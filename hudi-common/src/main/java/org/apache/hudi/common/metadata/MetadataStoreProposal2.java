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

package org.apache.hudi.common.metadata;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCommitMetadata;
import org.apache.hudi.avro.model.HoodieCompactionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieMetadataException;

/**
 *
 */
public class MetadataStoreProposal2 extends MetadataStore {
  private Path masterManifestPath;
  private Path deltaManifestPath;
  private Index masterManifest;
  private Index deltaManifest;
  protected String temporaryPath;
  protected FileSystem fs;
  private ExternalSpillableMap<String, String> newFilesMap;
  private Set<String> deletedFiles;
  private boolean readyForUpdate = false;
  private int maxFileIdInMasterManifest = -1;
  private int maxFileId = -1;
  private Map<String, Index> columnIndex = new HashMap<>();

  private static final int INDEX_VERSION = 2; //TODO
  private static final int MASTER_MANIFEST_VERSION = INDEX_VERSION; //TODO
  private static final int DELTA_MANIFEST_VERSION = INDEX_VERSION; //TODO
  private static final int PARTITION_INDEX_VERSION = INDEX_VERSION; //TODO
  private static final String PARTITION_INDEX_NAME = "_partition_";

  private static long MaxInMemorySizeInBytes = 1024 * 1024 * 1024;
  private static String KEY_ALL = "_all";
  private static String DELETE_FILE_SENTINAL = "_del_"; // any non-valid file path will work
  private static String KEY_MAXFILE_ID = "_maxfileid";
  private static String KEY_FILE_COUNT = "_filecount";
  private static String KEY_DELETED_FILE_COUNT = "_delfilecount";
  private static String KEY_PARTITION_COUNT = "_partitioncount";
  private static byte[] KEY_END = {(byte) 127};
  private static byte[] KEY_DELETED_FILES = {(byte) 127, (byte)1};
  private static byte[] KEY_ALL_PARTITIONS = {(byte) 127, (byte)2};

  public MetadataStoreProposal2(String basePath, Configuration conf) throws IOException {
    super(basePath, conf);
    this.temporaryPath = "/tmp";
    this.fs = FSUtils.getFs(metadataPath.toString(), configuration);
  }

  @Override
  public void create(String instantTime) throws IOException {
    // List all partitions in the basePath
    FileSystem baseFs = FSUtils.getFs(basePath, configuration);
    List<String> partitions = FSUtils.getAllFoldersWithPartitionMetaFile(baseFs, basePath);
    logger.info("Found " + partitions.size() + " partitions at instantTime " + instantTime);

    int[] fileCount = {0};
    partitions.stream().sorted().forEach(partition -> {
      // List all files in the partition
      try {
        FileStatus[] statuses = baseFs.listStatus(new Path(basePath, partition));
        for (FileStatus status : statuses) {
          if (status.isFile()) {
            String filePath = status.getPath().toString();
            add(getRelativePath(filePath));
            ++fileCount[0];
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to list files in partition " + partition + ": " + e);
      }
    });

    logger.info("Committing " + fileCount[0] + " files to metadata");
    commit(instantTime);
  }

  @Override
  public boolean init() throws Exception {
    // Find the latest master manifest
    FileStatus[] statuses = fs.globStatus(new Path(metadataPath + Path.SEPARATOR + "*.master_manifest"));
    if (statuses.length != 0) {
      // statues are sorted by file name. Pick the latest one.
      masterManifestPath = statuses[statuses.length - 1].getPath();

      // Find the latest delta manifest for the above master manifest
      statuses = fs.globStatus(new Path(metadataPath + Path.SEPARATOR + "*.delta_manifest"));
      String masterCommitTime = getCommitTimesFromPath(masterManifestPath)[0];
      // statues are sorted by file name. Pick the latest one that matches the master manifest found.
      for (int idx = statuses.length - 1; idx >= 0; --idx) {
        String deltaCommitTime = getCommitTimesFromPath(statuses[idx].getPath())[1];
        int compare = deltaCommitTime.compareTo(masterCommitTime);
        if (compare == 0) {
          deltaManifestPath = statuses[idx].getPath();
          break;
        } else if (compare < 0) {
          // Master manifest is newer so there is no delta manifest.
          break;
        } else {
          // Delta manifest refers to an older master manifest
          throw new HoodieMetadataException("Delta Manifest " + statuses[idx].getPath()
                                            + " does not match latest Master Manifest " + masterManifestPath);
        }
      }

      logger.info("Initializing MetadatStore for " + metadataPath + " with manifests " + masterManifestPath + " / "
          + deltaManifestPath == null ? "none" : deltaManifestPath);
    }

    if (masterManifestPath == null) {
      return false;
    }

    if (masterManifestPath != null) {
      masterManifest = getIndexFromPath(masterManifestPath);
      maxFileId = Integer.parseInt(masterManifest.getMetadata().get(KEY_MAXFILE_ID));
      maxFileIdInMasterManifest = maxFileId;
    }
    if (deltaManifestPath != null) {
      deltaManifest = getIndexFromPath(deltaManifestPath);
      maxFileId = Integer.parseInt(deltaManifest.getMetadata().get(KEY_MAXFILE_ID));
    }

    return true;
  }

  @Override
  public void commit(String instantTime) throws IOException {
    if (masterManifestPath == null) {
      createMasterManifest(instantTime);
    } else if (deltaManifestPath == null) {
      createDeltaManifest(instantTime);
    } else {
      updateDeltaManifest(instantTime);
    }
  }

  @Override
  public void merge(String string) throws Exception {
    // TODO Auto-generated method stub

  }

  private void createMasterManifest(String instantTime) throws IOException {
    // Assign fileID to each file and create a list of partition to fileIDs
    int[] fileIdCounter = {0};
    String tmpPath = temporaryPath + File.separator + System.currentTimeMillis() + ".fileIdToPathMap";
    ExternalSpillableMap<Integer, String> fileIdToPathMap = new ExternalSpillableMap<Integer, String>(
        (long)(MaxInMemorySizeInBytes * 0.9), tmpPath,
        new DefaultSizeEstimator<Integer>(), new DefaultSizeEstimator<String>());
    Map<String, List<Integer>> partitionToFileIds = new HashMap<>();
    newFilesMap.keySet().stream().sorted().forEach(fileName -> {
      String partition = newFilesMap.get(fileName);
      assert partition != null;
      Integer fileId = ++fileIdCounter[0];

      if (!partitionToFileIds.containsKey(partition)) {
        partitionToFileIds.put(partition, new LinkedList<Integer>());
      }
      partitionToFileIds.get(partition).add(fileId);
      String filePath = partition + File.separator + fileName;
      fileIdToPathMap.put(fileId, filePath);
    });

    masterManifestPath = makeMasterManifestPath(instantTime, MASTER_MANIFEST_VERSION);
    maxFileId = fileIdCounter[0];
    maxFileIdInMasterManifest = maxFileId;

    createPartitionIndex(partitionToFileIds);

    Map<String, String> metadata = new HashMap<>();
    metadata.put(KEY_MAXFILE_ID, String.valueOf(maxFileId));
    metadata.put(KEY_FILE_COUNT, String.valueOf(maxFileId));
    metadata.put(KEY_PARTITION_COUNT, String.valueOf(partitionToFileIds.size()));

    final Index index = getIndex(INDEX_VERSION, masterManifestPath);
    index.create(Option.of(metadata));

    fileIdToPathMap.keySet().stream().sorted().forEach(fileId -> {
      String filePath = fileIdToPathMap.get(fileId);
      try {
        index.put(fileId, filePath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Could not write to index " + masterManifestPath + ": " + e);
      }
    });

    index.commit();
    masterManifest = getIndexFromPath(masterManifestPath);
    newFilesMap.clear();
    columnIndex.clear();

    logger.info("Created a master manifest at " + masterManifestPath);
  }

  private void createDeltaManifest(String instantTime) throws IOException {
    // Assign fileID to each file and create a list of partition to fileIDs
    int[] fileIdCounter = {maxFileId};
    String tmpPath = temporaryPath + File.separator + System.currentTimeMillis() + ".fileIdToPathMap";
    Set<String> deltaDeletedFiles = new HashSet<>();
    ExternalSpillableMap<Integer, String> fileIdToPathMap = new ExternalSpillableMap<Integer, String>(
        (long)(MaxInMemorySizeInBytes * 0.9), tmpPath,
        new DefaultSizeEstimator<Integer>(), new DefaultSizeEstimator<String>());
    Map<String, List<Integer>> partitionToFileIds = new HashMap<>();
    newFilesMap.keySet().stream().sorted().forEach(fileName -> {
      String partition = newFilesMap.get(fileName);
      if (partition.equals(DELETE_FILE_SENTINAL)) {
        deltaDeletedFiles.add(fileName);
      } else {
        Integer fileId = ++fileIdCounter[0];

        if (!partitionToFileIds.containsKey(partition)) {
          partitionToFileIds.put(partition, new LinkedList<Integer>());
        }
        partitionToFileIds.get(partition).add(fileId);
        String filePath = partition + File.separator + fileName;
        fileIdToPathMap.put(fileId, filePath);
      }
    });

    int fileCount = fileIdCounter[0] - maxFileId - deltaDeletedFiles.size();
    maxFileId = fileIdCounter[0];

    Map<String, String> metadata = new HashMap<>();
    metadata.put(KEY_MAXFILE_ID, String.valueOf(maxFileId));
    metadata.put(KEY_FILE_COUNT, String.valueOf(fileCount));
    metadata.put(KEY_DELETED_FILE_COUNT, String.valueOf(deltaDeletedFiles.size()));
    metadata.put(KEY_PARTITION_COUNT, String.valueOf(partitionToFileIds.size()));

    deltaManifestPath = makeDeltaManifestPath(instantTime, getCommitTimesFromPath(masterManifestPath)[0],
        DELTA_MANIFEST_VERSION);
    final Index index = getIndex(INDEX_VERSION, deltaManifestPath);
    index.create(Option.of(metadata));

    fileIdToPathMap.keySet().stream().sorted().forEach(fileId -> {
      String filePath = fileIdToPathMap.get(fileId);
      try {
        index.put(fileId, filePath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Could not write to index " + deltaManifestPath + ": " + e);
      }
    });

    // Deleted files
    String allDeletedFiles = deltaDeletedFiles.stream().collect(Collectors.joining(","));
    index.put(KEY_DELETED_FILES, allDeletedFiles);

    // Newly added partitions
    String allPartitions = partitionToFileIds.keySet().stream().collect(Collectors.joining(","));
    //LOG.info("...... " + Arrays.toString(KEY_ALL_PARTITIONS));
    index.put(KEY_ALL_PARTITIONS, allPartitions);

    // New partition mappings
    partitionToFileIds.keySet().stream().sorted().forEach(partition -> {
      try {
        //LOG.info("...... " + Arrays.toString(formPrefixKey(KEY_ALL_PARTITIONS, partition)));
        index.put(formPrefixKey(KEY_ALL_PARTITIONS, partition),
            partitionToFileIds.get(partition).stream().sorted().collect(Collectors.toList()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    index.commit();
    deltaManifest = getIndexFromPath(deltaManifestPath);
    newFilesMap.clear();
    deletedFiles = null;

    logger.info("Created a delta manifest at " + deltaManifestPath);
  }

  private void updateDeltaManifest(String instantTime) throws IOException {
    // Load existing fileID mappings
    Map<String, Integer> existingFileIds = new HashMap<>();
    scanFiles(maxFileIdInMasterManifest + 1, (fileId, path) -> {
      existingFileIds.put(path, fileId);
      Path p = new Path(path);
      if (!newFilesMap.containsKey(p.getName().toString())) {
        newFilesMap.put(p.getName().toString(), p.getParent().toString());
        //LOG.info("exisitnFileIds: " + p.getName().toString() + " " + p.getParent().toString());
      }
      return true;
    });

    // Load deleted files
    loadDeletedFiles();

    // Load partition to file ID mappings
    Map<String, Set<Integer>> partitionToFileIds = new HashMap<>();
    deltaManifest.scan(KEY_ALL_PARTITIONS, (b1, b2) -> {
      if (Arrays.equals(b1, KEY_ALL_PARTITIONS)) {
        return true;
      }

      assert isPrefixKey(KEY_ALL_PARTITIONS, b1);
      String partition = new String(b1, KEY_ALL_PARTITIONS.length, b1.length - KEY_ALL_PARTITIONS.length);
      Set<Integer> fileIDs = new HashSet<>(Index.decodeAsListOfInteger(b2));
      partitionToFileIds.put(partition, fileIDs);
      return true;
    });

    // Assign fileID to each file and create a list of partition to fileIDs
    int[] fileIdCounter = {maxFileId};
    String tmpPath = temporaryPath + File.separator + System.currentTimeMillis() + ".fileIdToPathMap";
    ExternalSpillableMap<Integer, String> fileIdToPathMap = new ExternalSpillableMap<Integer, String>(
        (long)(MaxInMemorySizeInBytes * 0.9), tmpPath,
        new DefaultSizeEstimator<Integer>(), new DefaultSizeEstimator<String>());

    newFilesMap.keySet().stream().sorted().forEach(fileName -> {
      String partition = newFilesMap.get(fileName);
      if (partition.equals(DELETE_FILE_SENTINAL)) {
        deletedFiles.add(fileName);
      } else {
        Integer fileId = null;
        String filePath = partition + File.separator + fileName;
        if (existingFileIds.containsKey(filePath)) {
          fileId = existingFileIds.get(filePath);
        } else {
          // New file. Map it to partition and
          fileId = ++fileIdCounter[0];

          if (!partitionToFileIds.containsKey(partition)) {
            partitionToFileIds.put(partition, new HashSet<>());
          }
          partitionToFileIds.get(partition).add(fileId);
        }

        fileIdToPathMap.put(fileId, filePath);
      }
    });

    int fileCount = fileIdCounter[0] - maxFileIdInMasterManifest + 1 - deletedFiles.size();
    maxFileId = fileIdCounter[0];

    Map<String, String> metadata = new HashMap<>();
    metadata.put(KEY_MAXFILE_ID, String.valueOf(maxFileId));
    metadata.put(KEY_FILE_COUNT, String.valueOf(fileCount));
    metadata.put(KEY_DELETED_FILE_COUNT, String.valueOf(deletedFiles.size()));
    metadata.put(KEY_PARTITION_COUNT, String.valueOf(partitionToFileIds.size()));

    deltaManifestPath = makeDeltaManifestPath(instantTime, getCommitTimesFromPath(masterManifestPath)[0],
        DELTA_MANIFEST_VERSION);
    final Index index = getIndex(INDEX_VERSION, deltaManifestPath);
    index.create(Option.of(metadata));

    fileIdToPathMap.keySet().stream().sorted().forEach(fileId -> {
      String filePath = fileIdToPathMap.get(fileId);
      try {
        index.put(fileId, filePath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Could not write to index " + deltaManifestPath + ": " + e);
      }
    });

    // Deleted files
    String allDeletedFiles = deletedFiles.stream().collect(Collectors.joining(","));
    index.put(KEY_DELETED_FILES, allDeletedFiles);

    // Newly added partitions
    String allPartitions = partitionToFileIds.keySet().stream().collect(Collectors.joining(","));
    //LOG.info("...... " + Arrays.toString(KEY_ALL_PARTITIONS));
    index.put(KEY_ALL_PARTITIONS, allPartitions);

    // New partition mappings
    partitionToFileIds.keySet().stream().sorted().forEach(partition -> {
      try {
        //LOG.info("...... " + Arrays.toString(formPrefixKey(KEY_ALL_PARTITIONS, partition)));
        index.put(formPrefixKey(KEY_ALL_PARTITIONS, partition),
            partitionToFileIds.get(partition).stream().sorted().collect(Collectors.toList()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    index.commit();
    deltaManifest = getIndexFromPath(deltaManifestPath);
    newFilesMap.clear();
    assert getStats().deletedFileCount == deletedFiles.size();
    deletedFiles = null;

    logger.info("Created a delta manifest at " + deltaManifestPath);
  }

  private byte[] formPrefixKey(byte[] prefix, String suffix) throws UnsupportedEncodingException {
    byte[] suffixBytes = suffix.getBytes(Index.STRING_ENCODING);
    byte[] key = Arrays.copyOf(prefix, prefix.length + suffixBytes.length);
    System.arraycopy(suffixBytes, 0, key, prefix.length, suffixBytes.length);

    return key;
  }

  private boolean isPrefixKey(byte[] prefix, byte[] buffer) {
    if (buffer.length < prefix.length) {
      return false;
    }

    for (int i = 0; i < prefix.length; ++i) {
      if (prefix[i] != buffer[i]) {
        return false;
      }
    }

    return true;
  }

  private void createPartitionIndex(Map<String, List<Integer>> partitionToFileIds) throws IOException {
    String[] commitTimes = getCommitTimesFromPath(masterManifestPath);
    Path partitionIndexPath = makeColumnIndexPath(commitTimes[0], PARTITION_INDEX_NAME, PARTITION_INDEX_VERSION);

    String allPartitions = partitionToFileIds.keySet().stream().collect(Collectors.joining(","));
    Map<String, String> metadata = new HashMap<>();
    metadata.put(KEY_ALL, allPartitions);

    final Index index = getIndex(INDEX_VERSION, partitionIndexPath);
    index.create(Option.of(metadata));

    partitionToFileIds.keySet().stream().sorted().forEach(partition -> {
      try {
        assert partition.length() == 10;
        index.put(partition, partitionToFileIds.get(partition).stream().sorted().collect(Collectors.toList()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    index.commit();

    logger.info("Created a partition index at " + partitionIndexPath);
  }

  private void add(String filePath) throws IOException {
    assert filePath.charAt(0) != '/';
    String partition = new Path(filePath).getParent().toString();
    String fileName = filePath.substring(partition.length() + 1);
    add(partition, fileName);
  }

  private void add(String partition, String fileName) throws IOException {
    preUpdateChecks();

    assert fileName.charAt(0) != '/';
    if (newFilesMap.containsKey(fileName)) {
      // Should only happen with log files as they may be appended to
      assert !FSUtils.getFileExtensionFromLog(new Path(fileName)).isEmpty();
    } else {
      newFilesMap.put(fileName, partition);
    }
  }

  public void update(HoodieCommitMetadata metadata, String instantTime) throws IOException {
    preUpdateChecks();

    metadata.getPartitionToWriteStats().forEach((partitionPath, hoodieWriteStats) -> {
      hoodieWriteStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        String fileName = pathWithPartition.substring(partitionPath.length() + 1);
        try {
          add(partitionPath, fileName);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    });

    logger.info("Updated from " + instantTime + ".commit. #fileupdates=" + newFilesMap.size());
  }

  @Override
  public void update(HoodieCleanMetadata metadata, String instantTime) throws IOException {
    preUpdateChecks();

    metadata.getPartitionMetadata().forEach((partitionPath, partitionMetadata) -> {
      partitionMetadata.getSuccessDeleteFiles().forEach(path -> {
        if (newFilesMap.containsKey(path)) {
          newFilesMap.remove(path);
        } else {
          newFilesMap.put(path, DELETE_FILE_SENTINAL);
        }
      });
    });

    logger.info("Updated from " + instantTime + ".clean. #fileupdates=" + newFilesMap.size());
  }

  @Override
  public void update(HoodieRollbackMetadata metadata, String instantTime) throws IOException {
    preUpdateChecks();
    throw new RuntimeException("Not implemented");

  }

  @Override
  public void update(HoodieSavepointMetadata metadata, String instantTime) throws IOException {
    preUpdateChecks();
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void update(HoodieCompactionMetadata metadata, String instantTime) throws IOException {
    preUpdateChecks();
    throw new RuntimeException("Not implemented");

  }

  @Override
  public void update(org.apache.hudi.common.model.HoodieCommitMetadata metadata, String instantTime) throws IOException {
    preUpdateChecks();

    metadata.getPartitionToWriteStats().forEach((partitionPath, hoodieWriteStats) -> {
      hoodieWriteStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        String fileName = pathWithPartition.substring(partitionPath.length() + 1);
        assert fileName.charAt(0) != '/';
        if (newFilesMap.containsKey(fileName)) {
          // Should only happen with log files as they may be appended to
          assert !FSUtils.getFileExtensionFromLog(new Path(fileName)).isEmpty();
        } else {
          newFilesMap.put(fileName, partitionPath);
        }
      });
    });

    logger.info("Updated from compaction " + instantTime + ". #fileupdates=" + newFilesMap.size());
  }

  @Override
  public List<String> getAllPartitions() throws IOException {
    Index index = getColumnIndex(PARTITION_INDEX_NAME);
    Map<String, String> metadata = index.getMetadata();
    Set<String> partitions = new HashSet<>(Arrays.asList(metadata.get(KEY_ALL).split(",")));
    if (deltaManifest != null) {
      String newPartitions = deltaManifest.getValueAsString(KEY_ALL_PARTITIONS);
      partitions.addAll(Arrays.asList(newPartitions.split(",")));
    }

    List<String> partitionList = new LinkedList<>(partitions);
    assert partitionList.size() == partitions.size();
    return partitionList;
  }

  @Override
  public List<String> getAllFilesInPartition(String partition) throws IOException {
    partition = getRelativePath(partition);

    Index index = getColumnIndex(PARTITION_INDEX_NAME);
    List<Integer> fileIds = index.getValueAsListOfInteger(partition);
    if (fileIds == null) {
      // Partition must have been added after the master manifest was created
      fileIds = new LinkedList<>();
    }

    if (deltaManifest != null) {
      List<Integer> newFileIds = deltaManifest.getValueAsListOfInteger(formPrefixKey(KEY_ALL_PARTITIONS, partition));
      if (newFileIds != null) {
        fileIds.addAll(newFileIds);
      }
    }

    return fileIds.stream().map(fileId -> {
      try {
        return getFilePath(fileId);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new HoodieMetadataException("Failed to get path for fileId: " + e.getMessage());
      }
    }).collect(Collectors.toList());
  }

  @Override
  public String getFilePath(int fileId) throws Exception {
    loadDeletedFiles();

    assert maxFileIdInMasterManifest >= 0;

    String filePathStr;
    if (fileId <= maxFileIdInMasterManifest) {
      filePathStr = masterManifest.getValueAsString(fileId);
    } else {
      filePathStr = deltaManifest.getValueAsString(fileId);
    }

    // TODO: remove check?
    if (filePathStr != null) {
      Path p = new Path(filePathStr);
      if (deletedFiles.contains(p.getName().toString())) {
        return null;
      }
    }

    return filePathStr;
  }

  @Override
  public void scanFiles(int startFileId, BiFunction<Integer, String, Boolean> iter) throws IOException {
    loadDeletedFiles();

    if (masterManifest != null) {
      masterManifest.scan(startFileId, (b1, b2) -> {
        String filePathStr = new String(b2);
        Path filePath = new Path(filePathStr);
        if (!deletedFiles.contains(filePath.getName().toString())) {
          return iter.apply(Index.bytesToInt(b1), filePathStr);
        }

        return true;
      });
    }

    if (deltaManifest != null) {
      deltaManifest.scan(startFileId, (b1, b2) -> {
        //LOG.info("scan ... b1 " + Arrays.toString(b1));
        if (isPrefixKey(KEY_END, b1)) {
          // End of filId->path mappings
          return false;
        } else {
          String filePathStr = new String(b2);
          Path filePath = new Path(filePathStr);
          if (!deletedFiles.contains(filePath.getName().toString())) {
            return iter.apply(Index.bytesToInt(b1), filePathStr);
          }

          return true;
        }
      });
    }
  }

  private void loadDeletedFiles() throws IOException {
    if (deletedFiles == null && deltaManifest != null) {
      deletedFiles = new HashSet<>(Arrays.asList(deltaManifest.getValueAsString(KEY_DELETED_FILES).split(",")));
    } else if (deletedFiles == null) {
      deletedFiles = new HashSet<>();
    }
  }

  private void preUpdateChecks() throws IOException {
    if (!readyForUpdate) {
      String tmpPath = temporaryPath + File.separator + System.currentTimeMillis();
      newFilesMap = new ExternalSpillableMap((long)(MaxInMemorySizeInBytes * 0.9), tmpPath + ".newFilesMap",
          new DefaultSizeEstimator<String>(), new DefaultSizeEstimator<String>());

      readyForUpdate = true;
    }
  }

  private Path makeMasterManifestPath(String commitTime, int version) {
    String path = String.format("%s%s%s_%s.%d.master_manifest", metadataPath, Path.SEPARATOR, commitTime, commitTime,
        version);
    return new Path(path);
  }

  private Path makeDeltaManifestPath(String commitTime, String baseCommitTime, int version) {
    String path = String.format("%s%s%s_%s.%d.delta_manifest", metadataPath, Path.SEPARATOR, commitTime, baseCommitTime,
        version);
    return new Path(path);
  }

  private Path makeColumnIndexPath(String commitTime, String name, int version) {
    String path = String.format("%s%s%s_%s.%d.%s.idx", metadataPath, Path.SEPARATOR, commitTime, commitTime, version,
        name);
    return new Path(path);
  }

  private String[] getCommitTimesFromPath(Path path) {
    // Paths are of the format <commitTime>_<baseCommitTime>.version.<type>
    return path.getName().split("\\.")[0].split("_");
  }

  private int getVersionFromPath(Path path) {
    // Paths are of the format <commitTime>_<baseCommitTime>.version.<type>
    return Integer.parseInt(path.getName().split("\\.")[1]);
  }

  private Index getIndex(int version, Path path) { //TODO
    switch (version) {
      case 1:
        return new IndexTFile(configuration, path);
      case 2:
        return new IndexHFile(configuration, path);
      case 3:
        return new IndexRocksDB(configuration, path);
      default:
        throw new HoodieMetadataException("Invalid index version " + version);
    }
  }

  private Index getIndexFromPath(Path path) throws IOException { //TODO
    Index idx = getIndex(getVersionFromPath(path), path);
    idx.load();
    return idx;
  }

  private Index getColumnIndex(String name) throws IOException {
    if (!columnIndex.containsKey(name)) {
      // Index should refer to the instantTime of the master manifest
      String masterCommitTime = getCommitTimesFromPath(masterManifestPath)[0];
      String pathFmt = String.format("%s%s%s_%s.*.%s.idx", metadataPath, Path.SEPARATOR, masterCommitTime,
          masterCommitTime, name);
      FileStatus[] statuses = fs.globStatus(new Path(pathFmt));
      if (statuses.length > 1) {
        throw new HoodieMetadataException("Multipl e indexes found for " + name + ": " + statuses);
      }
      if (statuses.length == 0) {
        // Index not present
        return null;
      }

      Index idx = getIndex(getVersionFromPath(statuses[0].getPath()), statuses[0].getPath());
      idx.load();
      columnIndex.put(name, idx);
    }

    return columnIndex.get(name);
  }

  @Override
  public Stats getStats() {
    Stats stats = new Stats();

    try {
      if (masterManifestPath != null) {
        FileStatus[] statuses = fs.listStatus(masterManifestPath);
        stats.metadataTotalSize += statuses[0].getLen();
        stats.metadataFileSizes.put(masterManifestPath.toString(), statuses[0].getLen());
        stats.fileCount = Integer.parseInt(masterManifest.getMetadata().get(KEY_FILE_COUNT));

        Index partitionIndex = getColumnIndex(PARTITION_INDEX_NAME);
        statuses = fs.listStatus(partitionIndex.getPath());
        if (statuses.length > 0) {
          stats.metadataFileSizes.put(partitionIndex.getPath().toString(), statuses[0].getLen());
        }

        stats.masterInstantTime = getCommitTimesFromPath(masterManifestPath)[0];
      }
      if (deltaManifestPath != null) {
        FileStatus[] statuses = fs.listStatus(deltaManifestPath);
        stats.metadataTotalSize += statuses[0].getLen();
        stats.metadataFileSizes.put(deltaManifestPath.toString(), statuses[0].getLen());
        stats.fileCount += Integer.parseInt(deltaManifest.getMetadata().get(KEY_FILE_COUNT));
        stats.deletedFileCount = Integer.parseInt(deltaManifest.getMetadata().get(KEY_DELETED_FILE_COUNT));
      }
      stats.partitionCount = getAllPartitions().size();
      stats.maxFileId = maxFileId;
    } catch (Exception e) {
      e.printStackTrace();
      logger.warn("Failed to generate stats: " + e);
    }

    return stats;
  }
}
