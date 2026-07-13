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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSnapshotMetadataClient {
  private static String TEST_WRITE_TOKEN = "1-0-1";
  
  @TempDir
  public java.nio.file.Path folder;
  
  private String basePath;
  private String partitionPath;
  private String nonHudiPartitionPath;
  private String fullPartitionPath;
  private String fullNonHudiPartitionPath;
  private HoodieSnapshotMetadataClient snapshotMetadataClient;
  private HoodieTableMetaClient metaClient;
  private HoodieStorage storage;

  private String fileId1 = UUID.randomUUID().toString();
  private String fileId2 = UUID.randomUUID().toString();
  private String fileId3 = UUID.randomUUID().toString();
  private String fileId4 = UUID.randomUUID().toString();
  private String nonHudiFile1 = "file1.parquet";
  private String nonHudiFile2 = "file2.parquet";

  @BeforeEach
  public void setUp() throws IOException {
    basePath = folder.resolve("dataset").toString();
    partitionPath = "2016/05/01/";
    nonHudiPartitionPath = "non_hudi_partition/";
    fullPartitionPath = basePath + "/" + partitionPath;
    fullNonHudiPartitionPath = basePath + "/" + nonHudiPartitionPath;

    // Initialize the test table
    HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);

    // Initialize metaClient first to get fs
    snapshotMetadataClient = new HoodieSnapshotMetadataClient(HoodieTestUtils.getDefaultStorageConf(), basePath);
    metaClient = snapshotMetadataClient.getMetaClient();
    storage = metaClient.getStorage();

    // Set up the data files after fs is initialized
    setupDataFiles();

    // Reload the timeline to ensure it picks up all commits
    metaClient.reloadActiveTimeline();
  }

  private void setupDataFiles() throws IOException {
    // Create .hoodie directory first
    new File(basePath + "/.hoodie").mkdirs();
    
    // Put some files in the partition
    new File(fullPartitionPath).mkdirs();
    String cleanTime1 = "0";
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";

    // Create commit/clean files in the timeline path (respects V1 vs V2 layout)
    String timelineDir = metaClient.getTimelinePath().toString();
    new File(timelineDir).mkdirs();
    new File(timelineDir + "/" + cleanTime1 + ".clean").createNewFile();
    new File(timelineDir + "/" + commitTime1 + ".commit").createNewFile();
    new File(timelineDir + "/" + commitTime2 + ".commit").createNewFile();
    new File(timelineDir + "/" + commitTime3 + ".commit").createNewFile();
    new File(timelineDir + "/" + commitTime4 + ".commit").createNewFile();

    // Then create the data files
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, ".parquet")).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1, ".parquet")).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, ".parquet")).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2, ".parquet")).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2, ".parquet")).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3, ".parquet")).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3, ".parquet")).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
        .createNewFile();

    // Create a partition metadata file to mark this as a Hudi partition
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, "4",
            new StoragePath(basePath),
            new StoragePath(basePath, partitionPath),
            Option.empty());
    partitionMetadata.trySave();

    // Set up a non-Hudi partition with regular files
    new File(fullNonHudiPartitionPath).mkdirs();
    File nonHudiFile1File = new File(fullNonHudiPartitionPath + nonHudiFile1);
    nonHudiFile1File.createNewFile();
    File nonHudiFile2File = new File(fullNonHudiPartitionPath + nonHudiFile2);
    nonHudiFile2File.createNewFile();
    // Intentionally not creating a partition metadata file for this partition
  }

  @Test
  public void testSnapshotMetadata() throws IOException {
    assertEquals("4", snapshotMetadataClient.getLatestInstant().get());
    
    Set<String> fileIds = snapshotMetadataClient.getLatestSnapshotFiles(partitionPath).map(FSUtils::getFileIdFromFilePath)
        .collect(Collectors.toSet());
    
    //fileId4 has only log file. so ensure it doesnt show up in results.
    assertEquals(Stream.of(fileId1, fileId2, fileId3).collect(Collectors.toSet()), fileIds);
    
    Set<String> fileIdsAt2 = snapshotMetadataClient.getSnapshotFilesAt("2", partitionPath).map(FSUtils::getFileIdFromFilePath)
        .collect(Collectors.toSet());

    // only fileId1/fileId2 exist at instant 2.
    assertEquals(2, fileIdsAt2.size());
    assertEquals(Stream.of(fileId1, fileId2).collect(Collectors.toSet()), fileIdsAt2);
  }

  @Test
  public void testGetLatestSnapshotFilesWithHudiPartition() {
    // This test verifies that for Hudi partitions, only the latest file versions are returned
    Set<String> fileIds = snapshotMetadataClient.getLatestSnapshotFiles(partitionPath)
            .map(FSUtils::getFileIdFromFilePath)
            .collect(Collectors.toSet());

    // We should only get the latest versions of files (3 file IDs)
    Set<String> expectedFileIds = Stream.of(fileId1, fileId2, fileId3).collect(Collectors.toSet());
    assertEquals(expectedFileIds.size(), fileIds.size());
    assertEquals(expectedFileIds, fileIds);

    // Verify we don't get log files
    assertTrue(!fileIds.contains(fileId4), "Log file ID should not be present");
  }

  @Test
  public void testGetLatestSnapshotFilesWithNonHudiPartition() {
    // This test verifies that for non-Hudi partitions, all files are returned
    Set<String> fileNames = new HashSet<>();
    snapshotMetadataClient.getLatestSnapshotFiles(nonHudiPartitionPath)
            .forEach(path -> fileNames.add(path.getName()));

    // We should get all files in the partition (2 files)
    assertEquals(2, fileNames.size());
    Set<String> expectedFileNames = Stream.of(nonHudiFile1, nonHudiFile2).collect(Collectors.toSet());
    assertEquals(expectedFileNames, fileNames);
  }

  @Test
  public void testGetLatestSnapshotFilesWithPartitionMetadataForHudiPartition() {
    // This test verifies that for Hudi partitions, we get both the latest files and partition metadata
    Set<String> fileNames = new HashSet<>();
    snapshotMetadataClient.getLatestSnapshotFilesWithPartitionMetadata(partitionPath)
            .forEach(path -> fileNames.add(path.getName()));

    // Expected to find the 3 base files plus partition metadata file = 4 files
    assertEquals(4, fileNames.size());

    // Verify we have the partition metadata file
    assertTrue(fileNames.stream()
                    .anyMatch(name -> name.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)),
            "Partition metadata file should be present for Hudi partition");

    // Extract file IDs from the base filenames (excluding partition metadata file)
    Set<String> fileIds = fileNames.stream()
            .filter(name -> !name.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX))
            .map(FSUtils::getFileId)
            .collect(Collectors.toSet());

    // Verify we have the correct file IDs
    Set<String> expectedFileIds = Stream.of(fileId1, fileId2, fileId3).collect(Collectors.toSet());
    assertEquals(expectedFileIds, fileIds);
  }

  @Test
  public void testGetLatestSnapshotFilesWithPartitionMetadataForNonHudiPartition() {
    // This test verifies that for non-Hudi partitions, we get all files without partition metadata
    Set<String> fileNames = new HashSet<>();
    snapshotMetadataClient.getLatestSnapshotFilesWithPartitionMetadata(nonHudiPartitionPath)
            .forEach(path -> fileNames.add(path.getName()));

    // We should get just the 2 files in the partition, no partition metadata
    assertEquals(2, fileNames.size());
    Set<String> expectedFileNames = Stream.of(nonHudiFile1, nonHudiFile2).collect(Collectors.toSet());
    assertEquals(expectedFileNames, fileNames);

    // Verify there's no partition metadata file
    assertTrue(fileNames.stream()
                    .noneMatch(name -> name.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)),
            "Partition metadata file should not be present for non-Hudi partition");
  }

  @Test
  public void testGetSnapshotFilesAtWithHudiPartition() {
    // Test for Hudi partitions at a specific instant
    // Testing with instant "2" should return specific versions of files
    Set<String> fileIds = snapshotMetadataClient.getSnapshotFilesAt("2", partitionPath)
            .map(FSUtils::getFileIdFromFilePath)
            .collect(Collectors.toSet());

    // At instant "2", only fileId1 and fileId2 should exist
    Set<String> expectedFileIds = Stream.of(fileId1, fileId2).collect(Collectors.toSet());
    assertEquals(expectedFileIds, fileIds);
  }

  @Test
  public void testGetSnapshotFilesAtWithNonHudiPartition() {
    // Test for non-Hudi partitions at a specific instant
    // For non-Hudi partitions, instant doesn't matter - all files should be returned
    Set<String> fileNames = new HashSet<>();
    snapshotMetadataClient.getSnapshotFilesAt("2", nonHudiPartitionPath)
            .forEach(path -> fileNames.add(path.getName()));

    // Should return all files in the non-Hudi partition
    assertEquals(2, fileNames.size());
    Set<String> expectedFileNames = Stream.of(nonHudiFile1, nonHudiFile2).collect(Collectors.toSet());
    assertEquals(expectedFileNames, fileNames);
  }

  @Test
  public void testGetSnapshotFilesAtWithPartitionMetadataForHudiPartition() {
    // Test for Hudi partitions at a specific instant with partition metadata
    Set<String> fileNames = new HashSet<>();
    snapshotMetadataClient.getSnapshotFilesAtWithPartitionMetadata("2", partitionPath)
            .forEach(path -> fileNames.add(path.getName()));

    // At instant "2", we should have fileId1, fileId2, and the partition metadata = 3 files
    assertEquals(3, fileNames.size());

    // Verify we have the partition metadata file
    assertTrue(fileNames.stream()
                    .anyMatch(name -> name.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)),
            "Partition metadata file should be present for Hudi partition");

    // Extract file IDs from the base filenames (excluding partition metadata file)
    Set<String> fileIds = fileNames.stream()
            .filter(name -> !name.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX))
            .map(FSUtils::getFileId)
            .collect(Collectors.toSet());

    // Verify file IDs match what we expect at instant 2
    Set<String> expectedFileIds = Stream.of(fileId1, fileId2).collect(Collectors.toSet());
    assertEquals(expectedFileIds, fileIds);
  }

  @Test
  public void testGetSnapshotFilesAtWithPartitionMetadataForNonHudiPartition() {
    // Test for non-Hudi partitions at a specific instant with partition metadata
    Set<String> fileNames = new HashSet<>();
    snapshotMetadataClient.getSnapshotFilesAtWithPartitionMetadata("2", nonHudiPartitionPath)
            .forEach(path -> fileNames.add(path.getName()));

    // For non-Hudi partitions, we should just get all files without metadata
    assertEquals(2, fileNames.size());
    Set<String> expectedFileNames = Stream.of(nonHudiFile1, nonHudiFile2).collect(Collectors.toSet());
    assertEquals(expectedFileNames, fileNames);

    // Verify there's no partition metadata file
    assertTrue(fileNames.stream()
                    .noneMatch(name -> name.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)),
            "Partition metadata file should not be present for non-Hudi partition");
  }
}