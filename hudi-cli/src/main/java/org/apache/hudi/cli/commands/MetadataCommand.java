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

package org.apache.hudi.cli.commands;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metadata.MetadataStore;
import org.apache.hudi.common.metadata.MetadataStore.Stats;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * CLI command to operate on consolidated metadata.
 */
@Component
public class MetadataCommand implements CommandMarker {
  private List<String> testPartitions;
  private JavaSparkContext jsc;
  private static final Logger LOG = LogManager.getLogger(MetadataCommand.class);

  @CliCommand(value = "metadata create", help = "First time creation of consolidated metadata")
  public String create(
      @CliOption(key = {"clean"}, help = "Clean any older metadata and start fresh",
          unspecifiedDefaultValue = "false") final boolean clean)
      throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    Path metadataPath = new Path(MetadataStore.getMetadataDirectory(HoodieCLI.basePath));
    try {
      FileStatus[] statuses = HoodieCLI.fs.listStatus(metadataPath);
      if (statuses.length > 0 && !clean) {
        throw new RuntimeException("Metadata directory (" + metadataPath.toString() + ") not empty. Try --clean or update");
      }
    } catch (FileNotFoundException e) {
      // Metadata directory does not exist yet
      HoodieCLI.fs.mkdirs(metadataPath);
    }

    MetadataStore.create(metaClient, clean);

    return String.format("Created metadata in %s", metadataPath);
  }

  @CliCommand(value = "metadata update", help = "Update the consolidated metadata from commits since the creation")
  public String update(
                       @CliOption(key = {"merge"}, help = "Merge all commits into a single update",
                       unspecifiedDefaultValue = "false") final boolean merge)
                           throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    MetadataStore.update(metaClient, merge);

    return String.format("Updated metadata in %s", metaClient.getBasePath());
  }

  @CliCommand(value = "metadata stats", help = "Print stats about the metadata")
  public String stats() throws IOException {
    MetadataStore metadata = MetadataStore.get(HoodieCLI.basePath, HoodieCLI.conf);
    Stats stats = metadata.getStats();

    StringBuffer out = new StringBuffer("\n");
    out.append(String.format("Base path: %s\n", metadata.getBasePath()));
    out.append(String.format("Manifest total size: %,d\n", stats.metadataTotalSize));
    out.append(String.format("Manifest total files: %,d\n", stats.metadataFileSizes.values().stream().count()));
    out.append(String.format("Manifest paths/sizes: \n"));
    for (Map.Entry e : stats.metadataFileSizes.entrySet()) {
      out.append(String.format("  %s: %,d\n", e.getKey(), e.getValue()));
    }
    out.append(String.format("File counts: files=%,d, partitions=%,d\n", stats.fileCount, stats.partitionCount));
    out.append(String.format("Files/partition (avg): files=%,.2f\n", ((double)stats.fileCount / stats.partitionCount)));
    out.append(String.format("MaxFileId: %,d\n", stats.maxFileId));

    return out.toString();
  }

  @CliCommand(value = "metadata list-partitions", help = "Print a list of all partitions from the metadata")
  public String listPartitions() throws IOException {
    MetadataStore metadata = MetadataStore.get(HoodieCLI.basePath, HoodieCLI.conf);

    StringBuffer out = new StringBuffer("\n");
    List<String> partitions = metadata.getAllPartitions();
    int[] count = {0};
    partitions.stream().sorted((p1, p2) -> p2.compareTo(p1)).forEach(p -> {
      out.append(p);
      if (++count[0] % 15 == 0) {
        out.append("\n");
      } else {
        out.append(", ");
      }
    });

    return out.toString();
  }

  @CliCommand(value = "metadata list-files", help = "Print a list of all files in a partition from the metadata")
  public String listFiles(
      @CliOption(key = {"partition"}, help = "Name of the partition to list files", mandatory = true)
      final String partition) throws IOException {
    MetadataStore metadata = MetadataStore.get(HoodieCLI.basePath, HoodieCLI.conf);

    StringBuffer out = new StringBuffer("\n");
    List<String> files = metadata.getAllFilesInPartition(partition);
    files.stream().sorted((p1, p2) -> p2.compareTo(p1)).forEach(p -> {
      out.append(p);
      out.append("\n");
    });

    return out.toString();
  }

  @CliCommand(value = "metadata perf", help = "Run performance tests on the metadata")
  public String perf() throws Exception {
    if (jsc == null) {
      jsc = SparkUtil.initJavaSparkConf("HoodieClI");
    }
    HoodieTable table = HoodieTable.getHoodieTable(HoodieCLI.getTableMetaClient(),
        HoodieWriteConfig.newBuilder().withPath(HoodieCLI.basePath).build(), jsc);
    SyncableFileSystemView origView = table.getHoodieView();
    SyncableFileSystemView metaView = table.getMetadataView();

    double[] d1 = perfGetAllPartitions();
    double[] d2 = perfGetAllFilesInPartition();
    double[] d3 = perfSyncableFileSystemView(origView);
    origView.reset();
    double[] d4 = perfSyncableFileSystemView(metaView);

    StringBuffer out = new StringBuffer("\n");
    out.append(String.format("Partition lookup performance: (existing/with-consolidated-metadata/difference)\n"));
    out.append(String.format("   getAllPartitions: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d1[0], d1[1], percentDiff(d1[0], d1[1])));
    out.append(String.format("   getAllFilesInPartition: %.2fmsec / %.2fmsec / %,.2f%%\n", d2[0], d2[1], percentDiff(d2[0], d2[1])));
    out.append(String.format("\n"));
    out.append(String.format("SyncableFileSystemView performance  partition: (existing/with-consolidated-metadata/difference)\n"));
    out.append(String.format("   getAllBaseFiles: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d3[0], d4[0], percentDiff(d3[0], d4[0])));
    out.append(String.format("   getAllFileGroups: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d3[1], d4[1], percentDiff(d3[1], d4[1])));
    out.append(String.format("   getAllFileSlices: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d3[2], d4[2], percentDiff(d3[2], d4[2])));
    out.append(String.format("   getLatestBaseFiles: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d3[3], d4[3], percentDiff(d3[3], d4[3])));
    out.append(String.format("   getLatestFileSlices: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d3[4], d4[4], percentDiff(d3[4], d4[4])));
    out.append(String.format("   getLatestUnCompactedFileSlices: %,.2fmsec / %,.2fmsec / %,.2f%%\n", d3[5], d4[5], percentDiff(d3[5], d4[5])));

    return out.toString();
  }

  private double[] perfGetAllPartitions() throws Exception {
    // Non metadata case (slow, so only once)
    long t1 = System.currentTimeMillis();
    List<String> partitions1 = FSUtils.getAllFoldersWithPartitionMetaFile(HoodieCLI.fs, HoodieCLI.basePath);
    long t2 = System.currentTimeMillis();

    // Metadata case (faster, so we average out)
    MetadataStore metadata = MetadataStore.get(HoodieCLI.basePath, HoodieCLI.conf);
    List<String> partitions2 = null;
    for (int i = 0; i < 10; ++i) {
      partitions2 = metadata.getAllPartitions();
    }
    long t3 = System.currentTimeMillis();

    if (partitions1.size() != partitions2.size()) {
      LOG.warn("Size mismatch in perfGetAllPartitions. without-meta=" + partitions1.size()
          + ", with-meta=" + partitions2.size());
    }
    // save partition for later tests
    Collections.reverse(partitions1);
    testPartitions = partitions1.subList(1, partitions1.size());

    return new double[]{t2 - t1, (t3 - t2) / 10.0};
  }

  private double[] perfGetAllFilesInPartition() throws Exception {
    // Non metadata case
    int len1 = 0;
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < 5; ++i) {
      len1 = HoodieCLI.fs.listStatus(new Path(HoodieCLI.basePath, testPartitions.get(0))).length;
    }
    long t2 = System.currentTimeMillis();

    // Metadata case
    int len2 = 0;
    MetadataStore metadata = MetadataStore.get(HoodieCLI.basePath, HoodieCLI.conf);
    for (int i = 0; i < 10; ++i) {
      len2 = metadata.getAllFilesInPartition(testPartitions.get(0)).size();
    }
    long t3 = System.currentTimeMillis();

    if (len1 != len2) {
      LOG.warn("Size mismatch in perfGetAllFilesInPartition. without-meta=" + len1 + ", with-meta=" + len2);
    }

    return new double[]{(t2 - t1) / 5.0, (t3 - t2) / 10.0};
  }

  private double[] perfSyncableFileSystemView(SyncableFileSystemView view) throws Exception {
    Function<Consumer<SyncableFileSystemView>, Double> timerFunc = (func) -> {
      long t1 = System.currentTimeMillis();
      func.accept(view);
      long t2 = System.currentTimeMillis();
      return (double)t2 - t1;
    };

    return new double[] {
      timerFunc.apply(v -> v.getAllBaseFiles(testPartitions.get(10))),
      timerFunc.apply(v -> v.getAllFileGroups(testPartitions.get(20))),
      timerFunc.apply(v -> v.getAllFileSlices(testPartitions.get(30))),
      timerFunc.apply(v -> v.getLatestBaseFiles(testPartitions.get(40)).count()),
      timerFunc.apply(v -> v.getLatestFileSlices(testPartitions.get(50)).count()),
      timerFunc.apply(v -> v.getLatestUnCompactedFileSlices(testPartitions.get(60)).count())
    };
  }

  private double percentDiff(double d1, double d2) {
    return 100.0 * (d2 - d1) / d1;
  }
}
