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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 *
 * If the metadata table does not exist, RPC calls are used to retrieve file listings from the file system.
 * No updates are applied to the table and it is not synced.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadata.class);

  private String metadataBasePath;
  // Metadata table's timeline and metaclient
  private HoodieTableMetaClient metaClient;
  private List<FileSlice> latestFileSystemMetadataSlices;

  // Readers for the base and log file which store the metadata
  private transient HoodieFileReader<GenericRecord> baseFileReader;
  private transient HoodieMetadataMergedLogRecordScanner logRecordScanner;

  public HoodieBackedTableMetadata(Configuration conf, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory) {
    this(new HoodieLocalEngineContext(conf), metadataConfig, datasetBasePath, spillableMapDirectory);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory) {
    super(engineContext, metadataConfig, datasetBasePath, spillableMapDirectory);
    initIfNeeded();
  }

  private void initIfNeeded() {
    if (enabled && this.metaClient == null) {
      this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetBasePath);
      try {
        this.metaClient = new HoodieTableMetaClient(hadoopConf.get(), metadataBasePath);
        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
        latestFileSystemMetadataSlices = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath()).collect(Collectors.toList());
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
        this.metaClient = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
        this.metaClient = null;
      }
    } else {
      LOG.info("Metadata table is disabled.");
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key) {
    // This function can be called in parallel through multiple threads. For each thread, we determine the thread-local
    // versions of the baseFile and logRecord readers to use.
    // - If reuse is enabled, we use the same readers and dont close them
    // - if reuse is disabled, we open new readers in each thread and close them
    HoodieFileReader localFileReader = null;
    HoodieMetadataMergedLogRecordScanner localLogRecordScanner = null;
    synchronized (this) {
      if (!metadataConfig.enableReuse()) {
        // reuse is disabled so always open new readers
        try {
          Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner> readers = openReaders();
          localFileReader = readers.getKey();
          localLogRecordScanner = readers.getValue();
        } catch (IOException e) {
          throw new HoodieIOException("Error opening readers", e);
        }
      } else if (baseFileReader == null && logRecordScanner == null) {
        // reuse is enabled but we haven't opened the readers yet
        try {
          Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner> readers = openReaders();
          localFileReader = readers.getKey();
          localLogRecordScanner = readers.getValue();
          // cache the readers
          baseFileReader = localFileReader;
          logRecordScanner = localLogRecordScanner;
        } catch (IOException e) {
          throw new HoodieIOException("Error opening readers", e);
        }
      } else {
        // reuse the already open readers
        ValidationUtils.checkState((baseFileReader != null || logRecordScanner != null), "Readers should already be open");
        localFileReader = baseFileReader;
        localLogRecordScanner = logRecordScanner;
      }
    }

    try {
      ValidationUtils.checkState((localFileReader != null || localLogRecordScanner != null), "Readers should have been opened");
      return getRecordByKeyFromMetadata(key, localFileReader, localLogRecordScanner);
    } finally {
      if (!metadataConfig.enableReuse()) {
        // reuse is disabled so close the local copy
        close(localFileReader, localLogRecordScanner);
      }
    }
  }

  private Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key, HoodieFileReader baseFileReader,
      HoodieMetadataMergedLogRecordScanner logRecordScanner) {
    try {
      List<Long> timings = new ArrayList<>();
      HoodieTimer timer = new HoodieTimer().startTimer();

      // Retrieve record from base file
      HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
      if (baseFileReader != null) {
        HoodieTimer readTimer = new HoodieTimer().startTimer();
        Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(key);
        if (baseRecord.isPresent()) {
          hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
              metaClient.getTableConfig().getPayloadClass());
          metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, readTimer.endTimer()));
        }
      }
      timings.add(timer.endTimer());

      // Retrieve record from log file
      timer.startTimer();
      if (logRecordScanner != null) {
        Option<HoodieRecord<HoodieMetadataPayload>> logHoodieRecord = logRecordScanner.getRecordByKey(key);
        if (logHoodieRecord.isPresent()) {
          if (hoodieRecord != null) {
            // Merge the payloads
            HoodieRecordPayload mergedPayload = logHoodieRecord.get().getData().preCombine(hoodieRecord.getData());
            hoodieRecord = new HoodieRecord(hoodieRecord.getKey(), mergedPayload);
          } else {
            hoodieRecord = logHoodieRecord.get();
          }
        }
      }
      timings.add(timer.endTimer());
      LOG.info(String.format("Metadata read for key %s took [baseFileRead, logMerge] %s ms", key, timings));
      return Option.ofNullable(hoodieRecord);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from metadata table for key :" + key, ioe);
    }
  }

  /**
   * Returns the readers to the base and log files.
   *
   * If reuse is allowed then cached readers are returned. Otherwise new readers are opened.
   */
  private Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner> openReaders() throws IOException {
    HoodieFileReader baseFileReader = null;
    long[] timings = {0, 0};

    // Metadata is in sync till the latest completed instant on the dataset
    HoodieTimer timer = new HoodieTimer().startTimer();
    String latestInstantTime = getLatestDatasetInstantTime();
    ValidationUtils.checkArgument(latestFileSystemMetadataSlices.size() == 1, "must be at-least one validata metadata file slice");

    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = latestFileSystemMetadataSlices.get(0).getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));
      timings[0] = timer.endTimer();
      LOG.info(String.format("Opened metadata base file from %s at instant %s in %d ms", basefilePath,
          basefile.get().getCommitTime(), timings[0]));
    } else {
      timer.endTimer();
    }

    // Open the log record scanner using the log files from the latest file slice
    timer.startTimer();
    List<String> logFilePaths = latestFileSystemMetadataSlices.get(0).getLogFiles()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    HoodieMetadataMergedLogRecordScanner logRecordScanner = new HoodieMetadataMergedLogRecordScanner(metaClient.getFs(),
            metadataBasePath, logFilePaths, schema, latestMetaInstantTimestamp, MAX_MEMORY_SIZE_IN_BYTES, BUFFER_SIZE,
            spillableMapDirectory, null);

    timings[1] = timer.endTimer();
    LOG.info(String.format("Opened metadata log files from %s at instant (dataset instant=%s, metadata instant=%s) in %d ms",
        logFilePaths, latestInstantTime, latestMetaInstantTimestamp, timings[1]));

    metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, timings[0] + timings[1]));
    return Pair.of(baseFileReader, logRecordScanner);
  }

  private void close(HoodieFileReader localFileReader, HoodieMetadataMergedLogRecordScanner localLogScanner) {
    try {
      if (localFileReader != null) {
        localFileReader.close();
      }
      if (localLogScanner != null) {
        localLogScanner.close();
      }
    } catch (Exception e) {
      throw new HoodieException("Error closing resources during metadata table merge", e);
    }
  }

  @Override
  public synchronized void close() throws Exception {
    close(baseFileReader, logRecordScanner);
    baseFileReader = null;
    logRecordScanner = null;
  }

  /**
   * Return an ordered list of instants which have not been synced to the Metadata Table.
   */
  @Override
  protected List<HoodieInstant> findInstantsToSync() {
    initIfNeeded();

    // if there are no instants yet, return empty list, since there is nothing to sync here.
    if (!enabled || !metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
      return Collections.EMPTY_LIST;
    }

    // All instants on the data timeline, which are greater than the last instant on metadata timeline
    // are candidates for sync.
    String latestMetadataInstantTime = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
    HoodieDefaultTimeline candidateTimeline = datasetMetaClient.getActiveTimeline().findInstantsAfter(latestMetadataInstantTime, Integer.MAX_VALUE);
    Option<HoodieInstant> earliestIncompleteInstant = candidateTimeline.filterInflightsAndRequested().firstInstant();

    if (earliestIncompleteInstant.isPresent()) {
      return candidateTimeline.filterCompletedInstants()
          .findInstantsBefore(earliestIncompleteInstant.get().getTimestamp())
          .getInstants().collect(Collectors.toList());
    } else {
      return candidateTimeline.filterCompletedInstants()
          .getInstants().collect(Collectors.toList());
    }
  }

  /**
   * Return the timestamp of the latest compaction instant.
   */
  @Override
  public Option<String> getSyncedInstantTime() {
    if (!enabled) {
      return Option.empty();
    }

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    return timeline.getDeltaCommitTimeline().filterCompletedInstants()
        .lastInstant().map(HoodieInstant::getTimestamp);
  }

  public boolean enabled() {
    return enabled;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Map<String, String> stats() {
    return metrics.map(m -> m.getStats(true, metaClient, this)).orElse(new HashMap<>());
  }
}
