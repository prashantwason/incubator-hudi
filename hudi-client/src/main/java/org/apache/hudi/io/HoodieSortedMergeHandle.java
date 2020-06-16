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

package org.apache.hudi.io;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

@SuppressWarnings("Duplicates")
public class HoodieSortedMergeHandle<T extends HoodieRecordPayload> extends HoodieMergeHandle<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieSortedMergeHandle.class);

  private Queue<String> newRecordKeysSorted = new PriorityQueue<>();

  public HoodieSortedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T> hoodieTable,
       Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId, SparkTaskContextSupplier sparkTaskContextSupplier) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, sparkTaskContextSupplier);
    newRecordKeysSorted.addAll(keyToNewRecords.keySet());
  }

  /**
   * Called by compactor code path.
   */
  public HoodieSortedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T> hoodieTable,
      Map<String, HoodieRecord<T>> keyToNewRecordsOrig, String partitionPath, String fileId,
      HoodieBaseFile dataFileToBeMerged, SparkTaskContextSupplier sparkTaskContextSupplier) {
    super(config, instantTime, hoodieTable, keyToNewRecordsOrig, partitionPath, fileId, dataFileToBeMerged,
        sparkTaskContextSupplier);

    newRecordKeysSorted.addAll(keyToNewRecords.keySet());
  }

  /**
   * Go through an old record. Here if we detect a newer version shows up, we write the new one to the file.
   */
  @Override
  public void write(GenericRecord oldRecord) {
    String key = oldRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();

    // To maintain overall sorted order across updates and inserts, write any new inserts whose keys are less than
    // the oldRecord's key.
    while (!newRecordKeysSorted.isEmpty() && newRecordKeysSorted.peek().compareTo(key) <= 0) {
      String keyToPreWrite = newRecordKeysSorted.remove();
      if (keyToPreWrite.equals(key)) {
        // will be handled as an update later
        break;
      }

      // This is a new insert
      HoodieRecord<T> hoodieRecord = new HoodieRecord<>(keyToNewRecords.get(keyToPreWrite));
      if (writtenRecordKeys.contains(keyToPreWrite)) {
        throw new HoodieUpsertException("Insert/Update not in sorted order");
      }
      try {
        if (useWriterSchema) {
          writeRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(writerSchema));
        } else {
          writeRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(originalSchema));
        }
        insertRecordsWritten++;
        writtenRecordKeys.add(keyToPreWrite);
      } catch (IOException e) {
        throw new HoodieUpsertException("Failed to write records", e);
      }
    }

    super.write(oldRecord);
  }

  @Override
  public WriteStatus close() {
    // write out any pending records (this can happen when inserts are turned into updates)
    newRecordKeysSorted.stream().sorted().forEach(key -> {
      try {
        HoodieRecord<T> hoodieRecord = keyToNewRecords.get(key);
        if (!writtenRecordKeys.contains(hoodieRecord.getRecordKey())) {
          if (useWriterSchema) {
            writeRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(writerSchema));
          } else {
            writeRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(originalSchema));
          }
          insertRecordsWritten++;
        }
      } catch (IOException e) {
        throw new HoodieUpsertException("Failed to close UpdateHandle", e);
      }
    });
    newRecordKeysSorted.clear();
    keyToNewRecords.clear();

    return super.close();
  }
}
