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

package org.apache.hudi.integ.testsuite.generator;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.keygen.KeyGenerator;

/**
 * A GenericRecordGeneratorIterator which generates inserts, update and delete records from an existing data file.
 *
 * Implements {@link Iterator} to allow for iteration semantics.
 */
public class ExistingFileRecordGenerationIterator implements Iterator<HoodieRecord> {
  private Option<Schema> finalSchema = Option.empty();
  private HoodieFileReader reader;
  private Iterator<GenericRecord>  itr;
  private KeyGenerator keygen;
  private String payloadClassName;
  private Option<String> insertKeySuffix;
  private long numInserts;
  private long numUpserts;
  private long numDeletes;
  private boolean tagUpserts;
  private String commitTime;
  private String fileId;

  /**
   * @param conf Configuration
   * @param dataFilePath Path of the data file from which records are read
   * @param readerSchema Schema to use to read the records
   * @param finalSchema Schema to which records are converted
   * @param keygen Keygenerator to use
   * @param payloadClassName Name of the payload class
   * @param insertKeySuffix An optional suffix to add to the key of the insert records
   * @param numDeletes Number of deletes to generate
   * @param numInserts Number of inserts to generate
   * @param numUpserts Number of upserts to generate
   * @param tagUpserts Whether to tag upsert records with a HoodieRecordLocation
   */
  public ExistingFileRecordGenerationIterator(Configuration conf, Path dataFilePath, Schema readerSchema,
      Option<Schema> finalSchema, KeyGenerator keygen, String payloadClassName, Option<String> insertKeySuffix,
      long numDeletes, long numInserts, long numUpserts, long offset, boolean tagUpserts) {
    try {
      reader = HoodieFileReaderFactory.getFileReader(conf, dataFilePath);
      itr = reader.getRecordIterator(readerSchema);
    } catch (Exception e) {
      throw new HoodieException("Failed to open a reader for data file " + dataFilePath, e);
    }

    this.keygen = keygen;
    this.payloadClassName = payloadClassName;
    this.insertKeySuffix = insertKeySuffix;
    this.finalSchema = finalSchema;
    this.numDeletes = numDeletes;
    this.numInserts  = numInserts;
    this.numUpserts = numUpserts;
    this.tagUpserts = tagUpserts;
    if (tagUpserts) {
      commitTime = FSUtils.getCommitTime(dataFilePath.getName());
      fileId = FSUtils.getFileId(dataFilePath.getName());
    }

    while (offset-- > 0 && itr.hasNext()) {
      itr.next();
    }
  }

  @Override
  public boolean hasNext() {
    return itr.hasNext() && (this.numInserts > 0 || this.numDeletes > 0 || this.numUpserts > 0);
  }

  @Override
  public HoodieRecord next() {
    // Create the HoodieRecord
    GenericRecord gr = itr.next();
    HoodieKey key = keygen.getKey(gr);
    HoodieRecordPayload payload;

    if (finalSchema.isPresent()) {
      gr = HoodieAvroUtils.rewriteRecord(gr, finalSchema.get());
    }

    try {
      if (numDeletes > 0) {
        // TODO: Handle delete
        //--numDeletes;
        //payload = DataSourceUtils.createPayload(payloadClassName, Option.empty());
        //recordList.add(new HoodieRecord(key, payload));
        throw new HoodieException("Generation of deletes not supported yet");
      } else if (numInserts > 0) {
        // Generate an insert
        --numInserts;
        payload = DataSourceUtils.createPayload(payloadClassName, gr, "");
        HoodieKey insertKey;
        if (insertKeySuffix.isPresent()) {
          insertKey = new HoodieKey(key.getRecordKey() + insertKeySuffix.get(), key.getPartitionPath());
        } else {
          insertKey = new HoodieKey(key.getRecordKey(), key.getPartitionPath());
        }
        return new HoodieRecord(insertKey, payload);
      } else if (numUpserts > 0) {
        --numUpserts;
        payload = DataSourceUtils.createPayload(payloadClassName, gr, "");
        HoodieRecord rec = new HoodieRecord(key, payload);

        if (tagUpserts) {
          // Tag with location
          HoodieRecordLocation loc = new HoodieRecordLocation(commitTime, fileId);
          rec.unseal();
          rec.setCurrentLocation(loc);
          rec.seal();
          rec = HoodieIndexUtils.getTaggedRecord(rec, Option.of(loc));
        }
        return rec;
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to generate a record", e);
    }

    return null;
  }
}