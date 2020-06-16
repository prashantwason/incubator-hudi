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

package org.apache.hudi.io.storage;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HoodieHFileWriter writes IndexedRecords into an HFile. The record's key is used as the key and the
 * AVRO encoded record bytes are saved as the value.
 *
 * Limitations (compared to columnar formats like Parquet or ORC):
 *  1. Records should be added in order of keys
 *  2. There are no column stats
 */
public class HoodieHFileWriter<T extends HoodieRecordPayload, R extends IndexedRecord>
    implements HoodieFileWriter<R> {
  private static AtomicLong recordIndex = new AtomicLong(1);
  private static final Logger LOG = LogManager.getLogger(HoodieHFileWriter.class);

  public static final String KEY_SCHEMA = "schema";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";

  private final Path file;
  private HoodieHFileConfig hfileConfig;
  private final HoodieWrapperFileSystem fs;
  private final long maxFileSize;
  private final String instantTime;
  private final SparkTaskContextSupplier sparkTaskContextSupplier;
  private HFile.Writer writer;
  private String minRecordKey;
  private String maxRecordKey;

  public HoodieHFileWriter(String instantTime, Path file, HoodieHFileConfig hfileConfig, Schema schema,
      SparkTaskContextSupplier sparkTaskContextSupplier) throws IOException {

    Configuration conf = HoodieFileWriter.registerFileSystem(file, hfileConfig.getHadoopConf());
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, conf);
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(conf);
    this.hfileConfig = hfileConfig;

    // We cannot accurately measure the snappy compressed output file size. We are choosing a
    // conservative 10%
    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
    // stream and the actual file size reported by HDFS
    // this.maxFileSize = hfileConfig.getMaxFileSize()
    //    + Math.round(hfileConfig.getMaxFileSize() * hfileConfig.getCompressionRatio());
    this.maxFileSize = hfileConfig.getMaxFileSize();
    this.instantTime = instantTime;
    this.sparkTaskContextSupplier = sparkTaskContextSupplier;

    HFileContext context = new HFileContextBuilder().withBlockSize(hfileConfig.getBlockSize())
          .withCompression(hfileConfig.getCompressionAlgorithm())
          .build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    this.writer = HFile.getWriterFactory(conf, cacheConfig).withPath(this.fs, this.file).withFileContext(context).create();

    writer.appendFileInfo(KEY_SCHEMA.getBytes(), schema.toString().getBytes());
  }

  @Override
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
    String seqId =
        HoodieRecord.generateSequenceId(instantTime, sparkTaskContextSupplier.getPartitionIdSupplier().get(), recordIndex.getAndIncrement());
    HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord, record.getRecordKey(), record.getPartitionPath(),
        file.getName());
    HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord, instantTime, seqId);

    writeAvro(record.getRecordKey(), (IndexedRecord)avroRecord);
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String recordKey, IndexedRecord object) throws IOException {
    byte[] value = HoodieAvroUtils.avroToBytes((GenericRecord)object);
    KeyValue kv = new KeyValue(recordKey.getBytes(), null, null, value);
    writer.append(kv);

    if (hfileConfig.useBloomFilter()) {
      hfileConfig.getBloomFilter().add(recordKey);
      if (minRecordKey != null) {
        minRecordKey = minRecordKey.compareTo(recordKey) <= 0 ? minRecordKey : recordKey;
      } else {
        minRecordKey = recordKey;
      }

      if (maxRecordKey != null) {
        maxRecordKey = maxRecordKey.compareTo(recordKey) >= 0 ? maxRecordKey : recordKey;
      } else {
        maxRecordKey = recordKey;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (hfileConfig.useBloomFilter()) {
      final BloomFilter bloomFilter = hfileConfig.getBloomFilter();
      writer.appendFileInfo(KEY_MIN_RECORD.getBytes(), minRecordKey.getBytes());
      writer.appendFileInfo(KEY_MAX_RECORD.getBytes(), maxRecordKey.getBytes());
      writer.appendFileInfo(KEY_BLOOM_FILTER_TYPE_CODE.getBytes(),
          bloomFilter.getBloomFilterTypeCode().toString().getBytes());
      writer.appendMetaBlock(KEY_BLOOM_FILTER_META_BLOCK, new Writable() {
        @Override
        public void write(DataOutput out) throws IOException {
          out.write(bloomFilter.serializeToString().getBytes());
        }

        @Override
        public void readFields(DataInput in) throws IOException { }
      });
    }

    writer.close();
    writer = null;
  }
}
