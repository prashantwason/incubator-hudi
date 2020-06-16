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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieHFileReader<R extends IndexedRecord> implements HoodieFileReader {
  private static final Logger LOG = LogManager.getLogger(HoodieHFileReader.class);
  private Path path;
  private Configuration conf;
  private HFile.Reader reader;

  public static final String KEY_SCHEMA = "schema";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";

  public HoodieHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig) throws IOException {
    this.conf = configuration;
    this.path = path;
    this.reader = HFile.createReader(FSUtils.getFs(path.toString(), configuration), path, cacheConfig, conf);
  }

  public HoodieHFileReader(byte[] content) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path("hoodie");
    SeekableByteArrayInputStream bis = new SeekableByteArrayInputStream(content);
    FSDataInputStream fsdis = new FSDataInputStream(bis);
    this.reader = HFile.createReader(FSUtils.getFs("hoodie", conf), path, new FSDataInputStreamWrapper(fsdis),
        content.length, new CacheConfig(conf), conf);
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    Map<byte[], byte[]> fileInfo;
    try {
      fileInfo = reader.loadFileInfo();
      return new String[] { new String(fileInfo.get(KEY_MIN_RECORD.getBytes())),
          new String(fileInfo.get(KEY_MAX_RECORD.getBytes()))};
    } catch (IOException e) {
      throw new HoodieException("Could not read min/max record key out of file information block correctly from path", e);
    }
  }

  @Override
  public Schema getSchema() {
    try {
      Map<byte[], byte[]> fileInfo = reader.loadFileInfo();
      return new Schema.Parser().parse(new String(fileInfo.get(KEY_SCHEMA.getBytes())));
    } catch (IOException e) {
      throw new HoodieException("Could not read schema of file from path", e);
    }

  }

  @Override
  public BloomFilter readBloomFilter() {
    Map<byte[], byte[]> fileInfo;
    try {
      fileInfo = reader.loadFileInfo();
      ByteBuffer serializedFilter = reader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK, false);
      byte[] filterBytes = new byte[serializedFilter.remaining()];
      serializedFilter.get(filterBytes); // read the bytes that were written
      return BloomFilterFactory.fromString(new String(filterBytes),
          new String(fileInfo.get(KEY_BLOOM_FILTER_TYPE_CODE.getBytes())));
    } catch (IOException e) {
      throw new HoodieException("Could not read bloom filter from " + path, e);
    }
  }

  @Override
  public Set<String> filterRowKeys(Set candidateRowKeys) {
    try {
      List<Pair<String, R>> allRecords = readAllRecords();
      Set<String> rowKeys = new HashSet<>();
      allRecords.forEach(t -> {
        if (candidateRowKeys.contains(t.getFirst())) {
          rowKeys.add(t.getFirst());
        }
      });
      return rowKeys;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read row keys from " + path, e);
    }
  }

  public List<Pair<String, R>> readAllRecords(Schema schema) throws IOException {
    List<Pair<String, R>> recordList = new LinkedList<>();
    try {
      HFileScanner scanner = reader.getScanner(false, false);
      if (scanner.seekTo()) {
        do {
          Cell c = scanner.getKeyValue();
          byte[] keyBytes = Arrays.copyOfRange(c.getRowArray(), c.getRowOffset(), c.getRowOffset() + c.getRowLength());
          R record = readNextRecord(c, schema);
          recordList.add(new Pair<>(new String(keyBytes), record));
        } while (scanner.next());
      }

      return recordList;
    } catch (IOException e) {
      throw new HoodieException("Error reading hfile " + path + " as a dataframe", e);
    }
  }

  public List<Pair<String, R>> readAllRecords() throws IOException {
    Schema schema = new Schema.Parser().parse(new String(reader.loadFileInfo().get("schema".getBytes())));
    return readAllRecords(schema);
  }

  @Override
  public Iterator getRecordIterator(Schema schema) throws IOException {
    final HFileScanner scanner = reader.getScanner(false, false);
    return new Iterator<R>() {
      private R next = null;
      private boolean eof = false;

      @Override
      public boolean hasNext() {
        try {
          // To handle when hasNext() is called multiple times for idempotency and/or the first time
          if (this.next == null && !this.eof) {
            if (!scanner.isSeeked() && scanner.seekTo()) {
                this.next = (R)readNextRecord(scanner.getKeyValue(), schema);
            }
          }
          return this.next != null;
        } catch (IOException io) {
          throw new HoodieIOException("unable to read next record from hfile ", io);
        }
      }

      @Override
      public R next() {
        try {
          // To handle case when next() is called before hasNext()
          if (this.next == null) {
            if (!hasNext()) {
              throw new HoodieIOException("No more records left to read from hfile");
            }
          }
          R retVal = this.next;
          if (scanner.next()) {
            this.next = (R)readNextRecord(scanner.getKeyValue(), schema);
          } else {
            this.next = null;
            this.eof = true;
          }
          return retVal;
        } catch (IOException io) {
          throw new HoodieIOException("unable to read next record from parquet file ", io);
        }
      }
    };
  }

  private R readNextRecord(Cell c, Schema schema) throws IOException {
    byte[] value = Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
    return (R)HoodieAvroUtils.bytesToAvro(value, schema);
  }

  @Override
  public long getTotalRecords() {
    return reader.getEntries();
  }

  @Override
  public void close() {
    try {
      reader.close();
      reader = null;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  static class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (mark != 0) {
        throw new IllegalStateException();
      }

      reset();
      long skipped = skip(pos);

      if (skipped != pos) {
        throw new IOException();
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {

      if (position >= buf.length) {
        throw new IllegalArgumentException();
      }
      if (position + length > buf.length) {
        throw new IllegalArgumentException();
      }
      if (length > buffer.length) {
        throw new IllegalArgumentException();
      }

      System.arraycopy(buf, (int) position, buffer, offset, length);
      return length;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }
  }
}
