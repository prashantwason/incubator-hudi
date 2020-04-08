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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieMetadataException;

public class IndexHFile extends Index {
  private static int BlockSize = 4 * 1024;
  private static Algorithm CompressionAlogrithm = Algorithm.GZ;
  private static byte[] KEY_METADATA = {0};

  private HFile.Writer writer;
  protected HFile.Reader reader;
  protected HFileScanner scanner;

  public IndexHFile(Configuration conf, Path path) {
    super(conf, path);
  }

  @Override
  public void create(Option<Map<String, String>> metadata) throws IOException {
    FileSystem fs = FSUtils.getFs(path.toString(), conf);
    HFileContext context = new HFileContextBuilder().withBlockSize(BlockSize).withCompression(CompressionAlogrithm)
        .build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    writer = HFile.getWriterFactory(conf, cacheConfig).withPath(fs, path).withFileContext(context).create();
    writer.append(toKV(KEY_METADATA, serializeMetadata(metadata)));
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    if (Arrays.equals(KEY_METADATA, key)) {
      throw new IOException("Invalid key " + Arrays.toString(key) + ". key cannot be [0].");
    }
    if (key == null) {
      throw new IOException("Invalid key. key cannot be null.");
    }
    if (value == null) {
      throw new IOException("Invalid value. value cannot be null.");
    }

    writer.append(toKV(key, value));
  }

  @Override
  public void commit() throws IOException {
    writer.close();
    writer = null;
  }

  @Override
  public void load() throws FileNotFoundException, IOException {
    FileSystem fs = FSUtils.getFs(path.toString(), conf);
    CacheConfig config = new CacheConfig(conf);
    reader = HFile.createReader(fs, path, config, conf);
    scanner = reader.getScanner(true, true);
  }

  @Override
  public byte[] getValue(byte[] key) throws IOException {
    int ret = scanner.seekTo(toKV(key, null));
    if (ret != 0) {
      logger.warn("Could not seek to key " + Arrays.toString(key) + ": ret=" + ret + ", nextKey="
            + Arrays.toString(scanner.getKeyString().getBytes()));
      return null;
    }

    Cell c = scanner.getKeyValue();
    byte[] keyBytes = Arrays.copyOfRange(c.getRowArray(), c.getRowOffset(), c.getRowOffset() + c.getRowLength());
    byte[] value = Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
    if (!Arrays.equals(key, keyBytes)) {
      throw new HoodieMetadataException("getValue error: key and keybytes are not equal: " + Arrays.toString(key)
          + " " + Arrays.toString(keyBytes));
    }

    return value;
  }

  @Override
  public void scan(byte[] startKey, BiFunction<byte[], byte[], Boolean> iterFunc) throws IOException {
    if (scanner.seekTo(toKV(startKey, null)) == 1) {
      return;
    }

    do {
      Cell c = scanner.getKeyValue();
      byte[] key = Arrays.copyOfRange(c.getRowArray(), c.getRowOffset(), c.getRowOffset() + c.getRowLength());
      byte[] value = Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
      if (!iterFunc.apply(key, value)) {
        break;
      }
    } while (scanner.next());
  }

  @Override
  public Map<String, String> getMetadata() throws IOException {
    byte[] value = getValue(KEY_METADATA);
    return deserializeMetadata(value == null ? "" : new String(value));
  }

  static KeyValue toKV(byte[] key, byte[] value) {
    return new KeyValue(key, null, null, value);
  }
}
