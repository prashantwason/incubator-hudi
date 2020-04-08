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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;

public class IndexTFile extends Index {
  private static int BlockSize = 4 * 1024;
  private static String KeyComparator = "memcmp";
  private static String CompressionAlogrithm = TFile.COMPRESSION_GZ;
  private static byte[] KEY_METADATA = {0};

  private FSDataOutputStream fos;
  private TFile.Writer writer;
  protected TFile.Reader reader;
  protected TFile.Reader.Scanner scanner;
  private BytesWritable keyBytesWritable;

  public IndexTFile(Configuration conf, Path path) {
    super(conf, path);
  }

  @Override
  public void create(Option<Map<String, String>> metadata) throws IOException {
    fos = FSUtils.getFs(path.toString(), conf).create(path);
    writer = new TFile.Writer(fos, BlockSize, CompressionAlogrithm, KeyComparator, conf);
    writer.append(KEY_METADATA, serializeMetadata(metadata));
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

    writer.append(key, value);
  }

  @Override
  public void commit() throws IOException {
    writer.close();
    fos.flush();
    fos.close();

    writer = null;
    fos = null;
  }

  @Override
  public void load() throws FileNotFoundException, IOException {
    FileSystem fs = FSUtils.getFs(path.toString(), conf);
    FileStatus[] statuses = fs.listStatus(path);
    FSDataInputStream fis = fs.open(path);

    reader = new TFile.Reader(fis, statuses[0].getLen(), conf);
    scanner = reader.createScanner();
    keyBytesWritable = new BytesWritable();
  }

  @Override
  public byte[] getValue(byte[] key) throws IOException {
    return readFullValue(readEntryStream(key));
  }

  @Override
  public void scan(byte[] startKey, BiFunction<byte[], byte[], Boolean> iterFunc) throws IOException {
    scanner.seekTo(startKey);
    while (!scanner.atEnd()) {
      Entry entry = scanner.entry();
      entry.getKey(keyBytesWritable);
      byte[] value = readFullValue(entry.getValueStream());
      if (!iterFunc.apply(keyBytesWritable.copyBytes(), value)) {
        break;
      }

      if (!scanner.advance()) {
        break;
      }
    }
  }

  @Override
  public Map<String, String> getMetadata() throws IOException {
    byte[] value = getValue(KEY_METADATA);
    return deserializeMetadata(value == null ? "" : new String(value));
  }

  private DataInputStream readEntryStream(byte[] key) throws IOException {
    if (reader == null) {
      throw new RuntimeException("No index file loaded");
    }

    boolean found = scanner.seekTo(key);
    if (!found) {
      return null;
    }

    Entry entry = scanner.entry();
    entry.getKey(keyBytesWritable);
    assert Arrays.equals(keyBytesWritable.copyBytes(), key);
    return entry.getValueStream();
  }

  private byte[] readFullValue(DataInputStream dis) throws IOException {
    if (dis == null) {
      return null;
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] bbuf = new byte[64];
    int read = 0;
    while ((read = dis.read(bbuf, 0, bbuf.length)) != -1) {
      bos.write(bbuf, 0, read);
    }

    dis.close();
    return bos.toByteArray();
  }
}
