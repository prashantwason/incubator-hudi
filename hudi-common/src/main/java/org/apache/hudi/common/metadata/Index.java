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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class Index {
  protected final Logger logger;
  protected Configuration conf;
  protected Path path;

  protected static final String STRING_ENCODING = "UTF-8";

  public Index(Configuration conf, Path path) {
    this.conf = conf;
    this.path = path;
    this.logger = LogManager.getLogger(this.getClass());
  }

  public abstract void create(Option<Map<String, String>> metadata) throws IOException;

  public abstract void load() throws IOException;

  public abstract void put(byte[] key, byte[] value) throws IOException;

  public void put(String key, byte[] value) throws IOException {
    put(key.getBytes(STRING_ENCODING), value);
  }

  public void put(byte[] key, String value) throws IOException {
    put(key, value.getBytes(STRING_ENCODING));
  }

  public void put(int key, byte[] value) throws IOException {
    put(intToBytes(key), value);
  }

  public void put(Integer key, String value) throws Exception {
    put(intToBytes(key), value.getBytes(STRING_ENCODING));
  }

  public void put(byte[] key, Collection<Integer> value) throws IOException {
    final int encoded_size = intToBytes(0).length;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(value.size() * encoded_size);
    assert !value.isEmpty();
    for (int i : value) {
      bos.write(intToBytes(i));
    }

    assert (value.size() * encoded_size) == bos.size();
    put(key, bos.toByteArray());
    bos.close();
  }

  public void put(String key, List<Integer> value) throws IOException {
    put(key.getBytes(STRING_ENCODING), value);
  }

  public abstract void commit() throws IOException;

  public abstract byte[] getValue(byte[] key) throws IOException;

  public byte[] getValue(String key) throws IOException {
    return getValue(key.getBytes(STRING_ENCODING));
  }

  public String getValueAsString(int key) throws IOException {
    return getValueAsString(intToBytes(key));
  }

  public String getValueAsString(byte[] key) throws IOException {
    byte[] value = getValue(key);
    if (value == null) {
      return null;
    }
    return new String(value);
  }

  public List<Integer> getValueAsListOfInteger(String key) throws IOException {
    return getValueAsListOfInteger(key.getBytes(STRING_ENCODING));
  }

  public List<Integer> getValueAsListOfInteger(byte[] key) throws IOException {
    byte[] value = getValue(key);
    return decodeAsListOfInteger(value);
  }

  protected abstract void scan(byte[] startKey, BiFunction<byte[], byte[], Boolean> iterFunc) throws IOException;

  protected void scan(int startKey, BiFunction<byte[], byte[], Boolean> iterFunc) throws IOException {
    scan(intToBytes(startKey), iterFunc);
  }

  protected abstract Map<String, String> getMetadata() throws IOException;

  protected byte[] serializeMetadata(Option<Map<String, String>> metadata) throws UnsupportedEncodingException {
    Map<String, String> map = metadata.orElse(new HashMap<>());
    return map.keySet().stream()
      .map(key -> key + "=" + map.get(key))
      .collect(Collectors.joining("\0"))
      .getBytes(STRING_ENCODING);
  }

  protected Map<String, String> deserializeMetadata(String metadataStr) {
    return Arrays.stream(metadataStr.split("\0"))
      .map(entry -> entry.split("="))
      .collect(Collectors.toMap(entry -> entry[0], entry -> entry[1]));
  }

  public static byte[] intToBytes(int value) {
    byte[] d = new byte[4];
    d[0] = (byte)((value >> 24) & 0xFF);
    d[1] = (byte)((value >> 16) & 0xFF);
    d[2] = (byte)((value >> 8) & 0xFF);
    d[3] = (byte)(value & 0xFF);

    return d;
  }

  public static int bytesToInt(byte[] value) {
    int d = 0;
    d |= ((int)value[0] & 0xFF) << 24;
    d |= ((int)value[1] & 0xFF) << 16;
    d |= ((int)value[2] & 0xFF) << 8;
    d |= ((int)value[3]) & 0xFF;

    return d;
  }

  public static List<Integer> decodeAsListOfInteger(byte[] value) {
    if (value == null) {
      return null;
    }

    List<Integer> result = new LinkedList<>();
    final int encoded_size = intToBytes(0).length;
    assert value.length % encoded_size == 0;

    byte[] buf = new byte[encoded_size];
    ByteArrayInputStream bis = new ByteArrayInputStream(value);
    while (bis.read(buf, 0, buf.length) != -1) {
      result.add(bytesToInt(buf));
    }

    return result;
  }

  public Path getPath() {
    return path;
  }
}
