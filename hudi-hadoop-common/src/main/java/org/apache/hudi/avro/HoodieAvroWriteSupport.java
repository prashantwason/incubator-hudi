/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.avro;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Wrap AvroWriterSupport for plugging in the bloom filter.
 */
public class HoodieAvroWriteSupport<T> extends AvroWriteSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAvroWriteSupport.class);

  private final Option<HoodieBloomFilterWriteSupport<String>> bloomFilterWriteSupportOpt;
  private final Map<String, String> footerMetadata = new HashMap<>();
  protected final Properties properties;

  public HoodieAvroWriteSupport(MessageType schema, HoodieSchema hoodieSchema, Option<BloomFilter> bloomFilterOpt,
                                Properties properties) {
    super(schema, hoodieSchema.toAvroSchema(), ConvertingGenericData.INSTANCE);
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterAvroWriteSupport::new);
    this.properties = properties;
    String vectorMeta = HoodieSchema.buildVectorColumnsMetadataValue(hoodieSchema);
    if (!vectorMeta.isEmpty()) {
      footerMetadata.put(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY, vectorMeta);
    }
  }

  /**
   * Retained for binary compatibility with external write-support subclasses
   * (e.g. crypto extensions) compiled against the pre-HoodieSchema API.
   * New code should use the {@code HoodieSchema}/{@code Properties} constructor.
   */
  @Deprecated
  public HoodieAvroWriteSupport(MessageType schema, Schema avroSchema, Option<BloomFilter> bloomFilterOpt,
                                Map<String, String> writeConfigProps) {
    this(schema, HoodieSchema.fromAvroSchema(
            Objects.requireNonNull(avroSchema, "avroSchema must not be null on the deprecated 4-arg ctor; "
                + "external subclasses calling super(...) must pass a non-null Avro Schema")),
        bloomFilterOpt, mapToProperties(writeConfigProps));
    LOG.warn("HoodieAvroWriteSupport constructed via the deprecated 4-arg (MessageType, Schema, Option<BloomFilter>, "
        + "Map<String,String>) constructor. This path exists only for binary compatibility with external subclasses "
        + "compiled against the pre-HoodieSchema API; please migrate the caller to the (MessageType, HoodieSchema, "
        + "Option<BloomFilter>, Properties) constructor.");
  }

  private static Properties mapToProperties(Map<String, String> map) {
    Properties props = new Properties();
    if (map != null) {
      map.forEach((k, v) -> {
        if (k != null && v != null) {
          props.setProperty(k, v);
        }
      });
    }
    return props;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        CollectionUtils.combine(footerMetadata,
            bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
                .orElse(Collections.emptyMap())
        );

    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void add(String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
  }

  public void addFooterMetadata(String key, String value) {
    footerMetadata.put(key, value);
  }

  private static class HoodieBloomFilterAvroWriteSupport extends HoodieBloomFilterWriteSupport<String> {
    public HoodieBloomFilterAvroWriteSupport(BloomFilter bloomFilter) {
      super(bloomFilter);
    }

    @Override
    protected byte[] getUTF8Bytes(String key) {
      return StringUtils.getUTF8Bytes(key);
    }
  }
}
