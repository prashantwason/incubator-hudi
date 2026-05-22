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

package org.apache.hudi.avro;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the deprecated legacy 4-arg constructor on {@link HoodieAvroWriteSupport},
 * kept for binary compatibility with external write-support subclasses (e.g. crypto-aware
 * write-support implementations) compiled against the pre-HoodieSchema API.
 */
public class TestHoodieAvroWriteSupportLegacyCtor {

  private static final Schema AVRO_SCHEMA = Schema.createRecord(
      "Rec", null, "hudi.test", false,
      java.util.Collections.singletonList(
          new Schema.Field("v", Schema.create(Schema.Type.STRING), null, null)));

  private static MessageType parquetSchema() {
    return Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.OPTIONAL)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("v"))
        .named("Rec");
  }

  @Test
  public void legacyCtorPopulatesPropertiesFromMap() {
    Map<String, String> writeProps = new HashMap<>();
    writeProps.put("hoodie.foo", "bar");
    writeProps.put("hoodie.baz", "qux");

    HoodieAvroWriteSupport<?> ws = new HoodieAvroWriteSupport<>(
        parquetSchema(), AVRO_SCHEMA, Option.<BloomFilter>empty(), writeProps);

    assertNotNull(ws.properties);
    assertEquals("bar", ws.properties.getProperty("hoodie.foo"));
    assertEquals("qux", ws.properties.getProperty("hoodie.baz"));
    assertEquals(2, ws.properties.size());
  }

  @Test
  public void legacyCtorTreatsNullMapAsEmptyProperties() {
    HoodieAvroWriteSupport<?> ws = new HoodieAvroWriteSupport<>(
        parquetSchema(), AVRO_SCHEMA, Option.<BloomFilter>empty(), (Map<String, String>) null);

    assertNotNull(ws.properties);
    assertEquals(0, ws.properties.size());
  }

  @Test
  public void legacyCtorDropsNullKeysAndValues() {
    Map<String, String> writeProps = new HashMap<>();
    writeProps.put("present", "v");
    writeProps.put("nullValue", null);
    writeProps.put(null, "orphan");

    HoodieAvroWriteSupport<?> ws = new HoodieAvroWriteSupport<>(
        parquetSchema(), AVRO_SCHEMA, Option.<BloomFilter>empty(), writeProps);

    assertEquals(1, ws.properties.size(), "only the (present, v) entry should survive");
    assertEquals("v", ws.properties.getProperty("present"));
    assertNull(ws.properties.getProperty("nullValue"));
    assertNull(ws.properties.getProperty(""));
  }

  @Test
  public void legacyCtorRoundTripsToNewCtorBehavior() {
    Properties expected = new Properties();
    expected.setProperty("a", "1");
    expected.setProperty("b", "2");

    Map<String, String> asMap = new HashMap<>();
    asMap.put("a", "1");
    asMap.put("b", "2");

    HoodieAvroWriteSupport<?> viaLegacy = new HoodieAvroWriteSupport<>(
        parquetSchema(), AVRO_SCHEMA, Option.<BloomFilter>empty(), asMap);
    HoodieAvroWriteSupport<?> viaNew = new HoodieAvroWriteSupport<>(
        parquetSchema(),
        org.apache.hudi.common.schema.HoodieSchema.fromAvroSchema(AVRO_SCHEMA),
        Option.<BloomFilter>empty(), expected);

    assertEquals(viaNew.properties, viaLegacy.properties);
  }
}
