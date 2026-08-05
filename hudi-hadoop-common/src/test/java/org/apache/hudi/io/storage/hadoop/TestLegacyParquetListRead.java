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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for reading parquet files written with legacy 2-level list encoding
 * via {@link HoodieAvroParquetReader}, which sets ADD_LIST_ELEMENT_RECORDS=false.
 *
 * Validates that single-field records used as array elements (e.g., array<struct<name:string>>)
 * can be correctly read regardless of the parquet list encoding used when writing.
 */
public class TestLegacyParquetListRead {

  @TempDir
  public java.nio.file.Path tempDir;

  /**
   * Reproduces the ClassCastException scenario:
   * 1. Write parquet with legacy 2-level list encoding (WRITE_OLD_LIST_STRUCTURE=true)
   *    using a schema with array<struct<single_field>> (single-field record as array element)
   * 2. Read via HoodieAvroParquetReader (which sets ADD_LIST_ELEMENT_RECORDS=false)
   * 3. Verify reading succeeds without ClassCastException
   */
  @Test
  public void testReadLegacyParquetWithSingleFieldRecordArrayElements() throws Exception {
    // Schema: array<struct<type:string, values:array<struct<name:string>>>>
    // This matches the real badges_v2 field from schemaless_hbostore_udr-contact
    Schema badgeValueSchema = SchemaBuilder.record("BadgeValue")
        .namespace("test")
        .fields()
        .name("name").type().optional().stringType()
        .endRecord();

    Schema badgeValueArraySchema = Schema.createArray(badgeValueSchema);

    Schema badgeSchema = SchemaBuilder.record("Badge")
        .namespace("test")
        .fields()
        .name("type").type().optional().stringType()
        .name("values").type().optional().type(badgeValueArraySchema)
        .endRecord();

    Schema badgeArraySchema = Schema.createArray(badgeSchema);

    Schema rootSchema = SchemaBuilder.record("TestRecord")
        .namespace("test")
        .fields()
        .requiredString("id")
        .name("badges_v2").type().optional().type(badgeArraySchema)
        .endRecord();

    // Build test data
    GenericRecord badgeValue1 = new GenericData.Record(badgeValueSchema);
    badgeValue1.put("name", "premium");
    GenericRecord badgeValue2 = new GenericData.Record(badgeValueSchema);
    badgeValue2.put("name", "vip");

    GenericRecord badge = new GenericData.Record(badgeSchema);
    badge.put("type", "category");
    badge.put("values", Arrays.asList(badgeValue1, badgeValue2));

    GenericRecord record = new GenericData.Record(rootSchema);
    record.put("id", "record-1");
    record.put("badges_v2", Collections.singletonList(badge));

    // Write with legacy 2-level list encoding
    Path outputPath = new Path(tempDir.toUri().toString(), "test_legacy_list.parquet");
    Configuration writeConf = new Configuration();
    writeConf.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "true");

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
        .withSchema(rootSchema)
        .withConf(writeConf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {
      writer.write(record);
    }

    // Read via HoodieAvroParquetReader (sets ADD_LIST_ELEMENT_RECORDS=false internally)
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    StoragePath storagePath = new StoragePath(outputPath.toUri());
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(rootSchema);

    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, storagePath)) {
      try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(hoodieSchema)) {
        assertTrue(iterator.hasNext(), "Should have at least one record");
        GenericRecord result = (GenericRecord) iterator.next();

        assertEquals("record-1", result.get("id").toString());
        List<?> badges = (List<?>) result.get("badges_v2");
        assertEquals(1, badges.size());

        GenericRecord resultBadge = (GenericRecord) badges.get(0);
        assertEquals("category", resultBadge.get("type").toString());

        List<?> values = (List<?>) resultBadge.get("values");
        assertEquals(2, values.size());
        GenericRecord val0 = (GenericRecord) values.get(0);
        assertEquals("premium", val0.get("name").toString());
        GenericRecord val1 = (GenericRecord) values.get(1);
        assertEquals("vip", val1.get("name").toString());
      }
    }
  }

  /**
   * Simulates the Avro→Spark→Avro round-trip that the Aegis boundary framework performs.
   * Spark's type system loses Avro record names, so the round-tripped schema has generic
   * names (e.g., "values" instead of "BadgeValue", "badges_v2" instead of "Badge").
   * On parquet < 1.13.8, this triggers a ClassCastException in isElementType() because
   * the static AvroSchemaConverter produces a parquet schema with mismatched record names,
   * causing single-field records to be misidentified as the unwrapped element type.
   */
  @Test
  public void testReadLegacyParquetWithRoundTrippedSchema() throws Exception {
    // Original schema with named records
    Schema badgeValueSchema = SchemaBuilder.record("BadgeValue")
        .namespace("test")
        .fields()
        .name("name").type().optional().stringType()
        .endRecord();

    Schema badgeSchema = SchemaBuilder.record("Badge")
        .namespace("test")
        .fields()
        .name("type").type().optional().stringType()
        .name("values").type().optional().type(Schema.createArray(badgeValueSchema))
        .endRecord();

    Schema rootSchema = SchemaBuilder.record("TestRecord")
        .namespace("test")
        .fields()
        .requiredString("id")
        .name("badges_v2").type().optional().type(Schema.createArray(badgeSchema))
        .endRecord();

    // Build test data
    GenericRecord badgeValue1 = new GenericData.Record(badgeValueSchema);
    badgeValue1.put("name", "premium");
    GenericRecord badgeValue2 = new GenericData.Record(badgeValueSchema);
    badgeValue2.put("name", "vip");

    GenericRecord badge = new GenericData.Record(badgeSchema);
    badge.put("type", "category");
    badge.put("values", Arrays.asList(badgeValue1, badgeValue2));

    GenericRecord record = new GenericData.Record(rootSchema);
    record.put("id", "record-1");
    record.put("badges_v2", Collections.singletonList(badge));

    // Write with legacy 2-level list encoding using the original schema
    Path outputPath = new Path(tempDir.toUri().toString(), "test_roundtrip_schema.parquet");
    Configuration writeConf = new Configuration();
    writeConf.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "true");

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
        .withSchema(rootSchema)
        .withConf(writeConf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {
      writer.write(record);
    }

    // Simulate the Avro→Spark→Avro round-trip: record names change.
    // Spark converts array<Badge{type,values:array<BadgeValue{name}>}> to
    // StructType, then back to Avro, producing generic record names derived
    // from the field name (e.g., "badges_v2" instead of "Badge", "values" instead of "BadgeValue").
    Schema roundTrippedBadgeValueSchema = SchemaBuilder.record("values")
        .namespace("test")
        .fields()
        .name("name").type().optional().stringType()
        .endRecord();

    Schema roundTrippedBadgeSchema = SchemaBuilder.record("badges_v2")
        .namespace("test")
        .fields()
        .name("type").type().optional().stringType()
        .name("values").type().optional().type(Schema.createArray(roundTrippedBadgeValueSchema))
        .endRecord();

    Schema roundTrippedRootSchema = SchemaBuilder.record("record")
        .namespace("aegis")
        .fields()
        .requiredString("id")
        .name("badges_v2").type().optional().type(Schema.createArray(roundTrippedBadgeSchema))
        .endRecord();

    // Read the legacy-encoded file using the round-tripped schema (different record names).
    // On parquet < 1.13.8, isElementType() fails because the static AvroSchemaConverter
    // produces a parquet schema where the record name doesn't match the file's record name,
    // causing a ClassCastException when a single-field record element is unwrapped to its
    // primitive type.
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    StoragePath storagePath = new StoragePath(outputPath.toUri());
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(roundTrippedRootSchema);

    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, storagePath)) {
      try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(hoodieSchema)) {
        assertTrue(iterator.hasNext(), "Should have at least one record");
        GenericRecord result = (GenericRecord) iterator.next();

        assertEquals("record-1", result.get("id").toString());
        List<?> badges = (List<?>) result.get("badges_v2");
        assertEquals(1, badges.size());

        GenericRecord resultBadge = (GenericRecord) badges.get(0);
        assertEquals("category", resultBadge.get("type").toString());

        List<?> values = (List<?>) resultBadge.get("values");
        assertEquals(2, values.size());
        GenericRecord val0 = (GenericRecord) values.get(0);
        assertEquals("premium", val0.get("name").toString());
        GenericRecord val1 = (GenericRecord) values.get(1);
        assertEquals("vip", val1.get("name").toString());
      }
    }
  }

  /**
   * Same test but with non-legacy 3-level list encoding, as a control.
   */
  @Test
  public void testReadNonLegacyParquetWithSingleFieldRecordArrayElements() throws Exception {
    Schema badgeValueSchema = SchemaBuilder.record("BadgeValue")
        .namespace("test")
        .fields()
        .name("name").type().optional().stringType()
        .endRecord();

    Schema badgeValueArraySchema = Schema.createArray(badgeValueSchema);

    Schema badgeSchema = SchemaBuilder.record("Badge")
        .namespace("test")
        .fields()
        .name("type").type().optional().stringType()
        .name("values").type().optional().type(badgeValueArraySchema)
        .endRecord();

    Schema badgeArraySchema = Schema.createArray(badgeSchema);

    Schema rootSchema = SchemaBuilder.record("TestRecord")
        .namespace("test")
        .fields()
        .requiredString("id")
        .name("badges_v2").type().optional().type(badgeArraySchema)
        .endRecord();

    GenericRecord badgeValue1 = new GenericData.Record(badgeValueSchema);
    badgeValue1.put("name", "gold");

    GenericRecord badge = new GenericData.Record(badgeSchema);
    badge.put("type", "tier");
    badge.put("values", Collections.singletonList(badgeValue1));

    GenericRecord record = new GenericData.Record(rootSchema);
    record.put("id", "record-2");
    record.put("badges_v2", Collections.singletonList(badge));

    // Write with non-legacy 3-level list encoding
    Path outputPath = new Path(tempDir.toUri().toString(), "test_nonlegacy_list.parquet");
    Configuration writeConf = new Configuration();
    writeConf.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
        .withSchema(rootSchema)
        .withConf(writeConf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {
      writer.write(record);
    }

    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    StoragePath storagePath = new StoragePath(outputPath.toUri());
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(rootSchema);

    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, storagePath)) {
      try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(hoodieSchema)) {
        assertTrue(iterator.hasNext());
        GenericRecord result = (GenericRecord) iterator.next();

        assertEquals("record-2", result.get("id").toString());
        List<?> badges = (List<?>) result.get("badges_v2");
        assertEquals(1, badges.size());

        GenericRecord resultBadge = (GenericRecord) badges.get(0);
        assertEquals("tier", resultBadge.get("type").toString());

        List<?> values = (List<?>) resultBadge.get("values");
        assertEquals(1, values.size());
        GenericRecord val0 = (GenericRecord) values.get(0);
        assertEquals("gold", val0.get("name").toString());
      }
    }
  }
}
