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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the constructor-selection logic in
 * {@link HoodieAvroFileWriterFactory#newParquetFileWriter} for user-configured
 * {@code hoodie.avro.write.support.class} values.
 */
public class TestHoodieAvroFileWriterFactoryCtorSelection {

  @TempDir
  java.nio.file.Path tmpDir;

  private HoodieStorage storage;
  private HoodieSchema schema;
  private final LocalTaskContextSupplier supplier = new LocalTaskContextSupplier();

  @BeforeEach
  public void setUp() {
    storage = HoodieTestUtils.getStorage(tmpDir.toString());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    List<GenericRecord> records = dataGen.generateGenericRecords(1);
    schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());
    NewOnlyWriteSupportFixture.CONSTRUCTED.set(0);
    LegacyOnlyWriteSupportFixture.CONSTRUCTED.set(0);
  }

  @Test
  public void defaultWriteSupportUsesNewCtor() throws Exception {
    // Sanity: the stock HoodieAvroWriteSupport (default value of HOODIE_AVRO_WRITE_SUPPORT_CLASS)
    // exposes the new (MessageType, HoodieSchema, Option, Properties) ctor and is preferred.
    HoodieConfig config = new HoodieConfig();
    StoragePath path = new StoragePath(tmpDir.resolve("default.parquet").toAbsolutePath().toString());

    try (HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("100", path, config, schema, supplier)) {
      assertTrue(writer instanceof HoodieAvroParquetWriter);
    }
    assertTrue(storage.exists(path));
  }

  @Test
  public void subclassWithOnlyNewCtorIsInstantiatedViaNewCtor() throws Exception {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.HOODIE_AVRO_WRITE_SUPPORT_CLASS,
        NewOnlyWriteSupportFixture.class.getName());
    StoragePath path = new StoragePath(tmpDir.resolve("new_only.parquet").toAbsolutePath().toString());

    try (HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("101", path, config, schema, supplier)) {
      assertTrue(writer instanceof HoodieAvroParquetWriter);
    }
    assertEquals(1, NewOnlyWriteSupportFixture.CONSTRUCTED.get(),
        "New-ctor fixture should be instantiated exactly once");
    assertEquals(0, LegacyOnlyWriteSupportFixture.CONSTRUCTED.get(),
        "Legacy-ctor fixture should not be touched");
  }

  @Test
  public void subclassWithOnlyLegacyCtorFallsBackToLegacyCtor() throws Exception {
    // Mirrors the shape of external pre-HoodieSchema write-support subclasses (e.g. crypto-aware
    // write-support implementations): (MessageType, Schema, Option<BloomFilter>, Map<String,String>) only.
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.HOODIE_AVRO_WRITE_SUPPORT_CLASS,
        LegacyOnlyWriteSupportFixture.class.getName());
    StoragePath path = new StoragePath(tmpDir.resolve("legacy_only.parquet").toAbsolutePath().toString());

    try (HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("102", path, config, schema, supplier)) {
      assertTrue(writer instanceof HoodieAvroParquetWriter);
    }
    assertEquals(1, LegacyOnlyWriteSupportFixture.CONSTRUCTED.get(),
        "Legacy fallback ctor should be invoked exactly once");
    assertEquals(0, NewOnlyWriteSupportFixture.CONSTRUCTED.get());
  }

  @Test
  public void subclassWithNeitherCtorThrowsDescriptiveException() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.HOODIE_AVRO_WRITE_SUPPORT_CLASS,
        NoMatchingCtorFixture.class.getName());
    StoragePath path = new StoragePath(tmpDir.resolve("no_match.parquet").toAbsolutePath().toString());

    HoodieException ex = assertThrows(HoodieException.class, () ->
        new HoodieAvroFileWriterFactory(storage)
            .newParquetFileWriter("103", path, config, schema, supplier));
    String msg = ex.getMessage();
    assertTrue(msg.contains(NoMatchingCtorFixture.class.getName()), () -> "missing class name: " + msg);
    assertTrue(msg.contains("HoodieSchema"), () -> "missing new-ctor hint: " + msg);
    assertTrue(msg.contains("Map<String,String>"), () -> "missing legacy-ctor hint: " + msg);
  }

  /**
   * Fixture: exposes only the modern ctor. Used to confirm the preferred branch is taken.
   */
  public static class NewOnlyWriteSupportFixture extends HoodieAvroWriteSupport {
    public static final AtomicInteger CONSTRUCTED = new AtomicInteger(0);

    public NewOnlyWriteSupportFixture(MessageType schema, HoodieSchema hoodieSchema,
                                      Option<BloomFilter> bloomFilterOpt, Properties properties) {
      super(schema, hoodieSchema, bloomFilterOpt, properties);
      CONSTRUCTED.incrementAndGet();
    }
  }

  /**
   * Fixture: exposes only the legacy ctor (mirroring external subclasses compiled
   * against the pre-HoodieSchema API, e.g. external crypto-aware write-support subclasses).
   */
  public static class LegacyOnlyWriteSupportFixture extends HoodieAvroWriteSupport {
    public static final AtomicInteger CONSTRUCTED = new AtomicInteger(0);

    public LegacyOnlyWriteSupportFixture(MessageType schema, Schema avroSchema,
                                         Option<BloomFilter> bloomFilterOpt,
                                         Map<String, String> writeConfigProps) {
      super(schema, avroSchema, bloomFilterOpt, writeConfigProps);
      CONSTRUCTED.incrementAndGet();
    }
  }

  /**
   * Fixture: neither the new nor the legacy ctor is present. Reflection probes should
   * miss both and the factory should throw a descriptive {@link HoodieException}.
   */
  public static class NoMatchingCtorFixture extends HoodieAvroWriteSupport {
    public NoMatchingCtorFixture(MessageType schema) {
      super(schema, HoodieSchema.fromAvroSchema(
          Schema.createRecord("Empty", null, "hudi.test", false,
              java.util.Collections.emptyList())),
          Option.empty(), new Properties());
    }
  }
}
