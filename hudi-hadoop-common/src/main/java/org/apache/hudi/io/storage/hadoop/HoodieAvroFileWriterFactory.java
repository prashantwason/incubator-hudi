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

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieParquetConfigInjector;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieHFileConfig;
import org.apache.hudi.io.storage.HoodieOrcConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.avro.Schema;
import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_WRITER_TO_ALLOW_DUPLICATES;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;

public class HoodieAvroFileWriterFactory extends HoodieFileWriterFactory {

  /**
   * Cached resolution of which constructor variant a given HoodieAvroWriteSupport class exposes.
   * Keyed by FQN. Reflection results don't change for the lifetime of the JVM, so this is safe
   * to share statically and avoids paying the {@link ReflectionUtils#hasConstructor} cost for
   * every Parquet file the factory creates.
   */
  private static final ConcurrentHashMap<String, CtorVariant> WRITE_SUPPORT_CTOR_CACHE = new ConcurrentHashMap<>();

  private enum CtorVariant {
    /** Modern (MessageType, HoodieSchema, Option&lt;BloomFilter&gt;, Properties). */
    NEW,
    /** Legacy (MessageType, Schema, Option&lt;BloomFilter&gt;, Map&lt;String,String&gt;) for pre-HoodieSchema subclasses. */
    LEGACY,
    /** Class exposes neither recognized constructor. */
    UNSUPPORTED
  }

  public HoodieAvroFileWriterFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);

    Pair<StorageConfiguration, HoodieConfig> injectedConfigs = HoodieParquetConfigInjector.applyConfigInjector(path, storage.getConf(), config);
    StorageConfiguration storageConfiguration = injectedConfigs.getLeft();
    HoodieConfig hoodieConfig = injectedConfigs.getRight();

    HoodieAvroWriteSupport writeSupport = getHoodieAvroWriteSupport(schema, hoodieConfig, storageConfiguration, enableBloomFilter(populateMetaFields, hoodieConfig));

    String compressionCodecName = hoodieConfig.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    // Support PARQUET_COMPRESSION_CODEC_NAME is ""
    if (compressionCodecName.isEmpty()) {
      compressionCodecName = null;
    }
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
        CompressionCodecName.fromConf(compressionCodecName),
        hoodieConfig.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        hoodieConfig.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        hoodieConfig.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        storageConfiguration, hoodieConfig.getDoubleOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        hoodieConfig.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieAvroParquetWriter(path, parquetConfig, instantTime, taskContextSupplier, populateMetaFields);
  }

  protected HoodieFileWriter newParquetFileWriter(
      OutputStream outputStream, HoodieConfig config, HoodieSchema schema) throws IOException {
    String configInjectorClass = config.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, StringUtils.EMPTY_STRING);
    if (!StringUtils.isNullOrEmpty(configInjectorClass)) {
      throw new HoodieException("hoodie.parquet.write.config.injector.class is not supported with streaming writes with parquet");
    }
    HoodieAvroWriteSupport writeSupport = getHoodieAvroWriteSupport(schema, config, storage.getConf(), false);
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
        CompressionCodecName.fromConf(config.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE), // todo: 1024*1024*1024
        storage.getConf(), config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieParquetStreamWriter(new FSDataOutputStream(outputStream, null), parquetConfig);
  }

  protected HoodieFileWriter newHFileFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieHFileConfig hfileConfig = new HoodieHFileConfig(
        storage.getConf(),
        CompressionCodec.findCodecByName(
            config.getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME)),
        config.getInt(HoodieStorageConfig.HFILE_BLOCK_SIZE),
        config.getLong(HoodieStorageConfig.HFILE_MAX_FILE_SIZE),
        HoodieAvroHFileReaderImplBase.KEY_FIELD_NAME,
        filter,
        config.getBoolean(HFILE_WRITER_TO_ALLOW_DUPLICATES));
    return new HoodieAvroHFileWriter(instantTime, path, hfileConfig, schema, taskContextSupplier, config.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  protected HoodieFileWriter newOrcFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieOrcConfig orcConfig = new HoodieOrcConfig(storage.getConf(),
        CompressionKind.valueOf(config.getString(HoodieStorageConfig.ORC_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.ORC_STRIPE_SIZE),
        config.getInt(HoodieStorageConfig.ORC_BLOCK_SIZE),
        config.getLong(HoodieStorageConfig.ORC_FILE_MAX_SIZE), filter);
    return new HoodieAvroOrcWriter(instantTime, path, orcConfig, schema, taskContextSupplier);
  }

  private HoodieAvroWriteSupport getHoodieAvroWriteSupport(HoodieSchema schema,
                                                           HoodieConfig config,
                                                           StorageConfiguration storageConf,
                                                           boolean enableBloomFilter) {
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    String writeSupportClass = config.getStringOrDefault(HoodieStorageConfig.HOODIE_AVRO_WRITE_SUPPORT_CLASS);
    MessageType parquetSchema = getAvroSchemaConverter(
        (Configuration) storageConf.unwrapAs(Configuration.class)).convert(schema);

    CtorVariant variant = WRITE_SUPPORT_CTOR_CACHE.computeIfAbsent(writeSupportClass,
        HoodieAvroFileWriterFactory::resolveCtorVariant);

    switch (variant) {
      case NEW:
        // Preferred (current) ctor: (MessageType, HoodieSchema, Option<BloomFilter>, Properties)
        try {
          return (HoodieAvroWriteSupport) ReflectionUtils.loadClass(
              writeSupportClass, NEW_CTOR_SIGNATURE,
              parquetSchema, schema, filter, config.getProps());
        } catch (Throwable t) {
          throw new HoodieException("Failed to instantiate WriteSupport class " + writeSupportClass
              + " via the (MessageType, HoodieSchema, Option<BloomFilter>, Properties) constructor", t);
        }
      case LEGACY:
        // Legacy ctor used by pre-HoodieSchema external subclasses (e.g. external crypto write-support
        // subclasses): (MessageType, Schema, Option, Map<String,String>).
        try {
          return (HoodieAvroWriteSupport) ReflectionUtils.loadClass(
              writeSupportClass, LEGACY_CTOR_SIGNATURE,
              parquetSchema, schema.toAvroSchema(), filter, propertiesToMap(config.getProps()));
        } catch (Throwable t) {
          throw new HoodieException("Failed to instantiate WriteSupport class " + writeSupportClass
              + " via the deprecated (MessageType, Schema, Option<BloomFilter>, Map<String,String>) constructor", t);
        }
      case UNSUPPORTED:
      default:
        throw new HoodieException("WriteSupport class " + writeSupportClass
            + " does not expose a recognized constructor. Expected either "
            + "(MessageType, HoodieSchema, Option<BloomFilter>, Properties) or "
            + "(MessageType, Schema, Option<BloomFilter>, Map<String,String>).");
    }
  }

  /** Signature of the preferred (current) HoodieAvroWriteSupport constructor. */
  private static final Class<?>[] NEW_CTOR_SIGNATURE = {
      MessageType.class, HoodieSchema.class, Option.class, Properties.class};
  /** Signature of the deprecated 4-arg constructor retained for binary compat with external subclasses. */
  private static final Class<?>[] LEGACY_CTOR_SIGNATURE = {
      MessageType.class, Schema.class, Option.class, Map.class};

  /** Probe a HoodieAvroWriteSupport class once to pick which constructor variant it exposes. */
  private static CtorVariant resolveCtorVariant(String writeSupportClass) {
    if (ReflectionUtils.hasConstructor(writeSupportClass, NEW_CTOR_SIGNATURE, true)) {
      return CtorVariant.NEW;
    }
    if (ReflectionUtils.hasConstructor(writeSupportClass, LEGACY_CTOR_SIGNATURE, true)) {
      return CtorVariant.LEGACY;
    }
    return CtorVariant.UNSUPPORTED;
  }

  private static Map<String, String> propertiesToMap(Properties props) {
    Map<String, String> map = new HashMap<>(props.size());
    props.stringPropertyNames().forEach(k -> map.put(k, props.getProperty(k)));
    return map;
  }
}
