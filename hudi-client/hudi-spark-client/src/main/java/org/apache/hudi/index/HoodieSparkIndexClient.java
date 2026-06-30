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

package org.apache.hudi.index;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieIndexingConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.index.record.HoodieRecordIndex;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.index.BaseHoodieIndexClient;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP;
import static org.apache.hudi.index.HoodieIndexUtils.indexExists;
import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.IDENTITY_TRANSFORM;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;

@Slf4j
public class HoodieSparkIndexClient extends BaseHoodieIndexClient {

  private Option<SparkSession> sparkSessionOpt = Option.empty();
  private Option<HoodieWriteConfig> writeConfigOpt = Option.empty();
  private Option<HoodieEngineContext> engineContextOpt = Option.empty();

  public HoodieSparkIndexClient(SparkSession sparkSession) {
    this(Option.of(sparkSession), Option.empty(), Option.empty());
  }

  public HoodieSparkIndexClient(HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    this(Option.empty(), Option.of(writeConfig), Option.of(engineContext));
  }

  public HoodieSparkIndexClient(Option<SparkSession> sparkSessionOpt, Option<HoodieWriteConfig> writeConfig, Option<HoodieEngineContext> engineContext) {
    super();
    this.sparkSessionOpt = sparkSessionOpt;
    this.writeConfigOpt = writeConfig;
    this.engineContextOpt = engineContext;
  }

  @Override
  public void create(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options,
                     Map<String, String> tableProperties) throws Exception {
    if (indexType.equals(PARTITION_NAME_COLUMN_STATS) && isNativeColumnStats(options)) {
      // Native column_stats partition: always named PARTITION_NAME_COLUMN_STATS ("column_stats") regardless of
      // userIndexName, because it is a singleton metadata partition (only one can exist per table).
      // Routed here when no expression option is specified, or the expression is the identity function.
      createNativeColumnStatsIndex(metaClient, columns);
    } else if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX) || indexType.equals(PARTITION_NAME_BLOOM_FILTERS)
        || indexType.equals(PARTITION_NAME_COLUMN_STATS)) {
      createExpressionOrSecondaryIndex(metaClient, userIndexName, indexType, columns, options, tableProperties);
    } else {
      createRecordIndex(metaClient, userIndexName, indexType, options);
    }
  }

  // Returns true when no expression is specified or the expression is identity (no transformation).
  public static boolean isNativeColumnStats(Map<String, String> options) {
    String expr = options.getOrDefault(EXPRESSION_OPTION, IDENTITY_TRANSFORM);
    return expr.equals(IDENTITY_TRANSFORM);
  }

  private void createNativeColumnStatsIndex(HoodieTableMetaClient metaClient, Map<String, Map<String, String>> columns) {
    List<String> columnsToIndex = new ArrayList<>(columns.keySet());
    createOrUpdateColumnStatsIndexDefinition(metaClient, columnsToIndex);

    Map<String, String> overrideOpts = Collections.singletonMap(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, Option.empty(), Option.of(PARTITION_NAME_COLUMN_STATS), overrideOpts)) {
      HoodieIndexVersion currentVersion = HoodieIndexVersion.getCurrentVersion(
          metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.COLUMN_STATS);
      Option<String> indexInstantTime = doSchedule(
          writeClient, metaClient, PARTITION_NAME_COLUMN_STATS, MetadataPartitionType.COLUMN_STATS, currentVersion);
      if (indexInstantTime.isPresent()) {
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      log.error("Error while creating column stats index. Index will be dropped.", t);
      drop(metaClient, PARTITION_NAME_COLUMN_STATS, Option.empty());
      throw t;
    }
  }

  private void createRecordIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, String> options) {
    if (!userIndexName.equals(PARTITION_NAME_RECORD_INDEX)) {
      throw new HoodieIndexException("Record index should be named as record_index");
    }

    String fullIndexName = PARTITION_NAME_RECORD_INDEX;
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }

    Map<String, String> overrideOpts = Collections.emptyMap();
    if (HoodieRecordIndex.isPartitioned(options)) {
      overrideOpts = Collections.singletonMap(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    }
    HoodieIndexVersion version = HoodieIndexVersion.getCurrentVersion(metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);
    log.info("Creating index {} using version {}", fullIndexName, version);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, Option.empty(), Option.of(indexType), overrideOpts)) {
      // generate index plan
      HoodieIndexVersion currentVersion = HoodieIndexVersion.getCurrentVersion(
          metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);
      Option<String> indexInstantTime = doSchedule(
          writeClient, metaClient, fullIndexName, MetadataPartitionType.RECORD_INDEX, currentVersion);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      drop(metaClient, fullIndexName, Option.empty());
      throw t;
    }
  }

  @Override
  public void createOrUpdateColumnStatsIndexDefinition(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withIndexFunction(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(columnsToIndex)
        // Use the existing version if exists, otherwise fall back to the default version.
        .withVersion(existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, metaClient))
        .withIndexOptions(Collections.EMPTY_MAP)
        .build();
    log.info("Registering or updating index: {} of type: {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
    register(metaClient, indexDefinition);
  }

  private void createExpressionOrSecondaryIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType,
                                                Map<String, Map<String, String>> columns, Map<String, String> options, Map<String, String> tableProperties) throws Exception {
    HoodieIndexDefinition indexDefinition = HoodieIndexUtils.getSecondaryOrExpressionIndexDefinition(metaClient, userIndexName, indexType, columns, options, tableProperties);
    if (!metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()
        || !metaClient.getIndexForMetadataPartition(indexDefinition.getIndexName()).isPresent()) {
      log.info("Index definition is not present. Registering index: {} of type: {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
      register(metaClient, indexDefinition);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    log.info("Creating index {}", indexDefinition);
    Option<HoodieIndexDefinition> expressionIndexDefinitionOpt = Option.ofNullable(indexDefinition);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, expressionIndexDefinitionOpt, Option.of(indexType), Collections.emptyMap())) {
      MetadataPartitionType partitionType = indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? MetadataPartitionType.SECONDARY_INDEX : MetadataPartitionType.EXPRESSION_INDEX;
      // generate index plan
      HoodieIndexVersion currentVersion = HoodieIndexVersion.getCurrentVersion(metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);

      Option<String> indexInstantTime = doSchedule(
          writeClient, metaClient, indexDefinition.getIndexName(), partitionType, currentVersion);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      log.error("Error while creating index: {}. Index will be dropped.", indexDefinition.getIndexName(), t);
      drop(metaClient, indexDefinition.getIndexName(), Option.ofNullable(indexDefinition));
      throw t;
    }
  }

  private void drop(HoodieTableMetaClient metaClient, String indexName, Option<HoodieIndexDefinition> indexDefinitionOpt) {
    log.info("Dropping index {}", indexName);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.empty(), Collections.emptyMap())) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  @Override
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    log.info("Dropping index {}", indexName);
    Option<HoodieIndexDefinition> indexDefinitionOpt = metaClient.getIndexMetadata()
        .map(HoodieIndexMetadata::getIndexDefinitions)
        .map(definition -> definition.get(indexName));
    // Explicitly disable the partition being dropped so buildWriteConfig does not re-enable it
    // via the [HUDI-7472] preservation logic that reads existing metadata partitions.
    Map<String, String> dropOverrides = getDropOverrideConfigs(indexName);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.empty(), dropOverrides)) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  static Map<String, String> getDropOverrideConfigs(String indexName) {
    if (indexName.equals(PARTITION_NAME_COLUMN_STATS)) {
      return Collections.singletonMap(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    } else if (indexName.equals(PARTITION_NAME_BLOOM_FILTERS)) {
      return Collections.singletonMap(ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "false");
    } else if (indexName.equals(PARTITION_NAME_RECORD_INDEX)) {
      return Collections.singletonMap(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "false");
    }
    return Collections.emptyMap();
  }

  private SparkRDDWriteClient getWriteClient(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                             Option<String> indexTypeOpt, Map<String, String> configs) {
    try {
      String schemaStr;
      if (writeConfigOpt.isPresent() && StringUtils.nonEmpty(writeConfigOpt.get().getSchema())) {
        schemaStr = writeConfigOpt.get().getSchema();
      } else {
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        schemaStr = schemaUtil.getTableSchema(false).toString();
      }
      TypedProperties props = getProps(metaClient, indexDefinitionOpt, indexTypeOpt, schemaStr);
      if (!engineContextOpt.isPresent()) {
        engineContextOpt = Option.of(new HoodieSparkEngineContext(new JavaSparkContext(sparkSessionOpt.get().sparkContext())));
      }
      HoodieWriteConfig localWriteConfig = HoodieWriteConfig.newBuilder()
          .withPath(metaClient.getBasePath())
          .withProperties(props)
          .withEmbeddedTimelineServerEnabled(false)
          .withSchema(schemaStr)
          .withEngineType(EngineType.SPARK)
          .withProps(configs)
          .build();
      SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContextOpt.get(), localWriteConfig, Option.empty());
      // Validate using the effective config after any config-store enrichment applied inside
      // BaseHoodieClient.<init> (e.g. HoodieUberConfigStore, runClientInitCallbacks). Checking
      // the raw localWriteConfig here would reject configurations that are only resolved after
      // construction, including those supplied by init callbacks or cluster-managed config stores.
      HoodieWriteConfig effectiveConfig = writeClient.getConfig();
      if (effectiveConfig.getWriteConcurrencyMode().supportsMultiWriter() && StringUtils.isNullOrEmpty(effectiveConfig.getLockProviderClass())) {
        writeClient.close();
        throw new IllegalArgumentException(
            "To create index asynchronously, multi-writer configurations need to be enabled and hence 'hoodie.write.lock.provider' is expected to be set for such cases. "
                + "For single writer mode, feel free to set the config value to org.apache.hudi.client.transaction.lock.InProcessLockProvider and retry index creation");
      }
      return writeClient;
    } catch (Exception e) {
      throw new HoodieException("Failed to create write client while performing index operation ", e);
    }
  }

  private TypedProperties getProps(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                   Option<String> indexTypeOpt, String schemaStr) {
    TypedProperties typedProperties;
    if (writeConfigOpt.isPresent()) {
      typedProperties = TypedProperties.copy(writeConfigOpt.get().getProps());
    } else {
      typedProperties = metaClient.getTableConfig().getProps();
      JavaConverters.mapAsJavaMapConverter(sparkSessionOpt.get().sqlContext().getAllConfs()).asJava().forEach((k, v) -> {
        if (k.startsWith("hoodie.")) {
          typedProperties.put(k, v);
        }
      });
      typedProperties.put(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), schemaStr);
    }
    typedProperties.putAll(buildWriteConfig(metaClient, indexDefinitionOpt, indexTypeOpt));
    return typedProperties;
  }

  private static Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client, HoodieTableMetaClient metaClient,
                                           String indexName, MetadataPartitionType partitionType, HoodieIndexVersion version) {
    List<MetadataPartitionType> partitionTypes = Collections.singletonList(partitionType);
    if (metaClient.getTableConfig().getMetadataPartitions().isEmpty()) {
      throw new HoodieException("Metadata table is not yet initialized. Initialize FILES partition before any other partition " + Arrays.toString(partitionTypes.toArray()));
    }
    return client.scheduleIndexing(partitionTypes, Collections.singletonList(indexName));
  }

  private static Map<String, String> buildWriteConfig(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                                      Option<String> indexTypeOpt) {
    Map<String, String> writeConfig = new HashMap<>();
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      writeConfig.put(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());

      // [HUDI-7472] Ensure write-config contains the existing MDT partition to prevent those from getting deleted
      metaClient.getTableConfig().getMetadataPartitions().forEach(partitionPath -> {
        if (partitionPath.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
          writeConfig.put(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
        }

        if (partitionPath.equals(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath())) {
          writeConfig.put(ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "true");
        }

        if (partitionPath.equals(MetadataPartitionType.COLUMN_STATS.getPartitionPath())) {
          writeConfig.put(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
        }
      });

      if (indexTypeOpt.isPresent()) {
        String indexType = indexTypeOpt.get();
        if (indexType.equals(PARTITION_NAME_RECORD_INDEX)) {
          writeConfig.put(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
        }
      }
    }

    indexDefinitionOpt.ifPresent(indexDefinition ->
        HoodieIndexingConfig.fromIndexDefinition(indexDefinition).getProps().forEach((key, value) -> writeConfig.put(key.toString(), value.toString())));
    return writeConfig;
  }
}
