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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;

/**
 * A schema provider, that reads schema from the base and log files of an existing HUDI dataset.
 */
public class HudiDatasetSchemaProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_BASEPATH_PROP = "hoodie.deltastreamer.schemaprovider.source.basepath";
    private static final String TARGET_SCHEMA_BASEPATH_PROP = "hoodie.deltastreamer.schemaprovider.target.basepath";
    private static final String SCHEMA_INCLUDE_COMMIT_FILES_PROP = "hoodie.deltastreamer.schemaprovider.include.commit.files";
  }

  private final Schema sourceSchema;
  private final Schema targetSchema;

  public HudiDatasetSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_BASEPATH_PROP));

    final boolean includeCommitFiles = Boolean.parseBoolean(props.getString(Config.SCHEMA_INCLUDE_COMMIT_FILES_PROP, "true"));

    // Source schema
    final String sourceBasepath = props.getString(Config.SOURCE_SCHEMA_BASEPATH_PROP);
    this.sourceSchema = loadSchema(jssc, sourceBasepath, includeCommitFiles);

    // Target schema
    final String targetBasepath = props.getString(Config.TARGET_SCHEMA_BASEPATH_PROP, sourceBasepath);
    if (!targetBasepath.equals(sourceBasepath)) {
      this.targetSchema = loadSchema(jssc, targetBasepath, includeCommitFiles);
    } else {
      this.targetSchema = this.sourceSchema;
    }
  }

  private Schema loadSchema(JavaSparkContext jssc, String basepath, boolean includeCommitFiles) {
    try {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jssc.hadoopConfiguration(), basepath);
      TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
      return schemaUtil.getTableAvroSchemaWithoutMetadataFields();
    } catch (Exception ioe) {
      throw new HoodieException("Error reading schema", ioe);
    }
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public Schema getTargetSchema() {
    return targetSchema;
  }
}
