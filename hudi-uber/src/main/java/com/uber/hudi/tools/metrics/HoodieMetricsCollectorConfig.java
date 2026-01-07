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

package com.uber.hudi.tools.metrics;

import com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * Configuration class for HoodieMetricsCollector tool.
 * Contains command-line parameters for metrics collection.
 */
public class HoodieMetricsCollectorConfig implements Serializable {

  @Parameter(names = {"--databases", "-d"}, description = "Comma separated list of databases to scan for HUDI tables. Use '*' or leave empty to scan all databases.")
  public String databases = "*";

  @Parameter(names = {"--outputdir", "-o"}, description = "Output directory path where CSV files will be written", required = true)
  public String outputDir;

  @Parameter(names = {"--datacenter", "-dc"}, description = "Datacenter name (used in table CSV file naming)", required = true)
  public String datacenter;

  @Parameter(names = {"--lookback-hours", "-l"}, description = "Number of hours to look back for timeline metrics")
  public int lookbackHours = 24;

  @Parameter(names = {"--load-tables"}, description = "Whether to load tables from databases. If false, reads from existing hudi_tables.csv file.", arity = 1)
  public boolean loadTables = false;

  @Parameter(names = {"--help", "-h"}, help = true, description = "Display help")
  public Boolean help = false;

  @Override
  public String toString() {
    return "HoodieMetricsCollectorConfig{"
        + "databases='" + databases + '\''
        + ", outputDir='" + outputDir + '\''
        + ", datacenter='" + datacenter + '\''
        + ", lookbackHours=" + lookbackHours
        + ", loadTables=" + loadTables
        + '}';
  }
}
