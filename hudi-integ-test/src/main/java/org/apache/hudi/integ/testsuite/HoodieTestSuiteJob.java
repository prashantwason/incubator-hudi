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

package org.apache.hudi.integ.testsuite;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.integ.testsuite.dag.DagUtils;
import org.apache.hudi.integ.testsuite.dag.WorkflowDag;
import org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator;
import org.apache.hudi.integ.testsuite.dag.WriterContext;
import org.apache.hudi.integ.testsuite.dag.scheduler.DagScheduler;
import org.apache.hudi.integ.testsuite.fs.HoodieUnionFileSystem;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the entry point for running a Hudi Test Suite. Although this class has similarities with {@link HoodieDeltaStreamer} this class does not extend it since do not want to create a dependency
 * on the changes in DeltaStreamer.
 */
public class HoodieTestSuiteJob {

  private static volatile Logger log = LoggerFactory.getLogger(HoodieTestSuiteJob.class);

  private final HoodieTestSuiteConfig cfg;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;
  /**
   * Filesystem used.
   */
  private transient FileSystem fs;
  /**
   * Spark context.
   */
  private transient JavaSparkContext jsc;
  /**
   * Spark Session.
   */
  private transient SparkSession sparkSession;
  /**
   * Hive Config.
   */
  private transient HiveConf hiveConf;

  private BuiltinKeyGenerator keyGenerator;

  public HoodieTestSuiteJob(HoodieTestSuiteConfig cfg, JavaSparkContext jsc) throws IOException, SQLException {
    this.cfg = cfg;
    this.jsc = jsc;
    //cfg.propsFilePath = FSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    this.fs = FSUtils.getFs(cfg.inputBasePath, jsc.hadoopConfiguration());
    //this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    this.props = readProperties(cfg, jsc.hadoopConfiguration());
    log.info("Creating workload generator with configs : {}", props.toString());
    this.hiveConf = getDefaultHiveConf(jsc.hadoopConfiguration());
    this.keyGenerator = (BuiltinKeyGenerator) DataSourceUtils.createKeyGenerator(props);

    if (!fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient.initTableType(jsc.hadoopConfiguration(), cfg.targetBasePath,
          HoodieTableType.valueOf(cfg.tableType), cfg.targetTableName, "archived");
    }

    if (cfg.cleanInput) {
      Path inputPath = new Path(cfg.inputBasePath);
      if (fs.exists(inputPath)) {
        fs.delete(inputPath, true);
      }
    }

    if (cfg.cleanOutput) {
      Path outputPath = new Path(cfg.targetBasePath);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
    }

    if (!cfg.cleanOutput) {
      return;
    }

    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(this.props, this.cfg.targetBasePath, this.cfg.baseFileFormat);
    String dropMessage = "Dropped table " + hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName;
    if (hiveSyncConfig.useJdbc) {
      Connection con = DriverManager.getConnection(hiveSyncConfig.jdbcUrl, hiveSyncConfig.hiveUser, hiveSyncConfig.hivePass);
      con.createStatement().execute("DROP TABLE " + hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
      log.info(dropMessage);
    } else {
      try {
        HoodieHiveClient client = new HoodieHiveClient(hiveSyncConfig, this.hiveConf, this.fs);
        client.updateHiveSQL("DROP TABLE " + hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
        log.info(dropMessage);
      } catch (TableNotFoundException e) {
        log.info("Table not found; nothing to clean up.");
      }
    }
    log.info("Workspace cleanup concluded.");

    fs.mkdirs(new Path(cfg.inputBasePath + "/" + ".fs"));
  }

  private static HiveConf getDefaultHiveConf(Configuration cfg) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(cfg);
    return hiveConf;
  }

  public static void main(String[] args) throws Exception {
    final HoodieTestSuiteConfig cfg = new HoodieTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("workload-generator-" + cfg.outputTypeName
        + "-" + cfg.inputFormatName, cfg.sparkMaster);

    // Set the file system
    jssc.hadoopConfiguration().set(HoodieWrapperFileSystem.HOODIE_WRAPPER_FS_CONF, cfg.filesystemClass);
    if (cfg.filesystemClass.equals(HoodieUnionFileSystem.class.getName())) {
      TypedProperties props = readProperties(cfg, jssc.hadoopConfiguration());

      // Required settings for using HoodieUnionFileSystem
      jssc.hadoopConfiguration().set("fs.defaultfs", "file:///");
      jssc.hadoopConfiguration().set("fs.hdfs.impl", cfg.filesystemClass);
      jssc.hadoopConfiguration().set("fs.viewfs.impl", cfg.filesystemClass);
      jssc.hadoopConfiguration().set("fs.hdfs.impl.disable.cache", "true");
      jssc.hadoopConfiguration().set("fs.viewfs.impl.disable.cache", "true");

      // Base and delta directories
      jssc.hadoopConfiguration().set(HoodieUnionFileSystem.CONF_BASEURI, props.getProperty(HoodieUnionFileSystem.CONF_BASEURI));
      jssc.hadoopConfiguration().set(HoodieUnionFileSystem.CONF_DELTAURI, props.getProperty(HoodieUnionFileSystem.CONF_DELTAURI));

      // Other settings
      for (String key : props.stringPropertyNames()) {
        if (key.startsWith(HoodieUnionFileSystem.CONF_PREFIX)) {
          jssc.hadoopConfiguration().set(key, props.getProperty(key));
        }
      }
    }
    
    // Create a distributed registry for HoodieWrapperFileSystem
    Registry registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName(),
        DistributedRegistry.class.getName());
    ((DistributedRegistry)registry).register(jssc);
    Registry registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder",
        DistributedRegistry.class.getName());
    ((DistributedRegistry)registryMeta).register(jssc);

    new HoodieTestSuiteJob(cfg, jssc).runTestSuite();
  }

  private static TypedProperties readProperties(HoodieTestSuiteConfig cfg, Configuration conf) {
    String propsFilePath = FSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    FileSystem fs = FSUtils.getRawFs(cfg.inputBasePath, conf);
    return UtilHelpers.readConfig(fs, new Path(propsFilePath), cfg.configs).getConfig();
  }

  public WorkflowDag createWorkflowDag() throws IOException {
    WorkflowDag workflowDag = this.cfg.workloadYamlPath == null ? ((WorkflowDagGenerator) ReflectionUtils
        .loadClass((this.cfg).workloadDagGenerator)).build()
        : DagUtils.convertYamlPathToDag(
            FSUtils.getFs(this.cfg.workloadYamlPath, jsc.hadoopConfiguration(), true),
            this.cfg.workloadYamlPath);
    return workflowDag;
  }

  public void runTestSuite() {
    try {
      WorkflowDag workflowDag = createWorkflowDag();
      log.info("Workflow Dag => " + DagUtils.convertDagToYaml(workflowDag));
      long startTime = System.currentTimeMillis();
      WriterContext writerContext = new WriterContext(jsc, props, cfg, keyGenerator, sparkSession);
      writerContext.initContext(jsc);
      DagScheduler dagScheduler = new DagScheduler(workflowDag, writerContext, jsc);
      dagScheduler.schedule();
      log.info("Finished scheduling all tasks, Time taken {}", System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      log.error("Failed to run Test Suite ", e);
      throw new HoodieException("Failed to run Test Suite ", e);
    } finally {
      sparkSession.stop();
      jsc.stop();
    }
  }

  /**
   * The Hudi test suite uses {@link HoodieDeltaStreamer} to run some operations hence extend delta streamer config.
   */
  public static class HoodieTestSuiteConfig extends HoodieDeltaStreamer.Config {

    @Parameter(names = {"--input-base-path"}, description = "base path for input data"
        + "(Will be created if did not exist first time around. If exists, more data will be added to that path)",
        required = true)
    public String inputBasePath;

    @Parameter(names = {
        "--workload-generator-classname"}, description = "WorkflowDag of operations to generate the workload")
    public String workloadDagGenerator = WorkflowDagGenerator.class.getName();

    @Parameter(names = {
        "--workload-yaml-path"}, description = "Workflow Dag yaml path to generate the workload")
    public String workloadYamlPath;

    @Parameter(names = {"--delta-output-type"}, description = "Subclass of "
        + "org.apache.hudi.testsuite.workload.DeltaOutputMode to readAvro data.")
    public String outputTypeName = DeltaOutputMode.DFS.name();

    @Parameter(names = {"--delta-input-format"}, description = "Subclass of "
        + "org.apache.hudi.testsuite.workload.DeltaOutputMode to read avro data.")
    public String inputFormatName = DeltaInputType.AVRO.name();

    @Parameter(names = {"--input-file-size"}, description = "The min/max size of the input files to generate",
        required = true)
    public Long limitFileSize = 1024 * 1024 * 120L;

    @Parameter(names = {"--input-parallelism"}, description = "Parallelism to use when generation input files",
        required = false)
    public Integer inputParallelism = 0;

    @Parameter(names = {"--delete-old-input"}, description = "Delete older input files once they have been ingested",
        required = false)
    public Boolean deleteOldInput = false;

    @Parameter(names = {"--use-deltastreamer"}, description = "Choose whether to use HoodieDeltaStreamer to "
        + "perform ingestion. If set to false, HoodieWriteClient will be used")
    public Boolean useDeltaStreamer = false;

    @Parameter(names = {"--clean-input"}, description = "Clean the input folders and delete all files within it "
        + "before starting the Job")
    public Boolean cleanInput = false;

    @Parameter(names = {"--clean-output"}, description = "Clean the output folders and delete all files within it "
        + "before starting the Job")
    public Boolean cleanOutput = false;

    @Parameter(names = {"--filesystem-class"}, description = "Class name of the FileSystem implemetation to use")
    public String filesystemClass = HoodieWrapperFileSystem.class.getName();
  }
}
