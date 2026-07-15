package com.uber.hudi.tools.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.replication.table.HoodieReplicationMetadataClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.replication.util.ReplicationPropertiesManager;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.replication.HoodieReplicationContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.mockito.MockedStatic;
import org.apache.hudi.HoodieSparkSQLUtils;
import org.apache.spark.sql.SparkSession;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TestHoodieOperationReplication {

  /**
   * Inner class to simulate and test HoodieOperationReplication behavior.
   */
  private class HoodieOperationReplicationTest extends HoodieOperationReplication {
    public HoodieTableMetaClient metaClient;
    public HoodieTableMetaClient srcMetaClient;
    public HoodieTableMetaClient tgtMetaClient;
    public HoodieWriteConfig hoodieWriteConfig;
    public SparkRDDWriteClient sparkRDDWriteClient;
    public HoodieReplicationMetadataClient srcReplicationClient;
    public HoodieReplicationMetadataClient tgtReplicationClient;
    public ReplicationPropertiesManager replicationPropertiesManager;

    /**
     * Constructor to initialize mocks and configure behavior for testing.
     */
    public HoodieOperationReplicationTest() throws Exception {
      super();
      this.srcMetaClient = mock(HoodieTableMetaClient.class);
      this.tgtMetaClient = mock(HoodieTableMetaClient.class);
      this.hoodieWriteConfig = mock(HoodieWriteConfig.class);
      this.sparkRDDWriteClient = mock(SparkRDDWriteClient.class);
      this.srcReplicationClient = mock(HoodieReplicationMetadataClient.class);
      this.tgtReplicationClient = mock(HoodieReplicationMetadataClient.class);
      this.replicationPropertiesManager = mock(ReplicationPropertiesManager.class);
      // Mock HoodieActiveTimeline
      HoodieActiveTimeline mockSrcTimeline = mock(HoodieActiveTimeline.class);
      HoodieActiveTimeline mockTgtTimeline = mock(HoodieActiveTimeline.class);
      when(this.srcReplicationClient.getMetaClient()).thenReturn(this.srcMetaClient);
      when(this.srcMetaClient.getActiveTimeline()).thenReturn(mockSrcTimeline);
      when(this.srcMetaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
      HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
      HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);

      // Create a list of HoodieInstant objects with valid values
      List<HoodieInstant> srcInstants = Arrays.asList(
              new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20230101010101", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
              new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20230102020202", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
              new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20230103030303", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR)
      );
      when(this.srcMetaClient.getCommitsTimeline()).thenReturn(activeTimeline);
      when(activeTimeline.filterCompletedInstants()).thenReturn(activeTimeline);
      when(activeTimeline.getInstantsAsStream()).thenReturn(srcInstants.stream());

      when(this.tgtReplicationClient.getMetaClient()).thenReturn(this.tgtMetaClient);
      when(this.tgtMetaClient.getActiveTimeline()).thenReturn(mockTgtTimeline);
      HoodieStorage mockedStorage = mock(HoodieStorage.class);
      when(this.tgtMetaClient.getStorage()).thenReturn(mockedStorage);
      when(mockedStorage.exists(any(StoragePath.class))).thenReturn(true);
      // Stubs for getLeftoverFilesForArchival's metaPath/instant-file-name resolution
      StoragePath tgtMetaPath = mock(StoragePath.class);
      when(this.tgtMetaClient.getMetaPath()).thenReturn(tgtMetaPath);
      when(tgtMetaPath.toString()).thenReturn("target/base/path/.hoodie");
      TimelineLayout mockTimelineLayout = mock(TimelineLayout.class);
      InstantFileNameGenerator mockFileNameGenerator = mock(InstantFileNameGenerator.class);
      when(this.tgtMetaClient.getTimelineLayout()).thenReturn(mockTimelineLayout);
      when(mockTimelineLayout.getInstantFileNameGenerator()).thenReturn(mockFileNameGenerator);
      when(mockFileNameGenerator.getFileName(any(HoodieInstant.class)))
          .thenAnswer(inv -> inv.getArgument(0, HoodieInstant.class).requestedTime() + ".commit");
      // Mock HoodieInstant instances
      HoodieInstant mockSrcInstant = mock(HoodieInstant.class);
      HoodieInstant mockTgtInstant = mock(HoodieInstant.class);

      // Create lists of mocked HoodieInstant instances
      List<HoodieInstant> mockSrcInstants = Arrays.asList(mockSrcInstant);
      List<HoodieInstant> mockTgtInstants = Arrays.asList(mockTgtInstant);

      // Mock HoodieActiveTimeline to return the mocked instants
      when(mockSrcTimeline.filterCompletedInstants()).thenReturn(mockSrcTimeline);
      when(mockTgtTimeline.filterCompletedInstants()).thenReturn(mockTgtTimeline);
      when(mockSrcTimeline.getWriteTimeline()).thenReturn(mockSrcTimeline);
      when(mockTgtTimeline.getWriteTimeline()).thenReturn(mockTgtTimeline);
      when(mockSrcTimeline.getInstantsAsStream()).thenReturn(mockSrcInstants.stream());
      when(mockTgtTimeline.getInstantsAsStream()).thenReturn(mockTgtInstants.stream());

      // Mock the timestamps of the HoodieInstant instances
      when(mockSrcInstant.requestedTime()).thenReturn("20210101010101");
      when(mockTgtInstant.requestedTime()).thenReturn("20220101010101");

      // Mock ReplicationStep and ReplicationInfo
      HoodieReplicationMetadataClient.ReplicationStep mockStep = mock(HoodieReplicationMetadataClient.ReplicationStep.class);
      HoodieReplicationMetadataClient.ReplicationInfo mockReplicationInfo = mock(HoodieReplicationMetadataClient.ReplicationInfo.class);
      List<HoodieReplicationMetadataClient.ReplicationInfo> mockReplicationInfos = Arrays.asList(mockReplicationInfo, mockReplicationInfo);
      Map<HoodieReplicationMetadataClient.ReplicationStep, List<HoodieReplicationMetadataClient.ReplicationInfo>> orderedList = mock(Map.class);
      when(orderedList.get(mockStep)).thenReturn(mockReplicationInfos);
      when(this.tgtReplicationClient.getRollbackOrdering()).thenReturn(Arrays.asList(mockStep));
      when(this.tgtReplicationClient.getOrderedFilesForRollback(anyString())).thenReturn(orderedList);

      // mock ReplicationPropertiesManager
      Properties mockedProps = mock(Properties.class);
      when(this.replicationPropertiesManager.readProperties()).thenReturn(mockedProps);
    }

    @Override
    protected HoodieReplicationMetadataClient getHoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath, ReplicationDestination destination) {
      if (basePath.equals("source/base/path")) {
        return this.srcReplicationClient;
      } else if (basePath.equals("target/base/path")) {
        return this.tgtReplicationClient;
      }
      return null;
    }

    @Override
    protected HoodieReplicationMetadataClient getHoodieReplicationMetadataClient(StorageConfiguration<?> conf, String basePath) {
      if (basePath.equals("source/base/path")) {
        return this.srcReplicationClient;
      } else if (basePath.equals("target/base/path")) {
        return this.tgtReplicationClient;
      }
      return null;
    }
    
    @Override
    protected ReplicationPropertiesManager getReplicationPropertiesManager(HoodieTableMetaClient metaClient) {
      return this.replicationPropertiesManager;
    }

    @Override
    protected List<HoodieInstant> getTargetInstantsBeforeOldestSrcCommit(HoodieTableMetaClient metaClient,  String oldestInstantTimestampFromSrc, int maxSize) {
      List<HoodieInstant> targetInstantsBeforeOldestSrcCommit = Arrays.asList(
              new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20221201010101", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
              new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20221202020202", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR)
      );
      return targetInstantsBeforeOldestSrcCommit;

    }
  }

  /**
   * Test case to validate behavior when CHECK_DIVERGED_COMMITS operation succeeds.
   */
  @Test
  public void testCheckDivergedCommits() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.CHECK_DIVERGED_COMMITS;

    // Initialize arguments
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1,table2";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination
    };

    // Mock the Spark context
    JavaSparkContext jsc = mock(JavaSparkContext.class);
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    when(context.jsc()).thenReturn(jsc);
    doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();

    // Simulate a success scenario
    HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

    // Validate whether the replication operation completes successfully
    Result result = tool.execute(context, args);
    String expectedMessage = String.format("Replication operation complete");
    assertEquals(StatusCode.SUCCESS, result.statusCode);
    assertEquals(expectedMessage, result.message);
  }

  /**
   * Test case to validate behavior when replication operation succeeds.
   */
  @Test
  public void testRemoveDivergedCommits() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.REMOVE_DIVERGED_COMMITS;

    // Initialize arguments
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1,table2";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination
    };

    // Mock the Spark context
    JavaSparkContext jsc = mock(JavaSparkContext.class);
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    when(context.jsc()).thenReturn(jsc);
    doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();

    // Simulate a success scenario
    HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

    // Validate whether the replication operation completes successfully
    Result result = tool.execute(context, args);
    String expectedMessage = String.format("Replication operation complete");
    assertEquals(StatusCode.SUCCESS, result.statusCode);
    assertEquals(expectedMessage, result.message);
  }

  /**
   * Test case to validate behavior when VIEW_REPLICATION_ENABLED_DATASET operation succeeds.
   */
  @Test
  public void testViewReplicationEnabledDataset() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.VIEW_REPLICATION_ENABLED_DATASET;

    // Initialize arguments
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1,table2";
    String dataBases = "db1,db2";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination,
      "--databases", dataBases
    };


    try (MockedStatic<SparkSession> mockedSparkSession = mockStatic(SparkSession.class)) {
      // Mock the Spark context and related configurations
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);
      when(jsc.sc()).thenReturn(mock(SparkContext.class));

      // Mock SparkSession and its builder
      SparkSession.Builder mockBuilder = mock(SparkSession.Builder.class);
      SparkSession mockSparkSession = mock(SparkSession.class);
      mockedSparkSession.when(SparkSession::builder).thenReturn(mockBuilder);

      when(mockBuilder.enableHiveSupport()).thenReturn(mockBuilder);
      when(mockBuilder.sparkContext(any())).thenReturn(mockBuilder);
      when(mockBuilder.getOrCreate()).thenReturn(mockSparkSession);

      // Mock the Dataset and its behavior for SQL queries
      Dataset<Row> mockDataset = mock(Dataset.class);
      Row mockRow = mock(Row.class);
      when(mockSparkSession.sql(anyString())).thenReturn(mockDataset);
      when(mockDataset.collectAsList()).thenReturn(Arrays.asList(mockRow));
      when(mockRow.getString(0)).thenReturn("mocked_table_name");
      when(mockRow.getString(1)).thenReturn("mocked_table_path");

      // Simulate the tool for replication and execute the operation
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

      // Validate whether the replication operation completes successfully
      Result result = tool.execute(context, args);
      String expectedMessage = "Replication operation complete";

      // Assertions to verify the operation's success
      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  /**
   * Test case to validate behavior when VIEW_REPLICATION_PROPERTIES operation succeeds.
   */
  @Test
  public void testViewReplicationProperties() throws Exception {
    // Define the operation type for replication
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.VIEW_REPLICATION_PROPERTIES;

    // Initialize arguments for the replication operation
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1,table2";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination
    };

    // Mocking static methods and related components
    try (MockedStatic<HoodieTableMetaClient> mockedStaticMetaClient = mockStatic(HoodieTableMetaClient.class);
         MockedStatic<HoodieSparkSQLUtils> mockedStatic = mockStatic(HoodieSparkSQLUtils.class)) {

      // Mocks for HoodieTableMetaClient
      HoodieTableMetaClient.Builder mockBuilder = mock(HoodieTableMetaClient.Builder.class);
      HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);

      // Mock  for HoodieTableMetaClient builder
      mockedStaticMetaClient.when(HoodieTableMetaClient::builder).thenReturn(mockBuilder);
      when(mockBuilder.setConf(any(StorageConfiguration.class))).thenReturn(mockBuilder);
      when(mockBuilder.setBasePath(anyString())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockMetaClient);

      // Mocks for Spark context and configurations
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
      // Mock for Spark and Hadoop contexts
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);

      // Mock for HoodieSparkSQLUtils
      mockedStatic.when(() -> HoodieSparkSQLUtils.getBasePathFromTableName(any(JavaSparkContext.class), anyString()))
              .thenReturn("mocked/base/path");

      // Simulate the tool for replication and execute the operation
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();
      Result result = tool.execute(context, args);
      String expectedMessage = "Replication operation complete";

      // Validate whether the replication operation completes successfully
      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  /**
   * Test case to validate behavior when ENABLE_REPLICATION and DISABLE_REPLICATION operation succeeds.
   */
  @ParameterizedTest
  @ValueSource(strings = {"ENABLE_REPLICATION", "DISABLE_REPLICATION"})
  public void testEnableDisableReplication(String operation) throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.valueOf(operation);

    // Initialize arguments
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1,table2";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination
    };

    try (MockedStatic<HoodieSparkSQLUtils> mockedStatic = mockStatic(HoodieSparkSQLUtils.class);) {

      // Mock the Spark context
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);
      mockedStatic.when(() -> HoodieSparkSQLUtils.getBasePathFromTableName(any(JavaSparkContext.class), anyString()))
              .thenReturn("source/base/path");

      // Simulate a success scenario
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

      // Validate whether the replication operation completes successfully
      Result result = tool.execute(context, args);
      String expectedMessage = String.format("Replication operation complete");
      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  /**
   * Test case to validate behavior when REMOVE_REPLICATION_FLAG operation succeeds.
   */
  @Test
  public void testRemoveReplicationFlag() throws Exception {
    // Define the operation type for removing replication flags
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.REMOVE_REPLICATION_FLAG;

    // Initialize arguments for the replication operation
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1";
    String dataBases = "db1";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination,
      "--databases", dataBases
    };

    try (MockedStatic<HoodieTableMetaClient> mockedStaticMetaClient = mockStatic(HoodieTableMetaClient.class);
         MockedStatic<HoodieSparkSQLUtils> mockedStatic = mockStatic(HoodieSparkSQLUtils.class);
         MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class)) {

      // Mocks for HoodieTableMetaClient
      HoodieTableMetaClient.Builder mockBuilder = mock(HoodieTableMetaClient.Builder.class);
      HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);

      // Mocks for HoodieTableMetaClient builder and replication key retrieval
      mockedStaticMetaClient.when(HoodieTableMetaClient::builder).thenReturn(mockBuilder);
      // getCrossRegionReplicationEnabledConfigKey is now a real static method on HoodieReplicationContext
      when(mockBuilder.setConf(any(StorageConfiguration.class))).thenReturn(mockBuilder);
      when(mockBuilder.setBasePath(anyString())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockMetaClient);

      // Mocks for FileSystem
      HoodieStorage mockedFs = mock(HoodieStorage.class);
      // TODO: FSUtils.getFs removed in 1.x; production code now uses Path.getFileSystem()
      when(mockedFs.exists(any(StoragePath.class))).thenReturn(true).thenReturn(false);

      // Mocks for Spark context and configurations
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
      // Mocks for Spark and Hadoop contexts
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);

      // Mocks for HoodieSparkSQLUtils
      mockedStatic.when(() -> HoodieSparkSQLUtils.getBasePathFromTableName(any(JavaSparkContext.class), anyString()))
              .thenReturn("mocked/base/path");

      // Simulate the tool for replication and execute the operation
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

      // Validate whether the replication operation completes successfully
      Result result = tool.execute(context, args);
      String expectedMessage = "Replication operation complete";

      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  /**
   * Test case to validate behavior when REMOVE_OPERATIONAL_STATUS operation succeeds.
   */
  @Test
  public void testRemoveOperationalStatus() throws Exception {
    // Define the operation type for removing operational status
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.REMOVE_OPERATIONAL_STATUS;

    // Initialize arguments for the operation
    String basePath = "source/base/path";
    String targetBasePath = "target/base/path";
    String tables = "table1";
    String dataBases = "db1";
    String destination = "SECONDARY_REGION";
    String[] args = new String[]{
      "--operation", op.name(),
      "--basepath", String.join(",", basePath),
      "--target-basepath", String.join(",", targetBasePath),
      "--table", tables,
      "--destination", destination,
      "--databases", dataBases
    };

    try (MockedStatic<HoodieTableMetaClient> mockedStaticMetaClient = mockStatic(HoodieTableMetaClient.class);
         MockedStatic<HoodieSparkSQLUtils> mockedStatic = mockStatic(HoodieSparkSQLUtils.class);
         MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class)) {

      // Mocks for HoodieTableMetaClient
      HoodieTableMetaClient.Builder mockBuilder = mock(HoodieTableMetaClient.Builder.class);
      HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
      mockedStaticMetaClient.when(HoodieTableMetaClient::builder).thenReturn(mockBuilder);
      // getCrossRegionReplicationEnabledConfigKey is now a real static method on HoodieReplicationContext
      when(mockBuilder.setConf(any(StorageConfiguration.class))).thenReturn(mockBuilder);
      when(mockBuilder.setBasePath(anyString())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockMetaClient);

      // Mocks for FileSystem
      HoodieStorage mockedFs = mock(HoodieStorage.class);
      // TODO: FSUtils.getFs removed in 1.x; production code now uses Path.getFileSystem()
      when(mockedFs.exists(any(StoragePath.class))).thenReturn(true).thenReturn(false);

      // Mocks for Spark context and configurations
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
      // Mocks for Spark and Hadoop contexts
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);

      // Mocks for HoodieSparkSQLUtils
      mockedStatic.when(() -> HoodieSparkSQLUtils.getBasePathFromTableName(any(JavaSparkContext.class), anyString()))
              .thenReturn("mocked/base/path");

      // Simulate the tool for replication and execute the operation
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

      // Validate whether the replication operation completes successfully
      Result result = tool.execute(context, args);
      String expectedMessage = "Replication operation complete";

      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  /**
   * Test case to validate behavior when CHECK_ARCHIVAL_MISSED operation succeeds.
   */
  @Test
  public void testCheckArchivalMissed() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.CHECK_ARCHIVAL_MISSED;

    // Initialize arguments for the operation
    String bathPaths = "source/base/path";
    String targetBathPaths = "target/base/path";

    int maxSize = 5;
    String commitPrefix = "commitPrefix";
    String[] args = new String[]{
      "--operation", op.name(),
      "--max-size", String.valueOf(maxSize),
      "--commit-prefix", commitPrefix,
      "--basepath",  String.join(",", bathPaths),
      "--target-basepath",  String.join(",", targetBathPaths)
    };

    try (MockedStatic<HoodieTableMetaClient> mockedStaticMetaClient = mockStatic(HoodieTableMetaClient.class);
         MockedStatic<HoodieSparkSQLUtils> mockedStatic = mockStatic(HoodieSparkSQLUtils.class);
         MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class)) {

      // Mocks for HoodieTableMetaClient
      HoodieTableMetaClient.Builder mockBuilder = mock(HoodieTableMetaClient.Builder.class);
      HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
      mockedStaticMetaClient.when(HoodieTableMetaClient::builder).thenReturn(mockBuilder);
      // getCrossRegionReplicationEnabledConfigKey is now a real static method on HoodieReplicationContext
      when(mockBuilder.setConf(any(StorageConfiguration.class))).thenReturn(mockBuilder);
      when(mockBuilder.setBasePath(anyString())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockMetaClient);

      // Mocks for FileSystem
      HoodieStorage mockedFs = mock(HoodieStorage.class);
      // TODO: FSUtils.getFs removed in 1.x; production code now uses Path.getFileSystem()
      when(mockedFs.exists(any(StoragePath.class))).thenReturn(true).thenReturn(false);

      // Mocks for Spark context and configurations
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);

      // Create a Hadoop Configuration object
      Configuration hadoopConfig = new Configuration();
      hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000");
      hadoopConfig.set("hadoop.tmp.dir", "/tmp/hadoop");

      // Mocks for Spark and Hadoop contexts
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(hadoopConfig)).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);

      // Simulate the tool for replication and execute the operation
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

      // Validate whether the replication operation completes successfully
      Result result = tool.execute(context, args);
      String expectedMessage = "Successfully found leftover archivals";

      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  /**
   * Test case to validate behavior when REMOVE_ARCHIVAL_MISSED operation succeeds.
   */
  @Test
  public void testRemoveArchivalMissed() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.REMOVE_ARCHIVAL_MISSED;

    // Initialize arguments for the operation
    String bathPaths = "source/base/path";
    String targetBathPaths = "target/base/path";

    int maxSize = 5;
    String commitPrefix = "commitPrefix";
    String[] args = new String[]{
      "--operation", op.name(),
      "--max-size", String.valueOf(maxSize),
      "--commit-prefix", commitPrefix,
      "--basepath",  String.join(",", bathPaths),
      "--target-basepath",  String.join(",", targetBathPaths)
    };

    try (MockedStatic<HoodieTableMetaClient> mockedStaticMetaClient = mockStatic(HoodieTableMetaClient.class);
         MockedStatic<HoodieSparkSQLUtils> mockedStatic = mockStatic(HoodieSparkSQLUtils.class);
         MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class)) {

      // Mocks for HoodieTableMetaClient
      HoodieTableMetaClient.Builder mockBuilder = mock(HoodieTableMetaClient.Builder.class);
      HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
      mockedStaticMetaClient.when(HoodieTableMetaClient::builder).thenReturn(mockBuilder);
      // getCrossRegionReplicationEnabledConfigKey is now a real static method on HoodieReplicationContext
      when(mockBuilder.setConf(any(StorageConfiguration.class))).thenReturn(mockBuilder);
      when(mockBuilder.setBasePath(anyString())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockMetaClient);

      // Mocks for FileSystem
      HoodieStorage mockedFs = mock(HoodieStorage.class);
      // TODO: FSUtils.getFs removed in 1.x; production code now uses Path.getFileSystem()
      when(mockedFs.exists(any(StoragePath.class))).thenReturn(true).thenReturn(false);

      // Mocks for Spark context and configurations
      JavaSparkContext jsc = mock(JavaSparkContext.class);
      HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);

      // Create a Hadoop Configuration object
      Configuration hadoopConfig = new Configuration();
      hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000");
      hadoopConfig.set("hadoop.tmp.dir", "/tmp/hadoop");

      // Mocks for Spark and Hadoop contexts
      when(context.jsc()).thenReturn(jsc);
      doReturn(new HadoopStorageConfiguration(hadoopConfig)).when(context).getStorageConf();
      when(context.getJavaSparkContext()).thenReturn(jsc);

      // Simulate the tool for replication and execute the operation
      HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();

      // Validate whether the replication operation completes successfully
      Result result = tool.execute(context, args);
      String expectedMessage = "Successfully deleted leftover archivals";

      assertEquals(StatusCode.SUCCESS, result.statusCode);
      assertEquals(expectedMessage, result.message);
    }
  }

  @Test
  @Disabled("HUDI-6418: temporarily disable failing tests")
  public void testCheckIncrementalPerformance() throws Exception {
    String[] args = {"--operation", "CHECK_INCREMENTAL_PERFORMANCE"};
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();
    assertThrows(UnsupportedOperationException.class, () -> tool.execute(context, args));
  }

  @Test
  public void testCheckArchivalPerformanceV2() throws Exception {
    String[] args = {"--operation", "CHECK_ARCHIVAL_PERFORMANCE_V2"};
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    HoodieOperationReplicationTest tool = new HoodieOperationReplicationTest();
    assertThrows(UnsupportedOperationException.class, () -> tool.execute(context, args));
  }

  @Test
  public void testComparePrimaryRegions() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.COMPARE_PRIMARY_REGION;

    // Initialize arguments
    String tables = "test_db.test_table";
    String[] args = new String[]{
      "--operation", op.name(),
      "--table", tables
    };

    // Mock the Spark context
    JavaSparkContext jsc = mock(JavaSparkContext.class);
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    when(context.jsc()).thenReturn(jsc);
    doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();

    // Create a test instance and spy on it
    HoodieOperationReplicationTest tool = spy(new HoodieOperationReplicationTest());

    // Mock the getPrimaryRegion methods
    doReturn(Stream.of(Region.PHX).collect(Collectors.toSet())).when(tool).getPrimaryRegionTAS(anyString());
    doReturn(Stream.of(Region.PHX).collect(Collectors.toSet())).when(tool).getPrimaryRegionHiveSync(anyString());

    // Execute the operation
    Result result = tool.execute(context, args);

    // Verify the result
    assertEquals(StatusCode.SUCCESS, result.statusCode);
    assertEquals("Replication operation complete", result.message);
  }

  @Test
  public void testComparePrimaryRegionsWithMismatch() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.COMPARE_PRIMARY_REGION;

    // Initialize arguments
    String tables = "test_db.test_table";
    String[] args = new String[]{
      "--operation", op.name(),
      "--table", tables
    };

    // Mock the Spark context
    JavaSparkContext jsc = mock(JavaSparkContext.class);
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    when(context.jsc()).thenReturn(jsc);
    doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();

    // Create a test instance and spy on it
    HoodieOperationReplicationTest tool = spy(new HoodieOperationReplicationTest());

    // Mock the getPrimaryRegion methods with different regions
    doReturn(Stream.of(Region.PHX).collect(Collectors.toSet())).when(tool).getPrimaryRegionTAS(anyString());
    doReturn(Stream.of(Region.DCA).collect(Collectors.toSet())).when(tool).getPrimaryRegionHiveSync(anyString());

    // Execute the operation
    Result result = tool.execute(context, args);

    // Verify the result
    assertEquals(StatusCode.SUCCESS, result.statusCode);
    assertEquals("Replication operation complete", result.message);
  }

  @Test
  public void testComparePrimaryRegionsWithError() throws Exception {
    HoodieOperationReplication.Operation op = HoodieOperationReplication.Operation.COMPARE_PRIMARY_REGION;

    // Initialize arguments
    String tables = "test_db.test_table";
    String[] args = new String[]{
      "--operation", op.name(),
      "--table", tables
    };

    // Mock the Spark context
    JavaSparkContext jsc = mock(JavaSparkContext.class);
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    when(context.jsc()).thenReturn(jsc);
    doReturn(new HadoopStorageConfiguration(new Configuration())).when(context).getStorageConf();

    // Create a test instance and spy on it
    HoodieOperationReplicationTest tool = spy(new HoodieOperationReplicationTest());

    // Mock the getPrimaryRegion methods - TAS throws exception
    doThrow(new HoodieException("TAS error")).when(tool).getPrimaryRegionTAS(anyString());
    doReturn(Stream.of(Region.PHX).collect(Collectors.toSet())).when(tool).getPrimaryRegionHiveSync(anyString());

    // Execute the operation
    Result result = tool.execute(context, args);

    // Verify the result
    assertEquals(StatusCode.SUCCESS, result.statusCode);
    assertEquals("Replication operation complete", result.message);
  }
}
