package com.uber.hudi.tools.manager;

import com.beust.jcommander.Parameter;
import com.uber.hoodie.utilities.replication.HoodieReplicationConsistency;
import com.uber.hoodie.utilities.replication.HoodieReplicationConsistencyInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.replication.table.HoodieReplicationMetadataClient;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INIT_INSTANT_TS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import com.uber.hudi.tools.utils.HoodieFileCopyUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * HoodieOperationReplicationIntegTests - E2E Replication Integration Tests for Hudi Manager
 *
 * This class provides end-to-end replication integration testing capabilities for Hudi operations
 * across different setup types. It supports executing sequences of operations and
 * validating their consistency between source and target datasets.
 *
 * Example usage:
 *
 * 1. Basic restore and validation sequence (with specific instant):
 *    --basepath /path/to/source --target-basepath /path/to/target
 *    --setup-type e2e --operation-sequence "restore instant=20231201120000;delay seconds=300;validate_restore"
 *
 * 1a. Auto-restore to second-to-last ingestion instant:
 *     --basepath /path/to/source --target-basepath /path/to/target
 *     --setup-type e2e --operation-sequence "restore;delay seconds=300;validate_restore"
 *
 * 2. Clean operation with validation:
 *    --basepath /path/to/source --target-basepath /path/to/target
 *    --setup-type e2e --operation-sequence "clean;delay seconds=180;validate_clean"
 *
 * 3. Archive operation with comprehensive validation:
 *    --basepath /path/to/source --target-basepath /path/to/target
 *    --setup-type e2e --operation-sequence "archive;delay seconds=600;validate_archival max_difference=3;validate_timelines destination=SECONDARY"
 *
 * Supported setup types:
 * - USING_E2E: Direct end-to-end validation (implemented)
 * - USING_UNION_FS: Union filesystem approach (placeholder)
 * - USING_SHADOW_COPY: Shadow copy approach (placeholder)
 *
 * Supported operations:
 * - DELAY: Wait for specified seconds (use: delay seconds=N)
 * - RESTORE: Perform restore to specified instant (use: restore instant=timestamp, or just 'restore' to auto-select second-to-last ingestion instant)
 * - CLEAN: Execute clean operation
 * - ARCHIVE: Execute archive operation
 * - VALIDATE_RESTORE: Validate restore operation consistency
 * - VALIDATE_CLEAN: Validate clean operation consistency
 * - VALIDATE_ARCHIVAL: Validate archival operation consistency (use: validate_archival max_difference=N)
 * - VALIDATE_TIMELINES: Comprehensive timeline validation (use: validate_timelines destination=SECONDARY)
 */
public class HoodieOperationReplicationIntegTests implements HoodieOperation, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieOperationReplicationIntegTests.class);

  // Checkpoint type constants
  private static final String CHECKPOINT_TYPE_REPLICATED = "replicated";
  private static final String CHECKPOINT_TYPE_ARCHIVED = "archived";
  private static final String CHECKPOINT_TYPE_CLUSTERING = "clustering";

  public enum SetupType {
    USING_E2E("e2e"),
    USING_UNION_FS("union_fs"),
    USING_SHADOW_COPY("shadow_copy");

    public final String label;
    private static final Map<String, SetupType> LABEL_TO_SETUP = new HashMap<>();

    static {
      for (SetupType setup : values()) {
        LABEL_TO_SETUP.put(setup.label, setup);
      }
    }

    private SetupType(String label) {
      this.label = label;
    }

    public static SetupType valueOfLabel(String label) {
      return LABEL_TO_SETUP.get(label);
    }
  }

  public enum ReplicationIntegTestOperation {
    DELAY("delay"),
    RESTORE("restore"),
    CLEAN("clean"),
    ARCHIVE("archive"),
    VALIDATE_RESTORE("validate_restore"),
    VALIDATE_CLEAN("validate_clean"),
    VALIDATE_ARCHIVAL("validate_archival"),
    VALIDATE_TIMELINES("validate_timelines"),
    CLEAR_CHECKPOINTS("clear_checkpoints"),
    VERIFY_CHECKPOINTS("verify_checkpoints"),
    VERIFY_GET("verify_get"),
    VERIFY_THROW("verify_throw"),
    SHADOW_COPY("shadow_copy"),
    INCREMENTAL_REPLICATION("incremental_replication");

    public final String label;
    private static final Map<String, ReplicationIntegTestOperation> LABEL_TO_OPERATION = new HashMap<>();

    static {
      for (ReplicationIntegTestOperation op : values()) {
        LABEL_TO_OPERATION.put(op.label, op);
      }
    }

    private ReplicationIntegTestOperation(String label) {
      this.label = label;
    }

    public static ReplicationIntegTestOperation valueOfLabel(String label) {
      return LABEL_TO_OPERATION.get(label);
    }
  }

  public static class OperationStep implements Serializable {
    private final ReplicationIntegTestOperation operation;
    private final Map<String, String> parameters;

    public OperationStep(ReplicationIntegTestOperation operation, Map<String, String> parameters) {
      this.operation = operation;
      this.parameters = parameters != null ? parameters : new HashMap<>();
    }

    public ReplicationIntegTestOperation getOperation() {
      return operation;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    public String getParameter(String key) {
      return parameters.get(key);
    }

    public String getParameter(String key, String defaultValue) {
      return parameters.getOrDefault(key, defaultValue);
    }

    @Override
    public String toString() {
      return String.format("OperationStep{operation=%s, parameters=%s}", operation, parameters);
    }
  }

  public static class HoodieOperationReplicationIntegTestsConfig implements Serializable {
    @Parameter(names = {"--basepath"}, description = "Base path of the source dataset", required = true)
    public String basePath;

    @Parameter(names = {"--target-basepath"}, description = "Base path of the target dataset", required = true)
    public String targetBasePath;

    @Parameter(names = {"--setup-type"}, description = "Setup type for integration tests (e2e, union_fs, shadow_copy)")
    public String setupType = "e2e";

    @Parameter(names = {"--operation-sequence"}, description = "Semicolon-separated sequence of operations to execute", required = true)
    public String operationSequence;

    @Parameter(names = {"--num-executors"}, description = "Number of executors to use")
    public int numExecutors = 10;

    @Parameter(names = {"--enable-metadata"}, description = "Whether MDT should be enabled", required = true, arity = 1)
    public boolean enableMetadata;

    @Parameter(names = {"--hoodie-config"}, description = "Hoodie config override (key=value)")
    public List<String> hoodieConfigs = new ArrayList<>();

    @Parameter(names = {"--destination"}, description = "Replication destination for checkpoint operations")
    public ReplicationDestination destination;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public Result execute(HoodieSparkEngineContext context, String[] args) throws Exception {
    LOG.info("Executing integration tests operation");
    HoodieOperationReplicationIntegTestsConfig cfg = new HoodieOperationReplicationIntegTestsConfig();
    HoodieManagerUtil.parseArguments(cfg, args);

    // Validate configuration
    Result validationResult = validateConfig(cfg);
    if (validationResult.statusCode != StatusCode.SUCCESS) {
      return validationResult;
    }

    SetupType setup = SetupType.valueOfLabel(cfg.setupType);
    if (setup == null) {
      return new Result(StatusCode.INVALID_ARGUMENT, String.format("Invalid setup type: %s. Supported types: e2e, union_fs, shadow_copy", cfg.setupType));
    }

    List<OperationStep> operationSteps;
    try {
      operationSteps = parseOperationSequence(cfg.operationSequence);
      LOG.debug("Parsed operation sequence: {}", operationSteps);
    } catch (Exception e) {
      return new Result(StatusCode.INVALID_ARGUMENT, String.format("Failed to parse operation sequence: %s", e.getMessage()));
    }

    switch (setup) {
      case USING_E2E:
        return executeE2EOperations(context, cfg, operationSteps);
      case USING_UNION_FS:
        throw new UnsupportedOperationException("USING_UNION_FS setup is not implemented yet");
      case USING_SHADOW_COPY:
        throw new UnsupportedOperationException("USING_SHADOW_COPY setup is not implemented yet");
      default:
        throw new HoodieException(String.format("Unsupported setup type: %s", setup));
    }
  }

  private Result validateConfig(HoodieOperationReplicationIntegTestsConfig cfg) {
    if (cfg.basePath == null || cfg.basePath.trim().isEmpty()) {
      return new Result(StatusCode.INVALID_ARGUMENT, "Base path is required");
    }

    if (cfg.targetBasePath == null || cfg.targetBasePath.trim().isEmpty()) {
      return new Result(StatusCode.INVALID_ARGUMENT, "Target base path is required");
    }

    if (cfg.operationSequence == null || cfg.operationSequence.trim().isEmpty()) {
      return new Result(StatusCode.INVALID_ARGUMENT, "Operation sequence is required");
    }

    if (cfg.numExecutors <= 0) {
      return new Result(StatusCode.INVALID_ARGUMENT, "Number of executors must be positive");
    }

    return new Result(StatusCode.SUCCESS, "Configuration validation passed");
  }

  protected List<OperationStep> parseOperationSequence(String operationSequence) {
    List<OperationStep> steps = new ArrayList<>();
    String[] operations = operationSequence.split(";");

    for (String opStr : operations) {
      opStr = opStr.trim();
      String[] parts = opStr.split("\\s+", 2);
      String operationName = parts[0];

      ReplicationIntegTestOperation operation = ReplicationIntegTestOperation.valueOfLabel(operationName.toLowerCase());
      if (operation == null) {
        throw new HoodieException(String.format("Invalid operation: %s", operationName));
      }

      Map<String, String> parameters = new HashMap<>();
      if (parts.length > 1) {
        // Parse parameters in key=value format
        String paramStr = parts[1];
        String[] paramPairs = paramStr.split("\\s+");
        for (String pair : paramPairs) {
          String[] kv = pair.split("=", 2);
          if (kv.length == 2) {
            parameters.put(kv[0], kv[1]);
          }
        }
      }

      steps.add(new OperationStep(operation, parameters));
    }

    return steps;
  }

  private Result executeE2EOperations(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, List<OperationStep> operationSteps) {
    LOG.info("Executing E2E integration tests with {} operations", operationSteps.size());

    try {
      for (int i = 0; i < operationSteps.size(); i++) {
        OperationStep step = operationSteps.get(i);
        LOG.info("Executing step {}/{}: {}", i + 1, operationSteps.size(), step);

        Result stepResult = executeE2EOperation(context, cfg, step);
        if (stepResult.statusCode != StatusCode.SUCCESS) {
          return new Result(StatusCode.FAILURE, String.format("Step %d failed: %s", i + 1, stepResult.message));
        }
      }

      return new Result(StatusCode.SUCCESS, String.format("Successfully executed %d integration test operations", operationSteps.size()));
    } catch (Exception e) {
      LOG.error("Failed to execute integration test operations", e);
      return new Result(StatusCode.FAILURE, String.format("Integration test execution failed: %s", e.getMessage()));
    }
  }

  private Result executeE2EOperation(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    switch (step.getOperation()) {
      case DELAY:
        return executeDelay(step);
      case RESTORE:
        return executeRestore(context, cfg, step);
      case CLEAN:
        return executeClean(context, cfg, step);
      case ARCHIVE:
        return executeArchive(context, cfg, step);
      case VALIDATE_RESTORE:
        return executeValidateRestore(context, cfg, step);
      case VALIDATE_CLEAN:
        return executeValidateClean(context, cfg, step);
      case VALIDATE_ARCHIVAL:
        return executeValidateArchival(context, cfg, step);
      case VALIDATE_TIMELINES:
        return executeValidateTimelines(context, cfg, step);
      /* Checkpoint related Operations */
      case CLEAR_CHECKPOINTS:
        return executeClearCheckpoints(context, cfg, step);
      case VERIFY_CHECKPOINTS:
        return executeVerifyCheckpoints(context, cfg, step);
      case VERIFY_GET:
        return executeVerifyGet(context, cfg, step);
      case VERIFY_THROW:
        return executeVerifyThrow(context, cfg, step);
      /* Replication copy operations */
      case SHADOW_COPY:
        return executeShadowCopy(context, cfg, step);
      case INCREMENTAL_REPLICATION:
        return executeIncrementalReplication(context, cfg, step);
      default:
        throw new HoodieException(String.format("Unsupported operation: %s", step.getOperation()));
    }
  }

  private Result executeDelay(OperationStep step) {
    try {
      int seconds = Integer.parseInt(step.getParameter("seconds", "60"));
      LOG.info("Delaying for {} seconds", seconds);
      Thread.sleep(seconds * 1000L);
      return new Result(StatusCode.SUCCESS, String.format("Delayed for %d seconds", seconds));
    } catch (NumberFormatException e) {
      return new Result(StatusCode.INVALID_ARGUMENT, "Invalid delay seconds: " + step.getParameter("seconds"));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Result(StatusCode.FAILURE, "Delay operation was interrupted");
    }
  }

  private Result executeClearCheckpoints(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    Result destCheck = requireDestination(cfg, "clear checkpoints");
    if (destCheck != null) {
      return destCheck;
    }

    try {
      HoodieReplicationMetadataClient replicationClient = createConfiguredReplicationClient(context, cfg);

      String tableName = step.getParameter("table_name");
      if (tableName == null) {
        tableName = replicationClient.getMetaClient().getFullTableName();
      }

      // TODO: Implement properties-based checkpoint clearing via ReplicationPropertiesManager
      String message = "Properties-based checkpoint clearing not yet implemented";
      return new Result(StatusCode.SUCCESS, message);

    } catch (Exception e) {
      String errorMsg = String.format("Failed to clear checkpoints: %s", e.getMessage());
      LOG.error(errorMsg, e);
      return new Result(StatusCode.FAILURE, errorMsg);
    }
  }

  private Result executeVerifyCheckpoints(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    Result destCheck = requireDestination(cfg, "verify checkpoints");
    if (destCheck != null) {
      return destCheck;
    }

    try {
      HoodieReplicationMetadataClient replicationClient = createConfiguredReplicationClient(context, cfg);
      return performCheckpointVerification(replicationClient, cfg.basePath, cfg.destination);
    } catch (Exception e) {
      String errorMsg = String.format("Exception during checkpoint verification for %s: %s", cfg.basePath, e.getMessage());
      LOG.error(errorMsg, e);
      return new Result(StatusCode.FAILURE, "FAIL: " + errorMsg);
    }
  }

  private Result performCheckpointVerification(HoodieReplicationMetadataClient replicationClient,
                                               String basePath, ReplicationDestination destination) {
    List<String> testResults = new ArrayList<>();
    List<String> errors = new ArrayList<>();

    try {
      // Get most recent instant timestamps from active/archived timeline for validation
      Option<HoodieInstant> lastActiveInstant = replicationClient.getMetaClient()
          .getActiveTimeline().filterCompletedInstants().lastInstant();
      Option<HoodieInstant> lastArchivedInstant = replicationClient.getMetaClient()
          .getArchivedTimeline().filterCompletedInstants().lastInstant();

      if (!lastActiveInstant.isPresent()) {
        return new Result(StatusCode.FAILURE,
            "No completed instants on active timeline for " + basePath);
      }
      String replicatedCommitTime = lastActiveInstant.get().requestedTime();

      List<CheckpointOperation> allOperations = new ArrayList<>();

      allOperations.add(new CheckpointOperation(CheckpointOperationType.REPLICATED,
          "getLastReplicatedCommit", "setLastReplicatedCommit",
          () -> replicationClient.getLastReplicatedCommit(),
          timestamp -> replicationClient.setLastReplicatedCommit(timestamp),
          replicatedCommitTime));

      lastArchivedInstant.ifPresent(instant ->
          allOperations.add(new CheckpointOperation(CheckpointOperationType.ARCHIVED,
              "getLastArchivedCommit", "setLastArchivedCommit",
              () -> replicationClient.getLastArchivedCommit(),
              timestamp -> replicationClient.setLastArchivedCommit(timestamp),
              instant.requestedTime())));

      replicationClient.getMetaClient().getActiveTimeline()
          .getCompletedReplaceTimeline().lastInstant()
          .ifPresent(instant ->
              allOperations.add(new CheckpointOperation(CheckpointOperationType.CLUSTERING,
                  "getLastReplicatedClusteringCommit", "setLastReplicatedCommit",
                  () -> replicationClient.getLastReplicatedClusteringCommit(),
                  timestamp -> replicationClient.setLastReplicatedCommit(timestamp),
                  instant.requestedTime())));

      Map<String, List<String>> resultsByType = new HashMap<>();
      resultsByType.put("Get Operations", new ArrayList<>());
      resultsByType.put("Set Operations", new ArrayList<>());
      resultsByType.put("Persistence", new ArrayList<>());

      allOperations.forEach(op -> processCheckpointOperation(op, resultsByType, errors));

      resultsByType.forEach((type, results) -> {
        if (!results.isEmpty()) {
          testResults.add(type + " - " + String.join(", ", results));
        }
      });

      String summary = String.join("; ", testResults);
      if (errors.isEmpty()) {
        return new Result(StatusCode.SUCCESS, String.format("PASS: %s: %s", basePath, summary));
      } else {
        return new Result(StatusCode.FAILURE,
            String.format("FAIL: %s: %s; Errors: %s", basePath, summary, String.join("; ", errors)));
      }

    } catch (Exception e) {
      return new Result(StatusCode.FAILURE,
          String.format("FAIL: %s: Verification exception: %s", basePath, e.getMessage()));
    }
  }

  private void processCheckpointOperation(CheckpointOperation op, Map<String, List<String>> resultsByType, List<String> errors) {
    if (op.hasSetOperation() && op.testTimestamp != null) {
      boolean setSuccess = testVoidOperation(() -> op.setOperation.accept(op.testTimestamp), op.setOperationName, errors);
      resultsByType.get("Set Operations").add(String.format("%s: %s", op.displayName(), setSuccess));

      if (setSuccess) {
        String verifyResult = testCallable(op.getOperation, "verify " + op.getOperationName, errors);
        boolean matches = op.testTimestamp.equals(verifyResult);
        resultsByType.get("Persistence").add(String.format("%s Persistence - Expected: %s, Got: %s, Match: %s",
            op.displayName(), op.testTimestamp, verifyResult, matches));
        if (!matches) {
          errors.add(op.displayName() + " commit persistence verification failed");
        }
      }
    }

    String getResult = testCallable(op.getOperation, op.getOperationName, errors);
    resultsByType.get("Get Operations").add(String.format("%s: %s", op.displayName(),
        getResult != null ? getResult : "null"));
  }

  private String testCallable(Callable<String> operation, String operationName, List<String> errors) {
    try {
      String result = operation.call();
      LOG.debug("{} successful: {}", operationName, result);
      return result;
    } catch (Exception e) {
      String error = String.format("%s failed: %s", operationName, e.getMessage());
      errors.add(error);
      LOG.error(error, e);
      return null;
    }
  }

  private boolean testVoidOperation(VoidOperation operation, String operationName, List<String> errors) {
    try {
      operation.execute();
      LOG.debug("{} successful", operationName);
      return true;
    } catch (Exception e) {
      String error = String.format("%s failed: %s", operationName, e.getMessage());
      errors.add(error);
      LOG.error(error, e);
      return false;
    }
  }

  private Result executeVerifyGet(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    // Validate parameters
    Result validationResult = validateCheckpointParameters(step, cfg);
    if (validationResult.statusCode != StatusCode.SUCCESS) {
      return validationResult;
    }

    String type = step.getParameter("type");
    String expectedTimestamp = step.getParameter("timestamp");

    try {
      HoodieReplicationMetadataClient replicationClient = createConfiguredReplicationClient(context, cfg);

      // Get the actual checkpoint value using helper method
      String actualCheckpoint = getCheckpointByType(replicationClient, type);

      // Enhancement (b): If expectedTimestamp not specified, get default from timeline
      if (expectedTimestamp == null || expectedTimestamp.trim().isEmpty()) {
        expectedTimestamp = getDefaultTimestampByType(replicationClient.getMetaClient(), type);

        if (expectedTimestamp == null) {
          return new Result(StatusCode.FAILURE,
              String.format("No expected timestamp provided and unable to determine default from %s timeline", type));
        }
        LOG.info("Using default expected timestamp from timeline: {}", expectedTimestamp);
      }

      if (expectedTimestamp.equals(actualCheckpoint)) {
        return new Result(StatusCode.SUCCESS, String.format(
            "PASS: Verify get successful: %s checkpoint matches expected timestamp %s", type, expectedTimestamp));
      } else {
        return new Result(StatusCode.FAILURE, String.format(
            "FAIL: Verify get failed: %s checkpoint is %s, expected %s", type, actualCheckpoint, expectedTimestamp));
      }

    } catch (IllegalArgumentException e) {
      return new Result(StatusCode.INVALID_ARGUMENT, e.getMessage());
    } catch (Exception e) {
      String errorMsg = String.format("Failed to verify get %s checkpoint: %s", type, e.getMessage());
      LOG.error(errorMsg, e);
      return new Result(StatusCode.FAILURE, errorMsg);
    }
  }

  private Result executeVerifyThrow(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    String operation = step.getParameter("operation");
    String expectedExceptionType = step.getParameter("exception");

    if (operation == null) {
      return new Result(StatusCode.INVALID_ARGUMENT, "Parameter 'operation' is required for verify_throw");
    }

    try {
      // Execute the operation that should throw an exception
      switch (operation) {
        case "get_checkpoint":
          HoodieReplicationMetadataClient replicationClient = createConfiguredReplicationClient(context, cfg);
          String type = step.getParameter("type");
          if (type != null && (type.equals(CHECKPOINT_TYPE_REPLICATED) || type.equals(CHECKPOINT_TYPE_ARCHIVED) || type.equals(CHECKPOINT_TYPE_CLUSTERING))) {
            getCheckpointByType(replicationClient, type);
          } else {
            replicationClient.getLastReplicatedCommit();
          }
          break;
        default:
          return new Result(StatusCode.INVALID_ARGUMENT, "Unsupported operation for verify_throw: " + operation);
      }

      // If we reach here, no exception was thrown
      return new Result(StatusCode.FAILURE,
          String.format("Expected exception for operation '%s' but none was thrown", operation));

    } catch (Exception e) {
      // An exception was thrown as expected
      String actualExceptionType = e.getClass().getSimpleName();

      if (expectedExceptionType != null && !expectedExceptionType.equals(actualExceptionType)) {
        return new Result(StatusCode.FAILURE,
            String.format("Expected exception type '%s' but got '%s': %s",
                expectedExceptionType, actualExceptionType, e.getMessage()));
      }

      return new Result(StatusCode.SUCCESS, String.format(
          "PASS: Verify throw successful: Expected exception thrown - %s: %s",
          actualExceptionType, e.getMessage()));
    }
  }

  protected Result executeRestore(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    HoodieTableMetaClient metaClient = buildMetaClient(cfg.basePath);

    String instantTime = step.getParameter("instant");
    if (instantTime == null) {
      // If no instant is provided, get the second-to-last ingestion instant
      instantTime = getSecondToLastIngestionInstant(metaClient);
      if (instantTime == null) {
        return new Result(StatusCode.INVALID_ARGUMENT, "No instant parameter provided and unable to determine second-to-last ingestion instant");
      }
      LOG.info("No instant parameter provided. Using second-to-last ingestion instant: {}", instantTime);
    }

    LOG.info("Executing restore operation to instant: {}", instantTime);

    Map<String, String> restoreOverrides = new HashMap<>();
    restoreOverrides.put(HoodieWriteConfig.ROLLBACK_PARALLELISM_VALUE.key(), String.valueOf(cfg.numExecutors));
    restoreOverrides.put(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key(), String.valueOf(false));
    SparkRDDWriteClient writeClient = createWriteClient(context, cfg, metaClient, restoreOverrides);

    try {
      if (writeClient.restoreToInstant(instantTime, cfg.enableMetadata) == null) {
        return new Result(StatusCode.FAILURE, String.format("Failed to restore to instant %s", instantTime));
      }
      return new Result(StatusCode.SUCCESS, String.format("Restored to instant %s", instantTime));
    } finally {
      writeClient.close();
    }
  }

  private Result executeClean(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    HoodieTableMetaClient metaClient = buildMetaClient(cfg.basePath);

    Map<String, String> cleanOverrides = new HashMap<>();
    cleanOverrides.put(HoodieCleanConfig.CLEANER_PARALLELISM_VALUE.key(), String.valueOf(cfg.numExecutors));
    SparkRDDWriteClient writeClient = createWriteClient(context, cfg, metaClient, cleanOverrides);

    try {
      HoodieCleanMetadata cleanMetadata = writeClient.clean();
      if (cleanMetadata != null) {
        return new Result(StatusCode.SUCCESS,
            String.format("Clean operation completed with instant %s", cleanMetadata.getStartCleanTime()));
      } else {
        return new Result(StatusCode.SUCCESS, "Clean operation completed (no files to clean)");
      }
    } finally {
      writeClient.close();
    }
  }

  private Result executeArchive(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    HoodieTableMetaClient metaClient = buildMetaClient(cfg.basePath);

    Map<String, String> archiveOverrides = new HashMap<>();
    archiveOverrides.put(HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT.key(), "false");
    SparkRDDWriteClient writeClient = createWriteClient(context, cfg, metaClient, archiveOverrides);

    try {
      writeClient.archive();
      return new Result(StatusCode.SUCCESS, "Archive operation completed successfully");
    } finally {
      writeClient.close();
    }
  }

  /**
   * Common helper to compare the last completed instant from a specific timeline across source and target.
   * Returns SUCCESS with matching timestamp, or FAILURE with the mismatch details.
   */
  private Result validateTimelineInstants(HoodieOperationReplicationIntegTestsConfig cfg, String operationType,
      Function<HoodieTableMetaClient, Option<HoodieInstant>> instantExtractor) {
    HoodieTableMetaClient sourceMetaClient = buildMetaClient(cfg.basePath);
    HoodieTableMetaClient targetMetaClient = buildMetaClient(cfg.targetBasePath);

    Option<HoodieInstant> sourceInstant = instantExtractor.apply(sourceMetaClient);
    Option<HoodieInstant> targetInstant = instantExtractor.apply(targetMetaClient);

    if (!sourceInstant.isPresent() && !targetInstant.isPresent()) {
      return new Result(StatusCode.SUCCESS, String.format("No %s operations found on either timeline", operationType));
    }
    if (!sourceInstant.isPresent()) {
      return new Result(StatusCode.FAILURE, String.format("No %s operation found on source timeline", operationType));
    }
    if (!targetInstant.isPresent()) {
      return new Result(StatusCode.FAILURE, String.format("No %s operation found on target timeline", operationType));
    }

    String sourceTime = sourceInstant.get().requestedTime();
    String targetTime = targetInstant.get().requestedTime();
    if (!sourceTime.equals(targetTime)) {
      return new Result(StatusCode.FAILURE, String.format(
          "%s timestamps don't match - Source: %s, Target: %s", operationType, sourceTime, targetTime));
    }

    return new Result(StatusCode.SUCCESS, String.format("%s validation passed for instant %s", operationType, sourceTime));
  }

  private Result executeValidateRestore(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    try {
      Function<HoodieTableMetaClient, Option<HoodieInstant>> extractor =
          mc -> mc.getActiveTimeline().getRestoreTimeline().filterCompletedInstants().lastInstant();

      Result comparisonResult = validateTimelineInstants(cfg, "Restore", extractor);
      if (comparisonResult.statusCode != StatusCode.SUCCESS) {
        return comparisonResult;
      }

      // Additional restore-specific audit on target
      HoodieTableMetaClient targetMetaClient = buildMetaClient(cfg.targetBasePath);
      Option<HoodieInstant> targetRestore = extractor.apply(targetMetaClient);
      if (!targetRestore.isPresent()) {
        return comparisonResult;
      }

      HoodieOperationRestore restoreOperation = new HoodieOperationRestore();
      boolean auditPassed = restoreOperation.auditPostRestore(
          context, targetMetaClient, cfg.targetBasePath, targetRestore.get(), cfg.numExecutors);
      if (!auditPassed) {
        return new Result(StatusCode.FAILURE, String.format(
            "Target restore audit failed for instant %s: Some files expected to be deleted are still present",
            targetRestore.get().requestedTime()));
      }

      // Validate record counts match between source and target
      Result countResult = validateRecordCounts(context, cfg.basePath, cfg.targetBasePath, "Restore");
      if (countResult.statusCode != StatusCode.SUCCESS) {
        return countResult;
      }

      String restoreTime = targetRestore.get().requestedTime();
      LOG.info("Restore validation and audit passed - both timelines have restore instant: {}", restoreTime);
      return new Result(StatusCode.SUCCESS, String.format("Restore validation and audit passed for instant %s", restoreTime));

    } catch (Exception e) {
      LOG.error("Failed to validate restore operation", e);
      return new Result(StatusCode.FAILURE, String.format("Restore validation failed: %s", e.getMessage()));
    }
  }

  private Result executeValidateClean(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    try {
      HoodieTableMetaClient sourceMetaClient = HoodieTableMetaClient.builder()
          .setConf(new HadoopStorageConfiguration(new Configuration()))
          .setBasePath(cfg.basePath)
          .build();

      HoodieTableMetaClient targetMetaClient = HoodieTableMetaClient.builder()
          .setConf(new HadoopStorageConfiguration(new Configuration()))
          .setBasePath(cfg.targetBasePath)
          .build();

      // Get the latest clean instants from both timelines
      Option<HoodieInstant> sourceClean = sourceMetaClient.getActiveTimeline()
          .getCleanerTimeline()
          .filterCompletedInstants()
          .lastInstant();

      Option<HoodieInstant> targetClean = targetMetaClient.getActiveTimeline()
          .getCleanerTimeline()
          .filterCompletedInstants()
          .lastInstant();

      if (!sourceClean.isPresent() && !targetClean.isPresent()) {
        return new Result(StatusCode.SUCCESS, "No clean operations found on either timeline");
      }

      if (!sourceClean.isPresent()) {
        return new Result(StatusCode.FAILURE, "No clean operation found on source timeline");
      }

      if (!targetClean.isPresent()) {
        return new Result(StatusCode.FAILURE, "No clean operation found on target timeline");
      }

      // Compare clean timestamps
      String sourceCleanTime = sourceClean.get().requestedTime();
      String targetCleanTime = targetClean.get().requestedTime();

      if (!sourceCleanTime.equals(targetCleanTime)) {
        return new Result(StatusCode.FAILURE, String.format(
            "Clean timestamps don't match - Source: %s, Target: %s", sourceCleanTime, targetCleanTime));
      }

      // Validate record counts match between source and target
      Result countResult = validateRecordCounts(context, cfg.basePath, cfg.targetBasePath, "Clean");
      if (countResult.statusCode != StatusCode.SUCCESS) {
        return countResult;
      }

      LOG.info("Clean validation passed - both timelines have clean instant: {}", sourceCleanTime);
      return new Result(StatusCode.SUCCESS, String.format("Clean validation passed for instant %s", sourceCleanTime));

    } catch (Exception e) {
      LOG.error("Failed to validate clean operation", e);
      return new Result(StatusCode.FAILURE, String.format("Clean validation failed: %s", e.getMessage()));
    }
  }

  private Result executeValidateArchival(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    try {
      HoodieTableMetaClient sourceMetaClient = buildMetaClient(cfg.basePath);
      HoodieTableMetaClient targetMetaClient = buildMetaClient(cfg.targetBasePath);

      List<HoodieInstant> sourceInstants = sourceMetaClient.getActiveTimeline().filterCompletedInstants().getInstants();
      List<HoodieInstant> targetInstants = targetMetaClient.getActiveTimeline().filterCompletedInstants().getInstants();

      int sizeDifference = Math.abs(sourceInstants.size() - targetInstants.size());
      int maxAllowedDifference = Integer.parseInt(step.getParameter("max_difference", "5"));

      if (sizeDifference > maxAllowedDifference) {
        return new Result(StatusCode.FAILURE, String.format(
            "Archival validation failed - timeline size difference (%d) exceeds threshold (%d). Source: %d, Target: %d",
            sizeDifference, maxAllowedDifference, sourceInstants.size(), targetInstants.size()));
      }

      // Validate record counts match between source and target
      Result countResult = validateRecordCounts(context, cfg.basePath, cfg.targetBasePath, "Archival");
      if (countResult.statusCode != StatusCode.SUCCESS) {
        return countResult;
      }

      LOG.info("Archival validation passed - timeline sizes are within acceptable range");
      return new Result(StatusCode.SUCCESS, String.format(
          "Archival validation passed - Source: %d instants, Target: %d instants (difference: %d)",
          sourceInstants.size(), targetInstants.size(), sizeDifference));

    } catch (Exception e) {
      LOG.error("Failed to validate archival operation", e);
      return new Result(StatusCode.FAILURE, String.format("Archival validation failed: %s", e.getMessage()));
    }
  }

  private Result executeValidateTimelines(HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    try {
      // Get replication destination for validation
      String destinationStr = step.getParameter("destination");
      if (destinationStr == null) {
        return new Result(StatusCode.INVALID_ARGUMENT, "Timeline validation requires 'destination' parameter (e.g., SECONDARY, TERTIARY)");
      }

      ReplicationDestination replicationDestination;
      try {
        replicationDestination = ReplicationDestination.valueOf(destinationStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        return new Result(StatusCode.INVALID_ARGUMENT, String.format(
            "Invalid destination '%s'. Valid destinations: %s", destinationStr,
            java.util.Arrays.toString(ReplicationDestination.values())));
      }

      LOG.debug("Running replication consistency validation for destination: {}", replicationDestination);

      // Create replication metadata client to get the last replicated commit (replication checkpoint)
      HoodieReplicationMetadataClient replicationMetadataClient = new HoodieReplicationMetadataClient(
          new HadoopStorageConfiguration(new Configuration()), cfg.basePath, replicationDestination);

      String lastReplicationTime = replicationMetadataClient.getLastReplicatedCommit();
      if (lastReplicationTime.equals(INIT_INSTANT_TS)) {
        return new Result(StatusCode.SUCCESS, String.format(
            "Replication not yet enabled/started for %s -> %s as lastInstantTime is %s",
            cfg.basePath, cfg.targetBasePath, INIT_INSTANT_TS));
      }

      LOG.debug("Validating timeline consistency at replication checkpoint: {}", lastReplicationTime);

      // Create HoodieReplicationConsistency validator
      HoodieReplicationConsistency consistency = new HoodieReplicationConsistency(
          context, cfg.basePath, cfg.targetBasePath, lastReplicationTime);

      // Set any additional configuration
      String ignoreArchival = step.getParameter("ignore_archival", "false");
      consistency.setIgnoreArchivalConsistency(Boolean.parseBoolean(ignoreArchival));

      // Run comprehensive consistency validation
      HoodieReplicationConsistencyInfo consistencyInfo = consistency.checkAllConsistency(Option.empty());
      consistencyInfo.setReplicationDestination(replicationDestination.label);

      if (consistencyInfo.isSuccessful()) {
        return new Result(StatusCode.SUCCESS, String.format(
            "Timeline validation passed - replication consistency validated at checkpoint %s for region %s",
            lastReplicationTime, replicationDestination.label));
      } else {
        return new Result(StatusCode.FAILURE, String.format(
            "Timeline validation failed - replication consistency check failed for %s -> %s at checkpoint %s",
            cfg.basePath, cfg.targetBasePath, lastReplicationTime));
      }

    } catch (Exception e) {
      LOG.error("Failed to execute timeline validation", e);
      return new Result(StatusCode.FAILURE, String.format("Timeline validation failed: %s", e.getMessage()));
    }
  }

  // --------------- Shadow Copy & Incremental Replication Operations ---------------

  /**
   * Generic record count validation between source and target datasets.
   * Reads both Hudi tablepaths via Spark and compares their record counts.
   * Note: This method assumes that both sourcePath and targetPath are synced at the same commit time.
   *
   * @param context the Spark engine context
   * @param sourcePath base path of the source dataset
   * @param targetPath base path of the target dataset
   * @param operationName name of the operation being validated (for logging)
   * @return Result with SUCCESS if counts match, FAILURE otherwise
   */
  protected Result validateRecordCounts(HoodieSparkEngineContext context, String sourcePath, String targetPath, String operationName) {
    try {
      SparkSession sparkSession = SparkSession.active();

      long sourceCount = sparkSession.read().format("hudi").load(sourcePath).count();
      long targetCount = sparkSession.read().format("hudi").load(targetPath).count();

      LOG.info("{} record count validation - Source: {}, Target: {}", operationName, sourceCount, targetCount);

      if (sourceCount != targetCount) {
        return new Result(StatusCode.FAILURE, String.format(
            "%s record count mismatch - Source: %d, Target: %d", operationName, sourceCount, targetCount));
      }

      return new Result(StatusCode.SUCCESS, String.format(
          "%s record count validation passed - both datasets have %d records", operationName, sourceCount));
    } catch (Exception e) {
      LOG.error("Failed to validate record counts for {}", operationName, e);
      return new Result(StatusCode.FAILURE, String.format(
          "%s record count validation failed: %s", operationName, e.getMessage()));
    }
  }

  /**
   * Bootstrap operation: copies a full dataset (data files + timeline) from basePath to targetBasePath
   * at a specific commit, modeled on HoodieShadowPipeline.initializeDataset.
   *
   * Parameters:
   *   commit (optional)        - instant timestamp to copy at; defaults to latest completed instant
   *   copy_timeline (optional) - "true"/"false", default true; whether to copy .hoodie instant files
   *   parallelism (optional)   - copy parallelism; defaults to cfg.numExecutors
   */
  private Result executeShadowCopy(HoodieSparkEngineContext context,
      HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    try {
      HoodieTableMetaClient srcMetaClient = buildMetaClient(cfg.basePath);
      FileSystem srcFs = new Path(cfg.basePath).getFileSystem(new Configuration());

      // Resolve commit timestamp
      String commitTime = step.getParameter("commit");
      if (commitTime == null || commitTime.trim().isEmpty()) {
        Option<HoodieInstant> lastInstant = srcMetaClient.getActiveTimeline()
            .getCommitsTimeline().filterCompletedInstants().lastInstant();
        if (!lastInstant.isPresent()) {
          return new Result(StatusCode.FAILURE, "No completed instants found on source timeline");
        }
        commitTime = lastInstant.get().requestedTime();
      }
      LOG.info("Shadow copy: bootstrapping target at commit {}", commitTime);

      boolean copyTimeline = Boolean.parseBoolean(step.getParameter("copy_timeline", "true"));
      int parallelism = Integer.parseInt(step.getParameter("parallelism",
          String.valueOf(cfg.numExecutors)));

      // Initialize target table by copying hoodie.properties from source
      FileSystem destFs = new Path(cfg.targetBasePath).getFileSystem(new Configuration());
      Path destMetaPath = new Path(cfg.targetBasePath, HoodieTableMetaClient.METAFOLDER_NAME);
      destFs.mkdirs(destMetaPath);
      Path srcPropsPath = new Path(srcMetaClient.getMetaPath().toString(), "hoodie.properties");
      Path destPropsPath = new Path(destMetaPath, "hoodie.properties");
      if (srcFs.exists(srcPropsPath)) {
        FileUtil.copy(srcFs, srcPropsPath, destFs, destPropsPath, false, new Configuration());
        LOG.info("Shadow copy: copied hoodie.properties to target");
      }

      // Discover partitions and collect base files at or before the commit
      List<String> partitions = FSUtils.getAllPartitionPaths(context, srcMetaClient, false);
      LOG.info("Shadow copy: discovered {} partitions", partitions.size());

      // Build file system view to get latest base files at the commit
      HoodieTableMetaClient freshSrcMetaClient = buildMetaClient(cfg.basePath);
      HoodieTimeline commitsTimeline = freshSrcMetaClient.getActiveTimeline()
          .getCommitsTimeline().filterCompletedInstants();
      List<String[]> filesToCopy = new ArrayList<>();
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, freshSrcMetaClient, commitsTimeline);
      try {
        for (String partition : partitions) {
          filesToCopy.addAll(
              fsView.getLatestBaseFilesBeforeOrOn(partition, commitTime)
                  .map(baseFile -> new String[]{partition, baseFile.getFileName()})
                  .collect(Collectors.toList()));
        }
      } finally {
        fsView.close();
      }
      LOG.info("Shadow copy: {} data files to copy across {} partitions", filesToCopy.size(), partitions.size());

      // Copy data files with checksum verification
      HadoopStorageConfiguration serConf = new HadoopStorageConfiguration(new Configuration());
      int failedCount = 0;
      for (String[] partitionFile : filesToCopy) {
        String partition = partitionFile[0];
        String fileName = partitionFile[1];
        Path srcPath = new Path(cfg.basePath + Path.SEPARATOR + partition, fileName);
        Path destPath = new Path(cfg.targetBasePath + Path.SEPARATOR + partition, fileName);
        destFs.mkdirs(destPath.getParent());
        if (!HoodieFileCopyUtils.copyFileWithRetry(serConf, srcPath, destPath, 3)) {
          LOG.error("Shadow copy: failed to copy {}", srcPath);
          failedCount++;
        }
      }

      if (failedCount > 0) {
        return new Result(StatusCode.FAILURE,
            String.format("Shadow copy failed: %d of %d files failed to copy", failedCount, filesToCopy.size()));
      }

      // Copy timeline instant files for the commit
      if (copyTimeline) {
        String commitPattern = cfg.basePath + Path.SEPARATOR
            + HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + commitTime + "*";
        FileStatus[] commitFiles = srcFs.globStatus(new Path(commitPattern));
        int timelineFilesCopied = 0;
        if (commitFiles != null) {
          for (FileStatus commitFile : commitFiles) {
            if (commitFile.isFile()) {
              Path destFile = new Path(destMetaPath, commitFile.getPath().getName());
              HoodieFileCopyUtils.copyFileWithRetry(serConf, commitFile.getPath(), destFile, 3);
              timelineFilesCopied++;
            }
          }
        }
        LOG.info("Shadow copy: copied {} timeline files for commit {}", timelineFilesCopied, commitTime);
      }

      return new Result(StatusCode.SUCCESS,
          String.format("PASS: Shadow copy complete - %d data files copied at commit %s",
              filesToCopy.size(), commitTime));

    } catch (Exception e) {
      String errorMsg = String.format("Shadow copy failed: %s", e.getMessage());
      LOG.error(errorMsg, e);
      return new Result(StatusCode.FAILURE, errorMsg);
    }
  }

  /**
   * Incremental replication operation: simulates HiveSync's replication loop using the V3 API.
   * Iterates commit-by-commit, processing each ReplicationStep in canonical order and dispatching
   * file operations (copy, create marker, delete) based on ReplicationAction.
   *
   * Modeled after hive_reair's HudiCommitsReplicationState / AsyncHoodieCommitReplicationExecutor.
   *
   * Parameters:
   *   destination (required)        - ReplicationDestination (e.g. SECONDARY, TERTIARY)
   *   max_iterations (optional)     - safety limit for commit loop; default 100
   */
  private Result executeIncrementalReplication(HoodieSparkEngineContext context,
      HoodieOperationReplicationIntegTestsConfig cfg, OperationStep step) throws Exception {
    String destinationStr = step.getParameter("destination");
    if (destinationStr == null) {
      destinationStr = cfg.destination != null ? cfg.destination.name() : null;
    }
    if (destinationStr == null) {
      return new Result(StatusCode.INVALID_ARGUMENT,
          "incremental_replication requires 'destination' parameter (e.g. SECONDARY, TERTIARY)");
    }

    ReplicationDestination destination;
    try {
      destination = ReplicationDestination.valueOf(destinationStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      return new Result(StatusCode.INVALID_ARGUMENT,
          String.format("Invalid destination '%s'. Valid: %s", destinationStr,
              java.util.Arrays.toString(ReplicationDestination.values())));
    }

    int maxIterations = Integer.parseInt(step.getParameter("max_iterations", "100"));

    try {
      Configuration hadoopConf = (Configuration) context.getStorageConf().unwrap();
      HadoopStorageConfiguration serConf = new HadoopStorageConfiguration(hadoopConf);

      HoodieReplicationMetadataClient srcReplicationClient =
          new HoodieReplicationMetadataClient(new HadoopStorageConfiguration(hadoopConf), cfg.basePath, destination);

      HoodieReplicationMetadataClient tgtReplicationClient =
          new HoodieReplicationMetadataClient(new HadoopStorageConfiguration(hadoopConf), cfg.targetBasePath, destination);

      String currentLrt = srcReplicationClient.getLastReplicatedCommit();
      LOG.info("Incremental replication: starting from LRT={}, destination={}", currentLrt, destination);

      FileSystem srcFs = new Path(cfg.basePath).getFileSystem(hadoopConf);
      FileSystem tgtFs = new Path(cfg.targetBasePath).getFileSystem(hadoopConf);
      int commitsReplicated = 0;
      int totalFilesCopied = 0;
      int totalMarkersCreated = 0;
      int totalFilesDeleted = 0;
      int totalDirsDeleted = 0;

      for (int iteration = 0; iteration < maxIterations; iteration++) {
        Map<HoodieReplicationMetadataClient.ReplicationStep,
            List<HoodieReplicationMetadataClient.ReplicationInfo>> replicationPayload =
                srcReplicationClient.getOrderedFilesForReplicationV3(currentLrt);

        // Per-commit counters
        int commitFilesCopied = 0;
        int commitMarkersCreated = 0;
        int commitFilesDeleted = 0;
        int commitDirsDeleted = 0;

        // Process each step in canonical replication ordering
        for (HoodieReplicationMetadataClient.ReplicationStep replicationStep
            : srcReplicationClient.getReplicationOrdering()) {
          List<HoodieReplicationMetadataClient.ReplicationInfo> infos =
              replicationPayload.get(replicationStep);
          if (infos == null) {
            continue;
          }

          for (HoodieReplicationMetadataClient.ReplicationInfo info : infos) {
            if (info == null || info.files == null) {
              continue;
            }

            boolean isMeta = replicationStep.name().startsWith("META_");
            String srcBase = isMeta
                ? cfg.basePath + Path.SEPARATOR + ".hoodie" + Path.SEPARATOR + "metadata"
                : cfg.basePath;
            String tgtBase = isMeta
                ? cfg.targetBasePath + Path.SEPARATOR + ".hoodie" + Path.SEPARATOR + "metadata"
                : cfg.targetBasePath;

            switch (info.action) {
              case REPLICATE_FILES:
                for (String file : info.files) {
                  Path srcPath = new Path(srcBase + Path.SEPARATOR + info.relativePath, file);
                  if (!srcFs.exists(srcPath)) {
                    LOG.warn("Incremental replication: skipping copy, source file does not exist: {}", srcPath);
                    continue;
                  }
                  Path destPath = new Path(tgtBase + Path.SEPARATOR + info.relativePath, file);
                  tgtFs.mkdirs(destPath.getParent());
                  if (!HoodieFileCopyUtils.copyFileWithRetry(serConf, srcPath, destPath, 3)) {
                    LOG.error("Incremental replication: failed to copy {}", srcPath);
                    return new Result(StatusCode.FAILURE,
                        String.format("Failed to copy file %s during replication", srcPath));
                  }
                  commitFilesCopied++;
                }
                break;

              case CREATE_FILES:
                for (String file : info.files) {
                  Path markerPath = new Path(tgtBase + Path.SEPARATOR + info.relativePath, file);
                  tgtFs.mkdirs(markerPath.getParent());
                  tgtFs.create(markerPath, true).close();
                  commitMarkersCreated++;
                }
                break;

              case DELETE_FILES:
                for (String file : info.files) {
                  Path filePath = new Path(tgtBase + Path.SEPARATOR + info.relativePath, file);
                  if (tgtFs.exists(filePath)) {
                    tgtFs.delete(filePath, false);
                    commitFilesDeleted++;
                  }
                }
                break;

              case DELETE_DIRS:
                for (String dir : info.files) {
                  Path dirPath = new Path(tgtBase + Path.SEPARATOR + dir);
                  if (tgtFs.exists(dirPath)) {
                    tgtFs.delete(dirPath, true);
                    commitDirsDeleted++;
                  }
                }
                break;

              default:
                LOG.warn("Incremental replication: unhandled action {} in step {}",
                    info.action, replicationStep);
            }
          }
        }

        // Extract V3 control fields
        String newLrt = extractV3Field(replicationPayload,
            HoodieReplicationMetadataClient.ReplicationStep.NEW_LAST_REPLICATION_TIMESTAMP);
        String hasMore = extractV3Field(replicationPayload,
            HoodieReplicationMetadataClient.ReplicationStep.HAS_MORE_COMMITS);

        // Update LRT: target first (if file-based checkpoints), then source
        if (newLrt != null && !newLrt.isEmpty()) {
          tgtReplicationClient.setLastReplicatedCommit(newLrt);
          srcReplicationClient.setLastReplicatedCommit(newLrt);
          currentLrt = newLrt;
          commitsReplicated++;
        }

        totalFilesCopied += commitFilesCopied;
        totalMarkersCreated += commitMarkersCreated;
        totalFilesDeleted += commitFilesDeleted;
        totalDirsDeleted += commitDirsDeleted;

        LOG.info("Incremental replication: commit {} - copied={}, markers={}, deleted={}, dirs_deleted={}",
            newLrt, commitFilesCopied, commitMarkersCreated, commitFilesDeleted, commitDirsDeleted);

        if (!"true".equals(hasMore)) {
          LOG.info("Incremental replication: no more commits after LRT={}", currentLrt);
          break;
        }
      }

      String summary = String.format(
          "PASS: Incremental replication complete - %d commits replicated, "
              + "%d files copied, %d markers created, %d files deleted, %d dirs deleted",
          commitsReplicated, totalFilesCopied, totalMarkersCreated, totalFilesDeleted, totalDirsDeleted);
      return new Result(StatusCode.SUCCESS, summary);

    } catch (Exception e) {
      String errorMsg = String.format("Incremental replication failed: %s", e.getMessage());
      LOG.error(errorMsg, e);
      return new Result(StatusCode.FAILURE, errorMsg);
    }
  }

  /**
   * Extracts a V3 control field (HAS_MORE_COMMITS or NEW_LAST_REPLICATION_TIMESTAMP)
   * from the replication payload. These are stored as single-element ReplicationInfo entries.
   */
  private String extractV3Field(
      Map<HoodieReplicationMetadataClient.ReplicationStep,
          List<HoodieReplicationMetadataClient.ReplicationInfo>> payload,
      HoodieReplicationMetadataClient.ReplicationStep step) {
    List<HoodieReplicationMetadataClient.ReplicationInfo> infos = payload.get(step);
    if (infos != null && !infos.isEmpty() && infos.get(0).files != null && !infos.get(0).files.isEmpty()) {
      return infos.get(0).files.get(0);
    }
    return null;
  }

  private Result requireDestination(HoodieOperationReplicationIntegTestsConfig cfg, String operationName) {
    if (cfg.destination == null) {
      return new Result(StatusCode.INVALID_ARGUMENT,
          "Destination is required for " + operationName + " operation");
    }
    return null;
  }

  private HoodieTableMetaClient buildMetaClient(String basePath) {
    return HoodieTableMetaClient.builder()
        .setConf(new HadoopStorageConfiguration(new Configuration()))
        .setBasePath(basePath)
        .build();
  }

  private SparkRDDWriteClient createWriteClient(HoodieSparkEngineContext context,
      HoodieOperationReplicationIntegTestsConfig cfg, HoodieTableMetaClient metaClient,
      Map<String, String> additionalOverrides) {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(HoodieMetadataConfig.ENABLE.key(), String.valueOf(cfg.enableMetadata));
    overrides.putAll(additionalOverrides);
    overrides.putAll(HoodieManagerUtil.getKeyGeneratorConfigs(metaClient.getTableConfig()));
    HoodieWriteConfig writeConfig = HoodieManagerUtil.getHoodieWriteConfig(overrides, cfg.hoodieConfigs, cfg.basePath, metaClient);
    return new SparkRDDWriteClient(context, writeConfig);
  }

  private HoodieReplicationMetadataClient createConfiguredReplicationClient(
      HoodieSparkEngineContext context, HoodieOperationReplicationIntegTestsConfig cfg) {
    return new HoodieReplicationMetadataClient(
        new HadoopStorageConfiguration((Configuration) context.getStorageConf().unwrap()),
        cfg.basePath, cfg.destination);
  }

  /**
   * Validates checkpoint operation parameters.
   */
  private Result validateCheckpointParameters(OperationStep step, HoodieOperationReplicationIntegTestsConfig cfg) {
    // Validate destination is provided for checkpoint operations
    if (cfg.destination == null) {
      return new Result(StatusCode.INVALID_ARGUMENT,
          String.format("Destination is required for checkpoint operation: %s", step.getOperation()));
    }

    // Validate checkpoint type parameter for VERIFY_GET
    if (step.getOperation() == ReplicationIntegTestOperation.VERIFY_GET) {
      String type = step.getParameter("type");
      if (type == null) {
        return new Result(StatusCode.INVALID_ARGUMENT,
            String.format("Parameter 'type' is required for %s operation", step.getOperation()));
      }
      if (!type.equals(CHECKPOINT_TYPE_REPLICATED) && !type.equals(CHECKPOINT_TYPE_ARCHIVED) && !type.equals(CHECKPOINT_TYPE_CLUSTERING)) {
        return new Result(StatusCode.INVALID_ARGUMENT,
            String.format("Invalid checkpoint type '%s'. Valid types: %s, %s, %s", type,
                CHECKPOINT_TYPE_REPLICATED, CHECKPOINT_TYPE_ARCHIVED, CHECKPOINT_TYPE_CLUSTERING));
      }
    }

    return new Result(StatusCode.SUCCESS, "Parameter validation passed");
  }

  protected String getSecondToLastIngestionInstant(HoodieTableMetaClient metaClient) {
    try {
      HoodieTableType tableType = metaClient.getTableType();

      // Get all ingestion instants in reverse chronological order
      List<HoodieInstant> ingestionInstants = metaClient
          .getCommitsTimeline()
          .filterCompletedInstants()
          .getReverseOrderedInstants()
          .filter(instant -> {
            if (tableType.equals(HoodieTableType.MERGE_ON_READ) && instant.getAction().equals(COMMIT_ACTION)) {
              // Compaction writes are not ingestion writes
              return false;
            }
            // Check that write is either not a replacecommit or an ingestion replacecommit
            return !ClusteringUtils.isClusteringOrReplaceCommitAction(instant.getAction());
          })
          .collect(java.util.stream.Collectors.toList());

      // Return the second-to-last ingestion instant if available
      if (ingestionInstants.size() >= 2) {
        String secondToLastInstant = ingestionInstants.get(1).requestedTime();
        LOG.debug("Found second-to-last ingestion instant: {}", secondToLastInstant);
        return secondToLastInstant;
      } else {
        LOG.error("Either 0 or 1 ingestion instants found in the timeline");
        return null;
      }
    } catch (Exception e) {
      LOG.error("Failed to determine second-to-last ingestion instant", e);
      return null;
    }
  }

  /**
   * Gets the oldest timestamp from the archival timeline.
   */
  protected String getOldestArchivedTimestamp(HoodieTableMetaClient metaClient) {
    try {
      HoodieTimeline archivedTimeline = metaClient.getArchivedTimeline().filterCompletedInstants();
      if (!archivedTimeline.empty()) {
        String oldestTimestamp = archivedTimeline.firstInstant().get().requestedTime();
        LOG.debug("Found oldest archived timestamp: {}", oldestTimestamp);
        return oldestTimestamp;
      } else {
        LOG.debug("No archived instants found in timeline");
        return null;
      }
    } catch (Exception e) {
      LOG.error("Failed to determine oldest archived timestamp", e);
      return null;
    }
  }

  /**
   * Gets the oldest timestamp from the clustering (replacecommit) timeline.
   */
  protected String getOldestClusteringTimestamp(HoodieTableMetaClient metaClient) {
    try {
      List<HoodieInstant> clusteringInstants = metaClient
          .getCommitsTimeline()
          .filterCompletedInstants()
          .getReverseOrderedInstants()
          .filter(instant -> ClusteringUtils.isClusteringOrReplaceCommitAction(instant.getAction()))
          .collect(java.util.stream.Collectors.toList());

      if (!clusteringInstants.isEmpty()) {
        String oldestTimestamp = clusteringInstants.get(clusteringInstants.size() - 1).requestedTime();
        LOG.debug("Found oldest clustering timestamp: {}", oldestTimestamp);
        return oldestTimestamp;
      } else {
        LOG.debug("No clustering instants found in timeline");
        return null;
      }
    } catch (Exception e) {
      LOG.error("Failed to determine oldest clustering timestamp", e);
      return null;
    }
  }

  /**
   * Helper method to get checkpoint value based on type.
   */
  private String getCheckpointByType(HoodieReplicationMetadataClient replicationClient, String type) throws Exception {
    switch (type) {
      case CHECKPOINT_TYPE_REPLICATED:
        return replicationClient.getLastReplicatedCommit();
      case CHECKPOINT_TYPE_ARCHIVED:
        return replicationClient.getLastArchivedCommit();
      case CHECKPOINT_TYPE_CLUSTERING:
        return replicationClient.getLastReplicatedClusteringCommit();
      default:
        throw new IllegalArgumentException("Invalid checkpoint type: " + type);
    }
  }

  /**
   * Helper method to get default timestamp from timeline based on checkpoint type.
   */
  private String getDefaultTimestampByType(HoodieTableMetaClient metaClient, String type) {
    switch (type) {
      case CHECKPOINT_TYPE_REPLICATED:
        return getSecondToLastIngestionInstant(metaClient);
      case CHECKPOINT_TYPE_ARCHIVED:
        return getOldestArchivedTimestamp(metaClient);
      case CHECKPOINT_TYPE_CLUSTERING:
        return getOldestClusteringTimestamp(metaClient);
      default:
        return null;
    }
  }

  @FunctionalInterface
  private interface VoidOperation {
    void execute() throws Exception;
  }

  private enum CheckpointOperationType {
    REPLICATED("Replicated"),
    ARCHIVED("Archived"),
    CLUSTERING("Clustering"),
    OPERATIONAL_STATUS("Operational Status"),
    TABLE_MANAGEMENT("Table Management");

    final String displayName;

    CheckpointOperationType(String displayName) {
      this.displayName = displayName;
    }
  }

  private static class CheckpointOperation {
    final CheckpointOperationType type;
    final String getOperationName;
    final String setOperationName;
    final Callable<String> getOperation;
    final java.util.function.Consumer<String> setOperation;
    final String testTimestamp;

    CheckpointOperation(CheckpointOperationType type, String getOperationName, String setOperationName,
                       Callable<String> getOperation, java.util.function.Consumer<String> setOperation,
                       String testTimestamp) {
      this.type = type;
      this.getOperationName = getOperationName;
      this.setOperationName = setOperationName;
      this.getOperation = getOperation;
      this.setOperation = setOperation;
      this.testTimestamp = testTimestamp;
    }

    String displayName() {
      return type.displayName;
    }

    boolean hasSetOperation() {
      return setOperation != null;
    }
  }
}
