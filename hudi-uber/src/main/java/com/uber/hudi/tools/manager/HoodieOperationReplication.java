package com.uber.hudi.tools.manager;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.replication.table.HoodieReplicationMetadataClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.HoodieReplicationContext;
import org.apache.hudi.replication.table.ReplicationDestination;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.replication.util.ReplicationPropertiesManager;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.replication.client.HoodieReplicationMetadata;
import org.apache.hudi.replication.client.HoodieReplicationMetadataUtils;
import org.apache.hudi.replication.client.HoodieTASClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.HoodieSparkSQLUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class HoodieOperationReplication implements HoodieOperation, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieOperationReplication.class);

  private static final String SECONDARY_REPLICATION_TABLE = "data_sre_db.hadoop_hive_table_size_nodedup";
  private static final String TERTIARY_REPLICATION_TABLE = "data_sre_db.hadoop_hive_table_size_nodedup_dcacld";

  private static final String GET_REPLICATION_DATASET_QUERY =
      "select DISTINCT(concat(db_name, '.', tbl_name)) as table_name, tbl_path\n"
      + "  from %s\n"
      + "  where\n"
      + "    run_time in (\n"
      + "      select run_time\n"
      + "      from\n"
      + "        (\n"
      + "          select tier, max(run_time) as run_time\n"
      + "          from %s\n"
      + "          where datestr = '%s'\n"
      + "          group by tier\n"
      + "        )\n"
      + "    )\n"
      + "    and db_name in '(%s)'\n"
      + "    and dc_replication=true";

  enum Operation {
    VIEW_REPLICATION_PROPERTIES,
    VIEW_REPLICATION_ENABLED_DATASET,
    ENABLE_REPLICATION,
    DISABLE_REPLICATION,
    REMOVE_REPLICATION_FLAG,
    REMOVE_OPERATIONAL_STATUS,
    DISABLE_OPERATIONAL_STATUS,
    COMPARE_PRIMARY_REGION,
    REMOVE_DIVERGED_COMMITS,
    CHECK_DIVERGED_COMMITS,
    CHECK_ARCHIVAL_MISSED,
    REMOVE_ARCHIVAL_MISSED,
    CHECK_INCREMENTAL_PERFORMANCE,
    CHECK_ARCHIVAL_PERFORMANCE_V2
  }

  enum FixArchivalStatusCode {
    ARCHIVE_FILE_NUMBER_MISMATCH, // Different number of files
    ARCHIVE_FILE_NAME_MISMATCH, // Primary and secondary clusters differ in file name
    ARCHIVED_FOLDER_MISSING_ON_PRIMARY,
    ARCHIVED_FOLDER_MISSING_ON_SECONDARY,
    PARTIAL_DELETE, // Got delete error for some files
    ARCHIVED_INSTANTS_NOT_REPLICATED_YET, // HiveSync has not set any archival timestamp so far
    COMMITS_CLEANED,
    UNKNOWN
  }

  public static class HoodieOperationReplicationConfig implements Serializable {
    @Parameter(names = {"--operation"}, description = "Replication operation", required = true)
    public Operation op;

    @Parameter(names = {"--destination"}, description = "replication destination to check datasets if base paths not provided")
    public ReplicationDestination destination;

    @Parameter(names = {"--basepath"}, description = "base path of hoodie dataset to set the replication flag/archival fixer op.")
    public List<String> basePaths = new ArrayList<>();

    @Parameter(names = {"--target-basepath"}, description = "comma separated target base paths")
    public List<String> targetBasePaths = new ArrayList<>();

    @Parameter(names = {"--table"}, description = "full table name (dbname.tablename) to set the replication flag/for archival fixer op")
    public List<String> tables = new ArrayList<>();

    @Parameter(names = {"--databases"}, description = "comma separated databases to filter out specific database if querying from hivesync table")
    public String dataBases;

    @Parameter(names = {"--csv-file-path"}, description = "Base path of the dataset to use. This will be source base path for archival fixer op")
    public String csvFilePath;

    @Parameter(names = {"--commit-prefix"}, description = "Prefix of commits to include for instance 202301 for archival fixer op")
    public String commitPrefix = "";

    @Parameter(names = {"--max-size"}, description = "Maximum number of instant files to delete in one run for archival fixer op")
    public Integer maxSize = 8192;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public Result execute(HoodieSparkEngineContext context, String[] args) throws Exception {
    LOG.info("Executing replication operation");
    HoodieOperationReplicationConfig cfg = new HoodieOperationReplicationConfig();
    HoodieManagerUtil.parseArguments(cfg, args);

    switch (cfg.op) {
      case VIEW_REPLICATION_PROPERTIES:
        viewReplicationProperties(context, cfg.basePaths, cfg.tables);
        break;
      case VIEW_REPLICATION_ENABLED_DATASET:
        viewReplicationEnabledDatasets(context, cfg.destination, cfg.dataBases);
        break;
      case ENABLE_REPLICATION:
        setReplication(context, cfg.destination, cfg.basePaths, cfg.tables, true);
        break;
      case DISABLE_REPLICATION:
        setReplication(context, cfg.destination, cfg.basePaths, cfg.tables, false);
        break;
      case REMOVE_REPLICATION_FLAG:
        String replicationEnabledKey = HoodieReplicationContext.getCrossRegionReplicationEnabledConfigKey(cfg.destination);
        removeReplicationProperty(context, cfg.basePaths, cfg.tables, replicationEnabledKey);
        break;
      case REMOVE_OPERATIONAL_STATUS:
        String operationalStatusKey = HoodieReplicationContext.getReplicationOperationalStatusConfigKey(cfg.destination);
        removeReplicationProperty(context, cfg.basePaths, cfg.tables, operationalStatusKey);
        break;
      case DISABLE_OPERATIONAL_STATUS:
        setOperationalStatus(context, cfg.basePaths, cfg.tables, cfg.destination, false);
        break;
      case COMPARE_PRIMARY_REGION:
        comparePrimaryRegions(cfg.tables);
        break;
      case CHECK_DIVERGED_COMMITS:
        checkAndFixDivergedCommits(context, cfg.basePaths, cfg.targetBasePaths, cfg.tables, cfg.destination, false);
        break;
      case REMOVE_DIVERGED_COMMITS:
        checkAndFixDivergedCommits(context, cfg.basePaths, cfg.targetBasePaths, cfg.tables, cfg.destination, true);
        break;
      case CHECK_ARCHIVAL_MISSED:
        return checkAndFixArchivedCommits(context, cfg, false);
      case REMOVE_ARCHIVAL_MISSED:
        return checkAndFixArchivedCommits(context, cfg, true);
      case CHECK_INCREMENTAL_PERFORMANCE:
        throw new UnsupportedOperationException("Check incremental performance operation is not supported yet");
      case CHECK_ARCHIVAL_PERFORMANCE_V2:
        throw new UnsupportedOperationException("Check archival performance operation is not supported yet");
      default:
        throw new HoodieException(String.format("Unsupported operation: %s", cfg.op));
    }

    return new Result(StatusCode.SUCCESS, "Replication operation complete");
  }

  private void viewReplicationProperties(HoodieSparkEngineContext context, List<String> basePaths, List<String> tables) throws Exception {
    basePaths.addAll(getBasePaths(context, tables));
    for (String basePath : basePaths) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(new HadoopStorageConfiguration(new Configuration())).build();
      ReplicationPropertiesManager manager = this.getReplicationPropertiesManager(metaClient);
      Properties props = manager.readProperties();

      LOG.info(String.format("%s replication.properties %s", basePath, props));
    }
  }

  List<String> getBasePaths(HoodieSparkEngineContext context, List<String> tables) {
    List<String> basePaths = new ArrayList<>();
    for (String table : tables) {
      basePaths.add(HoodieSparkSQLUtils.getBasePathFromTableName(context.getJavaSparkContext(), table));
    }
    return basePaths;
  }

  private void viewReplicationEnabledDatasets(HoodieSparkEngineContext context, ReplicationDestination destination, String database) {
    SparkSession sparkSession = SparkSession.builder().enableHiveSupport().sparkContext(context.getJavaSparkContext().sc()).getOrCreate();
    final String query = prepareHivesyncQuery(destination, database);
    List<Pair<String, String>> basePaths = sparkSession.sql(query)
        .collectAsList()
        .stream()
        .map(row -> Pair.of(row.getString(0), row.getString(1)))
        .collect(Collectors.toList());

    for (Pair<String, String> p : basePaths) {
      LOG.info(String.format("%s %s", p.getLeft(), p.getRight()));
    }
  }

  private void setReplication(HoodieSparkEngineContext context, ReplicationDestination destRegion, List<String> basePaths, List<String> tables, boolean enable) {
    basePaths.addAll(getBasePaths(context, tables));

    LOG.info(String.format("Setting replication enable flag (%s) for the following tables", enable));
    for (int i = 0; i < tables.size(); i++) {
      LOG.info(String.format("%s %s", tables.get(i), basePaths.get(i)));
    }
    List<String> failures = setReplicationEnableFlag(basePaths, destRegion, enable);
    if (failures.isEmpty()) {
      return;
    }

    LOG.info("Replication enable flag setting failed for the following tables");
    for (String path : failures) {
      LOG.info(path);
    }
  }

  private String prepareHivesyncQuery(ReplicationDestination destination, String databases) {
    switch (destination) {
      case SECONDARY_REGION:
        return String.format(GET_REPLICATION_DATASET_QUERY, SECONDARY_REPLICATION_TABLE, SECONDARY_REPLICATION_TABLE, LocalDate.now(), databases);
      case TERTIARY_REGION:
        return String.format(GET_REPLICATION_DATASET_QUERY, TERTIARY_REPLICATION_TABLE, TERTIARY_REPLICATION_TABLE, LocalDate.now(), databases);
      default:
        throw new HoodieException(String.format("Unsupported region: %s", destination));
    }
  }

  private boolean setReplicationEnableFlag(String basePath, ReplicationDestination destination, boolean enable) {
    HoodieReplicationMetadataClient replicationClient = this.getHoodieReplicationMetadataClient(new HadoopStorageConfiguration(new Configuration()), basePath, destination);

    if (!replicationClient.setCrossRegionReplicationEnabled(destination, enable)) {
      LOG.error(String.format("Setting %s replication enabled flag for %s failed", destination, basePath));
      return false;
    }

    try {
      HoodieReplicationMetadataClient metaDataReplicationClient = this.getHoodieReplicationMetadataClient(new HadoopStorageConfiguration(new Configuration()), HoodieTableMetadata.getMetadataTableBasePath(basePath).toString(), destination);
      if (!metaDataReplicationClient.setCrossRegionReplicationEnabled(destination, enable)) {
        LOG.error(String.format("Setting %s replication enabled flag for %s failed", destination, HoodieTableMetadata.getMetadataTableBasePath(basePath).toString()));
      }
    } catch (TableNotFoundException e) {
      LOG.info(String.format("Metadata doesn't exist for %s", basePath));
    }

    LOG.info(String.format("Successfully set %s replication enabled flag for %s", destination, basePath));
    return true;
  }

  private List<String> setReplicationEnableFlag(List<String> basePaths, ReplicationDestination destination, boolean enable) {
    List<String> failedPaths = new ArrayList<>();
    for (String basePath : basePaths) {
      try {
        if (!setReplicationEnableFlag(basePath, destination, enable)) {
          failedPaths.add(basePath);
        }

      } catch (Exception e) {
        LOG.error(String.format("Failure for %s", basePath), e);
        failedPaths.add(basePath);
      }
    }
    return failedPaths;
  }

  private void removeReplicationProperty(HoodieSparkEngineContext context, List<String> basePaths, List<String> tables, String replicationKey) {
    basePaths.addAll(getBasePaths(context, tables));
    basePaths.forEach(basePath -> {
      try {
        Configuration hadoopConf = (Configuration) context.getStorageConf().unwrap();
        FileSystem fs = new Path(basePath).getFileSystem(hadoopConf);
        Path path = new Path(basePath + Path.SEPARATOR + ".hoodie", "replication.properties");
        if (fs.exists(path)) {
          HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(context.getStorageConf()).build();
          ReplicationPropertiesManager manager = this.getReplicationPropertiesManager(metaClient);
          try {
            manager.removeProperty(replicationKey);
          } catch (Exception e) {
            LOG.error(String.format("Failed to remove property %s for %s", replicationKey, basePath));
          }
        }
      } catch (IOException ignored) {
        LOG.info(ignored.toString());
      }
    });
  }

  private void setOperationalStatus(HoodieSparkEngineContext context, List<String> basePaths, List<String> tables, ReplicationDestination region, Boolean operationalStatus) {
    String operationalStatusKey = HoodieReplicationContext.getReplicationOperationalStatusConfigKey(region);
    basePaths.addAll(getBasePaths(context, tables));
    basePaths.forEach(basePath -> {
      try {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(context.getStorageConf()).build();
        ReplicationPropertiesManager manager = this.getReplicationPropertiesManager(metaClient);
        manager.setProperty(operationalStatusKey, operationalStatus.toString());
      } catch (Exception e) {
        LOG.error(String.format("Failed to set property %s for %s", operationalStatusKey, basePath));
      }
    });
  }

  private void checkAndFixDivergedCommits(HoodieSparkEngineContext context, List<String> basePaths,
                                          List<String> targetBasePaths, List<String> tables,
                                          ReplicationDestination destination, boolean fixDivergedCommits) {
    LOG.info(String.format("Base paths: %s", basePaths));
    LOG.info(String.format("Target Base paths: %s", targetBasePaths));
    LOG.info(String.format("Tables: %s", tables));
    LOG.info(String.format("Destination: %s", destination));
    LOG.info(String.format("FixDivergedCommits: %s", fixDivergedCommits));

    for (int i = 0; i < basePaths.size(); i++) {
      String basePath = basePaths.get(i);
      String targetBasePath = targetBasePaths.get(i);
      try {
        HoodieReplicationMetadataClient srcReplicationClient = this.getHoodieReplicationMetadataClient(
                context.getStorageConf(), basePath, destination);
        HoodieReplicationMetadataClient tgtReplicationClient = this.getHoodieReplicationMetadataClient(
                context.getStorageConf(), targetBasePath, destination);

        List<HoodieInstant> srcInstants = srcReplicationClient.getMetaClient().getActiveTimeline()
            .filterCompletedInstants().getWriteTimeline().getInstantsAsStream().collect(Collectors.toList());
        List<String> srcCommits = srcInstants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
        List<HoodieInstant> tgtInstants = tgtReplicationClient.getMetaClient().getActiveTimeline()
            .filterCompletedInstants().getWriteTimeline().getInstantsAsStream().collect(Collectors.toList());
        List<String> tgtCommits = tgtInstants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
        List<String> missedArchival = tgtCommits.stream().filter(c -> c.compareTo(srcCommits.get(0)) < 0)
            .collect(Collectors.toList());
        // commits not present on the source timeline and are greater than the oldest commit on the source timeline
        List<String> extraCommitsOnTarget = tgtCommits.stream().filter(c -> !srcCommits.contains(c))
            .filter(c -> !missedArchival.contains(c)).collect(Collectors.toList());

        LOG.warn(String.format("Commit timeline on source: %s", srcCommits));
        LOG.warn(String.format("Commit timeline on target: %s", tgtCommits));
        LOG.warn(String.format("Extra commits on target: %s", extraCommitsOnTarget));
        LOG.warn(String.format("Missed archival commits on target: %s", missedArchival));

        // Clean up the extra commits on target
        extraCommitsOnTarget.forEach(commit -> {
          LOG.info(String.format("Cleaning up commit %s on target", commit));
          Map<HoodieReplicationMetadataClient.ReplicationStep, List<HoodieReplicationMetadataClient.ReplicationInfo>>
              orderedList = tgtReplicationClient.getOrderedFilesForRollback(commit);
          HoodieTableMetaClient tgtMetaClient = tgtReplicationClient.getMetaClient();
          for (HoodieReplicationMetadataClient.ReplicationStep step : tgtReplicationClient.getRollbackOrdering()) {
            List<HoodieReplicationMetadataClient.ReplicationInfo> replicationInfos = orderedList.get(step);
            if (replicationInfos == null) {
              continue;
            }
            for (HoodieReplicationMetadataClient.ReplicationInfo replicationInfo : replicationInfos) {
              if (replicationInfo == null) {
                continue;
              }
              if (replicationInfo.action == HoodieReplicationMetadataClient.ReplicationAction.DELETE_FILES
                  && replicationInfo.files != null) {
                LOG.info(String.format("Deletion candidate: %s", replicationInfo));
                List<String> files = replicationInfo.files;
                String relativePath = replicationInfo.relativePath;

                // delete files in parallel
                context.foreach(files, file -> {
                  Path path = new Path(tgtMetaClient.getBasePath().toString() + Path.SEPARATOR
                      + relativePath + Path.SEPARATOR + file);

                  if (fixDivergedCommits) {
                    LOG.info(String.format("Cleaning up file %s", path));
                    try {
                      FileSystem tgtFs = path.getFileSystem(new Configuration());
                      if (!tgtFs.delete(path, false)) {
                        LOG.error(String.format("Deletion of file %s failed", path));
                      }
                    } catch (IOException e) {
                      LOG.error(String.format("Error deleting file %s with %s", path, e));
                    }
                  } else {
                    LOG.info(String.format("REMOVE_DIVERGED_COMMITS would have cleaned up file %s", path));
                  }
                }, files.size());
                LOG.info(String.format("Deleted %d files in partition %s for commit %s on target ", files.size(),
                    replicationInfo.relativePath, commit));
              }
            }
          }
        });
        LOG.warn(String.format("Finished checking/cleaning up %d extra commits on target", extraCommitsOnTarget.size()));
      } catch (Exception e) {
        LOG.error(String.format("Failed to check and fix diverged commits on %s with %s", targetBasePaths.get(i), e));
      }
    }
  }

  /**
   * Returns the list of commit metadata files from active timeline which are already archived in the source datacenter.
   */
  public List<Path> getLeftoverFilesForArchival(HoodieReplicationMetadataClient sourceReplicationClient,
                                                HoodieReplicationMetadataClient targetReplicationClient, int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException(String.format("Invalid batch size %s. Must be > 0.", maxSize));
    }
    List<HoodieInstant> srcInstants = sourceReplicationClient
            .getMetaClient()
            .getCommitsTimeline()
            .filterCompletedInstants()
            .getInstantsAsStream()
            .collect(Collectors.toList());

    if (srcInstants.isEmpty()) {
      throw new IllegalArgumentException("No completed instants found in primary. Basepath: " + sourceReplicationClient.getMetaClient().getBasePath());
    }

    final String oldestInstantTimestampFromSrc = srcInstants.get(0).requestedTime();
    LOG.info("Oldest instant timestamp from source: " + oldestInstantTimestampFromSrc);
    final Set<String> instantTimestampsFromSrc = srcInstants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    HoodieTableMetaClient metaClient = targetReplicationClient.getMetaClient();
    List<HoodieInstant> targetInstantsBeforeOldestSrcCommit = getTargetInstantsBeforeOldestSrcCommit(metaClient,  oldestInstantTimestampFromSrc,  maxSize);
    final String pathPrefix = metaClient.getMetaPath().toString() + "/";
    final InstantFileNameGenerator fileNameGenerator = metaClient.getTimelineLayout().getInstantFileNameGenerator();
    return targetInstantsBeforeOldestSrcCommit
            .stream()
            .filter(i -> !instantTimestampsFromSrc.contains(i.requestedTime()))
            .map(instant -> new Path(pathPrefix + fileNameGenerator.getFileName(instant)))
            .filter(f -> {
              try {
                return metaClient.getStorage().exists(
                    new org.apache.hudi.storage.StoragePath(f.toString()));
              } catch (IOException e) {
                LOG.warn("Failed to check existence of file: " + f, e);
              }
              return false;
            })
            .collect(Collectors.toList());
  }

  private Result checkAndFixArchivedCommits(HoodieSparkEngineContext context, HoodieOperationReplicationConfig cfg, boolean fixArchivalMissed) {
    org.apache.hudi.storage.StorageConfiguration<?> configuration = context.getStorageConf();
    if (StringUtils.isNullOrEmpty(cfg.csvFilePath)) {
      return xdcArchivalPerDataset(configuration, cfg.maxSize, cfg.commitPrefix, cfg.basePaths.get(0),
              cfg.targetBasePaths.get(0), fixArchivalMissed);
    }
    Dataset<Row> datasetPairsDF = context.getSqlContext()
            .read()
            .option("header", "false")
            .csv(cfg.csvFilePath);
    try {
      datasetPairsDF.persist(StorageLevel.MEMORY_AND_DISK());
      datasetPairsDF.show(5, false);
      int totalArchivalFixerOperations = (int) datasetPairsDF.count();
      LOG.info("Total number of datasets to run archival fixer tool: " + totalArchivalFixerOperations);
      if (totalArchivalFixerOperations == 0) {
        return new Result(StatusCode.SUCCESS, "No datasets to run archival fixer tool");
      }

      JavaRDD<Result> resultsRDD = datasetPairsDF
              .javaRDD()
              .repartition(totalArchivalFixerOperations)
              .map(row -> Pair.of(row.getString(0), row.getString(1)))
              .map(pair -> xdcArchivalPerDataset(configuration, cfg.maxSize, cfg.commitPrefix, pair.getLeft(),
                      pair.getRight(), fixArchivalMissed));
      resultsRDD = resultsRDD.filter(r -> r.statusCode != StatusCode.SUCCESS);
      if (resultsRDD.count() == 0) {
        return new Result(StatusCode.SUCCESS, "Successfully executed archival fixer tool on all datasets");
      }
      List<Result> failedResults = resultsRDD.collect();
      failedResults.forEach(res ->
              LOG.error(String.format("Failed to execute archival fixer with executorId %s, with message %s",
                      res.executorId, res.message)));
      return failedResults.get(0);
    } finally {
      datasetPairsDF.unpersist(true);
    }
  }

  private Result xdcArchivalPerDataset(org.apache.hudi.storage.StorageConfiguration<?> configuration, int maxSize, String commitPrefix,
                                       String basePath, String basePathTarget, boolean executeCleanup) {
    try {
      Configuration hadoopConf = (Configuration) configuration.unwrap();
      final HoodieReplicationMetadataClient replicationClientSource = this.getHoodieReplicationMetadataClient(new HadoopStorageConfiguration(hadoopConf), basePath);
      final HoodieReplicationMetadataClient replicationClientTarget = this.getHoodieReplicationMetadataClient(new HadoopStorageConfiguration(hadoopConf), basePathTarget);

      // Paranoid check to ensure that primary path is from "phx" and secondary from "dca".
      FileSystem secondaryFileSystem = new Path(basePathTarget).getFileSystem(hadoopConf);
      // Input is valid. Collect the list of files to be deleted.
      List<Path> extraFilesInTargetForArchival = getLeftoverFilesForArchival(replicationClientSource, replicationClientTarget, maxSize);
      if (extraFilesInTargetForArchival.isEmpty()) {
        LOG.info("No files to  delete. Archive commits cleaned up!");
        return new Result(StatusCode.SUCCESS, "No files to  delete. Archive commits cleaned up!");
      }

      List<Path> filesToDelete = extraFilesInTargetForArchival
              .stream()
              .filter(f -> f.getName().startsWith(commitPrefix))
              .sorted()
              .collect(Collectors.toList());

      filesToDelete.forEach(fileName -> LOG.info(String.format("DEL: %s", fileName)));
      if (executeCleanup) {
        executeDeletion(filesToDelete, secondaryFileSystem);
        return new Result(StatusCode.SUCCESS, "Successfully deleted leftover archivals");
      }

      return new Result(StatusCode.SUCCESS, "Successfully found leftover archivals");
    } catch (Exception e) {
      LOG.error(String.format("Failed to execute archival fixer tool on %s with Exception", basePathTarget), e);
      return new Result(StatusCode.FAILURE, e.getMessage() + " for " + basePathTarget);
    }
  }

  private void executeDeletion(List<Path> filesToDelete, FileSystem secondaryFileSystem) {
    int numFilesToDelete = filesToDelete.size();
    LOG.info("Number of files to delete: " + numFilesToDelete);
    int[] numFilesDeleted = {0};
    // Keeping it single-threaded to not overwhelm HDFS Namenode.
    filesToDelete.forEach(path -> {
      try {
        boolean deleted = secondaryFileSystem.delete(path, false);
        if (!deleted) {
          LOG.error(String.format("del_err: %s", path));
        }
        numFilesDeleted[0]++;
        LOG.info(String.format("deleted: %s", path));
        if (numFilesDeleted[0] % 10 == 0) {
          LOG.info(String.format("Num files deleted: %s , total: %s", numFilesDeleted[0], numFilesToDelete));
        }
      } catch (IOException e) {
        LOG.error(String.format("del_err:%s", path), e);
      }
    });
    LOG.info(String.format("to_delete: %s deleted_files: %s", numFilesToDelete, numFilesDeleted[0]));
    if (numFilesToDelete == numFilesDeleted[0]) {
      LOG.info(String.format("%s All commits cleaned up.", FixArchivalStatusCode.COMMITS_CLEANED.name()));
    } else {
      LOG.info(String.format("%s Some files were not deleted.", FixArchivalStatusCode.PARTIAL_DELETE));
    }
  }

  protected HoodieReplicationMetadataClient getHoodieReplicationMetadataClient(org.apache.hudi.storage.StorageConfiguration<?> conf, String basePath, ReplicationDestination destination) {
    return new HoodieReplicationMetadataClient(conf, basePath, destination);
  }

  protected HoodieReplicationMetadataClient getHoodieReplicationMetadataClient(org.apache.hudi.storage.StorageConfiguration<?> conf, String basePath) {
    return new HoodieReplicationMetadataClient(conf, basePath);
  }

  protected ReplicationPropertiesManager getReplicationPropertiesManager(HoodieTableMetaClient metaClient) {
    return new ReplicationPropertiesManager(metaClient);
  }

  protected List<HoodieInstant> getTargetInstantsBeforeOldestSrcCommit(HoodieTableMetaClient metaClient,  String oldestInstantTimestampFromSrc, int maxSize) {
    return metaClient.getTimelineLayout().getTimelineFactory()
      .createActiveTimeline(metaClient, false).getCommitsTimeline().findInstantsBefore(oldestInstantTimestampFromSrc)
      .getInstantsAsStream().limit(maxSize).collect(Collectors.toList());
  }

  private void comparePrimaryRegions(List<String> tables) {
    for (String table : tables) {
      try {
        Set<Region> tasPrimaryRegions = getPrimaryRegionTAS(table);
        Set<Region> hiveSyncPrimaryRegions = getPrimaryRegionHiveSync(table);
        if (tasPrimaryRegions.equals(hiveSyncPrimaryRegions)) {
          LOG.info("Primary regions are the same");
        }
        LOG.info(String.format("TAS primary regions: %s, HiveSync primary regions: %s",
            tasPrimaryRegions, hiveSyncPrimaryRegions));
      } catch (Exception e) {
        LOG.info("Failed to fetch primary regions for " + table, e);
      }
    }
  }

  protected Set<Region> getPrimaryRegionHiveSync(String table) throws Exception {
    String[] dbAndTableNames = table.split("\\.");
    ValidationUtils.checkArgument(dbAndTableNames.length == 2, "Invalid table name format: " + table);
    HoodieReplicationMetadata replicationMetadata = HoodieReplicationMetadataUtils
        .getReplicationMetadata(dbAndTableNames[0], dbAndTableNames[1]);

    return replicationMetadata.getPrimaryRegions();
  }

  protected Set<Region> getPrimaryRegionTAS(String table) throws IOException {
    try (HoodieTASClient tasClient = new HoodieTASClient()) {
      return tasClient.getAllowedUpdateRegions(table);
    }
  }
}
