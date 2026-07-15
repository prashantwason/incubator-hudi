package com.uber.hudi.tools.manager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hudi.tools.pipeline.StringToListParameterConverter;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

public class HoodieManager implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieManager.class);

  /**
   * The command line arguments which are supported.
   */
  public static class HoodieManagerConfig implements Serializable {
    @Parameter(names = {"--command"}, description = "hoodie commands. refer to HoodieOperationCommand for details", required = true, listConverter = StringToListParameterConverter.class)
    public List<String> commands = Collections.emptyList();

    @Parameter(names = {"--args"}, description = "comma separated key=value arguments for each command", required = true)
    public String args;

    @Parameter(names = { "--help", "-h" }, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) throws Exception {
    final HoodieManagerConfig cfg = new HoodieManagerConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    try {
      HoodieSparkEngineContext context = getEngineContext();
      for (String command : cfg.commands) {
        LOG.info("Command to execute: {}", command);
        HoodieOperation op = HoodieOperationFactory.create(command);
        HoodieTimer operationTimer = new HoodieTimer().startTimer();
        Result result = op.execute(context, parseArgs(cfg.args));
        result.operationTimeMs = operationTimer.endTimer();
        LOG.info(result.toString());
      }
    } catch (Exception e) {
      LOG.error("Hudi manager failure", e);
      throw e;
    }
  }

  static String[] parseArgs(String argStr) throws IllegalArgumentException {
    List<String> parsedArgs = new ArrayList<>();
    for (String arg : argStr.split(",")) {
      // each argument is supposed to have key,value which is separated by '='. ex. key=val
      // note that value itself can have '=' as part of the . ex. config="hoodie.metadata.enable=true"
      int separatorIdx = arg.indexOf("=");
      if (separatorIdx == -1) {
        throw new IllegalArgumentException("Invalid argument:" + arg);
      }
      parsedArgs.add(arg.substring(0, separatorIdx));
      parsedArgs.add(arg.substring(separatorIdx + 1));
    }

    LOG.info(String.format("Parsed args: %s", Arrays.toString(parsedArgs.toArray())));
    return parsedArgs.toArray(new String[0]);
  }

  static HoodieSparkEngineContext getEngineContext() {
    SparkConf sparkConf = UtilHelpers.buildSparkConf("HoodieManager", "yarn");
    SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
    return new HoodieSparkEngineContext(jsc);
  }
}
