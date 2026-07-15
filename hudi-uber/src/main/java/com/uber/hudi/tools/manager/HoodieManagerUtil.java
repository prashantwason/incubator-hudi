package com.uber.hudi.tools.manager;

import com.beust.jcommander.JCommander;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HoodieManagerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieManagerUtil.class);

  public static void parseArguments(Object cfg, String[] args) {
    JCommander cmd = new JCommander(cfg, null, args);
    if (args.length == 0) {
      cmd.usage();
      throw new IllegalArgumentException(String.format("Invalid arguments provided: %s", String.join(",", args)));
    }
  }

  public static Map<String, String> getKeyGeneratorConfigs(HoodieTableConfig tableConfig) {
    Map<String, String> configs = new HashMap<>();

    String keyGeneratorClassName = tableConfig.getKeyGeneratorClassName();
    if (keyGeneratorClassName == null) {
      keyGeneratorClassName = SimpleKeyGenerator.class.getName();
    }

    String partitionPathFieldName = tableConfig.getPartitionFieldProp();
    if (partitionPathFieldName == null) {
      partitionPathFieldName = "_hoodie_partition_path";
    }
    String recordKeyFieldName = tableConfig.getRecordKeyFieldProp();
    if (recordKeyFieldName == null) {
      recordKeyFieldName = HoodieRecord.RECORD_KEY_METADATA_FIELD;
    }
    configs.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyFieldName);
    configs.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathFieldName);
    configs.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClassName);
    return configs;
  }

  public static Map<String, String> parseHoodieConfigs(List<String> configOverrides) {
    Map<String, String> configs = new HashMap<>();
    for (String configOverride : configOverrides) {
      String[] tokens = configOverride.split("=", 2);
      if (tokens.length != 2) {
        LOG.error(String.format("Invalid hoodie config provided %s", configOverride));
        continue;
      }
      configs.put(tokens[0], tokens[1]);
    }
    return configs;
  }

  public static HoodieWriteConfig getHoodieWriteConfig(Map<String, String> configOverrides, String basePath, HoodieTableMetaClient metaClient) {
    HoodieWriteConfig hoodieWriteConfig = HoodieWriteConfig.newBuilder()
        .withProps(configOverrides)
        .withPath(basePath)
        .forTable(metaClient.getTableConfig().getTableName())
        .build();
    return hoodieWriteConfig;
  }

  public static HoodieWriteConfig getHoodieWriteConfig(Map<String, String> testSpecificConfigOverrides,
                                                         List<String> userProvidedConfigs,
                                                         String basePath,
                                                         HoodieTableMetaClient metaClient) {
    Map<String, String> mergedConfigs = new HashMap<>(testSpecificConfigOverrides);

    if (userProvidedConfigs != null && !userProvidedConfigs.isEmpty()) {
      Map<String, String> userConfigs = parseHoodieConfigs(userProvidedConfigs);
      mergedConfigs.putAll(userConfigs);
      LOG.info("Applied {} user-provided Hoodie configurations", userConfigs.size());
    }

    return getHoodieWriteConfig(mergedConfigs, basePath, metaClient);
  }
}
