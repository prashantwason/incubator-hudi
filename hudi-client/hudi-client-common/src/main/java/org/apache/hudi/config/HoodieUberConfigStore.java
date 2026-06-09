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

package org.apache.hudi.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

/**
 * A simplified config store for managing Hudi configurations stored on HDFS.
 *
 * <p>This config store manages two types of configurations:
 * <ul>
 *   <li><b>Enforced configs</b>: Mandatory configs that cannot be overridden by user-provided values.
 *       Stored in {@code hudi_config_enforced.conf}.</li>
 *   <li><b>Fallback defaults</b>: Default configs that only apply when not already set by the user.
 *       Stored in {@code hudi_config_fallback.conf}.</li>
 * </ul>
 *
 * <p>Config priority (lowest to highest):
 * <ol>
 *   <li>Code defaults (built into HoodieWriteConfig)</li>
 *   <li>User-provided config</li>
 *   <li>Fallback defaults (only if key not in user config)</li>
 *   <li>Enforced configs (always applied, highest priority)</li>
 * </ol>
 *
 * <p>Storage locations (by datacenter):
 * <ul>
 *   <li>DCA: hdfs://ns-router-dca1/user/hudi/config_store</li>
 *   <li>PHX: hdfs://ns-router-prod-phx/user/hudi/config_store</li>
 *   <li>Cloud: cfs://ns-cloudlake/user/hudi/config_store</li>
 * </ul>
 */
public class HoodieUberConfigStore {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieUberConfigStore.class);

  // Config store base path (appended to datacenter-specific prefix)
  private static final String CONFIG_STORE_PATH = "/user/hudi/config_store";

  // Config file names
  private static final String ENFORCED_CONFIG_FILE = "hudi_config_enforced.conf";
  private static final String FALLBACK_CONFIG_FILE = "hudi_config_fallback.conf";

  // Detect if running in a test environment (Maven Surefire)
  private static final boolean IS_TESTING = detectTestEnvironment();

  // Test-only override for config store path
  @VisibleForTesting
  static String testConfigStorePath = null;

  /**
   * Detects if running in a Maven Surefire test environment.
   * Surefire sets specific system properties that we can check.
   */
  private static boolean detectTestEnvironment() {
    return System.getProperty("surefire.test.class.path") != null
        || System.getProperty("surefire.real.class.path") != null;
  }

  /**
   * Sets the config store path for testing purposes.
   * This allows tests to use local test config files.
   *
   * @param path The path to the test config store directory
   */
  @VisibleForTesting
  public static void setTestConfigStorePath(String path) {
    testConfigStorePath = path;
  }

  /**
   * Clears the test config store path, reverting to normal behavior.
   * Should be called in test teardown.
   */
  @VisibleForTesting
  public static void clearTestConfigStorePath() {
    testConfigStorePath = null;
  }

  /**
   * Returns whether the code is running in a test environment.
   */
  @VisibleForTesting
  static boolean isTestingEnvironment() {
    return IS_TESTING;
  }

  private final Configuration hadoopConf;
  private final String configStorePath;

  /**
   * Private constructor that sets the config store path directly.
   *
   * @param hadoopConf Hadoop configuration
   * @param configStorePath The full path to the config store directory
   * @param isDirectPath Marker to distinguish from the public factory methods
   */
  private HoodieUberConfigStore(Configuration hadoopConf, String configStorePath, boolean isDirectPath) {
    this.hadoopConf = hadoopConf;
    this.configStorePath = configStorePath;
    LOG.info("Initialized HoodieUberConfigStore with path: {}", configStorePath);
  }

  /**
   * Creates a new HoodieUberConfigStore instance by auto-detecting datacenter from the base path.
   *
   * @param hadoopConf Hadoop configuration
   * @param basePath Base path of the Hudi table (used to detect datacenter)
   * @return HoodieUberConfigStore instance
   */
  public static HoodieUberConfigStore fromBasePath(Configuration hadoopConf, String basePath) {
    String datacenter = datacenterFromBasePath(basePath);
    String configStorePath = getConfigStorePathForDatacenter(datacenter);
    return new HoodieUberConfigStore(hadoopConf, configStorePath, true);
  }

  /**
   * Creates a new HoodieUberConfigStore instance with a specific datacenter.
   *
   * @param hadoopConf Hadoop configuration
   * @param datacenter Datacenter identifier (e.g., "phx", "dca", "phx_cloud", "dca_cloud")
   * @return HoodieUberConfigStore instance
   */
  public static HoodieUberConfigStore forDatacenter(Configuration hadoopConf, String datacenter) {
    String configStorePath = getConfigStorePathForDatacenter(datacenter);
    return new HoodieUberConfigStore(hadoopConf, configStorePath, true);
  }

  /**
   * Creates a new HoodieUberConfigStore instance with a direct config store path.
   * This is primarily used for testing.
   *
   * @param hadoopConf Hadoop configuration
   * @param configStorePath The full path to the config store directory
   * @return HoodieUberConfigStore instance
   */
  @VisibleForTesting
  static HoodieUberConfigStore withConfigStorePath(Configuration hadoopConf, String configStorePath) {
    return new HoodieUberConfigStore(hadoopConf, configStorePath, true);
  }

  /**
   * Returns the enforced configurations that cannot be overridden.
   *
   * @return Properties containing enforced configurations
   * @throws HoodieException if the config file cannot be read
   */
  public Properties getEnforcedConfigs() {
    return loadPropertiesFromFile(ENFORCED_CONFIG_FILE);
  }

  /**
   * Returns the fallback default configurations.
   * These are applied only when the user has not provided a value for the key.
   *
   * @return Properties containing fallback default configurations
   * @throws HoodieException if the config file cannot be read
   */
  public Properties getFallbackDefaults() {
    return loadPropertiesFromFile(FALLBACK_CONFIG_FILE);
  }

  /**
   * Updates the provided HoodieWriteConfig by applying config store configurations.
   *
   * <p>The priority order is:
   * <ol>
   *   <li>Start with the input config</li>
   *   <li>Apply fallback defaults (only for keys not already present)</li>
   *   <li>Apply enforced configs (always overrides, highest priority)</li>
   * </ol>
   *
   * @param inputConfig The HoodieWriteConfig provided by the user
   * @return A new HoodieWriteConfig with config store settings applied
   * @throws HoodieException if config files cannot be read
   */
  public HoodieWriteConfig updateConfig(HoodieWriteConfig inputConfig) {
    Properties resultProps = new Properties();

    // Step 1: Start with input config properties
    resultProps.putAll(inputConfig.getProps());

    // Step 2: Apply fallback defaults (only if key not already present)
    Properties fallbackDefaults = getFallbackDefaults();
    for (String key : fallbackDefaults.stringPropertyNames()) {
      if (!resultProps.containsKey(key)) {
        resultProps.setProperty(key, fallbackDefaults.getProperty(key));
        LOG.debug("Applied fallback default: {}={}", key, fallbackDefaults.getProperty(key));
      }
    }

    // Step 3: Apply enforced configs (always override)
    Properties enforcedConfigs = getEnforcedConfigs();
    for (String key : enforcedConfigs.stringPropertyNames()) {
      String oldValue = resultProps.getProperty(key);
      String newValue = enforcedConfigs.getProperty(key);
      resultProps.setProperty(key, newValue);
      if (oldValue != null && !oldValue.equals(newValue)) {
        LOG.info("Enforced config override: {}={} (was: {})", key, newValue, oldValue);
      } else {
        LOG.debug("Applied enforced config: {}={}", key, newValue);
      }
    }

    // Build and return the updated config
    String basePath = resultProps.getProperty(HoodieWriteConfig.BASE_PATH.key());
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(resultProps);

    return builder.build();
  }

  /**
   * Returns the config store path for this instance.
   *
   * @return The full HDFS path to the config store
   */
  public String getConfigStorePath() {
    return configStorePath;
  }

  /**
   * Loads properties from a file in the config store.
   *
   * @param filename The name of the config file
   * @return Properties loaded from the file
   * @throws HoodieException if the file cannot be read
   */
  private Properties loadPropertiesFromFile(String filename) {
    Path path = new Path(configStorePath, filename);

    try {
      FileSystem fs = path.getFileSystem(hadoopConf);

      if (!fs.exists(path)) {
        throw new HoodieException("Required config file not found: " + path
            + ". The application cannot proceed without this configuration file.");
      }

      Properties props = new Properties();
      try (InputStream is = fs.open(path)) {
        props.load(is);
      }

      LOG.info("Loaded {} properties from {}", props.size(), path);
      return props;
    } catch (IOException e) {
      throw new HoodieException("Failed to read config file: " + path
          + ". The application cannot proceed.", e);
    }
  }

  /**
   * Returns the full config store path for a given datacenter.
   *
   * @param datacenter Datacenter identifier (e.g., "phx", "dca", "phx_cloud", "dca_cloud")
   * @return Full HDFS path to the config store
   */
  private static String getConfigStorePathForDatacenter(String datacenter) {
    String prefix = getPathPrefixForDatacenter(datacenter);
    return prefix + CONFIG_STORE_PATH;
  }

  /**
   * Returns the HDFS path prefix for a given datacenter or host name.
   *
   * @param datacenter Datacenter identifier or host name (e.g., "phx", "ns-router-prod-phx", "ns-cloudlake")
   * @return HDFS path prefix
   */
  private static String getPathPrefixForDatacenter(String datacenter) {
    String dc = datacenter.toLowerCase();

    // Check for cloud datacenters
    if (dc.contains("cld") || dc.contains("cloud") || dc.contains("cloudlake")) {
      return "cfs://ns-cloudlake";
    }

    // Check for on-prem datacenters
    if (dc.contains("phx")) {
      return "hdfs://ns-router-prod-phx";
    } else if (dc.contains("dca")) {
      return "hdfs://ns-router-dca1";
    }

    throw new HoodieException("Invalid datacenter or host: " + datacenter
        + ". Expected host to contain one of: phx, dca, cloudlake");
  }

  /**
   * Maps a Hudi base path to the datacenter token expected by
   * {@link #getPathPrefixForDatacenter(String)}.
   *
   * <p>Dispatches on URI scheme, not host. GCS bucket names (e.g. {@code uber-prod-cv0hw})
   * contain no datacenter substring, so host-based matching would fail.
   * Cloud schemes ({@code gs}, {@code cfs}) map to {@code "cloudlake"} (a deployment class,
   * not a physical DC). HDFS/viewfs paths return the URI host.
   *
   * @param basePath Hudi table base path (e.g. {@code gs://bucket/...},
   *                 {@code hdfs://ns-router-prod-phx/...})
   * @return {@code "cloudlake"} for GCS/CFS paths; the URI host for HDFS/viewfs paths
   * @throws HoodieException if the scheme is unsupported or if an HDFS/viewfs URI has no host
   */
  static String datacenterFromBasePath(String basePath) {
    URI uri;
    try {
      uri = new Path(basePath).toUri();
    } catch (Exception e) {
      throw new HoodieException("Failed to parse basePath as URI: " + basePath, e);
    }
    String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
    switch (scheme) {
      case "gs":
      case "cfs":
        return "cloudlake";
      case "hdfs":
      case "viewfs":
        String host = extractHostFromPath(basePath);
        if (host == null || host.isEmpty()) {
          throw new HoodieException("Missing host in basePath: " + basePath);
        }
        return host;
      default:
        throw new HoodieException("Unsupported scheme '" + scheme + "' in basePath: " + basePath
            + ". Expected one of: gs, cfs, hdfs, viewfs");
    }
  }

  /**
   * Extracts the host part from an absolute path.
   *
   * @param absolutePath The absolute path (e.g., "hdfs://ns-router-prod-phx/data/tables/foo")
   * @return The host part (e.g., "ns-router-prod-phx")
   */
  private static String extractHostFromPath(String absolutePath) {
    try {
      Path path = new Path(absolutePath);
      java.net.URI uri = path.toUri();
      return uri.getHost() != null ? uri.getHost() : "";
    } catch (Exception e) {
      throw new HoodieException("Failed to extract host from path: " + absolutePath, e);
    }
  }

  /**
   * Applies config store settings to the provided write config.
   * This is a convenience method that creates a config store instance and applies settings.
   *
   * <p>In test environments (detected via Maven Surefire properties), this method will:
   * <ul>
   *   <li>Skip config store application if no test config path is set</li>
   *   <li>Use the test config path if set via {@link #setTestConfigStorePath(String)}</li>
   * </ul>
   *
   * @param hadoopConf Hadoop configuration
   * @param inputConfig The HoodieWriteConfig provided by the user
   * @return A new HoodieWriteConfig with config store settings applied, or the original config in test mode
   * @throws HoodieException if config store files cannot be read (in production)
   */
  public static HoodieWriteConfig applyConfigStore(Configuration hadoopConf, HoodieWriteConfig inputConfig) {
    // In test environment, skip config store unless test path is explicitly set
    if (IS_TESTING && testConfigStorePath == null) {
      LOG.debug("Skipping config store in test environment (no test config path set)");
      return inputConfig;
    }

    HoodieUberConfigStore configStore;
    if (testConfigStorePath != null) {
      // Use test config store path
      configStore = withConfigStorePath(hadoopConf, testConfigStorePath);
    } else {
      configStore = fromBasePath(hadoopConf, inputConfig.getBasePath());
    }

    HoodieWriteConfig updatedConfig = configStore.updateConfig(inputConfig);
    LOG.info("Applied config store settings from: {}", configStore.getConfigStorePath());
    return updatedConfig;
  }
}
