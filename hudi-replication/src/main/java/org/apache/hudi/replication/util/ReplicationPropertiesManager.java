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

package org.apache.hudi.replication.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/*
 * Allows updating replication.properties file for the dataset, from multiple writers.
 */

public class ReplicationPropertiesManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationPropertiesManager.class);

  // replication.properties
  public static final String REPLICATION_PROPERTIES_FILE = "replication.properties";
  public static final String REPLICATION_PROPERTIES_FILE_BACKUP = "replication.properties.backup";
  public static final String REPLICATION_PROPERTIES_LOCK = "replication.properties.lock";
  private StoragePath replicationPropertiesFile;
  private HoodieTableMetaClient metaClient;

  public ReplicationPropertiesManager(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    this.replicationPropertiesFile = new StoragePath(metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE);
  }

  /**
   * Add/update the property.
   */
  private Properties addProperty(Properties props, String key, String value) {
    if ((!props.containsKey(key) || !props.getProperty(key).equals(value)) && value != null) {
      props.setProperty(key, value);
    }
    return props;
  }

  /**
   * Write properties to the replication.properties file.
   */
  private void writeProperties(Properties props) throws Exception {
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), props, REPLICATION_PROPERTIES_FILE, REPLICATION_PROPERTIES_FILE_BACKUP, REPLICATION_PROPERTIES_LOCK);
  }

  /**
   * Read contents of the replication.properties file.
   * Note: relies on HDFS reader/writer lock/lease on file and not explicitly taking the lock.
   */
  public Properties readProperties() throws Exception {
    try {
      return HoodieTableConfig.fetchConfigs(metaClient.getStorage(), metaClient.getMetaPath(), REPLICATION_PROPERTIES_FILE, REPLICATION_PROPERTIES_FILE_BACKUP);
    } catch (HoodieIOException e) {
      LOG.warn(String.format("File doesn't exist. Creating %s", replicationPropertiesFile));

      // Add checksum related properties to replication.properties to be compliant with HoodieTableConfig
      Properties replicationProps = new TypedProperties();
      replicationProps.setProperty(HoodieTableConfig.NAME.key(), metaClient.getTableConfig().getTableName());
      replicationProps.setProperty(HoodieTableConfig.TYPE.key(), metaClient.getTableConfig().getTableType().toString());
      try (OutputStream out = metaClient.getStorage().create(replicationPropertiesFile, false)) {
        replicationProps.store(out, String.format("Bootstrap missing for %s", replicationPropertiesFile.getName()));
        HoodieTableConfig.storeProperties(replicationProps, out);
      }

      LOG.info(String.format("%s contains %s", replicationPropertiesFile.getName(), replicationProps));
      return replicationProps;
    }
  }

  /**
   * Set key value pair in properties file.  If multiple applications are updating the properties file,
   * waits until acquiring the lock or maxRetries have been reached.
   * @param key - key for the property being updated
   * @param value - key for the property being updated
   */
  public boolean setProperty(String key, String value) throws Exception {
    Properties newProps = new Properties();
    newProps.setProperty(key, value);
    return setProperty(newProps);
  }

  /**
   * Set key value pairs in properties file.  If multiple applications are updating the properties file,
   * waits until lock can be acquired or maximum retries configured for the lock have been reached.
   * @param newProps - new properties to be added/updated to the file
   * @return
   *
   * Note: supply only the delta of properties to be added in newProps.
   */
  public boolean setProperty(Properties newProps) throws Exception {
    try {
      Properties props = readProperties();
      for (Map.Entry entry : newProps.entrySet()) {
        props = addProperty(props, (String)entry.getKey(), (String)entry.getValue());
        LOG.info(String.format("Adding %s -> %s in %s", entry.getKey(), entry.getValue(), replicationPropertiesFile));
      }

      // BackFill hoodie.table.name into replication.properties from hoodie.properties if absent,
      // ensuring backward compatibility with older property files during checksum creation.
      if (!props.containsKey(HoodieTableConfig.NAME.key())) {
        LOG.info("{} is missing in properties, adding it back from the table config", HoodieTableConfig.NAME.key());
        props = addProperty(props, HoodieTableConfig.NAME.key(), metaClient.getTableConfig().getTableName());
      }

      writeProperties(props);
      return true;
    } catch (Exception e) {
      throw new HoodieException(String.format("Could not update properties file %s", replicationPropertiesFile), e);
    }
  }

  /**
   * Remove specified key value pair from properties file. If multiple applications are updating the properties file,
   * waits until lock can be acquired or maximum retries configured for the lock have been reached.
   */
  public boolean removeProperty(String key) throws Exception {
    try {
      HoodieTableConfig.delete(
          metaClient.getStorage(),
          metaClient.getMetaPath(),
          Collections.singleton(key),
          REPLICATION_PROPERTIES_FILE,
          REPLICATION_PROPERTIES_FILE_BACKUP,
          REPLICATION_PROPERTIES_LOCK);
      return true;
    } catch (Exception e) {
      throw new HoodieException(String.format("Could not update properties file %s", replicationPropertiesFile), e);
    }
  }
}
