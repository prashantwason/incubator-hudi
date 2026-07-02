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

package org.apache.hudi.replication.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.hudi.replication.table.Region;
import org.apache.hudi.replication.client.HoodieHttpClient;
import org.apache.hudi.replication.client.HoodieHttpClientMetricTags;
import org.apache.hudi.exception.HoodieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HoodieHiveSyncClient extends HoodieHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieHiveSyncClient.class);
  private static final String HIVESYNC_SERVICE_PROD = "hive-sync-web";
  private static final String HIVESYNC_METADATA_ENDPOINT = "/replication/metadata/v1";

  public HoodieHiveSyncClient() {
    super(HIVESYNC_SERVICE_PROD);
  }


  /**
   * Get replication metadata for the specified table.
   *
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @return HiveSyncMetadataClientResult which contains the reverse and forward replication metadata for the specified table
   */
  public HoodieReplicationMetadata getReplicationMetadata(String dbName, String tableName) throws Exception {
    String fullTableName = dbName + "." + tableName;
    LOG.info("Getting replication metadata for {}", fullTableName);
    HoodieHttpClientMetricTags metricTags = new HoodieHttpClientMetricTags()
        .withDatasetName(fullTableName)
        .withApiName("HIVE_SYNC_METADATA");
    try (CloseableHttpResponse response = execute(getMetadataRequest(dbName, tableName), metricTags)) {
      int statusCode = response.getStatusLine().getStatusCode();
      String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");

      if (statusCode == 200) {
        ObjectMapper objectMapper = new ObjectMapper();
        HiveSyncMetadataResponse metadataResponse = objectMapper.readValue(responseBody, HiveSyncMetadataResponse.class);
        List<HoodieRegionReplicationMetadata> replicationMetadataList = metadataResponse.getReplicationRegionMetadataList()
            .stream()
            .map(metadata -> (HoodieRegionReplicationMetadata) metadata)
            .collect(Collectors.toList());
        return new HoodieReplicationMetadata(fullTableName, replicationMetadataList);
      } else {
        throw new HoodieException(String.format("Status code: %s. Response body: %s.", statusCode, responseBody));
      }
    }
  }

  private HttpGet getMetadataRequest(String dbName, String tableName) throws URISyntaxException {
    HttpGet request = new HttpGet(getURL(HIVESYNC_METADATA_ENDPOINT));
    URI uri = new URIBuilder(request.getURI())
        .addParameter("dbName", dbName)
        .addParameter("tableName", tableName)
        .build();

    request.setURI(uri);

    return request;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class HiveSyncMetadataResponse {
    @JsonProperty("replicationMetadata")
    private String replicationRegionMetadataString;

    List<ReplicationRegionMetadata> getReplicationRegionMetadataList() {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        ReplicationRegionMetadata[] replicationRegionMetadataList = objectMapper
            .readValue(replicationRegionMetadataString, ReplicationRegionMetadata[].class);

        return Arrays.asList(replicationRegionMetadataList);
      } catch (Exception e) {
        return new ArrayList<>();
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReplicationRegionMetadata implements HoodieRegionReplicationMetadata, Serializable {
    @JsonProperty("from")
    private String from;
    @JsonProperty("to")
    private String to;
    @JsonProperty("replicationEnabled")
    private Boolean replicationEnabled;
    @JsonProperty("otrsOngoing")
    private Boolean otrsOngoing;
    @JsonProperty("otrsCreateTimeEpochSec")
    private Long otrsCreateTimeEpochSec;

    @Override
    public Region getSource() {
      return Region.getRegionFromString(from);
    }

    @Override
    public Region getDestination() {
      return Region.getRegionFromString(to);
    }

    @Override
    public boolean getReplicationEnabled() {
      return replicationEnabled;
    }

    @Override
    public boolean isOtrsOngoing() {
      return otrsOngoing;
    }

    @Override
    public long getOtrsDuration() {
      if (isOtrsOngoing()) {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - otrsCreateTimeEpochSec;
      }
      return 0L;
    }
  }
}
