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

package org.apache.hudi.sink.extensions;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.flink.configuration.Configuration;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Extension hook for Flink streaming commits.
 *
 * <p>This is intentionally generic: implementations can add commit metadata before commit,
 * and run arbitrary logic after commit (metrics, notifications, validation, etc.).
 *
 * <p>Implementations are loaded reflectively using the class name specified in
 * {@code FlinkOptions#STREAM_WRITE_COMMIT_HOOK_CLASS}.
 *
 * <p>Lifecycle: {@link #init(Configuration)} is called once by {@link StreamWriteCommitHookLoader}
 * immediately after instantiation, then {@link #getCommitExtraMetadata(long)} and
 * {@link #postCommit(PostCommitContext)} are called per checkpoint, and {@link #close()} is called
 * on shutdown.
 *
 * <p><b>Threading:</b> All methods ({@link #init}, {@link #getCommitExtraMetadata},
 * {@link #postCommit}, {@link #close}) are called exclusively from the
 * {@code StreamWriteOperatorCoordinator} thread. Implementations do not need to be thread-safe.
 */
public interface StreamWriteCommitHook extends Closeable {

  /**
   * Initialize the hook. Called once before any commit operations.
   *
   * <p>Implementations should store the {@code conf} if they need it later
   * (e.g., in {@link #getCommitExtraMetadata(long)}).
   */
  void init(Configuration conf);

  /**
   * Returns extra commit metadata to be persisted with this commit.
   *
   * <p>The returned map is merged into the commit metadata map passed to {@code writeClient.commit(...)}.
   * The {@link Configuration} passed to {@link #init(Configuration)} is available to implementations.
   *
   * @param checkpointId the Flink checkpoint ID for this commit
   * @return map of key/value pairs to include in commit metadata, or null to add nothing
   */
  Map<String, String> getCommitExtraMetadata(long checkpointId);

  /**
   * Post-commit hook called after a successful commit.
   *
   * @param context contextual information about the completed commit
   */
  void postCommit(PostCommitContext context);

  class PostCommitContext {
    private final long checkpointId;
    private final String instant;
    private final List<WriteStatus> writeResults;
    private final HoodieTableMetaClient metaClient;

    public PostCommitContext(
        long checkpointId,
        String instant,
        List<WriteStatus> writeResults,
        HoodieTableMetaClient metaClient) {
      this.checkpointId = checkpointId;
      this.instant = instant;
      this.writeResults = writeResults;
      this.metaClient = metaClient;
    }

    public long getCheckpointId() {
      return checkpointId;
    }

    public String getInstant() {
      return instant;
    }

    public List<WriteStatus> getWriteResults() {
      return writeResults;
    }

    public HoodieTableMetaClient getMetaClient() {
      return metaClient;
    }
  }
}
