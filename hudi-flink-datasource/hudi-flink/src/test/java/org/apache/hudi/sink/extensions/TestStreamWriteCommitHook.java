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
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link StreamWriteCommitHook}, {@link StreamWriteCommitHook.PostCommitContext},
 * and {@link StreamWriteCommitHookLoader}.
 */
public class TestStreamWriteCommitHook {

  // -----------------------------------------------------------------------
  //  Concrete hook implementations used across tests
  // -----------------------------------------------------------------------

  /**
   * A no-op implementation of the interface.
   * Public and static so that {@link StreamWriteCommitHookLoader} can reflectively instantiate it.
   */
  public static class NoOpHook implements StreamWriteCommitHook {
    @Override
    public void init(Configuration conf) {
    }

    @Override
    public Map<String, String> getCommitExtraMetadata(long checkpointId) {
      return null;
    }

    @Override
    public void postCommit(PostCommitContext context) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * A spy-style hook that records calls so tests can assert on lifecycle order.
   * Public and static so {@link StreamWriteCommitHookLoader} can load it by name.
   */
  public static class SpyHook implements StreamWriteCommitHook {
    public Configuration initConf;
    public long lastCheckpointId = -1;
    public PostCommitContext lastContext;
    public boolean closed;

    @Override
    public void init(Configuration conf) {
      this.initConf = conf;
    }

    @Override
    public Map<String, String> getCommitExtraMetadata(long checkpointId) {
      this.lastCheckpointId = checkpointId;
      return Collections.singletonMap("checkpoint_id", String.valueOf(checkpointId));
    }

    @Override
    public void postCommit(PostCommitContext context) {
      this.lastContext = context;
    }

    @Override
    public void close() {
      this.closed = true;
    }
  }

  /**
   * A hook that throws from {@link #init(Configuration)}.
   * Used to verify the loader returns null when init fails.
   */
  public static class InitFailingHook implements StreamWriteCommitHook {
    @Override
    public void init(Configuration conf) {
      throw new RuntimeException("init failed");
    }

    @Override
    public Map<String, String> getCommitExtraMetadata(long checkpointId) {
      return Collections.emptyMap();
    }

    @Override
    public void postCommit(PostCommitContext context) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * A hook that does NOT implement {@link StreamWriteCommitHook}.
   * Used to verify the loader rejects incompatible types.
   */
  public static class NotAHook {
    // intentionally empty
  }

  // -----------------------------------------------------------------------
  //  PostCommitContext tests
  // -----------------------------------------------------------------------

  @Test
  void testPostCommitContextGetters() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    List<WriteStatus> writeResults = Collections.singletonList(mock(WriteStatus.class));

    StreamWriteCommitHook.PostCommitContext ctx = new StreamWriteCommitHook.PostCommitContext(
        7L, "20240101000000", writeResults, metaClient);

    assertEquals("20240101000000", ctx.getInstant());
    assertEquals(7L, ctx.getCheckpointId());
    assertSame(writeResults, ctx.getWriteResults());
    assertSame(metaClient, ctx.getMetaClient());
  }

  // -----------------------------------------------------------------------
  //  StreamWriteCommitHookLoader tests
  // -----------------------------------------------------------------------

  @Test
  void testLoaderReturnsNullWhenOptionAbsent() {
    assertNull(StreamWriteCommitHookLoader.load(new Configuration()));
  }

  @Test
  void testLoaderReturnsNullForEmptyClassName() {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS, "");
    assertNull(StreamWriteCommitHookLoader.load(conf));
  }

  @Test
  void testLoaderReturnsNullForNonExistentClass() {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS, "com.example.DoesNotExist");
    assertNull(StreamWriteCommitHookLoader.load(conf));
  }

  @Test
  void testLoaderReturnsNullForIncompatibleClass() {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS, NotAHook.class.getName());
    assertNull(StreamWriteCommitHookLoader.load(conf));
  }

  @Test
  void testLoaderReturnsNullWhenHookInitFails() {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS, InitFailingHook.class.getName());
    assertNull(StreamWriteCommitHookLoader.load(conf));
  }

  @Test
  void testLoaderInstantiatesValidHook() throws IOException {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS, NoOpHook.class.getName());

    StreamWriteCommitHook hook = StreamWriteCommitHookLoader.load(conf);
    assertNotNull(hook);
    assertTrue(hook instanceof NoOpHook);
    hook.close();
  }

  // -----------------------------------------------------------------------
  //  Full lifecycle test
  // -----------------------------------------------------------------------

  @Test
  void testSpyHookLifecycle() throws IOException {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS, SpyHook.class.getName());

    SpyHook hook = (SpyHook) StreamWriteCommitHookLoader.load(conf);
    assertNotNull(hook);
    assertSame(conf, hook.initConf); // init was called by the loader

    Map<String, String> meta = hook.getCommitExtraMetadata(42L);
    assertEquals(42L, hook.lastCheckpointId);
    assertEquals("42", meta.get("checkpoint_id"));

    StreamWriteCommitHook.PostCommitContext ctx = new StreamWriteCommitHook.PostCommitContext(
        42L, "20240101000000", Collections.singletonList(mock(WriteStatus.class)), mock(HoodieTableMetaClient.class));
    hook.postCommit(ctx);
    assertSame(ctx, hook.lastContext);

    assertFalse(hook.closed);
    hook.close();
    assertTrue(hook.closed);
  }
}
