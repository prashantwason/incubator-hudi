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

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Loads {@link StreamWriteCommitHook} from Flink configuration.
 */
public final class StreamWriteCommitHookLoader {

  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteCommitHookLoader.class);

  private StreamWriteCommitHookLoader() {
  }

  @Nullable
  public static StreamWriteCommitHook load(Configuration conf) {
    String className = conf.getOptional(FlinkOptions.STREAM_WRITE_COMMIT_HOOK_CLASS).orElse(null);
    if (StringUtils.isNullOrEmpty(className)) {
      return null;
    }

    try {
      Object instance = ReflectionUtils.loadClass(className);
      if (instance instanceof StreamWriteCommitHook) {
        StreamWriteCommitHook hook = (StreamWriteCommitHook) instance;
        hook.init(conf);
        LOG.info("Loaded and initialized stream write commit hook: {}", className);
        return hook;
      }
      LOG.error(
          "Configured stream write commit hook '{}' does not implement StreamWriteCommitHook (actual type: {}), "
              + "disabling hook",
          className,
          instance.getClass().getName());
      return null;
    } catch (Throwable t) {
      LOG.error("Failed to load '{}', disabling hook", className, t);
      return null;
    }
  }
}

