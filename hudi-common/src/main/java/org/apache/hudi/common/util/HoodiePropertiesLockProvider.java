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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

/**
 * File-based lock provider for guarding {@code hoodie.properties}/{@code replication.properties} updates, based on
 * atomic file creation. Unlike {@code org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider} in
 * hudi-client-common, this does not depend on {@code HoodieWriteConfig}/{@code HoodieLockConfig}, so it can be used
 * from hudi-common (by {@link org.apache.hudi.common.table.HoodieTableConfig}) and hudi-replication without
 * introducing a circular module dependency.
 */
public class HoodiePropertiesLockProvider implements LockProvider<String> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodiePropertiesLockProvider.class);

  private final String lockName;
  private final HoodieStorage storage;
  private final StoragePath lockFile;
  private final int maxRetries;
  private final long retryIntervalMs;
  private final long timeoutMs;

  public HoodiePropertiesLockProvider(HoodieStorage storage, String lockName, LockConfiguration lockConfiguration) {
    this.lockName = lockName;
    this.storage = storage;
    String lockPath = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY);
    this.lockFile = new StoragePath(lockPath, lockName);
    this.maxRetries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY);
    this.retryIntervalMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
    // A lock is considered stale once it has outlived the full retry budget of a well-behaved holder,
    // so a crashed/hung writer's lock is reclaimed right as this waiter gives up retrying.
    this.timeoutMs = (long) maxRetries * retryIntervalMs;
  }

  public void acquireLock() {
    try {
      storage.create(lockFile, false).close();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to acquire lock", e);
    }
  }

  @Override
  public void close() {
    unlock();
  }

  private boolean hasLockExpired(long timeoutMs) {
    try {
      if (storage.exists(lockFile)) {
        long remaining = storage.getPathInfo(lockFile).getModificationTime() + timeoutMs - System.currentTimeMillis();
        if (remaining < 0) {
          LOG.warn(String.format("%s lock has timed out. Unlocking %s.", lockName, lockFile.getName()));
          unlock();
        }
        return remaining < 0;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Unable to check lock status on disk", e);
    }
    return true;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws HoodieLockException {
    try {
      int numRetries = 0;
      while (!hasLockExpired(timeoutMs) && (numRetries <= maxRetries)) {
        Thread.sleep(retryIntervalMs);
        numRetries++;
      }
      acquireLock();
      return true;
    } catch (HoodieIOException | InterruptedException e) {
      throw new HoodieLockException("Failed to acquire lock", e);
    }
  }

  @Override
  public void unlock() {
    try {
      if (storage.exists(lockFile)) {
        storage.deleteFile(lockFile);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Unable to delete lock on disk", e);
    }
  }

  @Override
  public String getLock() {
    return lockFile.toString();
  }
}
