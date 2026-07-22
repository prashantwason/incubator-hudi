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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.Option;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

/**
 * Split provider that serves splits from a single shared pool, ignoring the requesting subtask id
 * (work-stealing): whichever reader asks next gets the next pending` split. All readers therefore
 * stay busy until the pool is fully drained.
 *
 * <p>Intended for BOUNDED (batch) reads driven by
 * {@link org.apache.hudi.source.enumerator.HoodieStaticSplitEnumerator}, where the complete split
 * set is known up front and each split (a base file or file slice over a fixed instant range) is
 * independent and order-free, so any reader can safely read any split.
 *
 * <p>Contrast with {@link DefaultHoodieSplitProvider}, which statically pins each split to one
 * subtask (by hashing the file id / round-robin on split number) and never rebalances: a subtask
 * that drew a heavier share runs long while peers sit idle, producing a declining "staircase" tail
 * and mid-run dips whenever readers fall out of lock-step. Because assignment there balances split
 * <em>count</em> rather than bytes/records and cannot steal, even small per-subtask skew is not
 * recoverable. The global pool removes both effects for bounded reads.
 *
 * <p>NOT used for streaming reads: the continuous enumerator keeps per-subtask assignment (via
 * {@link DefaultHoodieSplitProvider}) so a file id's incremental splits stay affine to one reader.
 *
 * <p>Splits are served oldest-commit-first via {@link HoodieSourceSplitComparator}, matching the
 * per-subtask provider's ordering. Thread safe: a {@link PriorityBlockingQueue} backs the pool, and
 * {@link #pendingSplitCount()} may be read from I/O threads for the unassigned-splits gauge while
 * the coordinator thread assigns.
 */
public class GlobalHoodieSplitProvider implements HoodieSplitProvider {
  public static final int INITIAL_POOL_CAPACITY = 20;

  // Shared pool of unassigned splits, ordered by commit time (oldest first).
  private final Queue<HoodieSourceSplit> pendingSplits;
  private CompletableFuture<Void> availableFuture;

  public GlobalHoodieSplitProvider() {
    this.pendingSplits =
        new PriorityBlockingQueue<>(INITIAL_POOL_CAPACITY, new HoodieSourceSplitComparator());
  }

  @Override
  public Option<HoodieSourceSplit> getNext(int taskId, @Nullable String hostname) {
    // Work-stealing: subtask id / hostname are intentionally ignored so any requesting reader gets
    // the next split from the shared pool. Empty means the pool is drained; for the static
    // enumerator (shouldWaitForMoreSplits() == false) that correctly triggers signalNoMoreSplits.
    HoodieSourceSplit next = pendingSplits.poll();
    return next == null ? Option.empty() : Option.of(next);
  }

  @Override
  public void onDiscoveredSplits(Collection<HoodieSourceSplit> splits) {
    addSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<HoodieSourceSplit> splits) {
    // Splits handed back by a failed reader (addSplitsBack) return to the shared pool and are
    // picked up by whichever reader asks next (typically the restarted subtask), same as any other
    // pending split.
    addSplits(splits);
  }

  private void addSplits(Collection<HoodieSourceSplit> splits) {
    if (splits.isEmpty()) {
      return;
    }
    pendingSplits.addAll(splits);
    completeAvailableFuturesIfNeeded();
  }

  @Override
  public Collection<HoodieSourceSplitState> state() {
    return pendingSplits.stream()
        .map(split -> new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public int pendingSplitCount() {
    return pendingSplits.size();
  }

  @Override
  public long pendingRecords() {
    throw new UnsupportedOperationException(
        "Pending records is not supported in GlobalHoodieSplitProvider.");
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !pendingSplits.isEmpty()) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
