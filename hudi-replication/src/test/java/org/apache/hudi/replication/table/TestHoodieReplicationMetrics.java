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

package org.apache.hudi.replication.table;

import com.uber.m3.tally.m3.M3Reporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;
import org.apache.hudi.common.metrics.LocalRegistry;
import org.apache.hudi.common.metrics.Registry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

public class TestHoodieReplicationMetrics {

  private static final ImmutableMap<String, String> TEST_TAGS = new ImmutableMap.Builder<String, String>()
      .put("service", "hoodie")
      .put("component", "test")
      .build();

  @AfterEach
  void tearDown() {
    HoodieReplicationMetrics.closeM3Reporter();
  }

  @Test
  void testUpdateMetricsCount() {
    Registry registry = Registry.getRegistryOfClass("testTable1", "TestRegistry1", LocalRegistry.class.getName());
    HoodieReplicationMetrics metrics = new HoodieReplicationMetrics(registry, "testTable1");

    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);
    Map<String, Long> counts = registry.getAllCounts();
    assertEquals(1L, counts.get(HoodieReplicationMetrics.REPLICATED + ".count"));

    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);
    counts = registry.getAllCounts();
    assertEquals(2L, counts.get(HoodieReplicationMetrics.REPLICATED + ".count"));
  }

  @Test
  void testUpdateMetricsWithDuration() {
    Registry registry = Registry.getRegistryOfClass("testTable2", "TestRegistry2", LocalRegistry.class.getName());
    HoodieReplicationMetrics metrics = new HoodieReplicationMetrics(registry, "testTable2");

    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED, 500L);
    Map<String, Long> counts = registry.getAllCounts();
    assertEquals(1L, counts.get(HoodieReplicationMetrics.REPLICATED + ".count"));
    assertEquals(500L, counts.get(HoodieReplicationMetrics.REPLICATED + ".duration"));
  }

  @Test
  void testPublishMetricsDoesNotThrow() {
    Registry registry = Registry.getRegistryOfClass("testTable3", "TestRegistry3", LocalRegistry.class.getName());
    HoodieReplicationMetrics metrics = new HoodieReplicationMetrics(registry, "testTable3");

    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);
    metrics.updateMetrics(HoodieReplicationMetrics.COMMIT_REPLICATED, 100L);

    assertDoesNotThrow(() -> metrics.publishMetrics(3));

    Map<String, Long> counts = registry.getAllCounts();
    assertTrue(counts.containsKey("tableCount"));
    assertTrue(counts.containsKey("pendingReplicationCount"));
    assertEquals(3L, counts.get("pendingReplicationCount"));
  }

  @Test
  void testCloseM3ReporterIdempotent() {
    Registry registry = Registry.getRegistryOfClass("testTable4", "TestRegistry4", LocalRegistry.class.getName());
    new HoodieReplicationMetrics(registry, "testTable4");

    assertDoesNotThrow(HoodieReplicationMetrics::closeM3Reporter);
    assertDoesNotThrow(HoodieReplicationMetrics::closeM3Reporter);
  }

  @Test
  void testCounterVsTimerRouting() {
    M3Reporter mockReporter = mock(M3Reporter.class);
    HoodieReplicationMetrics.setM3ReporterForTest(mockReporter, TEST_TAGS);

    Registry registry = Registry.getRegistryOfClass("testRouting", "RoutingRegistry", LocalRegistry.class.getName());
    HoodieReplicationMetrics metrics = new HoodieReplicationMetrics(registry, "testRouting");

    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);
    metrics.updateMetrics(HoodieReplicationMetrics.COMMIT_REPLICATED, 500L);
    metrics.publishMetrics(2);

    verify(mockReporter).reportCounter(eq("replicated.count"), any(Map.class), eq(1L));
    verify(mockReporter).reportCounter(eq("commitReplicated.count"), any(Map.class), eq(1L));
    verify(mockReporter).reportTimer(eq("commitReplicated.duration"), any(Map.class), eq(Duration.ofMillis(500)));

    verify(mockReporter, never()).reportCounter(contains(".duration"), any(Map.class), anyLong());
    verify(mockReporter, never()).reportTimer(contains(".count"), any(Map.class), any(Duration.class));
  }

  @Test
  void testDeltaSemantics() {
    M3Reporter mockReporter = mock(M3Reporter.class);
    HoodieReplicationMetrics.setM3ReporterForTest(mockReporter, TEST_TAGS);

    Registry registry = Registry.getRegistryOfClass("testDelta", "DeltaRegistry", LocalRegistry.class.getName());
    HoodieReplicationMetrics metrics = new HoodieReplicationMetrics(registry, "testDelta");

    // First publish: registry has replicated.count=1
    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);
    metrics.publishMetrics(1);
    verify(mockReporter).reportCounter(eq("replicated.count"), any(Map.class), eq(1L));

    // Second publish: registry cumulative=2, but M3 should only get delta=1
    reset(mockReporter);
    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);
    metrics.publishMetrics(1);
    verify(mockReporter).reportCounter(eq("replicated.count"), any(Map.class), eq(1L));

    // Third publish: no new updates, delta=0 -> no reportCounter for replicated.count
    reset(mockReporter);
    metrics.publishMetrics(1);
    verify(mockReporter, never()).reportCounter(eq("replicated.count"), any(Map.class), anyLong());
  }

  /**
   * Stress-test: publishMetrics() from multiple threads on the same instance.
   * In production, publishMetrics() is called sequentially per table, so
   * lastReportedCounts (an unsynchronized HashMap) is safe. This test verifies
   * no exceptions under contention and that the total reported delta is bounded
   * (some double-counting is possible due to the intentional lack of instance-level
   * synchronization).
   */
  @Test
  void testConcurrentPublishMetrics() throws Exception {
    M3Reporter mockReporter = mock(M3Reporter.class);
    HoodieReplicationMetrics.setM3ReporterForTest(mockReporter, TEST_TAGS);

    Registry registry = Registry.getRegistryOfClass("testConcurrent", "ConcurrentRegistry", LocalRegistry.class.getName());
    HoodieReplicationMetrics metrics = new HoodieReplicationMetrics(registry, "testConcurrent");
    metrics.updateMetrics(HoodieReplicationMetrics.REPLICATED);

    int threadCount = 8;
    CountDownLatch startGate = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      futures.add(executor.submit(() -> {
        try {
          startGate.await();
          metrics.publishMetrics(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }));
    }

    startGate.countDown();

    for (Future<?> future : futures) {
      assertDoesNotThrow(() -> future.get());
    }

    executor.shutdown();

    ArgumentCaptor<Long> deltaCaptor = ArgumentCaptor.forClass(Long.class);
    verify(mockReporter, atLeastOnce()).reportCounter(
        eq("replicated.count"), any(Map.class), deltaCaptor.capture());
    long totalReportedDelta = deltaCaptor.getAllValues().stream().mapToLong(Long::longValue).sum();
    assertTrue(totalReportedDelta >= 1 && totalReportedDelta <= threadCount,
        "Total reported delta should be between 1 and " + threadCount
            + " (was " + totalReportedDelta + "); some double-counting is expected"
            + " under contention since lastReportedCounts is intentionally unsynchronized");
  }

  @Test
  void testMissingEnvVarsDefaultToUnknown() {
    HoodieReplicationMetrics.closeM3Reporter();

    Registry registry = Registry.getRegistryOfClass("testEnvVars", "EnvVarsRegistry", LocalRegistry.class.getName());
    new HoodieReplicationMetrics(registry, "testEnvVars");

    ImmutableMap<String, String> tags = HoodieReplicationMetrics.getM3CommonTags();
    if (tags != null) {
      assertEquals("unknown", tags.get("env"));
      assertEquals("unknown", tags.get("datacenter"));
    }
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    Registry registry = Registry.getRegistryOfClass("testSerde", "SerdeRegistry", LocalRegistry.class.getName());
    HoodieReplicationMetrics original = new HoodieReplicationMetrics(registry, "testSerde");
    original.updateMetrics(HoodieReplicationMetrics.REPLICATED);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    HoodieReplicationMetrics deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      deserialized = (HoodieReplicationMetrics) ois.readObject();
    }

    assertNull(deserialized.registry(), "transient metricsRegistry should be null after deserialization");
    assertDoesNotThrow(() -> deserialized.updateMetrics(HoodieReplicationMetrics.REPLICATED));
    assertDoesNotThrow(() -> deserialized.updateMetrics(HoodieReplicationMetrics.REPLICATED, 100L));
    assertDoesNotThrow(() -> deserialized.publishMetrics(1));
  }
}
