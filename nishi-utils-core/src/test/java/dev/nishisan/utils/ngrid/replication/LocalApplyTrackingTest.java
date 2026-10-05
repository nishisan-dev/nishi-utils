/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */
package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.SyncRequestPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Issue #191: the snapshot guard tracks accepted applies until they really finish. */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LocalApplyTrackingTest {
    private static final String TOPIC = "map:catalog";
    private static final NodeId LOCAL = NodeId.of("aaa-leader");
    private static final NodeId REQUESTER = NodeId.of("mmm-requester");
    @TempDir Path data;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final CountDownLatch releaseApply = new CountDownLatch(1);
    private ReplicationManager manager;
    private ClusterCoordinator coordinator;
    private ScriptedTransport transport;

    @AfterEach
    void close() throws Exception {
        releaseApply.countDown();
        try {
            if (manager != null) manager.close();
        } finally {
            if (coordinator != null) coordinator.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void rejectedSubmitFailsFutureAndRemovesSnapshotGuard() throws Exception {
        start(new TestHandler(), Duration.ofSeconds(5));
        executor().shutdownNow();
        CompletableFuture<ReplicationResult> write = manager.replicate(TOPIC, new byte[]{1});
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> write.get(2, TimeUnit.SECONDS));
        assertInstanceOf(RejectedExecutionException.class, failure.getCause());
        assertEquals(0, manager.getPendingOperationsCount());
        assertTrue(noTrackedApplies(), "a rejected task has no finally block to remove its sequence");
    }

    @Test
    void operationTimeoutDoesNotReleaseSnapshotGuardWhileApplyIsRunning() throws Exception {
        TestHandler handler = new TestHandler();
        start(handler, Duration.ofMillis(300));
        manager.replicate(TOPIC, new byte[]{1}).get(2, TimeUnit.SECONDS);
        handler.blockNext = true;
        CompletableFuture<ReplicationResult> blocked = manager.replicate(TOPIC, new byte[]{2});
        assertTrue(handler.started.await(2, TimeUnit.SECONDS));
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> blocked.get(3, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, failure.getCause());
        assertEquals(0, manager.getPendingOperationsCount());
        assertFalse(noTrackedApplies(), "timeout removes pending, but the apply can still change content");
        assertEquals(1L, snapshotLabel());
        releaseApply.countDown();
        await(this::noTrackedApplies, "the completed apply must release the guard");
        assertEquals(2L, snapshotLabel());
    }

    @Test
    void throwingApplyRemovesSnapshotGuardInFinally() throws Exception {
        TestHandler handler = new TestHandler();
        handler.throwNext = true;
        start(handler, Duration.ofSeconds(5));
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> manager.replicate(TOPIC, new byte[]{1}).get(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        await(this::noTrackedApplies, "failed apply must release the snapshot guard");
        assertEquals(0, manager.getPendingOperationsCount());
    }

    @Test
    void permanentCloseClearsGuardForAcceptedTaskDiscardedFromExecutorQueue() throws Exception {
        start(new TestHandler(), Duration.ofSeconds(5));
        CountDownLatch workersStarted = new CountDownLatch(4);
        for (int i = 0; i < 4; i++) {
            executor().submit(() -> {
                workersStarted.countDown();
                try {
                    releaseApply.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertTrue(workersStarted.await(2, TimeUnit.SECONDS));
        CompletableFuture<ReplicationResult> queued = manager.replicate(TOPIC, new byte[]{1});
        assertFalse(noTrackedApplies());
        manager.close();
        assertTrue(queued.isCompletedExceptionally());
        assertTrue(noTrackedApplies(), "shutdownNow discards queued task without running its finally");
    }

    private void start(TestHandler handler, Duration timeout) throws Exception {
        transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 1, null).withPairMode(true), scheduler);
        manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1).strictConsistency(false).leaderLocalApply(true)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(timeout).dataDirectory(data).build());
        manager.registerHandler(TOPIC, handler);
        manager.start();
        coordinator.start();
        await(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), "leader should be ready");
    }

    private long snapshotLabel() throws Exception {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", REQUESTER, LOCAL,
                new SyncRequestPayload(TOPIC, 0)));
        await(() -> !transport.sentOfType(MessageType.SYNC_RESPONSE).isEmpty(), "snapshot response expected");
        return transport.sentOfType(MessageType.SYNC_RESPONSE).getFirst()
                .payload(SyncResponsePayload.class).sequence();
    }

    private ExecutorService executor() throws Exception {
        Field field = ReplicationManager.class.getDeclaredField("executor");
        field.setAccessible(true);
        return (ExecutorService) field.get(manager);
    }

    @SuppressWarnings("unchecked")
    private boolean noTrackedApplies() {
        try {
            Field field = ReplicationManager.class.getDeclaredField("localAppliesInFlightByTopic");
            field.setAccessible(true);
            Map<String, NavigableSet<Long>> tracked = (Map<String, NavigableSet<Long>>) field.get(manager);
            return tracked.values().stream().allMatch(NavigableSet::isEmpty);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void await(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(10);
        }
        fail(message);
    }

    private final class TestHandler implements ReplicationHandler {
        private final CountDownLatch started = new CountDownLatch(1);
        private volatile boolean blockNext;
        private volatile boolean throwNext;

        @Override
        public void apply(UUID operationId, Object payload) throws Exception {
            if (throwNext) {
                throwNext = false;
                throw new IllegalStateException("injected apply failure");
            }
            if (blockNext) {
                blockNext = false;
                started.countDown();
                releaseApply.await();
            }
        }

        @Override
        public Object getSnapshot() {
            return new byte[]{1};
        }
    }
}
