/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;

class WriteBarrierRegressionTest {
    private final List<CountDownLatch> gates = new CopyOnWriteArrayList<>();
    private final Rpc rpc = new Rpc();
    private WriteDispatcher dispatcher;
    private RemoteSeriesHandle handle;

    private void open() {
        PlacementLookup lookup = new PlacementLookup() {
            public SeriesPlacement resolve(String key, String hash) { return SeriesPlacement.active("A", 0); }
            public SeriesPlacement resolveExisting(String key, Duration maxWait) { return resolve(key, null); }
            public Optional<SeriesPlacement> placementCached(String key) { return Optional.of(resolve(key, null)); }
            public void invalidate(String key) { }
            public void noteOwner(String key, String owner) { }
        };
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(20));
        dispatcher = new WriteDispatcher(rpc, lookup, retry, 1, Duration.ofMillis(10), 100,
                NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(3), key -> true,
                (key, owner) -> handle.ownerChanged(owner), Clock.systemUTC(), null, null);
        handle = new RemoteSeriesHandle("s", "unused", "unused", Map.of(), null, lookup, rpc, dispatcher,
                retry, Duration.ofSeconds(3), Duration.ofSeconds(3), Clock.systemUTC(), key -> { });
        handle.open();
    }

    @AfterEach
    void close() {
        gates.forEach(CountDownLatch::countDown);
        rpc.writes = (owner, request) -> ok();
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private CountDownLatch gate() {
        var gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    @Test
    void checkpointWaitsAcrossTwoRedirects() throws Exception { verifyBarrier(Commands.CHECKPOINT); }

    @Test
    void flushWaitsAcrossTwoRedirects() throws Exception { verifyBarrier(Commands.FLUSH); }

    @Test
    void closeWaitsAcrossTwoRedirects() throws Exception { verifyBarrier(Commands.CLOSE); }

    private void verifyBarrier(String command) throws Exception {
        var firstEntered = gate();
        var releaseFirst = gate();
        var finalEntered = gate();
        var releaseFinal = gate();
        rpc.writes = (owner, request) -> {
            if (owner.equals("A")) {
                firstEntered.countDown();
                await(releaseFirst);
                return moved("B");
            }
            if (owner.equals("B")) {
                return moved("C");
            }
            finalEntered.countDown();
            await(releaseFinal);
            return ok();
        };
        open();
        handle.write("ds", new Sample(1, 1));
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        var operation = CompletableFuture.runAsync(() -> {
            switch (command) {
                case Commands.CHECKPOINT -> handle.checkpoint();
                case Commands.FLUSH -> handle.flush();
                case Commands.CLOSE -> handle.close();
                default -> throw new AssertionError(command);
            }
        });
        assertThrows(TimeoutException.class, () -> operation.get(100, TimeUnit.MILLISECONDS));
        releaseFirst.countDown();
        assertTrue(finalEntered.await(5, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> operation.get(100, TimeUnit.MILLISECONDS));
        assertFalse(rpc.commands.contains(command + "@C"), "barreira remota não pode ultrapassar WRITE_BATCH");
        releaseFinal.countDown();
        operation.get(5, TimeUnit.SECONDS);
        assertEquals(1, dispatcher.samplesSent());
        assertTrue(rpc.commands.contains(command + "@C"));
    }

    @Test
    void nodeBarrierFollowsWritesEvenWhenRedirectAlreadyHappened() throws Exception {
        var entered = gate();
        var release = gate();
        rpc.writes = (owner, request) -> {
            if (owner.equals("A")) { return moved("B"); }
            entered.countDown();
            await(release);
            return ok();
        };
        open();
        handle.write("ds", new Sample(1, 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        var flush = CompletableFuture.runAsync(() -> dispatcher.flushNodeSync("A"));
        assertThrows(TimeoutException.class, () -> flush.get(100, TimeUnit.MILLISECONDS));
        release.countDown();
        flush.get(5, TimeUnit.SECONDS);
        assertEquals(1, dispatcher.samplesSent());
    }

    @Test
    void laterWritesDoNotExtendAnExistingBarrier() throws Exception {
        var firstEntered = gate();
        var releaseFirst = gate();
        var secondEntered = gate();
        var releaseSecond = gate();
        var calls = new AtomicInteger();
        rpc.writes = (owner, request) -> {
            if (calls.incrementAndGet() == 1) {
                firstEntered.countDown();
                await(releaseFirst);
            } else {
                secondEntered.countDown();
                await(releaseSecond);
            }
            return ok();
        };
        open();
        handle.write("ds", new Sample(1, 1));
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        var checkpoint = CompletableFuture.runAsync(handle::checkpoint);
        assertThrows(TimeoutException.class, () -> checkpoint.get(100, TimeUnit.MILLISECONDS));
        handle.write("ds", new Sample(2, 2));
        releaseFirst.countDown();
        assertTrue(secondEntered.await(5, TimeUnit.SECONDS));
        checkpoint.get(5, TimeUnit.SECONDS);
        assertEquals(1, dispatcher.samplesSent());
        assertTrue(rpc.commands.contains(Commands.CHECKPOINT + "@A"));
    }

    @Test
    void rejectedWritePreventsSuccessfulCheckpoint() {
        rpc.writes = (owner, request) -> new WriteBatchResponse(Map.of("s", SeriesStatus.ERROR),
                Map.of(), Map.of("s", "disk failure"));
        open();
        handle.write("ds", new Sample(1, 1));
        var error = assertThrows(NgrrdClusterException.class, handle::checkpoint);
        assertEquals(ErrorCode.REMOTE_ERROR, error.code());
        assertTrue(error.getMessage().contains("disk failure"));
        assertFalse(rpc.commands.contains(Commands.CHECKPOINT + "@A"));
    }

    @Test
    void redirectedBarrierRespectsTimeout() {
        rpc.writes = (owner, request) -> owner.equals("A") ? moved("B")
                : new WriteBatchResponse(Map.of("s", SeriesStatus.MIGRATING), Map.of(), Map.of());
        open();
        handle.write("ds", new Sample(1, 1));
        var error = assertThrows(NgrrdClusterException.class,
                () -> dispatcher.flushSeriesSync("s", "A", Duration.ofMillis(80)));
        assertEquals(ErrorCode.TIMEOUT, error.code());
        assertEquals(0, dispatcher.samplesSent());
    }

    @Test
    void globalBarrierDoesNotLockAlreadyAcknowledgedHistory() throws Exception {
        rpc.writes = (owner, request) -> ok();
        open();
        handle.write("ds", new Sample(1, 1));
        dispatcher.flushAllSync();
        // An unrelated operation on a fully acknowledged route must not delay a
        // global ACK barrier. Deterministic regression for scanning all history.
        var field = WriteDispatcher.class.getDeclaredField("routes");
        field.setAccessible(true);
        Object route = ((Map<?, ?>) field.get(dispatcher)).get("s");
        var lockField = route.getClass().getDeclaredField("lock");
        lockField.setAccessible(true);
        ReentrantLock lock = (ReentrantLock) lockField.get(route);
        CompletableFuture<Void> flush;
        lock.lock();
        try {
            flush = CompletableFuture.runAsync(dispatcher::flushAllSync);
            flush.get(1, TimeUnit.SECONDS);
        } finally {
            lock.unlock();
        }
    }

    @Test
    void reusedAcknowledgedRouteBecomesPendingAgainAndWakesAllWaiters() throws Exception {
        rpc.writes = (owner, request) -> ok();
        open();
        handle.write("ds", new Sample(1, 1));
        dispatcher.flushAllSync();
        var entered = gate();
        var release = gate();
        rpc.writes = (owner, request) -> {
            entered.countDown();
            await(release);
            return ok();
        };
        handle.write("ds", new Sample(2, 2));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        var global = CompletableFuture.runAsync(dispatcher::flushAllSync);
        var node = CompletableFuture.runAsync(() -> dispatcher.flushNodeSync("A"));
        assertThrows(TimeoutException.class, () -> global.get(100, TimeUnit.MILLISECONDS));
        assertThrows(TimeoutException.class, () -> node.get(100, TimeUnit.MILLISECONDS));
        release.countDown();
        global.get(5, TimeUnit.SECONDS);
        node.get(5, TimeUnit.SECONDS);
        assertEquals(2, dispatcher.samplesSent());
    }

    @Test
    void globalBarrierRetainsFailureEvenAfterLaterAck() {
        rpc.writes = (owner, request) -> new WriteBatchResponse(Map.of("s", SeriesStatus.ERROR),
                Map.of(), Map.of("s", "disk failure"));
        open();
        handle.write("ds", new Sample(1, 1));
        assertEquals(ErrorCode.REMOTE_ERROR,
                assertThrows(NgrrdClusterException.class, dispatcher::flushAllSync).code());
        rpc.writes = (owner, request) -> ok();
        handle.write("ds", new Sample(2, 2));
        Await.untilTrue("later sample acknowledged", Duration.ofSeconds(5), () -> dispatcher.samplesSent() == 1);
        assertEquals(ErrorCode.REMOTE_ERROR,
                assertThrows(NgrrdClusterException.class, dispatcher::flushAllSync).code());
        assertEquals(ErrorCode.REMOTE_ERROR,
                assertThrows(NgrrdClusterException.class, () -> dispatcher.flushNodeSync("A")).code());
    }

    @Test
    void globalBarrierWaitsForEarlierWriteDespiteAckOnAnotherNode() throws Exception {
        var slowEntered = gate();
        var releaseSlow = gate();
        rpc.writes = (owner, request) -> {
            if (owner.equals("A")) {
                slowEntered.countDown();
                await(releaseSlow);
            }
            var statuses = new java.util.HashMap<String, SeriesStatus>();
            request.writes().forEach(write -> statuses.put(write.seriesKey(), SeriesStatus.OK));
            return new WriteBatchResponse(statuses, Map.of(), Map.of());
        };
        open();
        handle.write("ds", new Sample(1, 1));
        assertTrue(slowEntered.await(5, TimeUnit.SECONDS));
        dispatcher.enqueue("B", new dev.nishisan.utils.oss.cluster.protocol.SeriesWrite("fast", "ds", 2, 2));
        Await.untilTrue("other node acknowledged", Duration.ofSeconds(5), () -> dispatcher.samplesSent() == 1);
        var flush = CompletableFuture.runAsync(dispatcher::flushAllSync);
        assertThrows(TimeoutException.class, () -> flush.get(100, TimeUnit.MILLISECONDS));
        releaseSlow.countDown();
        flush.get(5, TimeUnit.SECONDS);
        assertEquals(2, dispatcher.samplesSent());
    }

    @Test
    void globalBarrierFollowsRedirectAndDoesNotWaitForLaterAdmissions() throws Exception {
        var entered = gate();
        var release = gate();
        var laterEntered = gate();
        var releaseLater = gate();
        rpc.writes = (owner, request) -> {
            if (owner.equals("A")) return moved("B");
            if (request.writes().getFirst().tsEpochMs() == 1) {
                entered.countDown();
                await(release);
            } else {
                laterEntered.countDown();
                await(releaseLater);
            }
            return ok();
        };
        open();
        handle.write("ds", new Sample(1, 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        var thread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var flush = CompletableFuture.runAsync(() -> {
            thread.set(Thread.currentThread());
            dispatcher.flushAllSync();
        });
        Await.untilTrue("barrier captured its boundary", Duration.ofSeconds(5), () -> thread.get() != null
                && java.util.Arrays.stream(thread.get().getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals(WriteDispatcher.class.getName()) && frame.getMethodName().equals("awaitBarriers")));
        handle.write("ds", new Sample(2, 2));
        release.countDown();
        assertTrue(laterEntered.await(5, TimeUnit.SECONDS));
        flush.get(5, TimeUnit.SECONDS);
        assertEquals(1, dispatcher.samplesSent());
    }

    @Test
    void concurrentAdmissionsAndAckRemovalNeverSkipCompletedEnqueues() throws Exception {
        rpc.writes = (owner, request) -> ok();
        open();
        var admitted = new AtomicInteger();
        try (var producers = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int p = 0; p < 4; p++) {
                tasks.add(producers.submit(() -> {
                    for (int i = 0; i < 300; i++) {
                        handle.write("ds", new Sample(i, i));
                        admitted.incrementAndGet();
                        if (i % 7 == 0) Thread.yield();
                    }
                }));
            }
            for (int i = 0; i < 100; i++) {
                int beforeBarrier = admitted.get();
                dispatcher.flushAllSync();
                assertTrue(dispatcher.samplesSent() >= beforeBarrier,
                        "every completed enqueue before the barrier must have an ACK");
            }
            for (var task : tasks) task.get(10, TimeUnit.SECONDS);
        }
        dispatcher.flushAllSync();
        assertEquals(1200, admitted.get());
        assertEquals(1200, dispatcher.samplesSent());
    }

    private static WriteBatchResponse moved(String owner) {
        return new WriteBatchResponse(Map.of("s", SeriesStatus.WRONG_OWNER), Map.of("s", owner), Map.of());
    }

    private static WriteBatchResponse ok() {
        return new WriteBatchResponse(Map.of("s", SeriesStatus.OK), Map.of(), Map.of());
    }

    private static void await(CountDownLatch gate) {
        try {
            assertTrue(gate.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static class Rpc implements ClusterRpc {
        volatile BiFunction<String, WriteBatchRequest, WriteBatchResponse> writes;
        final List<String> commands = new CopyOnWriteArrayList<>();
        public <R> R call(NodeId target, String command, Object body, Class<R> type) {
            commands.add(command + "@" + target.value());
            if (command.equals(Commands.WRITE_BATCH)) {
                return type.cast(writes.apply(target.value(), (WriteBatchRequest) body));
            }
            return type.cast(new SeriesStatusResponse(SeriesStatus.OK, target.value(), null));
        }
        public NodeId localId() { return NodeId.of("client"); }
        public Optional<NodeId> leaderId() { return Optional.of(NodeId.of("A")); }
    }
}
