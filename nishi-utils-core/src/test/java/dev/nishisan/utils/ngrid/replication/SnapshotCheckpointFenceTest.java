package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.*;
import dev.nishisan.utils.ngrid.common.*;
import dev.nishisan.utils.map.*;
import dev.nishisan.utils.ngrid.map.*;
import dev.nishisan.utils.ngrid.HandoverListener;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.lang.reflect.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotCheckpointFenceTest {
    static final String TOPIC = "map:catalog";
    static final NodeId LOCAL = NodeId.of("zzz-follower"), LEADER = NodeId.of("aaa-leader");
    @TempDir Path directory;
    ScheduledExecutorService scheduler;
    ScriptedTransport transport;
    ClusterCoordinator coordinator;
    ReplicationManager manager;
    BlockingCheckpoint handler;
    MapClusterService<String, String> map;
    @BeforeEach void setup() throws Exception {
        scheduler = Executors.newScheduledThreadPool(2);
        transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 50),
                List.of(new NodeInfo(LEADER, "127.0.0.1", 2, Set.of(), 100)));
        coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(30),
                        Duration.ofSeconds(60), 2, null), scheduler);
        manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1).strictConsistency(false).leaderLocalApply(false)
                        .affinityHandbackMode(true).handoverCooldown(Duration.ofSeconds(60))
                        .handoverSnapshotTimeout(Duration.ofSeconds(5)).dataDirectory(directory).build());
        handler = new BlockingCheckpoint();
        manager.registerHandler(TOPIC, handler);
        manager.start();
        coordinator.start();
        await(() -> {
            heartbeat();
            return LEADER.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
        });
    }
    @AfterEach void close() throws Exception {
        handler.release.countDown();
        manager.close();
        if (map != null) map.close();
        coordinator.close();
        scheduler.shutdownNow();
    }
    void heartbeat() {
        transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", LEADER, null,
                HeartbeatPayload.now(50, 7, true, Map.of(TOPIC, 50L))));
    }
    Object get(String name) throws Exception {
        Field f = ReplicationManager.class.getDeclaredField(name); f.setAccessible(true); return f.get(manager);
    }
    void set(String name, Object value) throws Exception {
        Field f = ReplicationManager.class.getDeclaredField(name); f.setAccessible(true); f.set(manager, value);
    }
    Object invoke(String name, Class<?>[] types, Object... args) throws Exception {
        Method m = ReplicationManager.class.getDeclaredMethod(name, types); m.setAccessible(true);
        return m.invoke(manager, args);
    }
    ClusterMessage request() throws Exception { return request(TOPIC); }
    @SuppressWarnings("unchecked") ClusterMessage request(String topic) throws Exception {
        ((Set<String>) get("syncingTopics")).add(topic);
        assertEquals(true, invoke("requestSync", new Class<?>[]{String.class}, topic));
        return transport.sentOfType(MessageType.SYNC_REQUEST).getLast();
    }
    void respond(ClusterMessage request, boolean more, int chunk, long watermark) {
        transport.deliver(new ClusterMessage(UUID.randomUUID(), request.messageId(), MessageType.SYNC_RESPONSE,
                "sync", LEADER, LOCAL, new SyncResponsePayload(request.payload(SyncRequestPayload.class).topic(), watermark, chunk, more, new byte[0]), 5));
    }
    @SuppressWarnings({"unchecked", "rawtypes"}) void candidate(long attempt, long started) throws Exception {
        Field role = ReplicationManager.class.getDeclaredField("handbackRole"); role.setAccessible(true);
        java.util.concurrent.atomic.AtomicReference ref = (java.util.concurrent.atomic.AtomicReference) role.get(manager);
        ref.set(Enum.valueOf((Class) Class.forName(ReplicationManager.class.getName() + "$HandbackRole"), "CANDIDATE_INSTALLING"));
        set("handbackAttempt", attempt); set("handbackPeer", LEADER); set("handbackStartedMs", started);
        set("handbackGrantedEpoch", 7L);
        ((Set<String>) get("relayPendingBootstrap")).addAll(((Map<String, ?>) get("handlers")).keySet());
    }

    @Test @Timeout(20)
    void checkpointKeepsFrontierPendingAndWatchdogCannotResetIt() throws Exception {
        ClusterMessage request = request();
        respond(request, false, 0, 50);
        assertTrue(handler.entered.await(5, TimeUnit.SECONDS));
        assertEquals(0L, manager.appliedFrontiers().byTopic().getOrDefault(TOPIC, 0L));
        @SuppressWarnings("unchecked") Map<String, Long> activity = (Map<String, Long>) get("lastSyncActivityByTopic");
        activity.put(TOPIC, 1L);
        invoke("checkStuckSyncs", new Class<?>[0]);
        @SuppressWarnings("unchecked") Set<String> syncing = (Set<String>) get("syncingTopics");
        assertTrue(syncing.contains(TOPIC));
        heartbeat();
        assertTrue(coordinator.isAgreedLeaderHealthy(), "heartbeat still processes while checkpoint blocks");
        handler.release.countDown();
        await(() -> manager.appliedFrontiers().byTopic().getOrDefault(TOPIC, 0L) == 50L);
        assertEquals(1, handler.resets.get());
    }

    @Test @Timeout(20)
    void multipleTopicsPromoteOnlyAfterLastCheckpointCompletesWithinDeadline() throws Exception {
        String secondTopic = "map:placements";
        BlockingCheckpoint second = new BlockingCheckpoint();
        manager.registerHandler(secondTopic, second);
        candidate(1L, System.currentTimeMillis());
        respond(request(), false, 0, 50);
        assertTrue(handler.entered.await(5, TimeUnit.SECONDS));
        handler.release.countDown();
        await(() -> handler.completed.get() == 1);
        settled(TOPIC);
        assertFalse(coordinator.isLeader(), "first topic alone cannot promote the candidate");
        assertTrue(transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty());
        try {
            respond(request(secondTopic), false, 0, 50);
            assertTrue(second.entered.await(5, TimeUnit.SECONDS));
            heartbeat();
            assertFalse(coordinator.isLeader());
            second.release.countDown();
            await(() -> !transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty());
            settled(secondTopic);
            assertTrue(coordinator.isLeader());
            HandbackCompletePayload complete = transport.sentOfType(MessageType.HANDBACK_COMPLETE).getLast()
                    .payload(HandbackCompletePayload.class);
            assertEquals(Map.of(TOPIC, 50L, secondTopic, 50L), complete.cutoverByTopic());
        } finally {
            second.release.countDown();
        }
    }

    @Test @Timeout(20)
    void timedOutCheckpointCannotPromoteAndDoesNotAuthorizeCleanMarker() throws Exception {
        candidate(1L, System.currentTimeMillis());
        respond(request(), false, 0, 50);
        assertTrue(handler.entered.await(5, TimeUnit.SECONDS));
        set("handbackStartedMs", System.currentTimeMillis() - 10_000L);
        invoke("checkHandover", new Class<?>[0]);
        handler.release.countDown();
        await(() -> handler.completed.get() == 1);
        settled(TOPIC);
        manager.stop();
        assertFalse(coordinator.isLeader());
        assertTrue(transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty());
        assertFalse(manager.markCleanShutdownIfEligible(true));
        assertEquals(0L, manager.appliedFrontiers().byTopic().getOrDefault(TOPIC, 0L));
    }

    @Test @Timeout(20)
    void oldChunkZeroCannotBeReclassifiedIntoNewHandbackAttempt() throws Exception {
        candidate(1L, System.currentTimeMillis());
        ClusterMessage old = request();
        invoke("clearCandidateHandback", new Class<?>[]{String.class, boolean.class}, "test abort", false);
        invoke("abandonSyncChain", new Class<?>[]{String.class}, TOPIC);
        candidate(3L, System.currentTimeMillis());
        ClusterMessage current = request();
        respond(old, false, 0, 40);
        assertFalse(handler.entered.await(100, TimeUnit.MILLISECONDS));
        assertEquals(0, handler.resets.get());
        respond(current, false, 0, 50);
        assertTrue(handler.entered.await(5, TimeUnit.SECONDS));
        assertEquals(1, handler.resets.get());
        invoke("clearCandidateHandback", new Class<?>[]{String.class, boolean.class}, "test cleanup", false);
        handler.release.countDown();
    }

    @Test @Timeout(20)
    void lateCheckpointFromAbortedAttemptCannotCompleteReplacementAttempt() throws Exception {
        candidate(1L, System.currentTimeMillis());
        respond(request(), false, 0, 50);
        assertTrue(handler.entered.await(5, TimeUnit.SECONDS));
        invoke("clearCandidateHandback", new Class<?>[]{String.class, boolean.class}, "aborted", false);
        invoke("abandonSyncChain", new Class<?>[]{String.class}, TOPIC);
        candidate(3L, System.currentTimeMillis());
        respond(request(), false, 0, 60);
        handler.release.countDown();
        await(() -> !transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty());
        settled(TOPIC);
        assertEquals(2, handler.completed.get());
        List<ClusterMessage> completions = transport.sentOfType(MessageType.HANDBACK_COMPLETE);
        assertEquals(1, completions.size());
        assertEquals(Map.of(TOPIC, 60L), completions.getFirst().payload(HandbackCompletePayload.class).cutoverByTopic());
    }

    @Test @Timeout(20)
    void failedCheckpointNeverAdvancesFrontier() throws Exception {
        handler.fail = true;
        handler.release.countDown();
        respond(request(), false, 0, 50);
        await(() -> handler.completed.get() == 1);
        settled(TOPIC);
        manager.stop();
        assertEquals(0L, manager.appliedFrontiers().byTopic().getOrDefault(TOPIC, 0L));
        assertFalse(manager.markCleanShutdownIfEligible(true));
    }

    MapClusterService<String, String> durableMap() {
        map = new MapClusterService<>(manager, TOPIC, directory.resolve("maps"), "catalog",
                NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                        .snapshotIntervalTime(Duration.ofHours(1)).build());
        manager.registerHandler(TOPIC, new BlockingCheckpoint() {
            public void resetState() { map.resetState(); resets.incrementAndGet(); }
            public void installSnapshot(Object payload) { map.installSnapshot(payload); }
            public void onSnapshotInstalled() throws Exception {
                map.onSnapshotInstalled(); // Disk can contain stale state before session validation.
                handler.entered.countDown(); handler.release.await(); handler.completed.incrementAndGet();
            }
            public boolean onSnapshotAborted() throws Exception { return map.onSnapshotAborted(); }
            public void onSnapshotCommitted() { map.onSnapshotCommitted(); }
            public boolean hasDurableSnapshotCheckpoint() { return true; }
        });
        return map;
    }

    void respondMap(ClusterMessage request, boolean more, Map<String, String> data) {
        transport.deliver(new ClusterMessage(null, request.messageId(), MessageType.SYNC_RESPONSE,
                "sync", LEADER, LOCAL, new SyncResponsePayload(TOPIC, 50L, 0, more,
                MapReplicationCodec.encodeSnapshot(new HashMap<>(data))), 5));
    }

    @Test @Timeout(20)
    void promotionDuringPartialInstallRestoresMapBeforeReleasingWriteGate() throws Exception {
        var service = durableMap();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("prior", "trusted"));
        respondMap(request(), true, Map.of("partial", "untrusted"));
        await(() -> service.keySet().contains("partial"));
        coordinator.assumeLeadershipForHandback(7L); // Simulates legacy promotion without eligibility gate.
        await(() -> coordinator.isLeader() && !manager.isLeaderSyncing() && service.isHealthy());
        assertEquals(Set.of("prior"), service.keySet());
        assertDoesNotThrow(() -> service.apply(UUID.randomUUID(), MapReplicationCommand.put("after", "works")));
        assertTrue(((Map<?, ?>) get("physicalSnapshotInstalls")).isEmpty());
        assertTrue(((Set<?>) get("failedSnapshotInstalls")).isEmpty());
    }

    @Test @Timeout(20)
    void timeoutAfterCheckpointDurabilityRestoresPriorDiskAndDoesNotPromote() throws Exception {
        var service = durableMap();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("prior", "admitted-async"));
        candidate(1L, System.currentTimeMillis());
        respondMap(request(), false, Map.of("stale", "checkpointed"));
        assertTrue(handler.entered.await(5, TimeUnit.SECONDS));
        set("handbackStartedMs", System.currentTimeMillis() - 10_000L);
        invoke("checkHandover", new Class<?>[0]);
        handler.release.countDown();
        await(() -> service.isHealthy() && service.keySet().equals(Set.of("prior")));
        assertFalse(coordinator.isLeader());
        assertEquals(0L, manager.appliedFrontiers().byTopic().getOrDefault(TOPIC, 0L));
        assertTrue(transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty());
        service.close();
        map = null;
        try (var restarted = new MapClusterService<String, String>(manager, TOPIC,
                directory.resolve("maps"), "catalog", NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC).build())) {
            assertEquals(Set.of("prior"), restarted.keySet());
        }
    }

    @Test @Timeout(20)
    void explicitAbandonRestoresMapAndAllowsAnotherSnapshotSession() throws Exception {
        var service = durableMap();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("prior", "trusted"));
        respondMap(request(), true, Map.of("partial", "untrusted"));
        await(() -> service.keySet().contains("partial"));
        invoke("abandonSyncChain", new Class<?>[]{String.class}, TOPIC);
        await(() -> service.isHealthy() && service.keySet().equals(Set.of("prior")));
        handler.release.countDown();
        respondMap(request(), false, Map.of("replacement", "accepted"));
        await(() -> manager.appliedFrontiers().byTopic().getOrDefault(TOPIC, 0L) == 50L);
        assertEquals(Set.of("replacement"), service.keySet());
    }

    @Test @Timeout(20)
    void cutoverFailureRestoresImageButKeepsPromotionAndCleanShutdownBlocked() throws Exception {
        var service = durableMap();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("prior", "trusted"));
        java.nio.file.Path obstruction = directory.resolve("resend-obstruction");
        java.nio.file.Files.writeString(obstruction, "not a directory");
        ((ResendLogStore) get("resendLogStore")).close();
        set("resendLogStore", new ResendLogStore(obstruction, 100, Duration.ZERO, 0,
                Duration.ZERO, 1000, 0, true));
        handler.release.countDown();
        respondMap(request(), false, Map.of("replacement", "must-not-be-advertised"));
        await(() -> handler.completed.get() == 1
                && ((Map<?, ?>) uncheckedGet("physicalSnapshotInstalls")).isEmpty()
                && ((Set<?>) uncheckedGet("failedSnapshotInstalls")).contains(TOPIC)
                && service.isHealthy() && service.keySet().equals(Set.of("prior")));
        assertTrue(((Set<?>) get("failedSnapshotInstalls")).contains(TOPIC));
        assertTrue(((Set<?>) get("relayPendingBootstrap")).contains(TOPIC));
        assertEquals(-1L, manager.getAdvertisedHighWatermark());
        coordinator.assumeLeadershipForHandback(7L);
        assertTrue(manager.isLeaderSyncing(), "promotion cannot release a partially reanchored frontier");
        assertThrows(LeaderSyncingException.class, () -> manager.replicate(TOPIC, new byte[0]));
        manager.stop();
        assertFalse(manager.markCleanShutdownIfEligible(true));
    }

    @Test @Timeout(20)
    void delayedOldCleanupCannotClearNewerFailedInstallationGuards() throws Exception {
        respond(request(), true, 0, 50L);
        await(() -> handler.resets.get() == 1);
        Object old = ((Map<?, ?>) get("snapshotInstalls")).get(TOPIC);
        invoke("abandonSyncChain", new Class<?>[]{String.class}, TOPIC);
        await(() -> ((Map<?, ?>) uncheckedGet("physicalSnapshotInstalls")).isEmpty());
        handler.fail = true;
        handler.release.countDown();
        respond(request(), false, 0, 60L);
        await(() -> handler.completed.get() == 1 && ((Map<?, ?>) uncheckedGet("physicalSnapshotInstalls")).isEmpty());
        assertTrue(((Set<?>) get("failedSnapshotInstalls")).contains(TOPIC));
        @SuppressWarnings("unchecked")
        var locks = (Map<String, java.util.concurrent.locks.ReentrantLock>) get("snapshotInstallLocks");
        var lock = locks.get(TOPIC);
        lock.lock();
        try { invoke("restoreSnapshotInstallation", new Class<?>[]{String.class, old.getClass()}, TOPIC, old); }
        finally { lock.unlock(); }
        assertTrue(((Set<?>) get("failedSnapshotInstalls")).contains(TOPIC));
        assertTrue(((Set<?>) get("relayPendingBootstrap")).contains(TOPIC));
    }

    Object uncheckedGet(String name) {
        try { return get(name); } catch (Exception e) { throw new AssertionError(e); }
    }

    @Test @Timeout(20)
    void slowPromotionListenerDoesNotHoldLifecycleLockOrBlockScheduler() throws Exception {
        CountDownLatch listenerEntered = new CountDownLatch(1), listenerRelease = new CountDownLatch(1);
        manager.addHandoverListener(new HandoverListener() {
            @Override public void onPromotionComplete() {
                listenerEntered.countDown();
                try { listenerRelease.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });
        try {
            candidate(1L, System.currentTimeMillis());
            handler.release.countDown();
            respond(request(), false, 0, 50L);
            assertTrue(listenerEntered.await(5, TimeUnit.SECONDS));
            var lifecycle = (java.util.concurrent.locks.ReentrantLock) get("snapshotLifecycleLock");
            CountDownLatch schedulerCompleted = new CountDownLatch(1);
            scheduler.execute(() -> {
                lifecycle.lock();
                try { schedulerCompleted.countDown(); } finally { lifecycle.unlock(); }
            });
            assertTrue(schedulerCompleted.await(1, TimeUnit.SECONDS), "listener must not block lifecycle checks");
        } finally {
            listenerRelease.countDown();
        }
    }

    @Test @Timeout(20)
    void genericQueueAbortKeepsFailedGuardsUntilFreshFullSnapshot() throws Exception {
        String topic = "queue:abort";
        BlockingCheckpoint queue = new BlockingCheckpoint();
        queue.release.countDown();
        manager.registerHandler(topic, queue);
        respond(request(topic), true, 0, 50L);
        await(() -> queue.resets.get() == 1);
        invoke("abandonSyncChain", new Class<?>[]{String.class}, topic);
        await(() -> !((Map<?, ?>) uncheckedGet("physicalSnapshotInstalls")).containsKey(topic));
        assertTrue(((Set<?>) get("failedSnapshotInstalls")).contains(topic),
                "a handler without rollback cannot make partial state trusted");
        assertTrue(((Set<?>) get("relayPendingBootstrap")).contains(topic));
        assertFalse(manager.isLeadershipEligible());
        respond(request(topic), false, 0, 60L);
        await(() -> manager.appliedFrontiers().byTopic().getOrDefault(topic, 0L) == 60L
                && !((Set<?>) uncheckedGet("failedSnapshotInstalls")).contains(topic));
        assertFalse(((Set<?>) get("relayPendingBootstrap")).contains(topic));
    }

    @Test @Timeout(20)
    void queueWithoutDurabilityBarrierDoesNotEmitSyncDurable() throws Exception {
        String queueTopic = "queue:test";
        BlockingCheckpoint queue = new BlockingCheckpoint();
        queue.release.countDown();
        manager.registerHandler(queueTopic, queue);
        List<String> logs = new CopyOnWriteArrayList<>();
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            public void publish(java.util.logging.LogRecord record) { logs.add(record.getMessage()); }
            public void flush() { }
            public void close() { }
        };
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(ReplicationManager.class.getName());
        logger.addHandler(capture);
        try {
            respond(request(queueTopic), false, 0, 50L);
            await(() -> logs.stream().anyMatch(line -> line.startsWith("Sync completed for " + queueTopic)));
            assertFalse(logs.stream().anyMatch(line -> line.startsWith("Sync durable for " + queueTopic)));
        } finally {
            logger.removeHandler(capture);
        }
    }

    @SuppressWarnings("unchecked") void settled(String topic) throws Exception {
        Map<String, java.util.concurrent.locks.ReentrantLock> locks =
                (Map<String, java.util.concurrent.locks.ReentrantLock>) get("snapshotInstallLocks");
        java.util.concurrent.locks.ReentrantLock lock = locks.get(topic);
        assertNotNull(lock);
        lock.lock();
        lock.unlock();
    }

    static void await(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < limit) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(10);
        }
        fail("condition did not become true");
    }
    static class BlockingCheckpoint implements ReplicationHandler {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger resets = new AtomicInteger(), completed = new AtomicInteger();
        volatile boolean fail;
        public void apply(UUID operationId, Object payload) { }
        public void resetState() { resets.incrementAndGet(); }
        public void installSnapshot(Object payload) { }
        public void onSnapshotInstalled() throws Exception {
            entered.countDown(); release.await(); completed.incrementAndGet();
            if (fail) throw new java.io.IOException("injected checkpoint failure");
        }
    }
}
