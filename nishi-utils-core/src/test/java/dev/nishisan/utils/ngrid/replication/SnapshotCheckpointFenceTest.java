package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.*;
import dev.nishisan.utils.ngrid.common.*;
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
