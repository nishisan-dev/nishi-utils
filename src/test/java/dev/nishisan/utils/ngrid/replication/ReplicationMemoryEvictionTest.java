package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the memory eviction mechanisms in {@link ReplicationManager}
 * correctly cap the size of the {@code applied} dedup set and the operation
 * audit {@code log}.
 */
class ReplicationMemoryEvictionTest {

    @TempDir
    Path replicationDir;

    private FakeTransport transport;
    private ClusterCoordinator coordinator;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        NodeInfo local = new NodeInfo(NodeId.of("node-leader"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("node-follower"), "127.0.0.1", 0);

        transport = new FakeTransport(local, List.of(peer));
        scheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(peer);
        assertTrue(coordinator.isLeader(), "Local node should be leader");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (coordinator != null)
            coordinator.close();
        if (scheduler != null)
            scheduler.shutdownNow();
        if (transport != null)
            transport.close();
    }

    /**
     * Scenario 1: applied set never exceeds appliedSetMaxSize.
     *
     * Applies N+50 operations through a follower path simulation (direct
     * handleReplicationRequest) and validates that the dedup set stays bounded.
     */
    @Test
    @Timeout(value = 30)
    void appliedSetShouldNotExceedMaxSize() throws Exception {
        int maxSize = 50;
        ReplicationManager manager = buildManager(maxSize, 2000);
        AtomicInteger applyCount = new AtomicInteger();
        manager.registerHandler("t1", (opId, payload) -> applyCount.incrementAndGet());
        manager.start();

        int total = maxSize + 100;
        for (int i = 1; i <= total; i++) {
            simulateFollowerApply(manager, "t1", i, "payload-" + i);
        }

        // Allow executor tasks to settle
        Thread.sleep(500);

        int size = manager.getAppliedSetSize();
        assertTrue(size <= maxSize,
                "applied set should be capped at " + maxSize + " but was " + size);

        manager.close();
    }

    /**
     * Scenario 2: operation audit log does not exceed operationLogMaxSize after
     * trimLog is triggered explicitly via direct invocation (simulated scheduler).
     */
    @Test
    @Timeout(value = 30)
    void operationLogShouldNotExceedMaxSizeAfterTrim() throws Exception {
        int logMaxSize = 30;
        // Build manager with tight log limit and short operation timeout so trimLog
        // is scheduled quickly (every 2.5s = max(5000, 500*5))
        ReplicationManager manager = buildManager(5000, logMaxSize);
        manager.registerHandler("t2", (opId, payload) -> {
        });
        manager.start();

        // Replicate logMaxSize+50 operations on the leader path
        int total = logMaxSize + 50;
        for (int i = 0; i < total; i++) {
            CompletableFuture<ReplicationResult> f = manager.replicate("t2", "msg-" + i);
            // The follower ACKs immediately via the fake transport interceptor
            transport.drainAcksTo(manager);
            try {
                f.get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // may timeout if quorum unreachable in isolation — that's OK
            }
        }

        // Manually advance time by waiting for the scheduled trimLog to fire
        // (max(5000, 500*5) = 5000ms). We poll for convergence instead.
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (manager.getOperationLogSize() <= logMaxSize) {
                break;
            }
            Thread.sleep(200);
        }

        int size = manager.getOperationLogSize();
        assertTrue(size <= logMaxSize,
                "operation log should be trimmed to <= " + logMaxSize + " but was " + size);

        manager.close();
    }

    /**
     * Scenario 3: dedup still works after eviction from applied set.
     *
     * After filling and evicting the applied set, re-delivering an evicted UUID
     * should NOT cause double application because the sequence-based guard in
     * processSequenceBuffer prevents replay (old sequence is ignored).
     */
    @Test
    @Timeout(value = 30)
    void dedupGuardRemainsEffectiveViaSequenceFencing() throws Exception {
        int maxSize = 10;
        ReplicationManager manager = buildManager(maxSize, 2000);
        AtomicInteger applyCount = new AtomicInteger();
        manager.registerHandler("t3", (opId, payload) -> applyCount.incrementAndGet());
        manager.start();

        // Apply maxSize+5 ops to force eviction of early UUIDs
        List<UUID> earlyOpIds = new ArrayList<>();
        for (int i = 1; i <= maxSize + 5; i++) {
            UUID opId = simulateFollowerApply(manager, "t3", i, "p" + i);
            if (i <= 3) {
                earlyOpIds.add(opId);
            }
        }
        Thread.sleep(300);

        int beforeReplay = applyCount.get();

        // Re-deliver the 3 early operations (now potentially evicted from applied set)
        // Their sequences are all < nextExpected, so the sequence guard rejects them.
        for (int idx = 0; idx < earlyOpIds.size(); idx++) {
            simulateFollowerApplyWithId(manager, "t3", idx + 1, "p" + (idx + 1), earlyOpIds.get(idx));
        }
        Thread.sleep(300);

        int afterReplay = applyCount.get();
        assertEquals(beforeReplay, afterReplay,
                "No additional apply should occur for old sequences, even if UUID was evicted from applied set");

        manager.close();
    }

    // ── Helpers ──

    private ReplicationManager buildManager(int appliedSetMaxSize, int operationLogMaxSize) {
        return new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofMillis(500))
                        .dataDirectory(replicationDir)
                        .strictConsistency(false)
                        .appliedSetMaxSize(appliedSetMaxSize)
                        .operationLogMaxSize(operationLogMaxSize)
                        .build());
    }

    /**
     * Simulates a follower receiving a replication request by constructing and
     * dispatching a REPLICATION_REQUEST message directly to the manager.
     *
     * @return the UUID of the operation
     */
    private UUID simulateFollowerApply(ReplicationManager manager, String topic, long seq, String data) {
        UUID opId = UUID.randomUUID();
        return simulateFollowerApplyWithId(manager, topic, seq, data, opId);
    }

    private UUID simulateFollowerApplyWithId(ReplicationManager manager, String topic, long seq, String data,
            UUID opId) {
        var payload = new dev.nishisan.utils.ngrid.common.ReplicationPayload(opId, seq, 1L, topic, data);
        var msg = dev.nishisan.utils.ngrid.common.ClusterMessage.request(
                MessageType.REPLICATION_REQUEST,
                topic,
                NodeId.of("node-leader"),
                NodeId.of("node-leader"),
                payload);
        manager.onMessage(msg);
        return opId;
    }

    // ── Fake Transport ──

    private static final class FakeTransport implements Transport {

        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final java.util.concurrent.ConcurrentHashMap<NodeId, Boolean> connected = new java.util.concurrent.ConcurrentHashMap<>();
        // Captures sent ACK messages so tests can drain them to the manager
        private final java.util.concurrent.ConcurrentLinkedQueue<ClusterMessage> sentAcks = new java.util.concurrent.ConcurrentLinkedQueue<>();

        FakeTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo peer : peers) {
                connected.put(peer.nodeId(), true);
            }
        }

        /** Drains ACK messages back into the manager (simulates follower ACK). */
        void drainAcksTo(ReplicationManager manager) {
            ClusterMessage msg;
            while ((msg = sentAcks.poll()) != null) {
                manager.onMessage(msg);
            }
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            for (TransportListener l : listeners)
                l.onPeerConnected(peer);
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>(peers);
            all.add(local);
            return all;
        }

        @Override
        public void addListener(TransportListener l) {
            listeners.add(l);
        }

        @Override
        public void removeListener(TransportListener l) {
            listeners.remove(l);
        }

        @Override
        public void broadcast(ClusterMessage m) {
        }

        @Override
        public void send(ClusterMessage message) {
            // Intercept REPLICATION_ACK messages so drainAcksTo can replay them
            if (message.type() == MessageType.REPLICATION_ACK) {
                sentAcks.add(message);
            }
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public boolean isConnected(NodeId n) {
            return Boolean.TRUE.equals(connected.get(n));
        }

        @Override
        public boolean isReachable(NodeId n) {
            return isConnected(n);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() {
        }
    }
}
