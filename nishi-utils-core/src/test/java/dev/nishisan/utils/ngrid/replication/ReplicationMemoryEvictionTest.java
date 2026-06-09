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
     * Scenario 1: the applied dedup set never exceeds appliedSetMaxSize.
     *
     * Drives N + 100 committed operations through the leader path (replicate → quorum-1 async commit
     * in RELAY_STREAM), each of which records its operationId in the {@code applied} dedup set, and
     * validates that the set stays bounded by {@code trimApplied}.
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
            // RELAY_STREAM appends to the binlog synchronously; default encodePayload needs a byte[].
            manager.replicate("t1", ("payload-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .get(5, TimeUnit.SECONDS);
        }

        // Allow async leader-apply executor tasks to settle (applied.add happens in the apply callback)
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && manager.getAppliedSetSize() > maxSize) {
            Thread.sleep(100);
        }

        int size = manager.getAppliedSetSize();
        assertTrue(size <= maxSize,
                "applied set should be capped at " + maxSize + " but was " + size);

        manager.close();
    }

    /**
     * Scenario 2: the operation audit log does not exceed operationLogMaxSize after the scheduled
     * trimLog fires.
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

        // Replicate logMaxSize+50 operations on the leader path (RELAY_STREAM commits at quorum-1).
        int total = logMaxSize + 50;
        for (int i = 0; i < total; i++) {
            try {
                manager.replicate("t2", ("msg-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // best-effort; commit is async quorum-1
            }
        }

        // Poll for the scheduled trimLog (max(5000, 500*5) = 5000ms) to converge.
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

    // ── Fake Transport ──

    private static final class FakeTransport implements Transport {

        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final java.util.concurrent.ConcurrentHashMap<NodeId, Boolean> connected = new java.util.concurrent.ConcurrentHashMap<>();

        FakeTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo peer : peers) {
                connected.put(peer.nodeId(), true);
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
            // RELAY_STREAM: the leader does not push; followers pull. Nothing to capture here.
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
