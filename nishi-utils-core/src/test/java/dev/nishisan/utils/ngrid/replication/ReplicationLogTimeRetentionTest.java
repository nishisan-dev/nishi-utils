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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the temporal retention window for the leader-side resend log (op-log) introduced for
 * issue #122. Entries older than {@code replicationLogRetentionTime} are evicted, complementing the
 * count-based cap (whichever is reached first evicts). A resend for an evicted sequence is reported
 * as missing, which downstream feeds the existing gap-detection → snapshot fallback path.
 *
 * <p>Harness: a single local leader with {@code quorum=1}, {@code strictConsistency=false} and
 * {@code leaderLocalApply=false} so each {@code replicate(...)} commits and indexes SYNCHRONOUSLY on
 * the local ack — deterministic, no peer ACK required.</p>
 */
class ReplicationLogTimeRetentionTest {

    private static final String TOPIC = "t";

    @TempDir
    Path replicationDir;

    private CapturingTransport transport;
    private ClusterCoordinator coordinator;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        // Local node holds the highest NodeId so it is elected leader; a connected peer keeps the
        // cluster eligible for leadership. The peer never ACKs — quorum=1 commits on the local ack.
        NodeInfo local = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("aaa-peer"), "127.0.0.1", 0);

        transport = new CapturingTransport(local, List.of(peer));
        scheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(peer);
        assertTrue(coordinator.isLeader(), "Local node should be leader");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (coordinator != null) {
            coordinator.close();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (transport != null) {
            transport.close();
        }
    }

    @Test
    @Timeout(30)
    void agedEntriesAreEvictedByTimeWhileFreshOnesRemain() throws Exception {
        ReplicationManager manager = buildManager(10_000, Duration.ofMillis(150));
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();
        try {
            commit(manager, 4); // sequences 1..4
            assertEquals(4, manager.getReplicationLogSize(TOPIC), "all 4 entries should be indexed");

            Thread.sleep(300); // age the 4 entries past the 150ms window

            commit(manager, 1); // sequence 5 — triggers opportunistic head eviction of 1..4

            assertEquals(1, manager.getReplicationLogSize(TOPIC), "only the fresh entry should remain");
            assertTrue(manager.getReplicationLogTimeEvictedCount() >= 4,
                    "at least the 4 aged entries should have been time-evicted, was "
                            + manager.getReplicationLogTimeEvictedCount());
        } finally {
            manager.close();
        }
    }

    @Test
    @Timeout(30)
    void temporalRetentionDisabledByDefaultKeepsCountOnlyBehavior() throws Exception {
        // Duration.ZERO disables temporal eviction (the default).
        ReplicationManager manager = buildManager(10_000, Duration.ZERO);
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();
        try {
            commit(manager, 4);
            Thread.sleep(300);
            commit(manager, 1);

            assertEquals(5, manager.getReplicationLogSize(TOPIC), "no entry should be time-evicted when disabled");
            assertEquals(0, manager.getReplicationLogTimeEvictedCount(), "time-evicted counter must stay zero");
        } finally {
            manager.close();
        }
    }

    @Test
    @Timeout(30)
    void countCapStillEnforcedWhenTemporalRetentionEnabled() throws Exception {
        // Long temporal window (entries never age out during the test); count cap of 3 must win.
        ReplicationManager manager = buildManager(3, Duration.ofMinutes(30));
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();
        try {
            commit(manager, 8);

            assertEquals(3, manager.getReplicationLogSize(TOPIC), "count cap should bound the log to 3");
            assertEquals(0, manager.getReplicationLogTimeEvictedCount(),
                    "nothing should be time-evicted within the long window (count cap did the work)");
        } finally {
            manager.close();
        }
    }

    // ── Helpers ──

    private ReplicationManager buildManager(int retentionCount, Duration retentionTime) {
        return new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofMillis(500))
                        .dataDirectory(replicationDir)
                        .strictConsistency(false)
                        .leaderLocalApply(false)
                        .replicationLogRetention(retentionCount)
                        .replicationLogRetentionTime(retentionTime)
                        .build());
    }

    /** Commits {@code count} operations on the leader; each commits+indexes synchronously. */
    private void commit(ReplicationManager manager, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            // RELAY_STREAM appends each committed op to the durable binlog synchronously; the default
            // ReplicationHandler.encodePayload expects a byte[] wire payload.
            CompletableFuture<ReplicationResult> f = manager.replicate(TOPIC,
                    "data".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            f.get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    // ── Minimal transport: drives a real ClusterCoordinator + ReplicationManager ──

    private static final class CapturingTransport implements Transport {

        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final java.util.concurrent.ConcurrentHashMap<NodeId, Boolean> connected = new java.util.concurrent.ConcurrentHashMap<>();

        CapturingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo peer : peers) {
                connected.put(peer.nodeId(), true);
            }
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            for (TransportListener l : listeners) {
                l.onPeerConnected(peer);
            }
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
            // RELAY_STREAM: the leader does not push; nothing to capture here.
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
