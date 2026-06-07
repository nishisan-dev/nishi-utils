package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the leader-sync write gate and the clear-on-no-source fix for issue #123:
 *
 * <ul>
 *   <li>While a freshly promoted leader is still syncing ({@code isLeaderSyncing()==true}),
 *       {@code replicate()} rejects writes with {@link LeaderSyncingException} — defense in depth
 *       against advancing from stale state.</li>
 *   <li>When there is no reachable peer to sync from, {@code attemptLeaderSync} clears
 *       {@code leaderSyncing} instead of leaving the consumer blocked forever.</li>
 * </ul>
 */
class LeaderSyncGateTest {

    private static final String TOPIC = "t";

    @TempDir
    Path replicationDir;

    private ScheduledExecutorService scheduler;
    private FakeTransport transport;
    private ClusterCoordinator coordinator;
    private ReplicationManager manager;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (manager != null) {
            manager.close();
        }
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
    void replicateRejectsWritesWhileLeaderSyncing() throws Exception {
        // Local leader WITH an active peer → resolveSyncSource finds the peer, so leaderSyncing
        // stays true (a real catch-up is in flight).
        NodeInfo local = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("aaa-peer"), "127.0.0.1", 0);
        transport = new FakeTransport(local, List.of(peer));
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(peer);
        awaitLeadership();

        manager = newManager();
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();

        manager.onLeaderChanged(local.nodeId());

        assertTrue(manager.isLeaderSyncing(), "leader with a reachable sync source should be syncing");
        assertThrows(LeaderSyncingException.class, () -> manager.replicate(TOPIC, "x"),
                "writes must be rejected while the leader is syncing");
    }

    @Test
    @Timeout(30)
    void leaderSyncingClearsWhenNoReachableSyncSource() throws Exception {
        // Single node, NO peers → resolveSyncSource returns null. The clear-on-no-source fix must
        // release leaderSyncing so the node can lead immediately (it is the most-advanced replica).
        NodeInfo local = new NodeInfo(NodeId.of("solo-node"), "127.0.0.1", 0);
        transport = new FakeTransport(local, List.of());
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        awaitLeadership();

        manager = newManager();
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();

        manager.onLeaderChanged(local.nodeId());

        assertFalse(manager.isLeaderSyncing(),
                "with no reachable sync source the leaderSyncing flag must be cleared, not stuck");
        assertDoesNotThrow(() -> manager.replicate(TOPIC, "x"),
                "a leader with nothing to sync should accept writes immediately");
    }

    // ── Helpers ──

    private ReplicationManager newManager() {
        return new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofMillis(500))
                        .dataDirectory(replicationDir)
                        .strictConsistency(false)
                        .build());
    }

    private void awaitLeadership() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && !coordinator.isLeader()) {
            Thread.sleep(20);
        }
        assertTrue(coordinator.isLeader(), "local node should have been elected leader");
    }

    // ── Minimal fake transport ──

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
