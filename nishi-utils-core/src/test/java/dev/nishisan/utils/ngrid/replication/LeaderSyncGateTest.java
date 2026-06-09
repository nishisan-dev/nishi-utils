package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
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
        // On promotion the RELAY_STREAM failover drain-gate holds writes (leaderSyncing) until the
        // promoted node's relay backlog is fully applied. The apply consumers are what release the
        // gate, so this manager is intentionally NOT started: the gate stays held and the write-gate
        // rejection in replicate() is deterministic (no race with a drained empty relay).
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

        manager.onLeaderChanged(local.nodeId());

        assertTrue(manager.isLeaderSyncing(), "promoted leader holds the drain-gate until its relay drains");
        assertThrows(LeaderSyncingException.class,
                () -> manager.replicate(TOPIC, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "writes must be rejected while the leader is syncing");
    }

    @Test
    @Timeout(30)
    void leaderSyncingClearsWhenRelayDrainsOnLoneLeader() throws Exception {
        // Single node, NO peers and an EMPTY relay. In RELAY_STREAM a promoted node holds the failover
        // drain-gate until its relay backlog is fully applied; with nothing to drain it is the
        // most-advanced replica, so the per-topic apply consumer releases the gate (it does NOT pull a
        // snapshot to lead). joinPeerDiscoveryWindow=0 disables the lone-boot hold so the release is
        // immediate (no transient peer-discovery window to wait out).
        NodeInfo local = new NodeInfo(NodeId.of("solo-node"), "127.0.0.1", 0);
        transport = new FakeTransport(local, List.of());
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        awaitLeadership();

        manager = newManagerNoDiscoveryWindow();
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();

        manager.onLeaderChanged(local.nodeId());

        // The apply consumer releases the gate once the (empty) relay is fully drained.
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && manager.isLeaderSyncing()) {
            Thread.sleep(20);
        }
        assertFalse(manager.isLeaderSyncing(),
                "a lone leader with a drained relay must release the write gate, not stay stuck");
        assertDoesNotThrow(() -> manager.replicate(TOPIC, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "a leader with nothing to drain should accept writes once the gate releases");
    }

    @Test
    @Timeout(30)
    void staleSyncFromBehindPeerClearsLeaderSyncing() throws Exception {
        // Local is a follower (lower NodeId); the peer is the active elected leader, so when we later
        // simulate promotion, resolveSyncSource finds the peer and leaderSyncing stays true.
        NodeInfo local = new NodeInfo(NodeId.of("aaa-node"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        transport = new FakeTransport(local, List.of(peer));
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(peer); // synchronously marks the peer active

        manager = newManager();
        manager.registerHandler(TOPIC, (opId, payload) -> {
        });
        manager.start();

        // Advance local applied state to sequence 5 via a normal (non-stale) snapshot.
        manager.onMessage(ClusterMessage.request(MessageType.SYNC_RESPONSE, TOPIC,
                peer.nodeId(), local.nodeId(), new SyncResponsePayload(TOPIC, 5L, 0, false, "snap")));
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && manager.getLastAppliedSequence() < 5L) {
            Thread.sleep(20);
        }
        assertEquals(5L, manager.getLastAppliedSequence(), "initial snapshot should advance applied sequence");
        Thread.sleep(200); // nextExpected is set just after lastAppliedSequence under the lock

        // Simulate promotion: leaderSyncing becomes true with the active peer as sync source.
        manager.onLeaderChanged(local.nodeId());
        assertTrue(manager.isLeaderSyncing(), "promoted leader with an active peer should be syncing");

        // A behind peer answers with an OLDER snapshot (seq 3 < applied 5): the stale-sync path must
        // release the leader-sync guard, otherwise the write gate would reject writes forever even
        // though this leader already holds the newer state.
        manager.onMessage(ClusterMessage.request(MessageType.SYNC_RESPONSE, TOPIC,
                peer.nodeId(), local.nodeId(), new SyncResponsePayload(TOPIC, 3L, 0, false, "old")));

        long clearDeadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < clearDeadline && manager.isLeaderSyncing()) {
            Thread.sleep(20);
        }
        assertFalse(manager.isLeaderSyncing(),
                "stale sync from a behind peer must clear leaderSyncing, not block writes forever");
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

    private ReplicationManager newManagerNoDiscoveryWindow() {
        return new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofMillis(500))
                        .dataDirectory(replicationDir)
                        .strictConsistency(false)
                        .joinPeerDiscoveryWindow(Duration.ZERO)
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
