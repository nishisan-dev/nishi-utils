package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.FollowerProgressPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * #129 (3b): leader-pause-on-join. The leader pauses production (rejects writes) while a not-caught-up
 * follower joins, then resumes once the follower reports it has caught up — the join-path mirror of the
 * failover drain-gate. Bounded by a timeout and released on disconnect (covered by the disconnect path).
 */
class LeaderPauseOnJoinTest {

    private static final String TOPIC = "join-topic";
    private static final NodeInfo LEADER = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
    private static final NodeInfo FOLLOWER = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("leader-pause-join-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void gatesWritesWhileBehindFollowerJoinsThenResumes() throws Exception {
        RecordingTransport transport = new RecordingTransport(LEADER, List.of(FOLLOWER));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                scheduler);
        coordinator.start();
        transport.simulatePeerConnected(FOLLOWER);
        assertTrue(coordinator.isLeader(), "zzz-leader must be the elected leader");

        ReplicationManager leader = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .dataDirectory(tempDir)
                        .leaderPauseOnJoin(true)
                        .joinQuiesceMaxDuration(Duration.ofSeconds(30))
                        .build());
        leader.registerHandler(TOPIC, (operationId, payload) -> {
        });
        leader.start();

        try {
            // Produce a backlog (quorum 1 → the leader's own ack commits immediately).
            for (int i = 0; i < 10; i++) {
                leader.replicate(TOPIC, ("d-" + i).getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
            }
            long leaderSeq = leader.getGlobalSequence();

            // A follower joins with unknown/behind progress → the leader quiesces.
            leader.onMembershipChanged();
            assertTrue(leader.isJoinQuiescing(), "leader must quiesce when a behind follower joins");

            // Writes are rejected while quiescing (callers retry, like the failover gate).
            assertThrows(LeaderSyncingException.class,
                    () -> leader.replicate(TOPIC, "blocked".getBytes(StandardCharsets.UTF_8)),
                    "writes must be rejected while the leader pauses for a joining follower");

            // The follower reports it has caught up → the gate releases.
            ClusterMessage progress = ClusterMessage.request(MessageType.FOLLOWER_PROGRESS, "follower-progress",
                    FOLLOWER.nodeId(), LEADER.nodeId(), new FollowerProgressPayload(leaderSeq, 1L));
            transport.deliverToListeners(progress);
            assertFalse(leader.isJoinQuiescing(), "gate must release once the joining follower catches up");

            // Writes succeed again.
            leader.replicate(TOPIC, "ok".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        } finally {
            leader.close();
            coordinator.close();
        }
    }

    /** Minimal recording transport sufficient to drive a real ClusterCoordinator + ReplicationManager. */
    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        private RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            peers.forEach(p -> connected.put(p.nodeId(), true));
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
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }

        @Override
        public void addListener(TransportListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public void send(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            sent.add(message);
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return Boolean.TRUE.equals(connected.get(nodeId));
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return isConnected(nodeId);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() throws IOException {
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            listeners.forEach(l -> l.onPeerConnected(peer));
        }

        void deliverToListeners(ClusterMessage message) {
            listeners.forEach(l -> l.onMessage(message));
        }
    }
}
