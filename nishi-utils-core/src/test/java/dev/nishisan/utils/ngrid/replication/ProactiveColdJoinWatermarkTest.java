package dev.nishisan.utils.ngrid.replication;

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
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * Regression for #131 (follow-up of #129 3a): the proactive cold-join sync was starved when the
 * {@code ReplicationManager} is assembled manually (without {@code NGridNode}), because the leader
 * high-watermark supplier defaulted to {@code -1} (it was wired only by {@code NGridNode}), so the
 * follower's gate {@code leaderWatermark <= 0} always bailed — a fresh follower never converged
 * against a quiescent leader.
 *
 * <p>The fix: (1) {@code ReplicationManager.start()} wires the supplier itself, so heartbeats carry a
 * real watermark in any assembly; (2) the proactive cold-join no longer hard-depends on a positive
 * watermark — an unknown watermark is treated as "sync to be safe".
 */
class ProactiveColdJoinWatermarkTest {

    private static final String TOPIC = "cardinal-state";
    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("cold-join-watermark-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    /** Fix (1): a manually-assembled leader's heartbeats carry the real watermark, not the -1 default. */
    @Test
    void leaderHeartbeatCarriesRealWatermarkAfterManualStart() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo peerNode = new NodeInfo(NodeId.of("aaa-peer"), "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(leaderNode, List.of(peerNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(150), Duration.ofSeconds(10), 1), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(peerNode);
        assertTrue(coordinator.isLeader(), "zzz-leader must be the elected leader");

        // Manual assembly — NO external setLeaderHighWatermarkSupplier (the #131 footgun).
        ReplicationManager leader = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1).dataDirectory(tempDir).build());
        leader.registerHandler(TOPIC, (operationId, payload) -> {
        });
        leader.start();
        try {
            for (int i = 0; i < 5; i++) {
                leader.replicate(TOPIC, ("d-" + i).getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
            }
            long expected = leader.getGlobalSequence();

            // A heartbeat emitted after the writes must carry the real watermark (== globalSequence),
            // not -1. Without the fix the supplier stays the coordinator default (-1).
            boolean sawRealWatermark = awaitHeartbeatWatermark(transport, expected, 3_000);
            assertTrue(sawRealWatermark,
                    "leader heartbeat must carry watermark " + expected + " (supplier wired by start())");
        } finally {
            leader.close();
            coordinator.close();
        }
    }

    /** Fix (2): a fresh follower fires the proactive cold-join sync even with an unknown watermark. */
    @Test
    void proactiveColdJoinSyncFiresWhenLeaderWatermarkUnknown() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(150), Duration.ofSeconds(10), 1), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(leaderNode);
        // No heartbeat with a watermark is delivered, so getTrackedLeaderHighWatermark() stays -1.

        ReplicationManager follower = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(2)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                        .build());
        follower.registerHandler(TOPIC, (operationId, payload) -> {
        });
        follower.start();
        try {
            // checkLagAndSync runs every 2s; with the fix, the cold follower proactively requests a sync
            // despite the unknown (-1) watermark. Without the fix, the gate bails forever.
            boolean syncRequested = awaitMessageType(transport, MessageType.SYNC_REQUEST, 6_000);
            assertTrue(syncRequested,
                    "fresh follower must proactively request a sync against a leader even with unknown watermark");
        } finally {
            follower.close();
            coordinator.close();
        }
    }

    private boolean awaitHeartbeatWatermark(RecordingTransport transport, long expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (ClusterMessage m : transport.getSentMessages()) {
                if (m.type() == MessageType.HEARTBEAT
                        && m.payload(HeartbeatPayload.class).leaderHighWatermark() == expected) {
                    return true;
                }
            }
            Thread.sleep(25);
        }
        return false;
    }

    private boolean awaitMessageType(RecordingTransport transport, MessageType type, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (ClusterMessage m : transport.getSentMessages()) {
                if (m.type() == type) {
                    return true;
                }
            }
            Thread.sleep(25);
        }
        return false;
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

        List<ClusterMessage> getSentMessages() {
            return List.copyOf(sent);
        }
    }
}
