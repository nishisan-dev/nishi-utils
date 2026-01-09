package dev.nishisan.utils.ngrid.metrics;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.LeaderScorePayload;
import dev.nishisan.utils.ngrid.common.LeaderSuggestionPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.stats.StatsUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderReelectionServiceTest {

    @Test
    void leaderReelectionSuggestsNodeWithHigherIngressRate() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 0);

        FakeTransport transport = new FakeTransport(local, List.of(peer));
        ScheduledExecutorService coordinatorScheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService metricsScheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), coordinatorScheduler);
            coordinator.start();
            transport.simulatePeerConnected(peer);
            assertTrue(coordinator.isLeader(), "Local node should be leader for this test");

            StatsUtils stats = new StatsUtils();
            LeaderReelectionService service = new LeaderReelectionService(
                    transport,
                    coordinator,
                    stats,
                    metricsScheduler,
                    Duration.ofMillis(100),
                    Duration.ZERO,
                    Duration.ofSeconds(2),
                    1.0
            );
            service.start();

            stats.notifyHitCounter(NGridMetrics.ingressWrite(local.nodeId()));
            Thread.sleep(120);
            stats.notifyHitCounter(NGridMetrics.ingressWrite(local.nodeId()));

            LeaderScorePayload scorePayload = new LeaderScorePayload(peer.nodeId(), 100.0, System.currentTimeMillis());
            ClusterMessage scoreMessage = ClusterMessage.request(MessageType.LEADER_SCORE, "leader-score", peer.nodeId(), null, scorePayload);
            transport.dispatch(scoreMessage);

            ClusterMessage suggestion = transport.awaitBroadcast(MessageType.LEADER_SUGGESTION, 2, TimeUnit.SECONDS);
            assertNotNull(suggestion, "Expected leader suggestion to be broadcast");
            LeaderSuggestionPayload suggestionPayload = suggestion.payload(LeaderSuggestionPayload.class);
            assertEquals(peer.nodeId(), suggestionPayload.leaderId());

            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                if (peer.nodeId().equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))) {
                    break;
                }
                Thread.sleep(50);
            }
            assertEquals(peer.nodeId(), coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));

            service.close();
            coordinator.close();
        } finally {
            coordinatorScheduler.shutdownNow();
            metricsScheduler.shutdownNow();
            transport.close();
        }
    }

    private static final class FakeTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final LinkedBlockingQueue<ClusterMessage> broadcasts = new LinkedBlockingQueue<>();

        private FakeTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
        }

        @Override
        public void start() {
            // no-op
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
            broadcasts.add(message);
            listeners.forEach(listener -> listener.onMessage(message));
        }

        @Override
        public void send(ClusterMessage message) {
            listeners.forEach(listener -> listener.onMessage(message));
        }

        @Override
        public java.util.concurrent.CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            java.util.concurrent.CompletableFuture<ClusterMessage> f = new java.util.concurrent.CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("Not used in this test"));
            return f;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return true;
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return true;
        }

        @Override
        public void addPeer(NodeInfo peer) {
            // no-op
        }

        void simulatePeerConnected(NodeInfo peer) {
            listeners.forEach(listener -> listener.onPeerConnected(peer));
        }

        void dispatch(ClusterMessage message) {
            listeners.forEach(listener -> listener.onMessage(message));
        }

        ClusterMessage awaitBroadcast(MessageType type, long timeout, TimeUnit unit) throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (System.nanoTime() < deadline) {
                ClusterMessage msg = broadcasts.poll(50, TimeUnit.MILLISECONDS);
                if (msg == null) {
                    continue;
                }
                if (msg.type() == type) {
                    return msg;
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
