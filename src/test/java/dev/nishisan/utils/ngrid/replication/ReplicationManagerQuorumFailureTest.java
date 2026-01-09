package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationManagerQuorumFailureTest {

    @Test
    void replicateShouldTimeoutWhenPeerDisconnectsBeforeAck() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", 0);
        NodeInfo peer = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 0);

        FakeTransport transport = new FakeTransport(local, List.of(peer));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
            coordinator.start();

            // Simulate peer connection so coordinator sees 2 active members and local becomes leader.
            transport.simulatePeerConnected(peer);
            assertTrue(coordinator.isLeader(), "Local node should be leader for this test");

            ReplicationManager manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.of(2, java.time.Duration.ofMillis(500)));
            manager.registerHandler("topic", (operationId, payload) -> {
                // Local apply ok.
            });
            manager.start();

            CompletableFuture<ReplicationResult> future = manager.replicate("topic", "payload");

            // Drop peer before any ACK is delivered.
            transport.simulatePeerDisconnected(peer.nodeId());

            ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
            assertInstanceOf(java.util.concurrent.TimeoutException.class, ex.getCause());

            manager.close();
            coordinator.close();
        } finally {
            scheduler.shutdownNow();
            transport.close();
        }
    }

    private static final class FakeTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final java.util.concurrent.CopyOnWriteArraySet<TransportListener> listenersSet = new java.util.concurrent.CopyOnWriteArraySet<>();
        private final java.util.concurrent.ConcurrentHashMap<NodeId, Boolean> connected = new java.util.concurrent.ConcurrentHashMap<>();

        private FakeTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo peer : peers) {
                connected.put(peer.nodeId(), true);
            }
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
            listenersSet.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listenersSet.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
            // no-op
        }

        @Override
        public void send(ClusterMessage message) {
            // Drop REPLICATION_REQUEST messages: peer never acks in this fake.
            if (message.type() == MessageType.REPLICATION_REQUEST) {
                return;
            }
            // Drop everything else.
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("Not used in this test"));
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
            // no-op
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            for (TransportListener l : listenersSet) {
                l.onPeerConnected(peer);
            }
        }

        void simulatePeerDisconnected(NodeId peerId) {
            connected.put(peerId, false);
            for (TransportListener l : listenersSet) {
                l.onPeerDisconnected(peerId);
            }
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
