package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.replication.ReplicationConfig;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MapClusterServiceConcurrencyTest {

    @Test
    void concurrentPutsAndRemovesShouldNotDeadlockAndShouldConverge() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 0);

        InMemoryTransport transport = new InMemoryTransport(local);
        ExecutorService scheduler = Executors.newSingleThreadExecutor();
        try {
            ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor());
            coordinator.start();

            // Single-node "cluster": local is always leader and quorum=1, so replicate completes immediately.
            ReplicationManager replicationManager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.of(1, java.time.Duration.ofSeconds(2)));
            replicationManager.start();

            MapClusterService<String, String> map = new MapClusterService<>(replicationManager, "map:test", null);

            int threads = 8;
            int opsPerThread = 500;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                int threadId = t;
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "k-" + (i % 50);
                        if ((i + threadId) % 3 == 0) {
                            map.remove(key);
                        } else {
                            map.put(key, "v-" + threadId + "-" + i);
                        }
                    }
                    done.countDown();
                    return null;
                });
            }

            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "Operations should finish without deadlock");
            pool.shutdownNow();

            // Post-condition: map is usable and returns either empty or a value for known keys.
            for (int i = 0; i < 50; i++) {
                Optional<String> value = map.get("k-" + i);
                value.ifPresent(v -> assertTrue(v.startsWith("v-")));
            }

            map.close();
            replicationManager.close();
            coordinator.close();
        } finally {
            scheduler.shutdownNow();
            transport.close();
        }
    }

    /**
     * Minimal in-memory transport for single-node tests. It never delivers messages anywhere
     * but provides the required plumbing for ClusterCoordinator/ReplicationManager.
     */
    private static final class InMemoryTransport implements dev.nishisan.utils.ngrid.cluster.transport.Transport {
        private final NodeInfo local;
        private final java.util.concurrent.CopyOnWriteArraySet<TransportListener> listeners = new java.util.concurrent.CopyOnWriteArraySet<>();

        private InMemoryTransport(NodeInfo local) {
            this.local = local;
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
            return List.of(local);
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
            // no-op for single node
        }

        @Override
        public void send(ClusterMessage message) {
            // no-op
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
            future.completeExceptionally(new UnsupportedOperationException("sendAndAwait not supported in this test transport"));
            return future;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return nodeId.equals(local.nodeId());
        }

        @Override
        public void close() throws IOException {
            listeners.clear();
        }
    }
}


