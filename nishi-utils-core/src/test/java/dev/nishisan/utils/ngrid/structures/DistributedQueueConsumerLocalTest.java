package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
import dev.nishisan.utils.ngrid.replication.ReplicationConfig;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.queue.NQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DistributedQueueConsumerLocalTest {

    @TempDir
    Path tempDir;

    @Test
    void openConsumerShouldKeepIndependentOffsetsAndSupportSeek() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 0);
        InMemoryTransport transport = new InMemoryTransport(local);
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                Executors.newSingleThreadScheduledExecutor());
        coordinator.start();

        Path replicationDir = Files.createDirectories(tempDir.resolve("replication"));
        ReplicationManager replicationManager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofSeconds(2))
                        .dataDirectory(replicationDir)
                        .build());
        replicationManager.start();

        NQueue.Options options = NQueue.Options.defaults()
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                .withRetentionTime(Duration.ofHours(1));
        QueueClusterService<String> service = new QueueClusterService<>(
                Files.createDirectories(tempDir.resolve("queues")),
                "orders",
                replicationManager,
                options,
                1,
                null);

        try {
            service.queue().offer("a");
            service.queue().offer("b");
            service.queue().offer("c");

            DistributedQueue<String> queue = new DistributedQueue<>(transport, coordinator, service, "orders");

            try {
                DistributedQueueConsumer<String> consumer1 = queue.openConsumer("group-a", "consumer-1");
                DistributedQueueConsumer<String> consumer1Again = queue.openConsumer("group-a", "consumer-1");
                DistributedQueueConsumer<String> consumer2 = queue.openConsumer("group-a", "consumer-2");

                assertEquals(0L, consumer1.position());
                assertEquals(Optional.of("a"), consumer1.peek());
                assertEquals(0L, consumer1.position(), "peek must not advance the cursor");

                assertEquals(Optional.of("a"), consumer1.poll());
                assertEquals(1L, consumer1.position());

                assertEquals(1L, consumer1Again.position(), "Same logical consumer must reuse persisted cursor");
                assertEquals(Optional.of("b"), consumer1Again.poll());
                assertEquals(2L, consumer1Again.position());

                assertEquals(Optional.of("a"), consumer2.poll(), "Different logical consumer must keep its own cursor");
                assertEquals(1L, consumer2.position());

                consumer1Again.seek(0L);
                assertEquals(0L, consumer1Again.position());
                assertEquals(Optional.of("a"), consumer1Again.poll(), "seek should enable replay from an earlier offset");
            } finally {
                queue.close();
            }
        } finally {
            replicationManager.close();
            coordinator.close();
            transport.close();
        }
    }

    @Test
    void logicalConsumerShouldRejectDeleteOnConsumeQueues() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", 0);
        InMemoryTransport transport = new InMemoryTransport(local);
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                Executors.newSingleThreadScheduledExecutor());
        coordinator.start();

        Path replicationDir = Files.createDirectories(tempDir.resolve("replication-delete"));
        ReplicationManager replicationManager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofSeconds(2))
                        .dataDirectory(replicationDir)
                        .build());
        replicationManager.start();

        QueueClusterService<String> service = new QueueClusterService<>(
                Files.createDirectories(tempDir.resolve("queues-delete")),
                "jobs",
                replicationManager,
                NQueue.Options.defaults(),
                1,
                null);

        try {
            service.queue().offer("job-1");
            DistributedQueue<String> queue = new DistributedQueue<>(transport, coordinator, service, "jobs");
            try {
                DistributedQueueConsumer<String> consumer = queue.openConsumer("group-a", "worker-1");
                assertThrows(IllegalStateException.class, consumer::poll);
                assertEquals(Optional.of("job-1"), queue.poll(),
                        "Leader-local queue-style poll remains available for destructive consumption");
            } finally {
                queue.close();
            }
        } finally {
            replicationManager.close();
            coordinator.close();
            transport.close();
        }
    }

    private static final class InMemoryTransport implements Transport {
        private final NodeInfo local;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();

        private InMemoryTransport(NodeInfo local) {
            this.local = local;
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
        }

        @Override
        public void send(ClusterMessage message) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
            future.completeExceptionally(new UnsupportedOperationException("sendAndAwait not supported in this test"));
            return future;
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
        }

        @Override
        public void close() throws IOException {
            listeners.clear();
        }
    }
}
