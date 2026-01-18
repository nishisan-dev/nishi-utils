package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.QueueConfig;
import dev.nishisan.utils.ngrid.structures.QueueConfig.RetentionPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static dev.nishisan.utils.ngrid.ClusterTestUtils.awaitClusterConsensus;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test demonstrating multiple distributed queues in a single NGrid
 * cluster.
 * 
 * Tests the following scenario:
 * - 3-node cluster (seed, producer, consumer)
 * - 3 different queues: "orders", "events", "logs"
 * - Producer enqueues messages to each queue
 * - Consumer polls messages from each queue
 * - Validates queue isolation (messages don't leak between queues)
 */
@Disabled("Replication ordering bug - OFFER/POLL operations arrive out of order at replicas")
class MultiQueueClusterIntegrationTest {

    @TempDir
    Path tempDir;

    private NGridNode seedNode;
    private NGridNode producerNode;
    private NGridNode consumerNode;

    private int seedPort;
    private int producerPort;
    private int consumerPort;

    // Test message types - using records for automatic equals/hashCode
    // implementation
    record Order(String orderId, double amount) implements Serializable {
    }

    record Event(String eventType, long timestamp) implements Serializable {
    }

    record LogEntry(String level, String message) implements Serializable {
    }

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        // Allocate free ports
        seedPort = allocateFreeLocalPort();
        producerPort = allocateFreeLocalPort(Set.of(seedPort));
        consumerPort = allocateFreeLocalPort(Set.of(seedPort, producerPort));

        // Create seed node with 3 queues
        NodeInfo seedInfo = new NodeInfo(NodeId.of("seed"), "127.0.0.1", seedPort);

        NGridConfig seedConfig = NGridConfig.builder(seedInfo)
                .dataDirectory(tempDir.resolve("seed"))
                .addQueue(QueueConfig.builder("orders")
                        .retention(RetentionPolicy.timeBased(Duration.ofHours(24)))
                        .build())
                .addQueue(QueueConfig.builder("events")
                        .retention(RetentionPolicy.timeBased(Duration.ofHours(1)))
                        .build())
                .addQueue(QueueConfig.builder("logs")
                        .retention(RetentionPolicy.timeBased(Duration.ofDays(7)))
                        .build())
                .replicationFactor(2)
                .strictConsistency(false)
                .build();

        seedNode = new NGridNode(seedConfig);
        seedNode.start();

        // Create producer node with same 3 queues
        NodeInfo producerInfo = new NodeInfo(NodeId.of("producer"), "127.0.0.1", producerPort);

        NGridConfig producerConfig = NGridConfig.builder(producerInfo)
                .dataDirectory(tempDir.resolve("producer"))
                .addPeer(seedInfo)
                .addQueue(QueueConfig.builder("orders")
                        .retention(RetentionPolicy.timeBased(Duration.ofHours(24)))
                        .build())
                .addQueue(QueueConfig.builder("events")
                        .retention(RetentionPolicy.timeBased(Duration.ofHours(1)))
                        .build())
                .addQueue(QueueConfig.builder("logs")
                        .retention(RetentionPolicy.timeBased(Duration.ofDays(7)))
                        .build())
                .replicationFactor(2)
                .strictConsistency(false)
                .build();

        producerNode = new NGridNode(producerConfig);
        producerNode.start();

        // Wait for seed + producer to form cluster
        awaitClusterConsensus(seedNode, producerNode);

        // Create consumer node with same 3 queues
        NodeInfo consumerInfo = new NodeInfo(NodeId.of("consumer"), "127.0.0.1", consumerPort);

        NGridConfig consumerConfig = NGridConfig.builder(consumerInfo)
                .dataDirectory(tempDir.resolve("consumer"))
                .addPeer(seedInfo)
                .addQueue(QueueConfig.builder("orders")
                        .retention(RetentionPolicy.timeBased(Duration.ofHours(24)))
                        .build())
                .addQueue(QueueConfig.builder("events")
                        .retention(RetentionPolicy.timeBased(Duration.ofHours(1)))
                        .build())
                .addQueue(QueueConfig.builder("logs")
                        .retention(RetentionPolicy.timeBased(Duration.ofDays(7)))
                        .build())
                .replicationFactor(2)
                .strictConsistency(false)
                .build();

        consumerNode = new NGridNode(consumerConfig);
        consumerNode.start();

        // Wait for all nodes to form cluster
        awaitClusterConsensus(seedNode, producerNode, consumerNode);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (consumerNode != null)
            consumerNode.close();
        if (producerNode != null)
            producerNode.close();
        if (seedNode != null)
            seedNode.close();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void shouldHandleMultipleQueuesIndependently() {
        // === PHASE 1: Producer enqueues messages to each queue ===
        DistributedQueue<Order> ordersProducer = producerNode.getQueue("orders", Order.class);
        DistributedQueue<Event> eventsProducer = producerNode.getQueue("events", Event.class);
        DistributedQueue<LogEntry> logsProducer = producerNode.getQueue("logs", LogEntry.class);

        // Enqueue orders
        ordersProducer.offer(new Order("order-001", 99.99));
        ordersProducer.offer(new Order("order-002", 149.99));

        // Enqueue events
        eventsProducer.offer(new Event("user.login", System.currentTimeMillis()));
        eventsProducer.offer(new Event("user.logout", System.currentTimeMillis()));
        eventsProducer.offer(new Event("order.created", System.currentTimeMillis()));

        // Enqueue logs
        logsProducer.offer(new LogEntry("INFO", "System started"));
        logsProducer.offer(new LogEntry("WARN", "High memory usage"));

        // === PHASE 2: Consumer polls messages from each queue ===
        DistributedQueue<Order> ordersConsumer = consumerNode.getQueue("orders", Order.class);
        DistributedQueue<Event> eventsConsumer = consumerNode.getQueue("events", Event.class);
        DistributedQueue<LogEntry> logsConsumer = consumerNode.getQueue("logs", LogEntry.class);

        // Consume orders
        Optional<Order> order1 = ordersConsumer.poll();
        Optional<Order> order2 = ordersConsumer.poll();
        Optional<Order> order3 = ordersConsumer.poll(); // Should be empty

        assertTrue(order1.isPresent());
        assertTrue(order2.isPresent());
        assertFalse(order3.isPresent());

        assertEquals("order-001", order1.get().orderId);
        assertEquals(99.99, order1.get().amount, 0.01);
        assertEquals("order-002", order2.get().orderId);

        // Consume events
        Optional<Event> event1 = eventsConsumer.poll();
        Optional<Event> event2 = eventsConsumer.poll();
        Optional<Event> event3 = eventsConsumer.poll();
        Optional<Event> event4 = eventsConsumer.poll(); // Should be empty

        assertTrue(event1.isPresent());
        assertTrue(event2.isPresent());
        assertTrue(event3.isPresent());
        assertFalse(event4.isPresent());

        assertEquals("user.login", event1.get().eventType);
        assertEquals("user.logout", event2.get().eventType);
        assertEquals("order.created", event3.get().eventType);

        // Consume logs
        Optional<LogEntry> log1 = logsConsumer.poll();
        Optional<LogEntry> log2 = logsConsumer.poll();
        Optional<LogEntry> log3 = logsConsumer.poll(); // Should be empty

        assertTrue(log1.isPresent());
        assertTrue(log2.isPresent());
        assertFalse(log3.isPresent());

        assertEquals("INFO", log1.get().level);
        assertEquals("System started", log1.get().message);
        assertEquals("WARN", log2.get().level);

        // === PHASE 3: Verify queue isolation ===
        // Each queue should be completely independent
        // Verify all queues are now empty
        assertFalse(ordersConsumer.poll().isPresent());
        assertFalse(eventsConsumer.poll().isPresent());
        assertFalse(logsConsumer.poll().isPresent());
    }

    private static int allocateFreeLocalPort() throws IOException {
        return allocateFreeLocalPort(Set.of());
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }
}
