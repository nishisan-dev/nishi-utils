package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for Queue persistence and consistency after node restart.
 * These tests validate that queue data and offsets survive node restarts.
 */
class QueueRestartConsistencyTest {

    /**
     * Tests that queue data persists after node shutdown and restart.
     * This validates the basic persistence mechanism.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQueueDataPersistsAcrossRestart() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo info = new NodeInfo(NodeId.of("restart-test"), "127.0.0.1", port);
        Path baseDir = Files.createTempDirectory("ngrid-restart-test");
        Path dir = Files.createDirectories(baseDir.resolve("node"));

        // Phase 1: Create node, write data, close
        try (NGridNode node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(dir)
                .replicationFactor(1)
                .build())) {

            node.start();
            DistributedQueue<String> queue = node.getQueue("persistent-queue", String.class);

            // Write items
            for (int i = 0; i < 100; i++) {
                queue.offer("item-" + i);
            }

            // Verify first item
            Optional<String> firstItem = queue.peek();
            assertTrue(firstItem.isPresent(), "First item should exist before restart");
            assertEquals("item-0", firstItem.orElse(null));
        }

        // Phase 2: Restart node with same directory, verify data
        try (NGridNode node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(dir)
                .replicationFactor(1)
                .build())) {

            node.start();
            DistributedQueue<String> queue = node.getQueue("persistent-queue", String.class);

            // Verify first item still exists
            Optional<String> firstItem = queue.peek();
            assertTrue(firstItem.isPresent(),
                    "Queue data should persist after restart - first item should exist");
            assertEquals("item-0", firstItem.orElse(null),
                    "First item should still be item-0 after restart");
        }
    }

    /**
     * Tests that queue with items consumed maintains state after restart.
     * The queue head should persist and be consumable after restart.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testPartiallyConsumedQueueRestart() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo info = new NodeInfo(NodeId.of("partial-restart"), "127.0.0.1", port);
        Path baseDir = Files.createTempDirectory("ngrid-partial-restart");
        Path dir = Files.createDirectories(baseDir.resolve("node"));

        // Phase 1: Create queue, write data, consume partial
        try (NGridNode node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(dir)
                .replicationFactor(1)
                .build())) {

            node.start();
            DistributedQueue<String> queue = node.getQueue("partial-queue", String.class);

            // Write 100 items
            for (int i = 0; i < 100; i++) {
                queue.offer("item-" + i);
            }

            // Consume 50 items (destructive poll)
            for (int i = 0; i < 50; i++) {
                Optional<String> item = queue.poll();
                assertTrue(item.isPresent(), "Item " + i + " should be consumable");
            }
        }

        // Phase 2: Restart and verify remaining items
        try (NGridNode node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(dir)
                .replicationFactor(1)
                .build())) {

            node.start();
            DistributedQueue<String> queue = node.getQueue("partial-queue", String.class);

            // After consuming 50 items and restart, peek should return item-50
            Optional<String> nextItem = queue.peek();
            assertTrue(nextItem.isPresent(),
                    "Queue should have remaining items after restart");
            assertEquals("item-50", nextItem.orElse(null),
                    "After consuming 50 items and restart, peek should return item-50");
        }
    }

    /**
     * Tests that queue sequence state is preserved after restart.
     * New offers after restart should continue from the correct sequence.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSequenceStatePreservedAfterRestart() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo info = new NodeInfo(NodeId.of("sequence-restart"), "127.0.0.1", port);
        Path baseDir = Files.createTempDirectory("ngrid-sequence-restart");
        Path dir = Files.createDirectories(baseDir.resolve("node"));

        long sequenceBeforeRestart;

        // Phase 1: Create queue, write data, record sequence
        try (NGridNode node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(dir)
                .replicationFactor(1)
                .build())) {

            node.start();
            DistributedQueue<String> queue = node.getQueue("sequence-queue", String.class);

            for (int i = 0; i < 50; i++) {
                queue.offer("item-" + i);
            }

            sequenceBeforeRestart = node.replicationManager().getGlobalSequence();
            assertTrue(sequenceBeforeRestart >= 50,
                    "Sequence should be at least 50 after 50 offers");
        }

        // Phase 2: Restart and verify sequence continues
        try (NGridNode node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(dir)
                .replicationFactor(1)
                .build())) {

            node.start();
            DistributedQueue<String> queue = node.getQueue("sequence-queue", String.class);

            // Add more items
            for (int i = 50; i < 60; i++) {
                queue.offer("item-" + i);
            }

            long sequenceAfterRestart = node.replicationManager().getGlobalSequence();
            assertTrue(sequenceAfterRestart >= sequenceBeforeRestart + 10,
                    "Sequence should continue from where it left off. " +
                            "Before: " + sequenceBeforeRestart + ", After: " + sequenceAfterRestart);
        }
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
