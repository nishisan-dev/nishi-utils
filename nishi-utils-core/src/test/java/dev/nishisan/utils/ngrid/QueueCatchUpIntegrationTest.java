package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD Tests for Queue-specific catch-up functionality.
 * These tests should FAIL until QueueClusterService implements:
 * - getSnapshotChunk()
 * - resetState()
 * - installSnapshot()
 */
class QueueCatchUpIntegrationTest {

    /**
     * Tests queue catch-up after a follower joins late with significant lag.
     * Leader offers 600 items (above 500 threshold), then follower joins.
     * Follower should synchronize via snapshot.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQueueCatchUpAfterSignificantLag() throws Exception {
        NodeInfo infoL = new NodeInfo(NodeId.of("queue-leader"), "localhost", 9801);
        NodeInfo infoF = new NodeInfo(NodeId.of("queue-follower"), "localhost", 9802);

        Path base = Files.createTempDirectory("ngrid-queue-catchup-test");
        Path dirL = base.resolve("leader");
        Path dirF = base.resolve("follower");

        // 1. Start Leader only initially
        try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                .queueDirectory(dirL)
                .replicationFactor(1) // allow writes with only leader
                .heartbeatInterval(Duration.ofMillis(200))
                .build())) {

            leader.start();
            DistributedQueue<String> queueL = leader.getQueue("catchup-queue", String.class);

            // 2. Perform 600 offers (Threshold is 500)
            for (int i = 0; i < 600; i++) {
                queueL.offer("item-" + i);
            }

            assertEquals(600, leader.replicationManager().getGlobalSequence(),
                    "Leader should have processed 600 operations");

            // 3. Start Follower (joins late)
            try (NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                    .addPeer(infoL)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .heartbeatInterval(Duration.ofMillis(200))
                    .build())) {

                follower.start();

                // Wait for cluster convergence
                long start = System.currentTimeMillis();
                while (follower.coordinator().getTrackedLeaderHighWatermark() < 600) {
                    Thread.sleep(100);
                    if (System.currentTimeMillis() - start > 10000) {
                        throw new IllegalStateException(
                                "Follower didn't see leader watermark. Current: "
                                        + follower.coordinator().getTrackedLeaderHighWatermark());
                    }
                }

                // 4. Wait for Catch-up (Snapshot sync)
                // ReplicationManager checks lag every 2 seconds.
                start = System.currentTimeMillis();
                DistributedQueue<String> queueF = follower.getQueue("catchup-queue", String.class);

                // Wait for follower to catch up
                while (follower.replicationManager().getLastAppliedSequence() < 600) {
                    Thread.sleep(200);
                    if (System.currentTimeMillis() - start > 20000) {
                        throw new IllegalStateException(
                                "Queue catch-up failed or too slow. Last applied seq: "
                                        + follower.replicationManager().getLastAppliedSequence());
                    }
                }

                // 5. Verify queue has items by peeking
                Optional<String> firstItem = queueF.peek();
                assertTrue(firstItem.isPresent(), "Queue should have items after catch-up");
                assertEquals("item-0", firstItem.orElse(null), "First item should be item-0");

                // Verify sequence is caught up
                assertTrue(follower.replicationManager().getLastAppliedSequence() >= 600,
                        "Follower should have caught up to sequence 600");
            }
        }
    }

    /**
     * Tests multi-chunk catch-up for larger queues.
     * 2500 items should require multiple snapshot chunks (assuming chunk size
     * ~1000).
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testMultiChunkQueueCatchUp() throws Exception {
        NodeInfo infoL = new NodeInfo(NodeId.of("queue-leader-multi"), "localhost", 9901);
        NodeInfo infoF = new NodeInfo(NodeId.of("queue-follower-multi"), "localhost", 9902);

        Path base = Files.createTempDirectory("ngrid-queue-catchup-multi");
        Path dirL = base.resolve("leader");
        Path dirF = base.resolve("follower");

        try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                .queueDirectory(dirL)
                .replicationFactor(1)
                .heartbeatInterval(Duration.ofMillis(200))
                .build())) {

            leader.start();
            DistributedQueue<String> queueL = leader.getQueue("multi-queue", String.class);

            // Perform 2500 offers (requires multiple chunks)
            for (int i = 0; i < 2500; i++) {
                queueL.offer("item-" + i);
            }

            try (NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                    .addPeer(infoL)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .heartbeatInterval(Duration.ofMillis(200))
                    .build())) {

                follower.start();

                // Wait for cluster convergence
                long start = System.currentTimeMillis();
                while (follower.coordinator().getTrackedLeaderHighWatermark() < 2500) {
                    Thread.sleep(100);
                    if (System.currentTimeMillis() - start > 15000) {
                        throw new IllegalStateException(
                                "Follower didn't see leader watermark. Current: "
                                        + follower.coordinator().getTrackedLeaderHighWatermark());
                    }
                }

                // Wait for catch-up
                start = System.currentTimeMillis();
                DistributedQueue<String> queueF = follower.getQueue("multi-queue", String.class);

                while (follower.replicationManager().getLastAppliedSequence() < 2500) {
                    Thread.sleep(500);
                    if (System.currentTimeMillis() - start > 30000) {
                        throw new IllegalStateException(
                                "Multi-chunk queue catch-up failed. Last applied seq: "
                                        + follower.replicationManager().getLastAppliedSequence());
                    }
                }

                // Verify queue has items
                Optional<String> firstItem = queueF.peek();
                assertTrue(firstItem.isPresent(), "Queue should have items after multi-chunk catch-up");
                assertEquals("item-0", firstItem.orElse(null), "First item should be item-0");

                // Verify sequence is caught up
                assertTrue(follower.replicationManager().getLastAppliedSequence() >= 2500,
                        "Follower should have caught up to sequence 2500");
            }
        }
    }
}
