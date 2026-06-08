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

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end proof that LZ4 transport compression works across a real two-node cluster,
 * over both replication paths:
 * <ul>
 *   <li><strong>SYNC_RESPONSE</strong> (snapshot): a follower joins late, past the lag
 *       threshold, and converges via a (compressed) snapshot;</li>
 *   <li><strong>REPLICATION_REQUEST</strong> (live): further large writes after the join are
 *       replicated live and applied on the follower.</li>
 * </ul>
 * Items are deliberately large and repetitive so each frame clears the compression threshold
 * and is shipped LZ4-framed. Correct convergence proves compress → wire → decompress → apply.
 */
class TransportCompressionClusterTest {

    /** ~2 KiB repetitive suffix: well above the 512-byte compression threshold and very compressible. */
    private static final String BIG = "payload-".repeat(256);

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void compressedSnapshotAndLiveReplicationConverge() throws Exception {
        NodeInfo infoL = new NodeInfo(NodeId.of("lz4-leader"), "localhost", 9971);
        NodeInfo infoF = new NodeInfo(NodeId.of("lz4-follower"), "localhost", 9972);

        Path base = Files.createTempDirectory("ngrid-compression-cluster");
        Path dirL = base.resolve("leader");
        Path dirF = base.resolve("follower");

        try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                .queueDirectory(dirL)
                .replicationFactor(1) // allow writes with only the leader present
                .transportCompressionEnabled(true)
                .heartbeatInterval(Duration.ofMillis(200))
                .build())) {

            leader.start();
            assertTrue(leader.config().transportCompressionEnabled(),
                    "leader must have transport compression enabled");

            DistributedQueue<String> queueL = leader.getQueue("compressed-queue", String.class);

            // 600 large offers (> 500 sync threshold) BEFORE the follower joins -> snapshot path.
            for (int i = 0; i < 600; i++) {
                queueL.offer("item-" + i + ":" + BIG);
            }
            assertEquals(600, leader.replicationManager().getGlobalSequence(),
                    "leader should have processed 600 operations");

            try (NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                    .addPeer(infoL)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .transportCompressionEnabled(true)
                    .heartbeatInterval(Duration.ofMillis(200))
                    .build())) {

                follower.start();
                assertTrue(follower.config().transportCompressionEnabled(),
                        "follower must have transport compression enabled");

                // Register the queue handler on the follower BEFORE awaiting catch-up, so the
                // follower actually requests the snapshot for this topic (an unregistered topic
                // is never synced).
                DistributedQueue<String> queueF = follower.getQueue("compressed-queue", String.class);

                // See the leader's watermark, then catch up via the (compressed) snapshot.
                awaitCondition(() -> follower.coordinator().getTrackedLeaderHighWatermark() >= 600,
                        15000, "follower never observed the leader watermark of 600");
                awaitCondition(() -> follower.replicationManager().getLastAppliedSequence() >= 600,
                        30000, "follower did not converge via compressed snapshot");

                Optional<String> first = queueF.peek();
                assertTrue(first.isPresent(), "queue must have items after compressed snapshot sync");
                assertEquals("item-0:" + BIG, first.get(),
                        "first item content must survive compression round-trip exactly");

                // Live replication path: 50 more large offers, replicated while the follower is up.
                for (int i = 600; i < 650; i++) {
                    queueL.offer("item-" + i + ":" + BIG);
                }
                awaitCondition(() -> follower.replicationManager().getLastAppliedSequence() >= 650,
                        20000, "follower did not converge via compressed live replication");

                assertTrue(follower.replicationManager().getLastAppliedSequence() >= 650,
                        "follower should have caught up to sequence 650 over the compressed live path");
            }
        }
    }

    private static void awaitCondition(BooleanSupplierEx condition, long timeoutMs, String failMessage)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150);
        }
        fail(failMessage);
    }

    @FunctionalInterface
    private interface BooleanSupplierEx {
        boolean getAsBoolean() throws Exception;
    }
}
