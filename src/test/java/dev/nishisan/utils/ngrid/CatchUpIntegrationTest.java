package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatchUpIntegrationTest {

    @Test
    void testCatchUpAfterSignificantLag() throws Exception {
        NodeInfo infoL = new NodeInfo(NodeId.of("leader"), "localhost", 9601);
        NodeInfo infoF = new NodeInfo(NodeId.of("follower"), "localhost", 9602);

        Path base = Files.createTempDirectory("ngrid-catchup-test");
        Path dirL = base.resolve("leader");
        Path dirF = base.resolve("follower");

        // 1. Start Leader only initially
        try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                .queueDirectory(dirL)
                .replicationFactor(1) // allow writes with only leader
                .heartbeatInterval(Duration.ofMillis(200))
                .build())) {

            leader.start();
            DistributedMap<String, String> mapL = leader.getMap("sync-map", String.class, String.class);

            // 2. Perform 600 writes (Threshold is 500)
            for (int i = 0; i < 600; i++) {
                mapL.put("key" + i, "val" + i);
            }

            assertEquals(600, leader.replicationManager().getGlobalSequence());

            // 3. Start Follower and join
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
                        throw new IllegalStateException("Follower didn't see leader watermark. Current: " + follower.coordinator().getTrackedLeaderHighWatermark());
                    }
                }

                // 4. Wait for Catch-up (Snapshot sync)
                // ReplicationManager checks lag every 2 seconds.
                start = System.currentTimeMillis();
                DistributedMap<String, String> mapF = follower.getMap("sync-map", String.class, String.class);
                
                while (true) {
                    Optional<String> lastVal = mapF.get("key599", Consistency.EVENTUAL);
                    if (lastVal.isPresent() && "val599".equals(lastVal.get())) {
                        break;
                    }
                    Thread.sleep(200);
                    if (System.currentTimeMillis() - start > 15000) {
                        throw new IllegalStateException("Catch-up failed or too slow. Last applied seq: " + follower.replicationManager().getLastAppliedSequence());
                    }
                }

                // 5. Verify all data is present
                for (int i = 0; i < 600; i++) {
                    assertEquals("val" + i, mapF.get("key" + i, Consistency.EVENTUAL).orElse(null), "Missing key" + i);
                }
                
                assertTrue(follower.replicationManager().getLastAppliedSequence() >= 600);
            }
        }
    }

    @Test
    void testMultiChunkCatchUp() throws Exception {
        NodeInfo infoL = new NodeInfo(NodeId.of("leader-multi"), "localhost", 9701);
        NodeInfo infoF = new NodeInfo(NodeId.of("follower-multi"), "localhost", 9702);

        Path base = Files.createTempDirectory("ngrid-catchup-multi");
        Path dirL = base.resolve("leader");
        Path dirF = base.resolve("follower");

        try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                .queueDirectory(dirL)
                .replicationFactor(1)
                .heartbeatInterval(Duration.ofMillis(200))
                .build())) {

            leader.start();
            DistributedMap<String, String> mapL = leader.getMap("multi-map", String.class, String.class);

            // Perform 2500 writes (Chunk size is 1000)
            for (int i = 0; i < 2500; i++) {
                mapL.put("key" + i, "val" + i);
            }

            try (NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                    .addPeer(infoL)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .heartbeatInterval(Duration.ofMillis(200))
                    .build())) {

                follower.start();

                long start = System.currentTimeMillis();
                DistributedMap<String, String> mapF = follower.getMap("multi-map", String.class, String.class);
                
                while (true) {
                    // Check last key
                    Optional<String> lastVal = mapF.get("key2499", Consistency.EVENTUAL);
                    if (lastVal.isPresent() && "val2499".equals(lastVal.get())) {
                        break;
                    }
                    Thread.sleep(500);
                    if (System.currentTimeMillis() - start > 20000) {
                        throw new IllegalStateException("Multi-chunk catch-up failed or too slow. Last applied seq: " + follower.replicationManager().getLastAppliedSequence());
                    }
                }

                // Verify samples
                assertEquals("val0", mapF.get("key0", Consistency.EVENTUAL).orElse(null));
                assertEquals("val1234", mapF.get("key1234", Consistency.EVENTUAL).orElse(null));
                assertEquals("val2499", mapF.get("key2499", Consistency.EVENTUAL).orElse(null));
            }
        }
    }
}
