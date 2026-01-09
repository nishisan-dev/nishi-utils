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

class ConsistencyIntegrationTest {

    @Test
    void testConsistencyLevels() throws Exception {
        NodeInfo infoL = new NodeInfo(NodeId.of("leader"), "localhost", 9501);
        NodeInfo infoF = new NodeInfo(NodeId.of("follower"), "localhost", 9502);

        Path base = Files.createTempDirectory("ngrid-consistency-test");
        Path dirL = base.resolve("leader");
        Path dirF = base.resolve("follower");

        try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                .addPeer(infoF)
                .queueDirectory(dirL)
                .replicationFactor(2)
                .heartbeatInterval(Duration.ofMillis(100)) // fast heartbeats to propagate watermark
                .build());
             NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                     .addPeer(infoL)
                     .queueDirectory(dirF)
                     .replicationFactor(2)
                     .heartbeatInterval(Duration.ofMillis(100))
                     .build())) {

            leader.start();
            follower.start();

            // Wait for leader election and cluster convergence
            long start = System.currentTimeMillis();
            while (leader.coordinator().leaderInfo().isEmpty() || 
                   follower.coordinator().leaderInfo().isEmpty() ||
                   leader.coordinator().activeMembers().size() < 2 ||
                   follower.coordinator().activeMembers().size() < 2) {
                Thread.sleep(100);
                if (System.currentTimeMillis() - start > 10000) {
                    throw new IllegalStateException("Cluster did not form or converge. Leader members: " + leader.coordinator().activeMembers().size());
                }
            }

            // Ensure "leader" node is actually the leader (by ID "leader" vs "follower", "leader" > "follower" alphabetically? No, "l" > "f", so leader wins)
            // Wait, "leader" > "follower" ? 'l' is 108, 'f' is 102. Yes.
            assertTrue(leader.coordinator().isLeader(), "Node 'leader' should be the leader");

            DistributedMap<String, String> mapL = leader.getMap("test-map", String.class, String.class);
            DistributedMap<String, String> mapF = follower.getMap("test-map", String.class, String.class);

            // Write to leader
            mapL.put("key1", "value1");
            
            // Verify Leader has it
            assertEquals("value1", mapL.get("key1").orElse(null), "Leader should have the value locally");

            // Verify STRONG consistency (always correct)
            // Retry a few times if connection is flaky?
            Optional<String> res = mapF.get("key1");
            assertEquals("value1", res.orElse(null));

            // Wait for replication to settle locally on Follower
            // We can poll EVENTUAL until it sees it
            start = System.currentTimeMillis();
            while (true) {
                Optional<String> val = mapF.get("key1", Consistency.EVENTUAL);
                if (val.isPresent() && "value1".equals(val.get())) {
                    break;
                }
                Thread.sleep(50);
                if (System.currentTimeMillis() - start > 5000) {
                    throw new IllegalStateException("Replication too slow");
                }
            }

            // Now test BOUNDED consistency
            // Since we waited, lag should be 0.
            assertEquals("value1", mapF.get("key1", Consistency.bounded(5)).orElse(null));
            
            // Should also work with strict 0 lag
            assertEquals("value1", mapF.get("key1", Consistency.bounded(0)).orElse(null));
        }
    }
}
