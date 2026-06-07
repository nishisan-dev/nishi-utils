package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;

/**
 * End-to-end coverage of the relay-log follower ingestion path (#124): with
 * {@link FollowerIngestMode#RELAY_LOG}, a follower persists each replicated op to its
 * on-disk relay and applies it from a separate consumer, converging on both queue and
 * map state without any snapshot reset in regime.
 */
class RelayLogReplicationTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void followerAppliesQueueAndMapViaRelayLogWithoutSnapshot() throws Exception {
        NodeInfo infoA = new NodeInfo(NodeId.of("relay-a"), "localhost", 9841);
        NodeInfo infoB = new NodeInfo(NodeId.of("relay-b"), "localhost", 9842);
        Path base = Files.createTempDirectory("ngrid-relay-e2e");

        try (NGridNode a = relayNode(infoA, infoB, base.resolve("a"));
                NGridNode b = relayNode(infoB, infoA, base.resolve("b"))) {
            a.start();
            b.start();
            ClusterTestUtils.awaitClusterConsensus(a, b);

            // Pre-register handlers on BOTH nodes before traffic (starts the follower apply loop).
            a.getQueue("relay-queue", String.class);
            b.getQueue("relay-queue", String.class);
            a.getMap("relay-map", String.class, String.class);
            b.getMap("relay-map", String.class, String.class);

            NGridNode leader = a.coordinator().isLeader() ? a : b;
            NGridNode follower = (leader == a) ? b : a;

            int n = 200;
            DistributedQueue<String> queue = leader.getQueue("relay-queue", String.class);
            DistributedMap<String, String> map = leader.getMap("relay-map", String.class, String.class);
            for (int i = 0; i < n; i++) {
                queue.offer("item-" + i);
                map.put("k-" + i, "v-" + i);
            }

            long expected = leader.replicationManager().getGlobalSequence();
            assertEquals(2L * n, expected, "leader must have sequenced all queue + map ops");

            awaitApplied(follower, expected, 30_000);

            // Converged via the relay, NOT via a snapshot reset (the failure mode #124 removes).
            assertEquals(0L, follower.replicationManager().getSnapshotFallbackCount(),
                    "relay regime must converge without snapshot fallback");

            // Concrete local-replica checks on the follower.
            DistributedQueue<String> followerQueue = follower.getQueue("relay-queue", String.class);
            assertEquals("item-0", followerQueue.peek().orElse(null),
                    "follower queue head must be the first replicated item (applied from the relay)");

            DistributedMap<String, String> followerMap = follower.getMap("relay-map", String.class, String.class);
            assertEquals("v-0", followerMap.getOptional("k-0", Consistency.EVENTUAL).orElse(null));
            assertEquals("v-" + (n - 1),
                    followerMap.getOptional("k-" + (n - 1), Consistency.EVENTUAL).orElse(null));
        }
    }

    private static void awaitApplied(NGridNode follower, long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (follower.replicationManager().getLastAppliedSequence() < target) {
            if (System.currentTimeMillis() > deadline) {
                fail("follower did not converge via relay: applied="
                        + follower.replicationManager().getLastAppliedSequence() + " target=" + target);
            }
            Thread.sleep(100);
        }
    }

    private static NGridNode relayNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(2)
                .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }
}
