package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the outbound backpressure feature of issue #113 in a
 * running 2-node cluster.
 *
 * <p>
 * This test exercises the feature end-to-end: a per-connection
 * {@code outboundQueueCapacity} is configured via {@link NGridConfig}, a leader
 * writes a burst under {@code quorum=1} (best-effort, no RTT), and the test
 * verifies that (a) the cluster still converges — a representative sample of
 * keys (every {@code SAMPLE_STEP}) replicates/catches up to the follower despite
 * the bounded outbound queue — and (b) the per-node
 * outbound occupancy/drop metric (RF3) is exposed in
 * {@link NGridNode#operationalSnapshot()} for the leader→follower link.
 * </p>
 *
 * <p>
 * The <em>formal</em> proof of the bound + drop policy ("leader memory stays
 * stable") is the deterministic unit test
 * {@code OutboundChannelTest}; reproducing real socket-level overflow in-process
 * on localhost is timing-dependent, so it is intentionally not asserted here.
 * </p>
 */
class OutboundBackpressureConvergenceTest {

    private static final String MAP = "bp-map";
    private static final int WRITES = 2000;
    private static final int CAPACITY = 64;
    private static final int SAMPLE_STEP = 100;
    // Moderately fat payload emulating a fault-management EventDto.
    private static final String FAT = "x".repeat(1024);

    private static String value(int i) {
        return "val" + i + "-" + FAT;
    }

    @Test
    @Timeout(60)
    void clusterConvergesWithBoundedOutboundAndExposesMetric() throws Exception {
        NodeInfo infoA = new NodeInfo(NodeId.of("bp-node-a"), "localhost", 9811);
        NodeInfo infoB = new NodeInfo(NodeId.of("bp-node-b"), "localhost", 9812);

        Path base = Files.createTempDirectory("ngrid-outbound-bp");

        try (NGridNode a = new NGridNode(NGridConfig.builder(infoA)
                .addPeer(infoB)
                .queueDirectory(base.resolve("a"))
                .replicationFactor(1)           // quorum=1 -> best-effort, no RTT on put
                .strictConsistency(false)
                .outboundQueueCapacity(CAPACITY)
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
             NGridNode b = new NGridNode(NGridConfig.builder(infoB)
                .addPeer(infoA)
                .queueDirectory(base.resolve("b"))
                .replicationFactor(1)
                .strictConsistency(false)
                .outboundQueueCapacity(CAPACITY)
                .heartbeatInterval(Duration.ofMillis(200))
                .build())) {

            a.start();
            b.start();

            awaitBothActiveWithLeader(a, b, 20000);

            NGridNode leader = a.coordinator().isLeader() ? a : b;
            NGridNode follower = leader == a ? b : a;
            String followerId = (follower == a ? infoA : infoB).nodeId().value();

            // Register the map on BOTH nodes before traffic, so the follower has a
            // handler for the topic (see UnknownMapRequestHandler).
            DistributedMap<String, String> mapL = leader.getMap(MAP, String.class, String.class);
            DistributedMap<String, String> mapF = follower.getMap(MAP, String.class, String.class);

            // Fast burst under the bounded outbound queue. With quorum=1 the put does
            // not wait for the follower.
            for (int i = 0; i < WRITES; i++) {
                mapL.put("key" + i, value(i));
            }

            // The cluster must converge: every sampled key reaches the follower via
            // normal replication + catch-up, despite the bounded outbound queue.
            long start = System.currentTimeMillis();
            String missing;
            while ((missing = missingSampleKey(mapF)) != null) {
                Thread.sleep(200);
                if (System.currentTimeMillis() - start > 45000) {
                    throw new IllegalStateException("Follower did not converge. missing=" + missing
                            + " lastApplied=" + follower.replicationManager().getLastAppliedSequence()
                            + " trackedWatermark=" + follower.coordinator().getTrackedLeaderHighWatermark());
                }
            }

            // RF3: the per-node outbound occupancy/drop metric is exposed for the
            // leader→follower link in the operational snapshot (and dashboard).
            var snapshot = leader.operationalSnapshot();
            assertNotNull(snapshot.outboundQueueDepthByNode(), "depth metric map must exist");
            assertNotNull(snapshot.outboundDroppedByNode(), "dropped metric map must exist");
            assertTrue(snapshot.outboundQueueDepthByNode().containsKey(followerId),
                    "outbound depth metric must be exposed for the follower link " + followerId
                            + ", got " + snapshot.outboundQueueDepthByNode().keySet());
            assertTrue(snapshot.outboundDroppedByNode().containsKey(followerId),
                    "outbound dropped metric must be exposed for the follower link " + followerId);
        }
    }

    /**
     * Returns the first sampled key whose value has not yet converged on the
     * follower, or {@code null} if the whole sample is present and correct.
     */
    private static String missingSampleKey(DistributedMap<String, String> mapF) {
        for (int i = 0; i < WRITES; i += SAMPLE_STEP) {
            String key = "key" + i;
            if (!value(i).equals(mapF.getOptional(key, Consistency.EVENTUAL).orElse(null))) {
                return key;
            }
        }
        return null;
    }

    private static void awaitBothActiveWithLeader(NGridNode a, NGridNode b, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (a.coordinator().getActiveMembersCount() < 2
                || b.coordinator().getActiveMembersCount() < 2
                || (!a.coordinator().isLeader() && !b.coordinator().isLeader())) {
            Thread.sleep(100);
            if (System.currentTimeMillis() - start > timeoutMs) {
                throw new IllegalStateException("cluster did not converge: aMembers="
                        + a.coordinator().getActiveMembersCount() + " bMembers="
                        + b.coordinator().getActiveMembersCount());
            }
        }
    }
}
