package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
            // Cutover (decision A): the relay-log fully replaces the in-memory sequence buffer — it is
            // never touched in RELAY_LOG, no fallback to the legacy path.
            assertEquals(0L, follower.replicationManager().getInlineSequenceBufferSize(),
                    "RELAY_LOG must not use the legacy in-memory sequence buffer");

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

    /**
     * Fase 4: a fast burst makes the follower lag well past {@code SYNC_THRESHOLD} (500), which in
     * INLINE mode would trigger a catch-up snapshot. In RELAY_LOG the lag is absorbed by the relay and
     * worked off by the apply consumer — it must converge WITHOUT any snapshot fallback (the reset +
     * growing-snapshot death spiral the issue removes). Uses replicationFactor=1 so the leader does
     * not throttle to the follower's ack, guaranteeing the lag builds.
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void largeBurstLagsButNeverSnapshotsInRelayMode() throws Exception {
        NodeInfo infoA = new NodeInfo(NodeId.of("relay-burst-a"), "localhost", 9843);
        NodeInfo infoB = new NodeInfo(NodeId.of("relay-burst-b"), "localhost", 9844);
        Path base = Files.createTempDirectory("ngrid-relay-burst");

        try (NGridNode a = relayNode(infoA, infoB, base.resolve("a"), 1);
                NGridNode b = relayNode(infoB, infoA, base.resolve("b"), 1)) {
            a.start();
            b.start();
            ClusterTestUtils.awaitClusterConsensus(a, b);
            a.getQueue("burst-queue", String.class);
            b.getQueue("burst-queue", String.class);

            NGridNode leader = a.coordinator().isLeader() ? a : b;
            NGridNode follower = (leader == a) ? b : a;

            int n = 3000; // > 6x SYNC_THRESHOLD so the follower lags well past the snapshot threshold
            DistributedQueue<String> queue = leader.getQueue("burst-queue", String.class);
            for (int i = 0; i < n; i++) {
                queue.offer("item-" + i);
            }

            awaitApplied(follower, leader.replicationManager().getGlobalSequence(), 50_000);

            // The decisive assertion: no sync/snapshot was ever requested. Without Fase 4,
            // checkLagAndSync would fire requestSyncForAllTopics once the lag passed SYNC_THRESHOLD.
            assertEquals(0L, follower.replicationManager().getSyncRequestCount(),
                    "relay must absorb a large lag without requesting any snapshot/sync");
            assertEquals(0L, follower.replicationManager().getSnapshotFallbackCount(),
                    "relay must converge without snapshot fallback");
            assertEquals("item-0", follower.getQueue("burst-queue", String.class).peek().orElse(null),
                    "follower must converge via the relay");
        }
    }

    /**
     * ACEITE CENTRAL (#124): the death spiral reproduced and proven gone. Under sustained load the
     * follower lags far past every legacy snapshot trigger (SYNC_THRESHOLD=500, MAX_SEQUENCE_BUFFER),
     * which in INLINE drives the reset + growing-snapshot + leader-starvation spiral. In RELAY_LOG the
     * lag is absorbed by the durable relay and worked off by the apply consumer: the follower converges
     * with ZERO snapshot/sync requests, no reset, and bounded (not catastrophic) replica lag.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void deathSpiralEliminated_sustainedLagConvergesWithoutSnapshot() throws Exception {
        NodeInfo infoA = new NodeInfo(NodeId.of("relay-soak-a"), "localhost", 9848);
        NodeInfo infoB = new NodeInfo(NodeId.of("relay-soak-b"), "localhost", 9849);
        Path base = Files.createTempDirectory("ngrid-relay-soak");

        try (NGridNode a = relayNode(infoA, infoB, base.resolve("a"), 1);
                NGridNode b = relayNode(infoB, infoA, base.resolve("b"), 1)) {
            a.start();
            b.start();
            ClusterTestUtils.awaitClusterConsensus(a, b);
            a.getQueue("soak-queue", String.class);
            b.getQueue("soak-queue", String.class);

            NGridNode leader = a.coordinator().isLeader() ? a : b;
            NGridNode follower = (leader == a) ? b : a;

            int totalOps = 10_000; // sustained load — lag builds far beyond every snapshot threshold
            DistributedQueue<String> queue = leader.getQueue("soak-queue", String.class);
            for (int i = 0; i < totalOps; i++) {
                queue.offer("op-" + i);
            }

            long produced = leader.replicationManager().getGlobalSequence();
            awaitApplied(follower, produced, 80_000);

            // The acceptance: NO snapshot/sync ever fired despite a huge sustained lag (spiral gone),
            // and the follower kept its state from the start (no reset/loss).
            assertEquals(0L, follower.replicationManager().getSyncRequestCount(),
                    "sustained lag must NOT trigger any snapshot/sync (death spiral eliminated)");
            assertEquals(0L, follower.replicationManager().getSnapshotFallbackCount(),
                    "no snapshot fallback under sustained load");
            assertEquals(0L, follower.replicationManager().getInlineSequenceBufferSize(),
                    "relay path only — the in-memory buffer that drove the OOM/spiral is unused");
            assertEquals("op-0", follower.getQueue("soak-queue", String.class).peek().orElse(null),
                    "the follower never reset its state — head is still the first op");
        }
    }

    /**
     * Fase 5: after killing the leader, a surviving relay node may only become a <em>ready</em> leader
     * once its relay backlog has fully drained ({@code isLeaderSyncing()} stays true until then — the
     * failover drain-gate). Then writes succeed and the cluster stays consistent (no divergence).
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void leaderFailoverIsGatedOnRelayDrainAndStaysConsistent() throws Exception {
        NodeInfo i1 = new NodeInfo(NodeId.of("relay-fo-1"), "localhost", 9845);
        NodeInfo i2 = new NodeInfo(NodeId.of("relay-fo-2"), "localhost", 9846);
        NodeInfo i3 = new NodeInfo(NodeId.of("relay-fo-3"), "localhost", 9847);
        Path base = Files.createTempDirectory("ngrid-relay-failover");

        List<NGridNode> nodes = new ArrayList<>();
        nodes.add(failoverNode(i1, base.resolve("1"), i2, i3));
        nodes.add(failoverNode(i2, base.resolve("2"), i1, i3));
        nodes.add(failoverNode(i3, base.resolve("3"), i1, i2));
        try {
            for (NGridNode n : nodes) {
                n.start();
            }
            ClusterTestUtils.awaitClusterConsensus(nodes.get(0), nodes.get(1), nodes.get(2));
            for (NGridNode n : nodes) {
                n.getQueue("fo-queue", String.class);
            }

            NGridNode leader = findLeader(nodes);
            DistributedQueue<String> queue = leader.getQueue("fo-queue", String.class);
            for (int i = 0; i < 200; i++) {
                queue.offer("a-" + i);
            }
            Thread.sleep(150); // partial drain: a promoted follower may still hold a relay backlog

            // Kill the leader.
            leader.close();
            nodes.remove(leader);

            // A new leader is "ready" only after its relay drains (gate released).
            NGridNode newLeader = awaitNewLeader(nodes, 40_000);
            assertNotNull(newLeader, "a new leader must be elected and drain its relay before leading");

            // The gate is released -> writes succeed (retry briefly to absorb election settling).
            DistributedQueue<String> newQueue = newLeader.getQueue("fo-queue", String.class);
            for (int i = 0; i < 50; i++) {
                offerWithRetry(newQueue, "b-" + i, 15_000);
            }

            // The surviving follower stays consistent: original data present, new writes applied.
            NGridNode survivor = nodes.stream().filter(n -> n != newLeader).findFirst().orElseThrow();
            long target = newLeader.replicationManager().getLastAppliedSequence();
            awaitApplied(survivor, target, 30_000);
            assertEquals("a-0", survivor.getQueue("fo-queue", String.class).peek().orElse(null),
                    "original data must survive the failover (no loss, no divergence)");
        } finally {
            for (NGridNode n : nodes) {
                closeQuietly(n);
            }
        }
    }

    /**
     * P1 (review Codex): an unclean restart whose coalesced frontier was LOST (e.g. crash before the
     * flush) must bootstrap from a fresh snapshot instead of silently resuming at sequence 1 and letting
     * a leader resend re-apply (duplicate) the non-idempotent queue OFFER. Simulated by deleting the
     * clean marker AND {@code sequence-state.dat} of a follower before restarting it.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void uncleanRestartWithLostFrontierBootstrapsInsteadOfReapplying() throws Exception {
        NodeInfo[] infos = {
                new NodeInfo(NodeId.of("relay-uc-1"), "localhost", 9851),
                new NodeInfo(NodeId.of("relay-uc-2"), "localhost", 9852),
                new NodeInfo(NodeId.of("relay-uc-3"), "localhost", 9853)
        };
        Path base = Files.createTempDirectory("ngrid-relay-unclean");
        Path[] dirs = { base.resolve("1"), base.resolve("2"), base.resolve("3") };

        NGridNode[] nodes = new NGridNode[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = failoverNode(infos[i], dirs[i], infos[(i + 1) % 3], infos[(i + 2) % 3]);
        }
        List<NGridNode> live = new ArrayList<>(List.of(nodes));
        try {
            for (NGridNode n : live) {
                n.start();
            }
            ClusterTestUtils.awaitClusterConsensus(nodes[0], nodes[1], nodes[2]);
            for (NGridNode n : live) {
                n.getQueue("uc-queue", String.class);
            }

            int leaderIdx = leaderIndex(nodes);
            int victimIdx = (leaderIdx + 1) % 3; // a follower

            DistributedQueue<String> queue = nodes[leaderIdx].getQueue("uc-queue", String.class);
            for (int i = 0; i < 60; i++) {
                queue.offer("x-" + i);
            }
            long total = nodes[leaderIdx].replicationManager().getGlobalSequence();
            awaitApplied(nodes[victimIdx], total, 25_000);

            // Simulate an UNCLEAN crash with a LOST frontier on the victim follower.
            nodes[victimIdx].close();
            live.remove(nodes[victimIdx]);
            Path repl = dirs[victimIdx].resolve("replication");
            Files.deleteIfExists(repl.resolve("relay").resolve(".clean-shutdown"));
            Files.deleteIfExists(repl.resolve("sequence-state.dat"));

            // Restart the victim from the tampered (crashed) state.
            NGridNode restarted = failoverNode(infos[victimIdx], dirs[victimIdx],
                    infos[(victimIdx + 1) % 3], infos[(victimIdx + 2) % 3]);
            nodes[victimIdx] = restarted;
            live.add(restarted);
            restarted.start();
            ClusterTestUtils.awaitClusterConsensus(nodes[0], nodes[1], nodes[2]);
            restarted.getQueue("uc-queue", String.class);

            // The fix: it bootstraps (requests a snapshot) rather than replaying the relay from seq 1.
            long deadline = System.currentTimeMillis() + 30_000;
            while (restarted.replicationManager().getSyncRequestCount() == 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(150);
            }
            assertEquals(true, restarted.replicationManager().getSyncRequestCount() > 0,
                    "unclean restart with a lost frontier must bootstrap (snapshot), not silently re-apply");

            // ...and converge consistently (no divergence/duplication).
            NGridNode leaderNow = nodes[leaderIndex(nodes)];
            awaitApplied(restarted, leaderNow.replicationManager().getLastAppliedSequence(), 30_000);
            assertEquals("x-0", restarted.getQueue("uc-queue", String.class).peek().orElse(null),
                    "bootstrapped follower must hold the leader's state (no divergence)");
        } finally {
            for (NGridNode n : live) {
                closeQuietly(n);
            }
        }
    }

    private static int leaderIndex(NGridNode[] nodes) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null && nodes[i].coordinator().isLeader()) {
                return i;
            }
        }
        throw new IllegalStateException("no leader elected");
    }

    private static NGridNode failoverNode(NodeInfo self, Path dir, NodeInfo peerA, NodeInfo peerB) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peerA)
                .addPeer(peerB)
                .dataDirectory(dir)
                .replicationFactor(2)
                .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }

    private static NGridNode findLeader(List<NGridNode> nodes) {
        for (NGridNode n : nodes) {
            if (n.coordinator().isLeader()) {
                return n;
            }
        }
        throw new IllegalStateException("no leader elected");
    }

    private static NGridNode awaitNewLeader(List<NGridNode> nodes, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (NGridNode n : nodes) {
                if (n.coordinator().isLeader() && !n.replicationManager().isLeaderSyncing()) {
                    return n;
                }
            }
            Thread.sleep(200);
        }
        return null;
    }

    private static void offerWithRetry(DistributedQueue<String> queue, String value, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                queue.offer(value);
                return;
            } catch (RuntimeException e) {
                last = e; // gate held / election settling -> retry
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
            }
        }
        throw new IllegalStateException("offer never succeeded after failover", last);
    }

    private static void closeQuietly(NGridNode node) {
        try {
            node.close();
        } catch (IOException ignored) {
            // best-effort cleanup
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
        return relayNode(self, peer, dir, 2);
    }

    private static NGridNode relayNode(NodeInfo self, NodeInfo peer, Path dir, int replicationFactor) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(replicationFactor)
                .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }
}
