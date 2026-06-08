/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fase 5 (RELAY_STREAM) — durabilidade do stream através de restart e failover. Um restart limpo
 * retoma do cursor durável (a cauda do relay) sem snapshot; um failover promove um novo líder e o
 * cluster reconverge pelo stream (com o op-log espelhado dando continuidade ao binlog).
 */
class RelayStreamDurabilityTest {

    // Higher NodeId ("z") wins election → steady leader; "a" is the follower.
    private static final NodeInfo LEADER_INFO = new NodeInfo(NodeId.of("stream-dur-z"), "localhost", 9871);
    private static final NodeInfo FOLLOWER_INFO = new NodeInfo(NodeId.of("stream-dur-a"), "localhost", 9872);

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void cleanRestartResumesStreamFromCursorWithoutSnapshot() throws Exception {
        Path base = Files.createTempDirectory("ngrid-stream-restart");
        Path followerDir = base.resolve("a");

        NGridNode leader = pairNode(LEADER_INFO, FOLLOWER_INFO, base.resolve("z"));
        NGridNode follower = pairNode(FOLLOWER_INFO, LEADER_INFO, followerDir);
        try {
            leader.start();
            follower.start();
            ClusterTestUtils.awaitClusterConsensus(leader, follower);
            leader.getMap("rm", String.class, String.class);
            follower.getMap("rm", String.class, String.class);

            // A map gives a restart-safe convergence check (random access by key), unlike the global
            // applied counter which is per-session and would undercount after a resume.
            DistributedMap<String, String> map = leader.getMap("rm", String.class, String.class);
            for (int i = 0; i < 200; i++) {
                map.put("k-" + i, "v-" + i);
            }
            awaitMapValue(follower, "k-199", "v-199", 30_000);

            // Clean shutdown of the follower (writes the clean-shutdown marker → resume, not bootstrap).
            follower.close();

            // Restart the follower on the SAME data dir, then produce more (both nodes up again, so the
            // quorum is met). It must resume the stream from its durable cursor and catch up the new
            // backlog WITHOUT a snapshot.
            NGridNode restarted = pairNode(FOLLOWER_INFO, LEADER_INFO, followerDir);
            restarted.start();
            try {
                ClusterTestUtils.awaitClusterConsensus(leader, restarted);
                restarted.getMap("rm", String.class, String.class);

                for (int i = 200; i < 300; i++) {
                    putWithRetry(map, "k-" + i, "v-" + i, 15_000);
                }
                // The restarted follower resumes the stream from its durable cursor (post-restart
                // ops applied), without re-streaming from 1 and without a snapshot. (The durability of
                // the already-applied prefix is a backend concern, not the stream's responsibility.)
                awaitMapValue(restarted, "k-299", "v-299", 40_000);

                assertEquals(0L, restarted.replicationManager().getSnapshotFallbackCount(),
                        "clean restart must resume from the cursor, never snapshot");
                assertEquals(0L, restarted.replicationManager().getGapsDetected(),
                        "resumed stream must stay contiguous (no gaps)");
            } finally {
                closeQuietly(restarted);
            }
        } finally {
            closeQuietly(leader);
            closeQuietly(follower);
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void failoverElectsNewLeaderAndClusterReconvergesViaStream() throws Exception {
        NodeInfo i1 = new NodeInfo(NodeId.of("stream-fo-1"), "localhost", 9873);
        NodeInfo i2 = new NodeInfo(NodeId.of("stream-fo-2"), "localhost", 9874);
        NodeInfo i3 = new NodeInfo(NodeId.of("stream-fo-3"), "localhost", 9875);
        Path base = Files.createTempDirectory("ngrid-stream-failover");

        List<NGridNode> nodes = new ArrayList<>();
        nodes.add(quorumNode(i1, base.resolve("1"), i2, i3));
        nodes.add(quorumNode(i2, base.resolve("2"), i1, i3));
        nodes.add(quorumNode(i3, base.resolve("3"), i1, i2));
        try {
            for (NGridNode n : nodes) {
                n.start();
            }
            ClusterTestUtils.awaitClusterConsensus(nodes.get(0), nodes.get(1), nodes.get(2));
            for (NGridNode n : nodes) {
                n.getQueue("fq", String.class);
            }

            NGridNode leader = findLeader(nodes);
            DistributedQueue<String> queue = leader.getQueue("fq", String.class);
            for (int i = 0; i < 200; i++) {
                queue.offer("a-" + i);
            }
            Thread.sleep(150);

            // Kill the leader. Two of three remain → a new leader is elected (majority preserved).
            leader.close();
            nodes.remove(leader);
            NGridNode newLeader = awaitNewLeader(nodes, 40_000);
            assertNotNull(newLeader, "a new leader must be elected after failover");

            // New writes continue and the surviving follower reconverges via the stream.
            DistributedQueue<String> newQueue = newLeader.getQueue("fq", String.class);
            for (int i = 0; i < 50; i++) {
                offerWithRetry(newQueue, "b-" + i, 15_000);
            }
            NGridNode survivor = nodes.stream().filter(n -> n != newLeader).findFirst().orElseThrow();
            awaitApplied(survivor, newLeader.replicationManager().getLastAppliedSequence(), 40_000);

            assertEquals("a-0", survivor.getQueue("fq", String.class).peek().orElse(null),
                    "original data must survive the failover (no loss, no divergence)");
            assertEquals(0L, survivor.replicationManager().getGapsDetected(),
                    "the stream stays contiguous across failover");
        } finally {
            for (NGridNode n : nodes) {
                closeQuietly(n);
            }
        }
    }

    // 2-node pair (replicationFactor 1): both must be up to meet the dynamic majority for writes.
    private static NGridNode pairNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(1)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }

    // 3-node quorum (replicationFactor 2): tolerates one failure with a majority.
    private static NGridNode quorumNode(NodeInfo self, Path dir, NodeInfo peerA, NodeInfo peerB) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peerA)
                .addPeer(peerB)
                .dataDirectory(dir)
                .replicationFactor(2)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
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

    private static void awaitApplied(NGridNode node, long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (node.replicationManager().getLastAppliedSequence() < target) {
            if (System.currentTimeMillis() > deadline) {
                fail("node did not converge via stream: applied="
                        + node.replicationManager().getLastAppliedSequence() + " target=" + target);
            }
            Thread.sleep(100);
        }
    }

    private static void offerWithRetry(DistributedQueue<String> queue, String value, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                queue.offer(value);
                return;
            } catch (RuntimeException e) {
                last = e; // leaderless/quiescing/election settling → retry
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
            }
        }
        throw new IllegalStateException("offer never succeeded", last);
    }

    private static void putWithRetry(DistributedMap<String, String> map, String key, String value, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                map.put(key, value);
                return;
            } catch (RuntimeException e) {
                last = e; // leaderless/quiescing/election settling → retry
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
            }
        }
        throw new IllegalStateException("put never succeeded", last);
    }

    private static void awaitMapValue(NGridNode node, String key, String expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        DistributedMap<String, String> map = node.getMap("rm", String.class, String.class);
        while (System.currentTimeMillis() < deadline) {
            if (expected.equals(map.getOptional(key, Consistency.EVENTUAL).orElse(null))) {
                return;
            }
            Thread.sleep(100);
        }
        fail("follower did not converge key " + key + "=" + expected + " via stream");
    }

    private static void closeQuietly(NGridNode node) {
        try {
            node.close();
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
