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
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.replication.ReplicationManager.TopicReplicationStatus;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #177 — o high-watermark de um tópico ocioso no líder recém-eleito não pode ser {@code 0}.
 *
 * <p>O contador por tópico do líder ({@code sequenceByTopic}) só era semeado na primeira PRODUÇÃO do
 * tópico. Um nó promovido que aplicou {@code N} operações como follower e ainda não produziu nada
 * anunciava {@code leaderHighWatermark = 0} nos {@code RELAY_STREAM_BATCH}, e os followers passavam a
 * reportar "lag desconhecido" enquanto o tópico ficasse ocioso. O watermark do líder precisa refletir a
 * fronteira aplicada que ele efetivamente detém ({@code N}).
 */
class PromotedLeaderTopicWatermarkTest {

    private static final String MAP_NAME = "hwm-map";
    private static final String TOPIC = MapClusterService.TOPIC_PREFIX + MAP_NAME;
    private static final int OPS = 40;

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void liderPromovidoAnunciaAFronteiraAplicadaDoTopicoOcioso() throws Exception {
        Set<Integer> usedPorts = new HashSet<>();
        NodeInfo a = new NodeInfo(NodeId.of("hwm-a"), "127.0.0.1", allocateFreeLocalPort(usedPorts));
        NodeInfo b = new NodeInfo(NodeId.of("hwm-b"), "127.0.0.1", allocateFreeLocalPort(usedPorts));
        NodeInfo c = new NodeInfo(NodeId.of("hwm-c"), "127.0.0.1", allocateFreeLocalPort(usedPorts));
        Path base = Files.createTempDirectory("ngrid-promoted-hwm");

        List<NGridNode> nodes = new ArrayList<>();
        try {
            nodes.add(streamNode(a, base.resolve("a"), b, c));
            nodes.add(streamNode(b, base.resolve("b"), a, c));
            nodes.add(streamNode(c, base.resolve("c"), a, b));
            for (NGridNode node : nodes) {
                node.start();
            }
            ClusterTestUtils.awaitClusterConsensus(nodes.toArray(new NGridNode[0]));
            for (NGridNode node : nodes) {
                node.getMap(MAP_NAME, String.class, String.class);
            }

            NGridNode oldLeader = awaitLeader(nodes, 20_000);
            DistributedMap<String, String> map = oldLeader.getMap(MAP_NAME, String.class, String.class);
            for (int i = 0; i < OPS; i++) {
                map.put("k-" + i, "v-" + i);
            }
            long produced = oldLeader.replicationManager().getLeaderHighWatermark(TOPIC);
            assertEquals(OPS, produced, "the original leader produced one sequence per put on the topic");

            // Every follower must have APPLIED the whole topic before the failover.
            List<NGridNode> survivors = new ArrayList<>(nodes);
            survivors.remove(oldLeader);
            for (NGridNode follower : survivors) {
                awaitAppliedFrontier(follower, produced, 30_000);
            }

            oldLeader.close();
            nodes.remove(oldLeader);

            NGridNode newLeader = awaitLeader(survivors, 30_000);
            NGridNode follower = survivors.get(0) == newLeader ? survivors.get(1) : survivors.get(0);
            awaitAdoptedLeader(follower, newLeader, 30_000);

            // (a) The promoted leader has produced nothing on the idle topic, yet it holds everything up
            // to `produced`: its own high-watermark must say so.
            assertEquals(produced, newLeader.replicationManager().getLeaderHighWatermark(TOPIC),
                    "a freshly promoted leader must advertise the applied frontier of an idle topic");

            // (b) The remaining follower learns the watermark from the new leader's RELAY_STREAM_BATCH.
            // Let several fetch cycles (50 ms poll) run against the new leader so the pre-failover value
            // is surely overwritten, then require it to hold steadily.
            Thread.sleep(2_000);
            long deadline = System.currentTimeMillis() + 1_000;
            while (System.currentTimeMillis() < deadline) {
                TopicReplicationStatus status = follower.replicationManager().getTopicReplicationStatuses()
                        .get(TOPIC);
                assertNotNull(status, "the follower must expose the map topic status");
                assertEquals(produced, status.leaderHighWatermark(),
                        "the follower must learn the promoted leader's real watermark (not 0 = unknown)");
                assertEquals(0L, status.lag(), "a caught-up follower reports zero lag");
                Thread.sleep(100);
            }
        } finally {
            for (NGridNode node : nodes) {
                closeQuietly(node);
            }
        }
    }

    private static NGridNode streamNode(NodeInfo self, Path dir, NodeInfo... peers) {
        NGridConfig.Builder builder = NGridConfig.builder(self)
                .dataDirectory(dir)
                .replicationFactor(1)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200));
        for (NodeInfo peer : peers) {
            builder.addPeer(peer);
        }
        return new NGridNode(builder.build());
    }

    private static NGridNode awaitLeader(List<NGridNode> candidates, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (NGridNode node : candidates) {
                if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                    return node;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no leader elected in time");
    }

    private static void awaitAdoptedLeader(NGridNode follower, NGridNode leader, long timeoutMs)
            throws InterruptedException {
        NodeId leaderId = leader.coordinator().leaderInfo().map(NodeInfo::nodeId).orElseThrow();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Optional<NodeId> adopted = follower.coordinator().leaderInfo().map(NodeInfo::nodeId);
            if (adopted.isPresent() && adopted.get().equals(leaderId)
                    && follower.replicationManager().isStreaming(TOPIC)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("follower did not adopt the promoted leader " + leaderId + " in time");
    }

    private static void awaitAppliedFrontier(NGridNode node, long target, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            TopicReplicationStatus status = node.replicationManager().getTopicReplicationStatuses().get(TOPIC);
            if (status != null && status.nextExpectedSequence() - 1L >= target) {
                return;
            }
            Thread.sleep(100);
        }
        fail("node " + node.transport().local().nodeId() + " did not apply the topic up to " + target);
    }

    private static void closeQuietly(NGridNode node) {
        try {
            node.close();
        } catch (IOException ignored) {
            // best-effort teardown
        }
    }

    private static int allocateFreeLocalPort(Set<Integer> used) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && used.add(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }
}
