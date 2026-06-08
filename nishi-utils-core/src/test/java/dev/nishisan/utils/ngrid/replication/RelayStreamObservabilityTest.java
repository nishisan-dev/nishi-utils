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
import dev.nishisan.utils.ngrid.replication.ReplicationManager.TopicReplicationStatus;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fase 6 (RELAY_STREAM) — observabilidade por tópico: cursor de pull, high-watermark do líder, piso
 * de retenção (oldest), lag, bytes/s do stream e flag de streaming ficam visíveis no follower.
 */
class RelayStreamObservabilityTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void followerExposesPerTopicStreamMetrics() throws Exception {
        NodeInfo a = new NodeInfo(NodeId.of("obs-a"), "localhost", 9881);
        NodeInfo b = new NodeInfo(NodeId.of("obs-b"), "localhost", 9882);
        Path base = Files.createTempDirectory("ngrid-stream-obs");

        try (NGridNode na = streamNode(a, b, base.resolve("a"));
                NGridNode nb = streamNode(b, a, base.resolve("b"))) {
            na.start();
            nb.start();
            ClusterTestUtils.awaitClusterConsensus(na, nb);
            na.getQueue("obs-queue", String.class);
            nb.getQueue("obs-queue", String.class);

            NGridNode leader = na.coordinator().isLeader() ? na : nb;
            NGridNode follower = (leader == na) ? nb : na;

            int n = 300;
            DistributedQueue<String> queue = leader.getQueue("obs-queue", String.class);
            for (int i = 0; i < n; i++) {
                queue.offer("item-" + i);
            }
            awaitApplied(follower, leader.replicationManager().getGlobalSequence(), 30_000);

            ReplicationManager rm = follower.replicationManager();
            // Find the data topic (the one the stream actually carried bytes for).
            Map<String, TopicReplicationStatus> statuses = rm.getTopicReplicationStatuses();
            TopicReplicationStatus data = statuses.values().stream()
                    .filter(s -> s.streamBytesIn() > 0)
                    .max((x, y) -> Long.compare(x.streamCursor(), y.streamCursor()))
                    .orElse(null);
            assertNotNull(data, "a streamed data topic must be visible in the per-topic status");

            assertTrue(data.streamCursor() > 0, "the pull cursor must have advanced");
            assertTrue(data.leaderHighWatermark() > 0, "the follower must have learned the leader watermark");
            assertTrue(data.leaderOldestSequence() >= 1, "the follower must have learned the retained floor");
            assertTrue(data.streamBytesIn() > 0, "stream bytes must be counted");
            assertTrue(data.streaming(), "the topic must be marked as streaming");
            assertTrue(data.lag() >= 0, "lag must be non-negative");

            // The direct getters agree with the status record.
            String topic = data.topic();
            assertEquals(data.streamCursor(), rm.getRelayStreamCursor(topic));
            assertEquals(data.leaderHighWatermark(), rm.getLeaderHighWatermark(topic));
            assertTrue(rm.isStreaming(topic));
            assertTrue(rm.getStreamBytesIn(topic) > 0);

            // Restart seed: the applied-progress metric is restored from the durable frontier (not 0).
            assertTrue(follower.replicationManager().getLastAppliedSequence() > 0);
        }
    }

    private static NGridNode streamNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(1)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }

    private static void awaitApplied(NGridNode node, long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (node.replicationManager().getLastAppliedSequence() < target) {
            if (System.currentTimeMillis() > deadline) {
                fail("follower did not converge: applied="
                        + node.replicationManager().getLastAppliedSequence() + " target=" + target);
            }
            Thread.sleep(100);
        }
    }
}
