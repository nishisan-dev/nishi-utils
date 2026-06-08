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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fase 4 (RELAY_STREAM) — o follower PUXA o op-log do líder como stream sequencial dirigido por
 * cursor durável: persiste em ordem e aplica no seu ritmo. Em regime permanente NÃO há gap, NAK nem
 * snapshot-storm — o que reproduz e elimina a tempestade de resend observada em pré-prod.
 */
class RelayStreamReplicationTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void streamConvergesQueueAndMapWithoutNakOrSnapshot() throws Exception {
        NodeInfo infoA = new NodeInfo(NodeId.of("stream-a"), "localhost", 9861);
        NodeInfo infoB = new NodeInfo(NodeId.of("stream-b"), "localhost", 9862);
        Path base = Files.createTempDirectory("ngrid-stream-e2e");

        try (NGridNode a = streamNode(infoA, infoB, base.resolve("a"));
                NGridNode b = streamNode(infoB, infoA, base.resolve("b"))) {
            a.start();
            b.start();
            ClusterTestUtils.awaitClusterConsensus(a, b);

            a.getQueue("stream-queue", String.class);
            b.getQueue("stream-queue", String.class);
            a.getMap("stream-map", String.class, String.class);
            b.getMap("stream-map", String.class, String.class);

            NGridNode leader = a.coordinator().isLeader() ? a : b;
            NGridNode follower = (leader == a) ? b : a;

            int n = 200;
            DistributedQueue<String> queue = leader.getQueue("stream-queue", String.class);
            DistributedMap<String, String> map = leader.getMap("stream-map", String.class, String.class);
            for (int i = 0; i < n; i++) {
                queue.offer("item-" + i);
                map.put("k-" + i, "v-" + i);
            }

            long expected = leader.replicationManager().getGlobalSequence();
            assertEquals(2L * n, expected, "leader must have sequenced all queue + map ops");

            awaitApplied(follower, expected, 30_000);

            // The stream is contiguous by construction: zero gaps, zero NAK, zero snapshot fallback.
            assertEquals(0L, follower.replicationManager().getGapsDetected(),
                    "RELAY_STREAM must never detect a gap (the pull is contiguous)");
            assertEquals(0L, follower.replicationManager().getSnapshotFallbackCount(),
                    "RELAY_STREAM must converge without snapshot fallback");
            assertEquals(0L, follower.replicationManager().getSyncRequestCount(),
                    "RELAY_STREAM must not request any snapshot/sync");
            assertEquals(0L, follower.replicationManager().getInlineSequenceBufferSize(),
                    "RELAY_STREAM must not use the legacy in-memory sequence buffer");

            DistributedQueue<String> followerQueue = follower.getQueue("stream-queue", String.class);
            assertEquals("item-0", followerQueue.peek().orElse(null),
                    "follower queue head must be the first streamed item");
            DistributedMap<String, String> followerMap = follower.getMap("stream-map", String.class, String.class);
            assertEquals("v-0", followerMap.getOptional("k-0", Consistency.EVENTUAL).orElse(null));
            assertEquals("v-" + (n - 1),
                    followerMap.getOptional("k-" + (n - 1), Consistency.EVENTUAL).orElse(null));
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void streamContiguousUnderFirehoseNoNak() throws Exception {
        NodeInfo infoA = new NodeInfo(NodeId.of("stream-fh-a"), "localhost", 9863);
        NodeInfo infoB = new NodeInfo(NodeId.of("stream-fh-b"), "localhost", 9864);
        Path base = Files.createTempDirectory("ngrid-stream-firehose");

        try (NGridNode a = streamNode(infoA, infoB, base.resolve("a"));
                NGridNode b = streamNode(infoB, infoA, base.resolve("b"))) {
            a.start();
            b.start();
            ClusterTestUtils.awaitClusterConsensus(a, b);
            a.getQueue("firehose", String.class);
            b.getQueue("firehose", String.class);

            NGridNode leader = a.coordinator().isLeader() ? a : b;
            NGridNode follower = (leader == a) ? b : a;

            int n = 3000; // > 6x SYNC_THRESHOLD: the follower lags well past the snapshot threshold
            DistributedQueue<String> queue = leader.getQueue("firehose", String.class);
            for (int i = 0; i < n; i++) {
                queue.offer("item-" + i);
            }

            awaitApplied(follower, leader.replicationManager().getGlobalSequence(), 60_000);

            // The decisive assertions: under a firehose the stream never falls back to NAK or snapshot.
            assertEquals(0L, follower.replicationManager().getGapsDetected(),
                    "firehose must not produce a single gap in RELAY_STREAM");
            assertEquals(0L, follower.replicationManager().getSyncRequestCount(),
                    "firehose must be absorbed by the stream without any snapshot/sync");
            assertEquals(0L, follower.replicationManager().getSnapshotFallbackCount(),
                    "firehose must not trigger snapshot fallback");

            DistributedQueue<String> followerQueue = follower.getQueue("firehose", String.class);
            assertEquals("item-0", followerQueue.peek().orElse(null));
        }
    }

    private static NGridNode streamNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(1) // RELAY_STREAM is async (leader commits on its own op-log)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }

    private static void awaitApplied(NGridNode follower, long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (follower.replicationManager().getLastAppliedSequence() < target) {
            if (System.currentTimeMillis() > deadline) {
                fail("follower did not converge via stream: applied="
                        + follower.replicationManager().getLastAppliedSequence() + " target=" + target);
            }
            Thread.sleep(100);
        }
    }
}
