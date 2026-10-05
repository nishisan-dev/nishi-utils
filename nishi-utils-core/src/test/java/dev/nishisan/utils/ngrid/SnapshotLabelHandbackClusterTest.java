/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.replication.ReplicationManager.TopicReplicationStatus;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
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
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — in-process reproduction of the CTP catalog incident: a snapshot served by a freshly promoted
 * leader carried a label that did not belong to the topic's own frontier, the installer re-anchored on
 * it and, once leading, numbered new operations below the cursor of a third node, which silently
 * discarded them as duplicates (lag {@code 0}).
 *
 * <p>Two topics on very different scales, as in production ({@code map:_ngrid-queue-offsets} at
 * ~4.46M, {@code map:ngrrd.catalog} at ~13.58M): a LARGE map with many writes and a SMALL map with a
 * handful. The label of the large topic must never take a value from another topic or from a scalar.
 * The large map's name ({@code catalog-e}) lands in bin 0 of the handler {@code ConcurrentHashMap}, so it
 * is the "primary topic" the pre-8.10.1 scalar handback re-anchor would have written into.
 *
 * <p>Scenario (three nodes with D11 handback, affinity A &gt; B &gt; C):
 * <ol>
 *   <li>B leads with C and produces {@code P1} operations on the large map and a few on the small one;</li>
 *   <li>A joins, takes leadership through a handback and produces {@code P2}; B and C apply
 *       {@code P1+P2}, but B's raw production counter of the large topic stays at {@code P1};</li>
 *   <li>A leaves; B is promoted without writing;</li>
 *   <li>A returns and requests the handback: B serves the snapshots. Before the fix the large topic was
 *       labeled {@code P1}; A re-anchored on it, led from there and B re-anchored along
 *       (HANDBACK_COMPLETE);</li>
 *   <li>A writes new keys on both maps the moment it takes over (as the production application does).
 *       C, whose cursor is {@code P1+P2}, must receive them.</li>
 * </ol>
 *
 * <p>In production the trigger was the stalemate escape and the dual-leader resolution (D9/D10c), not a
 * handback; the defect is the same — the label of a {@code SYNC_RESPONSE} served by a freshly promoted
 * leader — and the handback is the deterministic way to hand leadership to the node that installed that
 * snapshot. The cross-topic scalar path (a pre-8.8.0 candidate) cannot be produced by an all-8.10.1
 * cluster and is covered at the {@code ReplicationManager} level by
 * {@code LegacyHandbackCompleteCrossTopicTest}.
 *
 * <p>The convergence deadline of C ({@link #CONVERGENCE_TIMEOUT_MS}) is below the minimum duration of
 * the follower-ahead self-heal (30 s): this test proves the label fix, not the safety net.
 */
class SnapshotLabelHandbackClusterTest {

    private static final String LARGE_MAP = "catalog-e";
    private static final String LARGE_TOPIC = MapClusterService.TOPIC_PREFIX + LARGE_MAP;
    private static final String SMALL_MAP = "offsets";
    private static final String SMALL_TOPIC = MapClusterService.TOPIC_PREFIX + SMALL_MAP;
    private static final int PHASE1_OPS = 30;
    private static final int PHASE2_OPS = 40;
    private static final int SMALL_OPS = 3;
    private static final int NEW_OPS = 20;
    private static final long CONVERGENCE_TIMEOUT_MS = 15_000L;

    private final List<NGridNode> running = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (NGridNode node : running) {
            closeQuietly(node);
        }
        running.clear();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void terceiroNoRecebeAsEscritasDoLiderQueInstalouOSnapshotDeUmLiderRecemPromovido() throws Exception {
        Set<Integer> ports = new HashSet<>();
        NodeInfo infoA = new NodeInfo(NodeId.of("label-a"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 100);
        NodeInfo infoB = new NodeInfo(NodeId.of("label-b"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 50);
        NodeInfo infoC = new NodeInfo(NodeId.of("label-c"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 10);
        Path base = Files.createTempDirectory("ngrid-snapshot-label");
        Path dirA = Files.createDirectories(base.resolve("a"));
        Path dirB = Files.createDirectories(base.resolve("b"));
        Path dirC = Files.createDirectories(base.resolve("c"));

        // (1) B and C start without A; B (highest affinity present) leads and produces P1.
        NGridNode b = start(newNode(infoB, dirB, infoA, infoC));
        NGridNode c = start(newNode(infoC, dirC, infoA, infoB));
        awaitReadyLeader(b, 60_000);
        DistributedMap<String, String> largeB = largeMap(b);
        DistributedMap<String, String> smallB = smallMap(b);
        largeMap(c);
        smallMap(c);
        for (int i = 0; i < PHASE1_OPS; i++) {
            putWithRetry(largeB, "p1-" + i, "v");
        }
        for (int i = 0; i < SMALL_OPS; i++) {
            putWithRetry(smallB, "s-" + i, "v");
        }
        awaitFrontier(c, LARGE_TOPIC, PHASE1_OPS, 30_000);
        awaitFrontier(c, SMALL_TOPIC, SMALL_OPS, 30_000);

        // (2) A joins and takes leadership through a handback; it produces P2 on the large map.
        NGridNode a = start(newNode(infoA, dirA, infoB, infoC));
        largeMap(a);
        smallMap(a);
        awaitReadyLeader(a, 120_000);
        awaitFollowerOf(b, a, 30_000);
        awaitFollowerOf(c, a, 30_000);
        DistributedMap<String, String> largeA = largeMap(a);
        for (int i = 0; i < PHASE2_OPS; i++) {
            putWithRetry(largeA, "p2-" + i, "v");
        }
        long largeAfterPhase2 = frontier(a, LARGE_TOPIC);
        long smallAfterPhase2 = frontier(a, SMALL_TOPIC);
        assertTrue(largeAfterPhase2 >= PHASE1_OPS + PHASE2_OPS,
                "A must have numbered its writes above P1 (frontier=" + largeAfterPhase2 + ")");
        assertTrue(smallAfterPhase2 < largeAfterPhase2, "the two topics must run on different scales");
        awaitFrontier(b, LARGE_TOPIC, largeAfterPhase2, 30_000);
        awaitFrontier(c, LARGE_TOPIC, largeAfterPhase2, 30_000);

        // (3) A leaves; B is promoted without writing.
        closeQuietly(a);
        running.remove(a);
        awaitReadyLeader(b, 60_000);
        awaitFollowerOf(c, b, 30_000);
        assertEquals(largeAfterPhase2, frontier(b, LARGE_TOPIC), "the promoted B holds everything it applied");

        // (4) A returns and requests the handback from B, which serves freshly promoted snapshots.
        NGridNode aBack = start(newNode(infoA, dirA, infoB, infoC));
        DistributedMap<String, String> largeABack = largeMap(aBack);
        DistributedMap<String, String> smallABack = smallMap(aBack);

        // (5) Like the production application, A writes the moment it takes over: the first write lands
        // before the next recompute, so the fresh-leader yield (revisão #178, which defers to a peer that
        // is ahead) cannot mask a wrong numbering.
        awaitCondition(() -> aBack.coordinator().isLeader(), 120_000, 2L,
                () -> "A did not take leadership through the handback");
        // A's frontiers right after the cutover, before any write: the labels B served.
        long largeCutover = frontier(aBack, LARGE_TOPIC);
        long smallCutover = frontier(aBack, SMALL_TOPIC);
        for (int i = 0; i < NEW_OPS; i++) {
            putWithRetry(largeABack, "new-" + i, "v");
            putWithRetry(smallABack, "new-" + i, "v");
        }
        awaitReadyLeader(aBack, 30_000);
        awaitFollowerOf(b, aBack, 30_000);
        awaitFollowerOf(c, aBack, 30_000);
        long largeLeader = frontier(aBack, LARGE_TOPIC);
        long smallLeader = frontier(aBack, SMALL_TOPIC);
        for (String topic : List.of(LARGE_TOPIC, SMALL_TOPIC)) {
            DistributedMap<String, String> mapC = LARGE_TOPIC.equals(topic) ? largeMap(c) : smallMap(c);
            long leaderFrontier = LARGE_TOPIC.equals(topic) ? largeLeader : smallLeader;
            awaitCondition(() -> missingNewKeys(mapC).isEmpty(), CONVERGENCE_TIMEOUT_MS,
                    () -> "C did not receive the leader's new writes on " + topic + ": missing "
                            + missingNewKeys(mapC) + " (C cursor=" + c.replicationManager().getRelayStreamCursor(topic)
                            + ", leader frontier=" + leaderFrontier + ", C lag="
                            + c.replicationManager().getReplicationLag(topic) + ")");
            awaitFrontier(c, topic, leaderFrontier, CONVERGENCE_TIMEOUT_MS);
            assertTrue(c.replicationManager().getRelayStreamCursor(topic) <= leaderFrontier,
                    "C's cursor must not stay above the leader's frontier on " + topic);
            DistributedMap<String, String> mapB = LARGE_TOPIC.equals(topic) ? largeMap(b) : smallMap(b);
            awaitCondition(() -> missingNewKeys(mapB).isEmpty(), CONVERGENCE_TIMEOUT_MS,
                    () -> "B (demoted interim) did not receive the new writes on " + topic + ": missing "
                            + missingNewKeys(mapB));
        }
        assertTrue(largeLeader >= largeAfterPhase2 + NEW_OPS,
                "A must number the new writes above the frontier B held (" + largeAfterPhase2
                        + "), not from a stale or foreign value (A frontier=" + largeLeader + ")");
        // The cause, checked deterministically: every snapshot B served was labeled with that topic's own
        // frontier — never with the counter of an old term nor with a value of another topic. Without the
        // fix the symptoms above depend on a race (sometimes the fresh-leader yield of revisão #178 hands
        // over to C before the first write and the ensuing leadership churn converges); the labels do not.
        assertEquals(largeAfterPhase2, largeCutover,
                "the large topic's snapshot served by the freshly promoted B must be labeled with its own"
                        + " frontier (" + largeAfterPhase2 + ")");
        assertEquals(smallAfterPhase2, smallCutover,
                "the small topic's snapshot must be labeled with its own frontier (" + smallAfterPhase2 + ")");
    }

    // ---- support ----

    private static DistributedMap<String, String> largeMap(NGridNode node) {
        return node.getMap(LARGE_MAP, String.class, String.class);
    }

    private static DistributedMap<String, String> smallMap(NGridNode node) {
        return node.getMap(SMALL_MAP, String.class, String.class);
    }

    private NGridNode start(NGridNode node) {
        node.start();
        running.add(node);
        return node;
    }

    private static NGridNode newNode(NodeInfo self, Path dir, NodeInfo... peers) {
        NGridConfig.Builder builder = NGridConfig.builder(self)
                .dataDirectory(dir)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .minClusterSize(1)
                .bootDiscoveryWindow(Duration.ofSeconds(2))
                .affinityHandbackMode(true)
                .handoverMaxDuration(Duration.ofSeconds(30))
                .handoverSnapshotTimeout(Duration.ofSeconds(30))
                .handoverRequestTimeout(Duration.ofSeconds(5))
                .handoverCooldown(Duration.ofSeconds(3));
        for (NodeInfo peer : peers) {
            builder.addPeer(peer);
        }
        return new NGridNode(builder.build());
    }

    private static List<String> missingNewKeys(DistributedMap<String, String> map) {
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < NEW_OPS; i++) {
            if (map.getOptional("new-" + i, Consistency.EVENTUAL).isEmpty()) {
                missing.add("new-" + i);
            }
        }
        return missing;
    }

    private static void putWithRetry(DistributedMap<String, String> map, String key, String value)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                map.put(key, value);
                return;
            } catch (RuntimeException e) {
                last = e; // leader in transition, draining or frozen by the handback: retry
                Thread.sleep(5);
            }
        }
        throw new AssertionError("put of " + key + " did not complete in 30 s", last);
    }

    private static long frontier(NGridNode node, String topic) {
        TopicReplicationStatus status = node.replicationManager().getTopicReplicationStatuses().get(topic);
        return status == null ? -1L : status.nextExpectedSequence() - 1L;
    }

    private static void awaitFrontier(NGridNode node, String topic, long target, long timeoutMs)
            throws InterruptedException {
        awaitCondition(() -> frontier(node, topic) >= target, timeoutMs,
                () -> "node " + node.transport().local().nodeId() + " did not apply " + topic + " up to " + target
                        + " (frontier=" + frontier(node, topic) + ")");
    }

    private static void awaitReadyLeader(NGridNode node, long timeoutMs) throws InterruptedException {
        awaitCondition(() -> node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()
                        && !node.replicationManager().isHandoverFreezing(), timeoutMs,
                () -> "node " + node.transport().local().nodeId() + " did not become a ready leader (leader seen: "
                        + node.coordinator().leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
    }

    private static void awaitFollowerOf(NGridNode follower, NGridNode leader, long timeoutMs)
            throws InterruptedException {
        NodeId leaderId = leader.transport().local().nodeId();
        awaitCondition(() -> {
            Optional<NodeId> adopted = follower.coordinator().leaderInfo().map(NodeInfo::nodeId);
            return adopted.isPresent() && adopted.get().equals(leaderId)
                    && !follower.coordinator().isLeader()
                    && follower.replicationManager().isStreaming(LARGE_TOPIC)
                    && follower.replicationManager().isStreaming(SMALL_TOPIC);
        }, timeoutMs, () -> "node " + follower.transport().local().nodeId() + " did not start following " + leaderId);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs,
            java.util.function.Supplier<String> message) throws InterruptedException {
        awaitCondition(condition, timeoutMs, 100L, message);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs, long pollMs,
            java.util.function.Supplier<String> message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(pollMs);
        }
        fail(message.get());
    }

    private static void closeQuietly(NGridNode node) {
        try {
            node.close();
        } catch (IOException ignored) {
            // teardown best-effort
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
