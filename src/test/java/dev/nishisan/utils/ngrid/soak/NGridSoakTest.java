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

package dev.nishisan.utils.ngrid.soak;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Long-running soak test for NGrid cluster resilience under sustained load
 * with controlled leader churn.
 *
 * <p>
 * This test validates that the cluster maintains data integrity (zero
 * duplicates, zero losses) under continuous operation with periodic leader
 * kills and restarts.
 * </p>
 *
 * <p>
 * <b>Configuration via System Properties:</b>
 * </p>
 * <ul>
 * <li>{@code ngrid.soak.durationMinutes} — test duration (default: 10)</li>
 * <li>{@code ngrid.soak.opsPerSecond} — target throughput (default: 100)</li>
 * <li>{@code ngrid.soak.churnIntervalSeconds} — interval between leader kills
 * (default: 45)</li>
 * </ul>
 *
 * <p>
 * Activation: requires {@code -Dngrid.soak.enabled=true}
 * </p>
 *
 * @since 2.1.0
 */
@EnabledIfSystemProperty(named = "ngrid.soak.enabled", matches = "true")
class NGridSoakTest {

    private static final Logger LOG = Logger.getLogger(NGridSoakTest.class.getName());
    private static final String QUEUE_NAME = "soak-queue";

    // ── Configuration ──
    private final int durationMinutes = Integer.getInteger("ngrid.soak.durationMinutes", 10);
    private final int opsPerSecond = Integer.getInteger("ngrid.soak.opsPerSecond", 100);
    private final int churnIntervalSeconds = Integer.getInteger("ngrid.soak.churnIntervalSeconds", 45);

    @Test
    void soakTestWithLeaderChurn() throws Exception {
        LOG.info("[SOAK] Starting soak test: duration=" + durationMinutes + "min, ops/s=" + opsPerSecond
                + ", churn=" + churnIntervalSeconds + "s");

        SoakTestReporter reporter = new SoakTestReporter();

        // ── Setup cluster ──
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        NodeInfo info1 = new NodeInfo(NodeId.of("soak-1"), "127.0.0.1", port1);
        NodeInfo info2 = new NodeInfo(NodeId.of("soak-2"), "127.0.0.1", port2);
        NodeInfo info3 = new NodeInfo(NodeId.of("soak-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-soak-test");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(10);
        Duration heartbeat = Duration.ofMillis(200);

        NGridNode[] nodes = new NGridNode[3];
        NodeInfo[] infos = { info1, info2, info3 };
        Path[] dirs = { dir1, dir2, dir3 };

        // Create all nodes
        for (int i = 0; i < 3; i++) {
            nodes[i] = createNode(infos[i], infos, dirs[i], opTimeout, heartbeat);
            nodes[i].start();
        }

        awaitClusterStability(nodes, infos);

        // Pre-create queues on all nodes
        for (NGridNode node : nodes) {
            node.getQueue(QUEUE_NAME, String.class);
        }

        // ── Control flags ──
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong offeredCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        ConcurrentHashMap<String, Boolean> consumed = new ConcurrentHashMap<>();
        AtomicLong duplicateCount = new AtomicLong(0);

        long deadlineMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(durationMinutes);

        // ── Producer thread ──
        Thread producer = new Thread(() -> {
            long intervalNanos = 1_000_000_000L / opsPerSecond;
            while (running.get() && System.currentTimeMillis() < deadlineMs) {
                long seqNum = offeredCount.incrementAndGet();
                String item = "soak-" + seqNum;
                try {
                    NGridNode leader = findLeader(nodes);
                    if (leader == null) {
                        errorCount.incrementAndGet();
                        Thread.sleep(500);
                        continue;
                    }
                    long start = System.nanoTime();
                    DistributedQueue<String> queue = leader.getQueue(QUEUE_NAME, String.class);
                    queue.offer(item);
                    reporter.recordOfferLatency(System.nanoTime() - start);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    LOG.log(Level.FINE, "[SOAK] Offer error for " + item + ": " + e.getMessage());
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        break;
                    }
                }
                // Throttle
                long sleep = intervalNanos / 1_000_000;
                if (sleep > 0) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            LOG.info("[SOAK] Producer stopped. Total offered: " + offeredCount.get());
        }, "soak-producer");

        // ── Consumer thread ──
        Thread consumer = new Thread(() -> {
            while (running.get() && System.currentTimeMillis() < deadlineMs) {
                for (NGridNode node : nodes) {
                    if (node == null)
                        continue;
                    try {
                        DistributedQueue<String> queue = node.getQueue(QUEUE_NAME, String.class);
                        while (true) {
                            var item = queue.poll();
                            if (item.isEmpty())
                                break;
                            String val = item.get();
                            Boolean previous = consumed.put(val, Boolean.TRUE);
                            if (previous != null) {
                                duplicateCount.incrementAndGet();
                                LOG.warning("[SOAK] DUPLICATE detected: " + val);
                            }
                        }
                    } catch (Exception e) {
                        LOG.log(Level.FINE, "[SOAK] Consumer error: " + e.getMessage());
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    break;
                }
            }
            LOG.info("[SOAK] Consumer stopped. Total consumed: " + consumed.size());
        }, "soak-consumer");

        // ── Churn thread ──
        Thread churn = new Thread(() -> {
            while (running.get() && System.currentTimeMillis() < deadlineMs) {
                try {
                    int jitter = ThreadLocalRandom.current().nextInt(
                            churnIntervalSeconds / 2, churnIntervalSeconds);
                    Thread.sleep(TimeUnit.SECONDS.toMillis(jitter));

                    if (!running.get())
                        break;

                    // Find and kill current leader
                    int leaderIdx = -1;
                    for (int i = 0; i < nodes.length; i++) {
                        if (nodes[i] != null && nodes[i].coordinator().isLeader()) {
                            leaderIdx = i;
                            break;
                        }
                    }

                    if (leaderIdx >= 0) {
                        String nodeId = infos[leaderIdx].nodeId().value();
                        LOG.info("[SOAK] Killing leader: " + nodeId);
                        reporter.recordChurnEvent("KILL", nodeId);
                        closeQuietly(nodes[leaderIdx]);
                        nodes[leaderIdx] = null;

                        // Wait for new leader election
                        Thread.sleep(3000);

                        // Restart the node
                        LOG.info("[SOAK] Restarting node: " + nodeId);
                        reporter.recordChurnEvent("RESTART", nodeId);
                        nodes[leaderIdx] = createNode(infos[leaderIdx], infos,
                                dirs[leaderIdx], opTimeout, heartbeat);
                        nodes[leaderIdx].start();

                        // Wait for cluster to stabilize
                        Thread.sleep(5000);

                        // Re-create queue on restarted node
                        nodes[leaderIdx].getQueue(QUEUE_NAME, String.class);
                    }
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "[SOAK] Churn error", e);
                }
            }
            LOG.info("[SOAK] Churn thread stopped.");
        }, "soak-churn");

        // ── Metrics thread ──
        Thread metrics = new Thread(() -> {
            while (running.get() && System.currentTimeMillis() < deadlineMs) {
                try {
                    Thread.sleep(60_000); // Every 60 seconds
                    for (NGridNode node : nodes) {
                        if (node != null) {
                            try {
                                reporter.recordSnapshot(node.operationalSnapshot());
                            } catch (Exception e) {
                                LOG.log(Level.FINE, "[SOAK] Snapshot error", e);
                            }
                        }
                    }
                    LOG.info("[SOAK] Metrics checkpoint: offered=" + offeredCount.get()
                            + " consumed=" + consumed.size() + " duplicates=" + duplicateCount.get()
                            + " errors=" + errorCount.get());
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "soak-metrics");

        // ── Execute ──
        producer.start();
        consumer.start();
        churn.start();
        metrics.start();

        // Wait for deadline
        long remaining = deadlineMs - System.currentTimeMillis();
        if (remaining > 0) {
            Thread.sleep(remaining);
        }

        // ── Shutdown ──
        running.set(false);
        LOG.info("[SOAK] Signaling shutdown...");

        producer.join(10_000);
        consumer.join(10_000);
        churn.join(10_000);
        metrics.join(10_000);

        // ── Final drain — give consumer 30s to drain remaining items ──
        LOG.info("[SOAK] Final drain phase...");
        long drainDeadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < drainDeadline) {
            boolean drained = true;
            for (NGridNode node : nodes) {
                if (node == null)
                    continue;
                try {
                    DistributedQueue<String> queue = node.getQueue(QUEUE_NAME, String.class);
                    while (true) {
                        var item = queue.poll();
                        if (item.isEmpty())
                            break;
                        drained = false;
                        String val = item.get();
                        Boolean previous = consumed.put(val, Boolean.TRUE);
                        if (previous != null) {
                            duplicateCount.incrementAndGet();
                        }
                    }
                } catch (Exception ignored) {
                }
            }
            if (drained)
                break;
            Thread.sleep(200);
        }

        // ── Assertions ──
        long totalOffered = offeredCount.get();
        long totalConsumed = consumed.size();
        long totalDuplicates = duplicateCount.get();

        // Count lost messages
        long lostMessages = 0;
        for (long i = 1; i <= totalOffered; i++) {
            if (!consumed.containsKey("soak-" + i)) {
                lostMessages++;
            }
        }

        LOG.info("[SOAK] Final results: offered=" + totalOffered + " consumed=" + totalConsumed
                + " duplicates=" + totalDuplicates + " lost=" + lostMessages);

        boolean pass = totalDuplicates == 0 && lostMessages == 0 && totalConsumed > 0;

        // ── Generate report ──
        reporter.setFinalResults(totalOffered, totalConsumed, totalDuplicates, lostMessages, pass);
        Path reportPath = Paths.get("target", "soak-report.md");
        reporter.writeReport(reportPath);
        LOG.info("[SOAK] Report written to: " + reportPath.toAbsolutePath());

        // ── Cleanup ──
        for (NGridNode node : nodes) {
            closeQuietly(node);
        }

        // ── Final assertions ──
        assertEquals(0, totalDuplicates, "Soak test must have zero duplicates");
        assertEquals(0, lostMessages,
                "Soak test must have zero lost messages (offered=" + totalOffered
                        + ", consumed=" + totalConsumed + ")");
        assertTrue(totalConsumed > 0, "Soak test must consume at least some messages");
    }

    // ── Helpers ──

    private NGridNode createNode(NodeInfo self, NodeInfo[] allInfos, Path dir,
            Duration opTimeout, Duration heartbeat) throws IOException {
        NGridConfig.Builder builder = NGridConfig.builder(self)
                .queueDirectory(dir)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(heartbeat);

        for (NodeInfo peer : allInfos) {
            if (!peer.nodeId().equals(self.nodeId())) {
                builder.addPeer(peer);
            }
        }

        return new NGridNode(builder.build());
    }

    private NGridNode findLeader(NGridNode[] nodes) {
        for (NGridNode node : nodes) {
            if (node != null) {
                try {
                    if (node.coordinator().isLeader()) {
                        return node;
                    }
                } catch (Exception ignored) {
                }
            }
        }
        return null;
    }

    private void awaitClusterStability(NGridNode[] nodes, NodeInfo[] infos) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (System.currentTimeMillis() < deadline) {
            boolean allReady = true;
            for (NGridNode node : nodes) {
                if (node == null) {
                    allReady = false;
                    continue;
                }
                if (node.coordinator().leaderInfo().isEmpty()
                        || node.coordinator().activeMembers().size() < nodes.length) {
                    allReady = false;
                }
            }
            // Check leader agreement
            if (allReady) {
                var l0 = nodes[0].coordinator().leaderInfo();
                boolean agree = true;
                for (int i = 1; i < nodes.length; i++) {
                    if (!l0.equals(nodes[i].coordinator().leaderInfo())) {
                        agree = false;
                    }
                }
                if (agree) {
                    LOG.info("[SOAK] Cluster stabilized. Leader: "
                            + l0.map(l -> l.nodeId().value()).orElse("none"));
                    return;
                }
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                break;
            }
        }
        throw new IllegalStateException("[SOAK] Cluster did not stabilize in time");
    }

    private void closeQuietly(NGridNode node) {
        if (node == null)
            return;
        try {
            node.close();
        } catch (IOException ignored) {
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        return allocateFreeLocalPort(Set.of());
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }
}
