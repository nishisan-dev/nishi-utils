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
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Safety properties of the orchestrated affinity handback (issue tems#9, D11): with handback mode
 * ENABLED, (1) a genuine failover (the leader dies) must still promote the survivor via the normal
 * election — the handback gate only suppresses the watermark reclaim while a healthy DISTINCT leader
 * exists; and (2) an aborted handback (here: the app's PREP flush fails) must un-freeze and RETAIN
 * leadership on the incumbent — never a stuck-frozen leader, never a dual-leader.
 */
class HandbackSafetyE2ETest {

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void genuineFailoverStillPromotesSurvivorWithHandbackEnabled() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);
        Path baseDir = Files.createTempDirectory("d11-failover");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        node1 = newNode(info1, info2, dir1);
        node1.start();
        awaitLeader(node1, 20_000);
        DistributedQueue<String> q1 = node1.getQueue("q", String.class);
        node2 = newNode(info2, info1, dir2);
        node2.start();
        node2.getQueue("q", String.class);
        for (int i = 0; i < 50; i++) {
            offerWithRetry(q1, "op-" + i);
        }

        // The leader (higher-affinity node-1) dies: there is no healthy leader to hand back from, so
        // the survivor (lower-affinity node-2) must promote through the normal failover election.
        closeQuietly(node1);
        node1 = null;
        awaitLeader(node2, 30_000);
        assertTrue(node2.coordinator().isLeader(), "survivor must promote on a genuine failover");
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void abortedHandbackRetainsLeadershipAndUnfreezes() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);
        Path baseDir = Files.createTempDirectory("d11-abort");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        // node-1 leads, then leaves; node-2 (interim) leads and its PREP flush is rigged to FAIL,
        // forcing the handback to abort.
        AtomicBoolean aborted = new AtomicBoolean(false);
        HandoverListener failingPrep = new HandoverListener() {
            @Override
            public void onHandoverPrepare() {
                throw new IllegalStateException("simulated flush failure");
            }

            @Override
            public void onHandoverAborted(String reason) {
                aborted.set(true);
            }
        };

        node1 = newNode(info1, info2, dir1);
        node1.start();
        awaitLeader(node1, 20_000);
        DistributedQueue<String> q1 = node1.getQueue("q", String.class);
        node2 = newNode(info2, info1, dir2);
        node2.start();
        node2.replicationManager().addHandoverListener(failingPrep);
        node2.getQueue("q", String.class);
        for (int i = 0; i < 40; i++) {
            offerWithRetry(q1, "a-" + i);
        }
        awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 30_000);

        closeQuietly(node1);
        node1 = null;
        awaitLeader(node2, 30_000);
        DistributedQueue<String> q2 = node2.getQueue("q", String.class);
        for (int i = 0; i < 40; i++) {
            offerWithRetry(q2, "b-" + i);
        }

        // node-1 returns and requests a handback; node-2's PREP throws → abort → node-2 RETAINS.
        node1 = newNode(info1, info2, dir1);
        node1.start();
        node1.getQueue("q", String.class);

        // The abort must fire and node-2 must un-freeze (writes accepted again) while staying leader.
        long deadline = System.currentTimeMillis() + 40_000;
        while (System.currentTimeMillis() < deadline && !aborted.get()) {
            sleep(100);
        }
        assertTrue(aborted.get(), "the handback PREP failure must trigger onHandoverAborted");
        assertFalse(node2.replicationManager().isHandoverFreezing(),
                "the interim leader must un-freeze after the abort");
        assertTrue(node2.coordinator().isLeader(), "the interim leader must retain leadership on abort");
        // Production resumes on the retained leader (the freeze was lifted).
        boolean accepted = false;
        long writeDeadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < writeDeadline) {
            try {
                node2.getQueue("q", String.class).offer("post-abort");
                accepted = true;
                break;
            } catch (RuntimeException e) {
                sleep(200);
            }
        }
        assertTrue(accepted, "the retained leader must accept writes again after the abort");
    }

    // ---- helpers ----

    private NGridNode newNode(NodeInfo self, NodeInfo peer, Path dir) {
        return new NGridNode(NGridConfig.builder(self)
                .addPeer(peer)
                .dataDirectory(dir)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(300))
                .pairMode(true)
                .minClusterSize(1)
                .bootDiscoveryWindow(Duration.ofSeconds(2))
                .leaderPauseOnJoin(true)
                .joinQuiesceMaxDuration(Duration.ofSeconds(10))
                .affinityHandbackMode(true)
                .handoverMaxDuration(Duration.ofSeconds(20))
                .handoverRequestTimeout(Duration.ofSeconds(5))
                .handoverCooldown(Duration.ofSeconds(2))
                .build());
    }

    private void offerWithRetry(DistributedQueue<String> queue, String item) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                queue.offer(item);
                return;
            } catch (RuntimeException e) {
                String name = e.getClass().getSimpleName();
                if (name.contains("LeaderSyncing") || name.contains("LeaseExpired")
                        || name.contains("IllegalState") || name.contains("Timeout")
                        || name.contains("Quorum")) {
                    sleep(50);
                    continue;
                }
                throw e;
            }
        }
        fail("offer did not succeed in time for " + item);
    }

    private void awaitApplied(NGridNode node, long target, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.replicationManager().getLastAppliedSequence() >= target) {
                return;
            }
            sleep(100);
        }
        fail("node did not reach applied " + target + " in " + timeoutMs + "ms");
    }

    private void awaitLeader(NGridNode node, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                return;
            }
            sleep(100);
        }
        fail("node did not become a ready leader in " + timeoutMs + "ms");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void closeQuietly(NGridNode node) {
        if (node == null) {
            return;
        }
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
        throw new IOException("Unable to allocate a free local port after multiple attempts");
    }
}
