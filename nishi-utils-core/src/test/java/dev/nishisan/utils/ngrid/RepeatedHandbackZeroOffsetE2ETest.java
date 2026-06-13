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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression proof for the residual "offset de linhagem" the D11 handback left behind (issue tems#9,
 * D11). The canonical {@link AutomaticLeaderHandbackE2ETest} measures the offset ONCE, at a quiescent
 * instant, after a SINGLE handback — where the demoted incumbent was the lineage-faithful interim
 * leader, so the offset is trivially zero even without the fix. The real symptom (observed live: the
 * new leader's applied {@code <} the follower's applied, growing under load, "counter-scale desync"
 * logged forever) only appears across REPEATED handbacks under a sustained firehose, where the demoted
 * incumbent keeps its pre-handback odometer base (no re-anchor) and the follower's blind apply-count
 * drifts on relay re-pulls.
 *
 * <p>This test drives several consecutive idas-e-voltas under a non-stop firehose and asserts that
 * after EACH cycle the lineage offset is exactly zero ({@code leader.applied == leader.global ==
 * follower.applied}) and that the {@code ClusterCoordinator} "peer watermark above its own applied"
 * warning never fires. It fails on the pre-fix code (the offset reappears/accumulates) and passes with
 * the demoted-incumbent re-anchor (fix A) plus the lineage-faithful follower odometer (fix B).
 */
class RepeatedHandbackZeroOffsetE2ETest {

    private static final String QUEUE = "d11-repeat-queue";
    private static final int CYCLES = 4;
    private static final int MIN_OPS_PER_PHASE = 80;
    private static final String DESYNC_MARKER = "peer watermark above its own applied";
    private static final String CC_LOGGER = "dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator";

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void repeatedHandbacksUnderFirehoseKeepZeroOffsetAndNoLeaderBehindWarning() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("d11-repeat-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        // Catch the counter-scale desync warning from EITHER node (JUL loggers are shared by name).
        DesyncWarningSentinel sentinel = new DesyncWarningSentinel();
        Logger ccLogger = Logger.getLogger(CC_LOGGER);
        ccLogger.setLevel(Level.ALL);
        ccLogger.addHandler(sentinel);

        AtomicReference<Producer> producerRef = new AtomicReference<>();
        HandbackProbe probe1 = new HandbackProbe(producerRef);
        HandbackProbe probe2 = new HandbackProbe(producerRef);

        Producer producer = null;
        Thread producerThread = null;
        try {
            // Deterministic boot: node-1 (preferred) leads alone, node-2 joins empty and pairs.
            node1 = newNode(info1, info2, dir1);
            node1.start();
            node1.replicationManager().addHandoverListener(probe1);
            awaitLeader(node1, 20_000);
            node1.getQueue(QUEUE, String.class);
            node2 = newNode(info2, info1, dir2);
            node2.start();
            node2.replicationManager().addHandoverListener(probe2);
            DistributedQueue<String> q2 = node2.getQueue(QUEUE, String.class);
            awaitStablePair(10_000, 90_000);

            producer = new Producer(q2);
            producerRef.set(producer);
            producerThread = new Thread(producer, "d11-repeat-firehose");
            producerThread.start();
            producer.awaitAcked(MIN_OPS_PER_PHASE, 60_000); // node-1 leads, node-2 syncs the history

            for (int cycle = 1; cycle <= CYCLES; cycle++) {
                final int c = cycle;

                // (a) node-1 leaves CLEAN with node-2 paired + caught up → node-2 promotes (failover).
                producer.pause();
                awaitApplied(node1, node1.replicationManager().getGlobalSequence(), 60_000);
                awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 60_000);
                closeQuietly(node1);
                node1 = null;
                awaitLeader(node2, 30_000);
                producer.resume();

                // (b) node-2 produces a tail node-1 does not have — the lineage that must be handed back.
                int ackedBeforeReturn = producer.ackedCount();
                producer.awaitAcked(ackedBeforeReturn + MIN_OPS_PER_PHASE, 60_000);

                // (c) node-1 returns → rejoins as follower → requests the orchestrated handback.
                node1 = newNode(info1, info2, dir1);
                node1.start();
                node1.replicationManager().addHandoverListener(probe1);
                node1.getQueue(QUEUE, String.class);
                awaitLeader(node1, 90_000); // handback completes: node-1 is the ready leader again
                producer.resume();          // un-pause if onHandoverPrepare paused it

                // Keep the firehose running a moment so the post-handback follower path (the drift seam) runs.
                producer.awaitAcked(producer.ackedCount() + MIN_OPS_PER_PHASE, 60_000);

                // (d) Drain to per-cycle quiescence and assert the lineage offset is ZERO.
                producer.pause();
                long leaderGlobal = node1.replicationManager().getGlobalSequence();
                awaitApplied(node1, leaderGlobal, 60_000);
                awaitApplied(node2, leaderGlobal, 60_000);
                sleep(500); // settle a heartbeat

                long n1Applied = node1.replicationManager().getLastAppliedSequence();
                long n1Global = node1.replicationManager().getGlobalSequence();
                long n2Applied = node2.replicationManager().getLastAppliedSequence();
                // The DEFINITIVE, deterministic proof: after each ida-e-volta the lineage offset is zero.
                // Pre-fix this accumulates (the demoted incumbent keeps its odometer base, the follower's
                // blind count drifts on re-pull); with the demoted-incumbent re-anchor (A) + the
                // lineage-faithful follower odometer (B) the three counters coincide.
                assertEquals(n1Global, n1Applied,
                        "cycle " + c + ": leader applied must equal its global (lineage offset zeroed)");
                assertEquals(n1Applied, n2Applied,
                        "cycle " + c + ": follower applied must equal the leader's applied (no residual offset) — "
                                + "leader=" + n1Applied + " follower=" + n2Applied + " Δ=" + (n2Applied - n1Applied));
                producer.resume();
            }

            // Final steady soak under the firehose: prove the warning stays silent in steady state.
            sentinel.reset();
            producer.resume();
            long soakUntil = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < soakUntil) {
                assertTrue(node1.coordinator().isLeader(), "node-1 must remain the stable leader during the soak");
                sleep(200);
            }
            assertEquals(0, sentinel.count(),
                    "steady-state desync warning must not fire after the handbacks — last=" + sentinel.last());

            producer.stop();
            producerThread.join(30_000);
            producer.rethrowUnexpected();
        } finally {
            ccLogger.removeHandler(sentinel);
            if (producer != null) {
                producer.stop();
            }
            if (producerThread != null) {
                producerThread.join(10_000);
            }
        }
    }

    /** Captures the ClusterCoordinator counter-scale desync warning from any node (shared JUL logger). */
    private static final class DesyncWarningSentinel extends Handler {
        private final AtomicInteger count = new AtomicInteger();
        private volatile String last;

        @Override
        public void publish(LogRecord record) {
            String msg = record != null ? record.getMessage() : null;
            if (msg != null && msg.contains(DESYNC_MARKER)) {
                count.incrementAndGet();
                last = msg;
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        int count() {
            return count.get();
        }

        void reset() {
            count.set(0);
            last = null;
        }

        String last() {
            return last;
        }
    }

    /** Handover probe mirroring the canonical test: pauses the producer on PREP so the watermark is clean. */
    private static final class HandbackProbe implements HandoverListener {
        private final AtomicReference<Producer> producerRef;

        HandbackProbe(AtomicReference<Producer> producerRef) {
            this.producerRef = producerRef;
        }

        @Override
        public void onHandoverPrepare() {
            Producer p = producerRef.get();
            if (p != null) {
                p.pause();
            }
        }
    }

    /** Continuous producer with transient retry; records each acked item, in order. */
    private final class Producer implements Runnable {
        private final DistributedQueue<String> queue;
        private final List<String> acked = new CopyOnWriteArrayList<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicBoolean paused = new AtomicBoolean(false);
        private final AtomicLong sequence = new AtomicLong();
        private final AtomicReference<RuntimeException> unexpected = new AtomicReference<>();

        Producer(DistributedQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (running.get()) {
                if (paused.get()) {
                    sleep(20);
                    continue;
                }
                String item = "op-" + sequence.get();
                try {
                    queue.offer(item);
                    acked.add(item);
                    sequence.incrementAndGet();
                    sleep(5);
                } catch (RuntimeException e) {
                    String name = e.getClass().getSimpleName();
                    if (name.contains("LeaderSyncing") || name.contains("LeaseExpired")
                            || name.contains("IllegalState") || name.contains("Timeout")
                            || name.contains("Quorum")) {
                        sleep(50); // transient: handover/quiesce/promotion — retry the SAME item
                        continue;
                    }
                    unexpected.set(e);
                    return;
                }
            }
        }

        void pause() {
            paused.set(true);
            sleep(300);
        }

        void resume() {
            paused.set(false);
        }

        void stop() {
            running.set(false);
        }

        int ackedCount() {
            return acked.size();
        }

        void awaitAcked(int target, long timeoutMs) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                rethrowUnexpected();
                if (acked.size() >= target) {
                    return;
                }
                sleep(100);
            }
            fail("producer did not reach " + target + " acks in " + timeoutMs + "ms (current=" + acked.size() + ")");
        }

        void rethrowUnexpected() {
            RuntimeException e = unexpected.get();
            if (e != null) {
                throw new IllegalStateException("producer died with a non-transient exception", e);
            }
        }
    }

    // ---- helpers (mirror of AutomaticLeaderHandbackE2ETest) ----

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
                .handoverMaxDuration(Duration.ofSeconds(30))
                .handoverSnapshotTimeout(Duration.ofSeconds(30))
                .handoverRequestTimeout(Duration.ofSeconds(5))
                .handoverCooldown(Duration.ofSeconds(3))
                .build());
    }

    private void awaitStablePair(long holdMs, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long stableSince = -1;
        while (System.currentTimeMillis() < deadline) {
            boolean stable = node1.coordinator().isLeader()
                    && !node1.replicationManager().isLeaderSyncing()
                    && !node2.coordinator().isLeader()
                    && node2.replicationManager().getLastAppliedSequence() >= node1
                            .replicationManager().getAdvertisedHighWatermark();
            long now = System.currentTimeMillis();
            if (stable) {
                if (stableSince < 0) {
                    stableSince = now;
                }
                if (now - stableSince >= holdMs) {
                    return;
                }
            } else {
                stableSince = -1;
            }
            sleep(100);
        }
        fail("the pair did not stabilize (node-1 leader + node-2 paired) in " + timeoutMs + "ms");
    }

    private void awaitApplied(NGridNode node, long target, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.replicationManager().getLastAppliedSequence() >= target) {
                return;
            }
            sleep(100);
        }
        fail("node did not reach applied " + target + " in " + timeoutMs + "ms (current="
                + node.replicationManager().getLastAppliedSequence() + ")");
    }

    private void awaitLeader(NGridNode node, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                return;
            }
            sleep(100);
        }
        fail("node " + node.coordinator().leaderInfo() + " did not become a ready leader in " + timeoutMs + "ms");
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
