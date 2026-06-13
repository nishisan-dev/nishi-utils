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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * E2E proof of the orchestrated automatic affinity handback (issue tems#9, D11): when the
 * highest-affinity node returns to a cluster that already has a healthy leader, it must NOT reclaim
 * leadership through the lineage-blind watermark gates — it requests an explicit stop-the-world
 * snapshot handover. The incumbent freezes production, the app flushes, a full snapshot transfers,
 * the candidate cuts over (SET — counters re-anchored on the incumbent's lineage) and asserts
 * leadership, and the incumbent demotes cleanly.
 *
 * <p>Asserts the choreography actually ran (the handover callbacks fired — distinguishing it from the
 * legacy reclaim), a SINGLE clean leader transition (no dual-leader window), post-handback counter
 * coherence (the returning leader's applied == its global == the follower's applied: the lineage
 * offset is zero), and zero loss/duplication of acked items.
 */
class AutomaticLeaderHandbackE2ETest {

    private static final String QUEUE = "d11-queue";
    private static final int MIN_OPS_PHASE1 = 100;
    private static final int MIN_OPS_PHASE2 = 100;
    private static final long MAX_DUAL_LEADER_WINDOW_MS = 3_000;

    private NGridNode node1;
    private NGridNode node2;

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void preferredLeaderReturnHandsBackViaSnapshotWithZeroOffsetAndNoLoss() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        NodeInfo info1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", port1, Set.of(), 100);
        NodeInfo info2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", port2, Set.of(), 50);

        Path baseDir = Files.createTempDirectory("d11-e2e");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));

        AtomicReference<Producer> producerRef = new AtomicReference<>();
        HandbackProbe probe1 = new HandbackProbe(producerRef); // candidate side (records onPromotionComplete)
        HandbackProbe probe2 = new HandbackProbe(producerRef); // interim-leader side (prepare/demote + pause)

        // Sequential, deterministic boot (mirror of the D10 harness): node-1 leads alone, node-2 joins
        // empty. Boot churn would create discarded branches whose ops inflate the scalar counters.
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

        Producer producer = new Producer(q2);
        producerRef.set(producer);
        Thread producerThread = new Thread(producer, "d11-firehose");
        producerThread.start();

        try {
            // (1) node-1 leads under the firehose; node-2 syncs the history.
            producer.awaitAcked(MIN_OPS_PHASE1, 60_000);

            // (2) node-1 leaves CLEAN with node-2 paired; node-2 promotes (genuine failover, untouched).
            producer.pause();
            awaitApplied(node1, node1.replicationManager().getGlobalSequence(), 60_000);
            awaitApplied(node2, node1.replicationManager().getGlobalSequence(), 60_000);
            closeQuietly(node1);
            node1 = null;
            awaitLeader(node2, 30_000);
            producer.resume();

            // (3) node-2 produces the tail as the interim leader — a lineage node-1 does not have.
            int ackedBeforeRejoin = producer.ackedCount();
            producer.awaitAcked(ackedBeforeRejoin + MIN_OPS_PHASE2, 60_000);

            // (4) node-1 returns. With handback mode it rejoins as a follower and requests an explicit
            // handover instead of reclaiming via the watermark gates.
            node1 = newNode(info1, info2, dir1);
            node1.start();
            node1.replicationManager().addHandoverListener(probe1);
            node1.getQueue(QUEUE, String.class);
            DualLeaderWatcher watcher = new DualLeaderWatcher();
            Thread watcherThread = new Thread(watcher, "d11-dual-watcher");
            watcherThread.start();

            // (5) The handback completes: node-1 becomes the ready leader.
            awaitLeader(node1, 90_000);
            producer.resume(); // un-pause if the handover's onHandoverPrepare paused it

            // (6) Stability beyond a few heartbeats.
            long stableUntil = System.currentTimeMillis() + 4_000;
            while (System.currentTimeMillis() < stableUntil) {
                assertTrue(node1.coordinator().isLeader(), "the handed-back leadership must be stable");
                sleep(100);
            }

            watcher.stop();
            watcherThread.join(5_000);
            producer.stop();
            producerThread.join(30_000);
            producer.rethrowUnexpected();

            // (7) The handback CHOREOGRAPHY actually ran (distinguishes it from the legacy reclaim):
            // the incumbent prepared + demoted, and the candidate completed its promotion.
            assertTrue(probe2.prepared.get(), "interim leader should have run onHandoverPrepare");
            assertTrue(probe2.demoted.get(), "interim leader should have run onDemotedAfterHandover");
            assertTrue(probe1.promoted.get(), "candidate should have run onPromotionComplete");

            // (8) Never two leaders beyond a transient overlap.
            assertTrue(watcher.maxDualWindowMs() < MAX_DUAL_LEADER_WINDOW_MS,
                    "max continuous dual-leader window was " + watcher.maxDualWindowMs()
                            + "ms — the handover must be a single clean transition");

            // (9) Counter coherence: the snapshot cutover SET re-anchored the returning leader on the
            // incumbent's lineage — its applied == its global (no lineage offset), and the follower
            // converges to the same frontier.
            assertEquals(node1.replicationManager().getGlobalSequence(),
                    node1.replicationManager().getLastAppliedSequence(),
                    "returning leader applied must equal its global (lineage offset zeroed)");
            awaitApplied(node2, node1.replicationManager().getLastAppliedSequence(), 60_000);
            assertEquals(node1.replicationManager().getLastAppliedSequence(),
                    node2.replicationManager().getLastAppliedSequence(),
                    "follower applied must equal the leader's applied (no residual offset)");

            // (10) CONTENT PROOF: every acked item present exactly once in the final leader.
            DistributedQueue<String> q1b = node1.getQueue(QUEUE, String.class);
            List<String> drained = drainQueue(q1b, 120_000);
            Set<String> drainedSet = new HashSet<>(drained);
            assertEquals(drained.size(), drainedSet.size(), "no duplicates in the final leader's queue");
            List<String> lost = new ArrayList<>();
            for (String acked : producer.ackedItems()) {
                if (!drainedSet.contains(acked)) {
                    lost.add(acked);
                }
            }
            assertTrue(lost.isEmpty(), "acked items lost across the handback (" + lost.size() + "): "
                    + lost.subList(0, Math.min(10, lost.size())) + " — acked=" + producer.ackedCount()
                    + ", drained=" + drained.size());
        } finally {
            producer.stop();
            producerThread.join(10_000);
        }
    }

    /**
     * Handover probe: records the choreography callbacks. On the interim leader, onHandoverPrepare
     * pauses the producer and drains briefly — mirroring the real "stop consuming + flush the pipe"
     * so the frozen watermark captures every acked op (the lib test's external producer is the
     * analog of Cardinal's Kafka consumer).
     */
    private static final class HandbackProbe implements HandoverListener {
        private final AtomicReference<Producer> producerRef;
        final AtomicBoolean prepared = new AtomicBoolean(false);
        final AtomicBoolean demoted = new AtomicBoolean(false);
        final AtomicBoolean promoted = new AtomicBoolean(false);

        HandbackProbe(AtomicReference<Producer> producerRef) {
            this.producerRef = producerRef;
        }

        @Override
        public void onHandoverPrepare() {
            prepared.set(true);
            Producer p = producerRef.get();
            if (p != null) {
                p.pause(); // stop new offers + let an in-flight one settle before the snapshot watermark
            }
        }

        @Override
        public void onDemotedAfterHandover(NodeId newLeader) {
            demoted.set(true);
        }

        @Override
        public void onPromotionComplete() {
            promoted.set(true);
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
            sleep(300); // let an in-flight offer settle before the snapshot watermark is captured
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

        List<String> ackedItems() {
            return new ArrayList<>(acked);
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
            fail("producer did not reach " + target + " acks in " + timeoutMs + "ms (current="
                    + acked.size() + ")");
        }

        void rethrowUnexpected() {
            RuntimeException e = unexpected.get();
            if (e != null) {
                throw new IllegalStateException("producer died with a non-transient exception", e);
            }
        }
    }

    /** Samples both nodes and measures the longest CONTINUOUS window where both answer isLeader(). */
    private final class DualLeaderWatcher implements Runnable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile long maxDualWindowMs = 0;

        @Override
        public void run() {
            long dualSince = -1;
            while (running.get()) {
                NGridNode n1 = node1;
                NGridNode n2 = node2;
                boolean dual = n1 != null && n2 != null
                        && n1.coordinator().isLeader() && n2.coordinator().isLeader();
                long now = System.currentTimeMillis();
                if (dual) {
                    if (dualSince < 0) {
                        dualSince = now;
                    }
                    maxDualWindowMs = Math.max(maxDualWindowMs, now - dualSince);
                } else {
                    dualSince = -1;
                }
                sleep(50);
            }
        }

        void stop() {
            running.set(false);
        }

        long maxDualWindowMs() {
            return maxDualWindowMs;
        }
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
                .handoverMaxDuration(Duration.ofSeconds(30))
                .handoverSnapshotTimeout(Duration.ofSeconds(30))
                .handoverRequestTimeout(Duration.ofSeconds(5))
                .handoverCooldown(Duration.ofSeconds(3))
                .build());
    }

    private List<String> drainQueue(DistributedQueue<String> queue, long timeoutMs) {
        List<String> drained = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        int emptyStreak = 0;
        while (System.currentTimeMillis() < deadline && emptyStreak < 10) {
            try {
                Optional<String> item = queue.poll();
                if (item.isPresent()) {
                    drained.add(item.get());
                    emptyStreak = 0;
                } else {
                    emptyStreak++;
                    sleep(100);
                }
            } catch (RuntimeException e) {
                String name = e.getClass().getSimpleName();
                if (name.contains("LeaderSyncing") || name.contains("LeaseExpired")
                        || name.contains("IllegalState")) {
                    sleep(100);
                    continue;
                }
                throw e;
            }
        }
        return drained;
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
        fail("node " + node.coordinator().leaderInfo() + " did not become a ready leader in "
                + timeoutMs + "ms");
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
