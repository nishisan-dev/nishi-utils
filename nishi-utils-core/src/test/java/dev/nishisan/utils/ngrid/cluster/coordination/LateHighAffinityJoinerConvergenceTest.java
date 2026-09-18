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

package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.MapConfig;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression: a node of the HIGHEST leadership affinity that joins (or returns) BEHIND the cluster's
 * state — a clean data dir, or a data dir that missed writes while it was down — must neither wedge
 * the cluster into a three-way leader disagreement nor starve its own catch-up. Expected: within a
 * bounded time every member agrees on a single leader, and the joiner converges (eventual reads on
 * it return the writes it missed).
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class LateHighAffinityJoinerConvergenceTest {

    private static final String MAP_NAME = "late-joiner-map";
    private static final Duration HEARTBEAT = Duration.ofMillis(200);
    private static final Duration CONVERGENCE_BUDGET = Duration.ofSeconds(15);
    private static final int WRITES = 30;

    @TempDir
    Path baseDir;

    private final List<NGridNode> nodes = new ArrayList<>();
    private final Set<Integer> usedPorts = new HashSet<>();
    private boolean affinityHandback = false;
    private final Map<String, Integer> logMarkers = new ConcurrentHashMap<>();
    private final List<String> timeline = new CopyOnWriteArrayList<>();
    private final long startedAtMs = System.currentTimeMillis();
    private final Handler logHandler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (message == null) {
                return;
            }
            for (String marker : List.of("Leader epoch", "refuses leadership", "stalemate", "Dual-leader",
                    "stepped down", "stuck without a chunk", "Refusing RELAY_STREAM_FETCH", "Requesting sync")) {
                if (message.contains(marker)) {
                    logMarkers.merge(marker, 1, Integer::sum);
                }
            }
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                mark("WARN " + message);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @AfterEach
    void tearDown() {
        Logger.getLogger("dev.nishisan.utils.ngrid").removeHandler(logHandler);
        if (Boolean.getBoolean("ngrid.test.timeline")) {
            System.out.println(diagnostics(nodes));
        }
        nodes.forEach(this::closeQuietly);
    }

    @Test
    void cleanHighestAffinityJoinerConvergesWithoutLeaderDisagreement() throws Exception {
        Logger.getLogger("dev.nishisan.utils.ngrid").addHandler(logHandler);
        List<NodeInfo> infos = infos(3);
        // Only node-1 and node-2 start; node-3 (highest affinity by NodeId) joins later, clean.
        for (NodeInfo info : infos.subList(0, 2)) {
            nodes.add(startNode(info, peersOf(info, infos), baseDir.resolve(info.nodeId().value())));
        }
        ClusterTestUtils.awaitClusterConsensus(nodes.toArray(NGridNode[]::new));
        writeThroughLeader("early", WRITES);

        NGridNode joiner = startNode(infos.get(2), peersOf(infos.get(2), infos), baseDir.resolve("node-3"));
        nodes.add(joiner);
        mark("joined node-3");

        awaitAgreedLeaderAmong(nodes, CONVERGENCE_BUDGET);
        awaitEventualReads(joiner, "early", WRITES, CONVERGENCE_BUDGET);
    }

    @Test
    void behindHighestAffinityNodeRestartingFromItsDataDirConverges() throws Exception {
        Logger.getLogger("dev.nishisan.utils.ngrid").addHandler(logHandler);
        List<NodeInfo> infos = infos(3);
        for (NodeInfo info : infos) {
            nodes.add(startNode(info, peersOf(info, infos), baseDir.resolve(info.nodeId().value())));
        }
        ClusterTestUtils.awaitClusterConsensus(nodes.toArray(NGridNode[]::new));
        writeThroughLeader("early", WRITES);
        // RELAY_STREAM acks at the leader and followers pull asynchronously: wait until both followers
        // hold the early writes, so the failover below continues the lineage instead of forking it.
        awaitEventualReads(nodes.get(0), "early", WRITES, CONVERGENCE_BUDGET);
        awaitEventualReads(nodes.get(1), "early", WRITES, CONVERGENCE_BUDGET);

        NGridNode highest = nodes.get(2);
        mark("close node-3");
        highest.close();
        nodes.remove(highest);
        awaitAgreedLeaderAmong(nodes, CONVERGENCE_BUDGET);
        writeThroughLeader("late", WRITES); // node-3 misses these

        NGridNode returned = startNode(infos.get(2), peersOf(infos.get(2), infos), baseDir.resolve("node-3"));
        nodes.add(returned);
        mark("restarted node-3");

        awaitAgreedLeaderAmong(nodes, CONVERGENCE_BUDGET);
        awaitEventualReads(returned, "late", WRITES, CONVERGENCE_BUDGET);
    }

    @Test
    void behindHighestAffinityNodeRestartingConvergesWithOrchestratedHandback() throws Exception {
        affinityHandback = true;
        behindHighestAffinityNodeRestartingFromItsDataDirConverges();
    }

    // ---- helpers ----

    private List<NodeInfo> infos(int count) throws IOException {
        List<NodeInfo> infos = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            infos.add(new NodeInfo(NodeId.of("node-" + i), "127.0.0.1", allocateFreeLocalPort(), Set.of(), 0));
        }
        return infos;
    }

    private static List<NodeInfo> peersOf(NodeInfo self, List<NodeInfo> all) {
        return all.stream().filter(p -> !p.equals(self)).toList();
    }

    /** Writes {@code count} keys through whichever node currently leads, retrying transient handoff errors. */
    private void writeThroughLeader(String prefix, int count) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            String key = prefix + "-" + i;
            long deadline = System.currentTimeMillis() + 15_000;
            while (true) {
                try {
                    currentLeaderNode().getMap(MAP_NAME, String.class, String.class).put(key, "v" + i);
                    break;
                } catch (RuntimeException e) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("could not write " + key + diagnostics(nodes), e);
                    }
                    Thread.sleep(100);
                }
            }
        }
        mark("wrote " + count + " keys with prefix " + prefix);
    }

    private NGridNode currentLeaderNode() {
        for (NGridNode node : nodes) {
            if (node.coordinator().isLeader()) {
                return node;
            }
        }
        throw new IllegalStateException("no local leader among the running nodes");
    }

    private void awaitEventualReads(NGridNode node, String prefix, int count, Duration timeout)
            throws InterruptedException {
        DistributedMap<String, String> map = node.getMap(MAP_NAME, String.class, String.class);
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int missing = count;
        while (System.currentTimeMillis() < deadline) {
            missing = 0;
            for (int i = 0; i < count; i++) {
                Optional<String> value = map.getOptional(prefix + "-" + i, Consistency.EVENTUAL);
                if (value.isEmpty() || !value.get().equals("v" + i)) {
                    missing++;
                }
            }
            if (missing == 0) {
                mark("node " + node.transport().local().nodeId() + " converged on " + prefix + "-*");
                return;
            }
            Thread.sleep(200);
        }
        fail(node.transport().local().nodeId() + " did not converge: " + missing + "/" + count + " keys '"
                + prefix + "-*' missing after " + timeout + diagnostics(nodes));
    }

    private void awaitAgreedLeaderAmong(List<NGridNode> group, Duration timeout) throws InterruptedException {
        Set<NodeId> groupIds = group.stream().map(n -> n.transport().local().nodeId()).collect(Collectors.toSet());
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<NodeId> first = group.get(0).coordinator().leaderInfo().map(NodeInfo::nodeId);
            boolean agreed = first.isPresent() && groupIds.contains(first.get())
                    && group.stream().allMatch(n -> n.coordinator().leaderInfo().map(NodeInfo::nodeId).equals(first));
            if (agreed) {
                mark("agreed leader " + first.get());
                return;
            }
            Thread.sleep(100);
        }
        fail("no agreed leader among " + groupIds + " within " + timeout + diagnostics(group));
    }

    private int countPresent(NGridNode node, String prefix) {
        try {
            DistributedMap<String, String> map = node.getMap(MAP_NAME, String.class, String.class);
            int present = 0;
            for (int i = 0; i < WRITES; i++) {
                if (map.getOptional(prefix + "-" + i, Consistency.EVENTUAL).isPresent()) {
                    present++;
                }
            }
            return present;
        } catch (RuntimeException e) {
            return -1;
        }
    }

    private void mark(String event) {
        timeline.add(String.format("[+%6d ms] %s", System.currentTimeMillis() - startedAtMs, event));
    }

    private String diagnostics(List<NGridNode> group) {
        StringBuilder sb = new StringBuilder("\n--- diagnostics ---\n");
        for (NGridNode node : group) {
            sb.append(node.transport().local().nodeId()).append(": leader=")
                    .append(node.coordinator().leaderInfo().map(n -> n.nodeId().value()).orElse("<none>"))
                    .append(", isLeader=").append(node.coordinator().isLeader())
                    .append(", applied=").append(node.replicationManager().getLastAppliedSequence())
                    .append(", globalSeq=").append(node.replicationManager().getGlobalSequence())
                    .append(", mapCursor=").append(node.replicationManager().getRelayStreamCursor("map:" + MAP_NAME))
                    .append(", mapLeaderHwm=").append(node.replicationManager().getLeaderHighWatermark("map:" + MAP_NAME))
                    .append(", early=").append(countPresent(node, "early")).append("/").append(WRITES)
                    .append(", late=").append(countPresent(node, "late")).append("/").append(WRITES)
                    .append(", activeMembers=")
                    .append(node.coordinator().activeMembers().stream().map(n -> n.nodeId().value()).toList())
                    .append('\n');
        }
        sb.append("log markers: ").append(logMarkers).append('\n');
        sb.append("timeline:\n");
        timeline.forEach(line -> sb.append("  ").append(line).append('\n'));
        return sb.toString();
    }

    private NGridNode startNode(NodeInfo info, List<NodeInfo> peers, Path dir) throws IOException {
        Files.createDirectories(dir);
        NGridConfig.Builder builder = NGridConfig.builder(info)
                .dataDirectory(dir)
                .replicationFactor(2)
                .heartbeatInterval(HEARTBEAT)
                .affinityHandbackMode(affinityHandback)
                .mapDirectory(dir.resolve("maps"))
                .addMap(MapConfig.builder(MAP_NAME).persistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC).build());
        peers.forEach(builder::addPeer);
        NGridNode node = new NGridNode(builder.build());
        mark("start " + info.nodeId().value());
        node.start();
        node.coordinator().addLeadershipListener(newLeader -> mark(info.nodeId().value() + " sees leader="
                + (newLeader != null ? newLeader.value() : "<none>")));
        return node;
    }

    private void closeQuietly(NGridNode node) {
        try {
            node.close();
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }

    private int allocateFreeLocalPort() throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && usedPorts.add(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port after multiple attempts");
    }
}
