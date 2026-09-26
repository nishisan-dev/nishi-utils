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
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.MapConfig;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression: a member that joins the mesh and then leaves cleanly (e.g. a short-lived client
 * carrying roles {@code client} + {@link NodeInfo#ROLE_LEADER_INELIGIBLE}) must not weigh on the
 * leadership quorum of the members that stay. Before the fix, the departed member lingered in the
 * transport's known-peer set and inflated the dynamic majority, so once the incumbent died the
 * survivors could no longer reach the (inflated) quorum and the cluster stayed leaderless; repeated
 * join/leave cycles made the cluster lose its leader even without any failure.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class DepartedMemberQuorumElectionTest {

    private static final String MAP_NAME = "departed-member-map";
    private static final Duration HEARTBEAT = Duration.ofMillis(200);
    private static final Duration ELECTION_BUDGET = Duration.ofSeconds(15);

    @TempDir
    Path baseDir;

    private final List<NGridNode> nodes = new ArrayList<>();
    private final List<NGridNode> clients = new ArrayList<>();
    private final Set<Integer> usedPorts = new HashSet<>();
    private final Map<String, Integer> coordinatorLogMarkers = new ConcurrentHashMap<>();
    /** Timestamped phases, per-node leadership views and coordinator warnings, for failure diagnostics. */
    private final List<String> timeline = new CopyOnWriteArrayList<>();
    private final long startedAtMs = System.currentTimeMillis();
    private final Handler coordinatorLogHandler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (message == null) {
                return;
            }
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                mark("WARN " + message);
            }
            for (String marker : List.of("Leader epoch", "refuses leadership", "outranked", "stalemate",
                    "quorum", "syncing", "stepped down", "Dual-leader", "lease expired")) {
                if (message.contains(marker)) {
                    coordinatorLogMarkers.merge(marker, 1, Integer::sum);
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    /**
     * Transport records of traffic still aimed at a peer: failed dials ("Unable to connect to",
     * "Direct connection failed for", "No connection available for") and relays dropping messages for it
     * ("Dropping relayed message") — a departed member still being dialed/routed to by every heartbeat
     * broadcast. FINE records included (the logger level is lowered for the test).
     */
    private static final List<String> TRAFFIC_TO_PEER_MARKERS = List.of("Unable to connect to",
            "Direct connection failed for", "No connection available for", "Dropping relayed message");
    private final List<String> trafficToPeerRecords = new CopyOnWriteArrayList<>();
    private final Logger transportLogger = Logger.getLogger(TcpTransport.class.getName());
    private Level previousTransportLevel;
    private final Handler transportLogHandler = new Handler() {
        private final java.util.logging.Formatter formatter = new java.util.logging.SimpleFormatter();

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (message != null && TRAFFIC_TO_PEER_MARKERS.stream().anyMatch(message::startsWith)) {
                trafficToPeerRecords.add(formatter.formatMessage(record));
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @BeforeEach
    void setUp() throws Exception {
        Logger.getLogger(ClusterCoordinator.class.getName()).addHandler(coordinatorLogHandler);
        previousTransportLevel = transportLogger.getLevel();
        transportLogger.setLevel(Level.FINE);
        transportLogger.addHandler(transportLogHandler);
        List<NodeInfo> infos = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            infos.add(new NodeInfo(NodeId.of("node-" + i), "127.0.0.1", allocateFreeLocalPort(), Set.of(), 0));
        }
        for (NodeInfo info : infos) {
            List<NodeInfo> peers = infos.stream().filter(p -> !p.equals(info)).toList();
            nodes.add(startNode(info, peers, baseDir.resolve(info.nodeId().value())));
        }
        ClusterTestUtils.awaitClusterConsensus(nodes.toArray(NGridNode[]::new));
    }

    @AfterEach
    void tearDown() {
        Logger.getLogger(ClusterCoordinator.class.getName()).removeHandler(coordinatorLogHandler);
        transportLogger.removeHandler(transportLogHandler);
        transportLogger.setLevel(previousTransportLevel);
        if (Boolean.getBoolean("ngrid.test.timeline")) {
            System.out.println(diagnostics(nodes));
        }
        clients.forEach(this::closeQuietly);
        nodes.forEach(this::closeQuietly);
    }

    @Test
    void survivorsElectAfterLeaderFailureEvenThoughAnIneligibleMemberJoinedAndLeft() throws Exception {
        DistributedMap<String, String> seedMap = nodes.get(0).getMap(MAP_NAME, String.class, String.class);
        for (int i = 0; i < 20; i++) {
            seedMap.put("seed-" + i, "v" + i);
        }

        joinWriteReadAndLeave("client-1");

        NGridNode leader = currentLeaderNode();
        List<NGridNode> survivors = nodes.stream().filter(n -> n != leader).toList();
        mark("close leader " + leader.transport().local().nodeId().value());
        leader.close();

        awaitAgreedLeaderAmong(survivors, ELECTION_BUDGET);
    }

    @Test
    void repeatedClientJoinAndLeaveDoesNotStripTheClusterOfItsLeader() throws Exception {
        for (int round = 1; round <= 3; round++) {
            joinWriteReadAndLeave("client-" + round);
            awaitAgreedLeaderAmong(nodes, ELECTION_BUDGET);
        }
        // The clients that left are gone for good: no storage keeps them as members or known peers, and
        // none keeps dialing them (each heartbeat broadcast used to, logging "No connection available").
        assertDepartedClientsForgotten(List.of("client-1", "client-2", "client-3"));
        // The leader must not merely reappear: it must stay. Observe over a few heartbeat timeouts.
        long deadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < deadline) {
            for (NGridNode node : nodes) {
                assertTrue(node.coordinator().leaderInfo().isPresent(),
                        () -> "leader vanished after client churn" + diagnostics(nodes));
            }
            Thread.sleep(100);
        }
    }

    /** A leader-ineligible client joins the mesh, writes and reads through the map, then leaves. */
    private void joinWriteReadAndLeave(String clientId) throws Exception {
        NodeInfo clientInfo = new NodeInfo(NodeId.of(clientId), "127.0.0.1", allocateFreeLocalPort(),
                Set.of("client", NodeInfo.ROLE_LEADER_INELIGIBLE), 0);
        List<NodeInfo> peers = nodes.stream().map(n -> n.transport().local()).toList();
        NGridNode client = startNode(clientInfo, peers, baseDir.resolve(clientId));
        clients.add(client);

        List<NGridNode> all = new ArrayList<>(nodes);
        all.add(client);
        ClusterTestUtils.awaitClusterConsensus(all.toArray(NGridNode[]::new));
        mark("consensus with " + clientId);

        DistributedMap<String, String> clientMap = client.getMap(MAP_NAME, String.class, String.class);
        clientMap.put(clientId + "-key", clientId + "-value");
        assertEquals(clientId + "-value", clientMap.get(clientId + "-key"));

        mark("close " + clientId);
        client.close();
        clients.remove(client);
        mark("closed " + clientId);
    }

    private void assertDepartedClientsForgotten(List<String> clientIds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && !departedClientsForgotten(clientIds)) {
            Thread.sleep(100);
        }
        assertTrue(departedClientsForgotten(clientIds),
                () -> "clientes que saíram seguem conhecidos pelos storages" + diagnostics(nodes));
        trafficToPeerRecords.clear();
        Thread.sleep(HEARTBEAT.toMillis() * 8); // several heartbeat broadcasts
        List<String> dialed = trafficToPeerRecords.stream()
                .filter(text -> clientIds.stream().anyMatch(text::contains))
                .toList();
        assertTrue(dialed.isEmpty(), () -> "storages seguem discando clientes que saíram: " + dialed);
    }

    private boolean departedClientsForgotten(List<String> clientIds) {
        for (NGridNode node : nodes) {
            boolean member = node.coordinator().activeMembers().stream()
                    .anyMatch(m -> clientIds.contains(m.nodeId().value()));
            boolean known = node.transport().peers().stream()
                    .anyMatch(p -> clientIds.contains(p.nodeId().value()));
            if (member || known) {
                return false;
            }
        }
        return true;
    }

    private NGridNode currentLeaderNode() {
        NodeId leaderId = nodes.get(0).coordinator().leaderInfo().map(NodeInfo::nodeId)
                .orElseThrow(() -> new AssertionError("no leader before the failure" + diagnostics(nodes)));
        return nodes.stream()
                .filter(n -> n.transport().local().nodeId().equals(leaderId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("leader " + leaderId + " is not one of the nodes"));
    }

    /** Waits until every node in {@code group} agrees on the same leader, which is a member of the group. */
    private void awaitAgreedLeaderAmong(List<NGridNode> group, Duration timeout) throws InterruptedException {
        Set<NodeId> groupIds = group.stream().map(n -> n.transport().local().nodeId()).collect(Collectors.toSet());
        long start = System.currentTimeMillis();
        long deadline = start + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<NodeId> first = group.get(0).coordinator().leaderInfo().map(NodeInfo::nodeId);
            boolean agreed = first.isPresent() && groupIds.contains(first.get())
                    && group.stream().allMatch(n -> n.coordinator().leaderInfo().map(NodeInfo::nodeId).equals(first));
            if (agreed) {
                return;
            }
            Thread.sleep(100);
        }
        fail("no agreed leader among " + groupIds + " within " + timeout + diagnostics(group));
    }

    private void mark(String event) {
        timeline.add(String.format("[+%6d ms] %s", System.currentTimeMillis() - startedAtMs, event));
    }

    private String diagnostics(List<NGridNode> group) {
        StringBuilder sb = new StringBuilder("\n--- diagnostics ---\n");
        for (NGridNode node : group) {
            NodeId id = node.transport().local().nodeId();
            sb.append(id).append(": leader=")
                    .append(node.coordinator().leaderInfo().map(n -> n.nodeId().value()).orElse("<none>"))
                    .append(", activeMembers=")
                    .append(node.coordinator().activeMembers().stream().map(n -> n.nodeId().value()).toList())
                    .append(", transport.peers=")
                    .append(node.transport().peers().stream()
                            .map(p -> p.nodeId().value() + ":" + p.port()).toList())
                    .append('\n');
        }
        sb.append("coordinator log markers: ").append(coordinatorLogMarkers).append('\n');
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
