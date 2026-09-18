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

package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.map.NMapPersistenceMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * M0 — {@code DistributedMap<K, record>}: a public {@code record} used as a value must survive
 * replication (put on the leader, get on the follower) and come back as an instance of the record
 * itself, never as a {@code LinkedHashMap} (the same class of regression as issue #82, verified here
 * specifically for Jackson's native {@code record} support).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class DistributedMapRecordReplicationTest {

    /**
     * Public record used as the distributed map's value, simulating a catalog
     * {@code SeriesPlacement} (the real use case for the ngrrd cluster).
     */
    public record PlacementStub(String owner, int state, long ts) {
    }

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;

    @BeforeEach
    void setUp() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        NodeInfo info1 = new NodeInfo(NodeId.of("record-1"), "127.0.0.1", port1);
        NodeInfo info2 = new NodeInfo(NodeId.of("record-2"), "127.0.0.1", port2);
        NodeInfo info3 = new NodeInfo(NodeId.of("record-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-record-replication");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(10);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .dataDirectory(dir1)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(300))
                .mapDirectory(dir1.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .build());

        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .dataDirectory(dir2)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(300))
                .mapDirectory(dir2.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .build());

        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .dataDirectory(dir3)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(300))
                .mapDirectory(dir3.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .build());

        node1.start();
        node2.start();
        node3.start();

        ClusterTestUtils.awaitClusterConsensus(node1, node2, node3);

        node1.getMap("record-map", String.class, PlacementStub.class);
        node2.getMap("record-map", String.class, PlacementStub.class);
        node3.getMap("record-map", String.class, PlacementStub.class);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    void recordValueIsPreservedAfterReplicationToTheFollower() throws Exception {
        NGridNode leader = requireLeader();
        NGridNode follower = requireFollower();

        DistributedMap<String, PlacementStub> map =
                leader.getMap("record-map", String.class, PlacementStub.class);

        PlacementStub expected = new PlacementStub("node-a", 1, 1_700_000_000_000L);
        map.put("series-1", expected);

        DistributedMap<String, PlacementStub> followerMap =
                follower.getMap("record-map", String.class, PlacementStub.class);

        // Consistency.EVENTUAL reads the follower's own LOCAL copy (unlike the STRONG default, which
        // routes to the leader) — this is the read that actually proves the replica arrived and kept
        // its concrete type.
        PlacementStub actual = awaitEventualValue(followerMap, "series-1", expected);

        assertInstanceOf(PlacementStub.class, actual,
                "the value on the follower should be a PlacementStub, not " + actual.getClass().getName());
        assertEquals(expected, actual);
    }

    /**
     * Record-specific regression: {@code entrySet()} on the follower must return record instances,
     * not {@code LinkedHashMap} (the symptom of issue #82 when the codec fails to preserve a
     * final/record value's concrete type).
     */
    @Test
    void entrySetOnFollowerReturnsRecordInstancesNotLinkedHashMap() throws Exception {
        NGridNode leader = requireLeader();
        NGridNode follower = requireFollower();

        DistributedMap<String, PlacementStub> map =
                leader.getMap("record-map", String.class, PlacementStub.class);

        map.put("series-a", new PlacementStub("node-a", 1, 10L));
        map.put("series-b", new PlacementStub("node-b", 2, 20L));

        DistributedMap<String, PlacementStub> followerMap =
                follower.getMap("record-map", String.class, PlacementStub.class);

        // entrySet() already reads the node's own local state (no routing to the leader); await both
        // entries' replica arriving instead of a fixed sleep.
        Set<Map.Entry<String, PlacementStub>> entries = awaitEntrySetSize(followerMap, 2);

        for (Map.Entry<String, PlacementStub> entry : entries) {
            assertInstanceOf(PlacementStub.class, entry.getValue(),
                    "the entrySet() value for '" + entry.getKey() + "' should be PlacementStub, not "
                            + entry.getValue().getClass().getName());
        }
    }

    private PlacementStub awaitEventualValue(DistributedMap<String, PlacementStub> map, String key,
            PlacementStub expected) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Optional<PlacementStub> value = map.getOptional(key, Consistency.EVENTUAL);
            if (value.isPresent() && expected.equals(value.get())) {
                return value.get();
            }
            sleepQuietly(100);
        }
        fail("The follower's local (EVENTUAL) replica did not converge to " + expected
                + " in time for key '" + key + "'");
        return null; // unreachable
    }

    private Set<Map.Entry<String, PlacementStub>> awaitEntrySetSize(
            DistributedMap<String, PlacementStub> map, int expectedSize) {
        long deadline = System.currentTimeMillis() + 10_000;
        Set<Map.Entry<String, PlacementStub>> last = Set.of();
        while (System.currentTimeMillis() < deadline) {
            last = map.entrySet();
            if (last.size() == expectedSize) {
                return last;
            }
            sleepQuietly(100);
        }
        fail("The follower's entrySet() did not converge to " + expectedSize
                + " entries in time (observed=" + last.size() + ")");
        return null; // unreachable
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private NGridNode requireLeader() {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (NGridNode n : new NGridNode[]{node1, node2, node3}) {
                if (n != null && n.coordinator().isLeader() && !n.replicationManager().isLeaderSyncing()) {
                    return n;
                }
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for leader");
            }
        }
        fail("No leader elected within timeout");
        return null;
    }

    private NGridNode requireFollower() {
        for (NGridNode n : new NGridNode[]{node1, node2, node3}) {
            if (n != null && !n.coordinator().isLeader()) {
                return n;
            }
        }
        fail("No follower found");
        return null;
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
