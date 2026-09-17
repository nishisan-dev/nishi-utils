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

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.map.NMapPersistenceMode;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M0 — leadership eligibility by role ({@link NodeInfo#ROLE_LEADER_INELIGIBLE}). A node marked with
 * this role must never be chosen leader by any node's {@link ClusterCoordinator}, even with the
 * highest configured priority, and every node must agree on that (the role travels in
 * {@link NodeInfo} via handshake/gossip).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LeaderIneligibleRoleClusterTest {

    private static final NodeId ELIGIBLE_1_ID = NodeId.of("eligible-1");
    private static final NodeId ELIGIBLE_2_ID = NodeId.of("eligible-2");
    private static final NodeId INELIGIBLE_ID = NodeId.of("ineligible-1");

    @TempDir
    Path baseDir;

    private NGridNode eligible1;
    private NGridNode eligible2;
    private NGridNode ineligible;

    @BeforeEach
    void setUp() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        NodeInfo info1 = new NodeInfo(ELIGIBLE_1_ID, "127.0.0.1", port1, Set.of(), 10);
        NodeInfo info2 = new NodeInfo(ELIGIBLE_2_ID, "127.0.0.1", port2, Set.of(), 10);
        // Priority 100 (the highest in the cluster) and role leader-ineligible: must never be elected.
        NodeInfo info3 = new NodeInfo(INELIGIBLE_ID, "127.0.0.1", port3,
                Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), 100);

        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration heartbeat = Duration.ofMillis(200);

        eligible1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .dataDirectory(dir1)
                .replicationFactor(2)
                .heartbeatInterval(heartbeat)
                .mapDirectory(dir1.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                // pairMode + minClusterSize(1): the election is not hidden behind the dynamic-
                // majority requirement — the "nobody leads when only the ineligible node remains"
                // test needs to exercise the eligibility filter, not the quorum check.
                .pairMode(true)
                .minClusterSize(1)
                .build());

        eligible2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .dataDirectory(dir2)
                .replicationFactor(2)
                .heartbeatInterval(heartbeat)
                .mapDirectory(dir2.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .pairMode(true)
                .minClusterSize(1)
                .build());

        ineligible = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .dataDirectory(dir3)
                .replicationFactor(2)
                .heartbeatInterval(heartbeat)
                .mapDirectory(dir3.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .pairMode(true)
                .minClusterSize(1)
                .build());

        eligible1.start();
        eligible2.start();
        ineligible.start();
    }

    @AfterEach
    void tearDown() {
        closeQuietly(eligible1);
        closeQuietly(eligible2);
        closeQuietly(ineligible);
    }

    @Test
    void ineligibleNodeIsNeverElectedEvenWithHigherPriority() throws Exception {
        ClusterTestUtils.awaitClusterConsensus(eligible1, eligible2, ineligible);

        NodeId leaderId = eligible1.coordinator().leaderInfo()
                .map(NodeInfo::nodeId)
                .orElseThrow(() -> new AssertionError("a leader should be present"));

        assertNotEquals(INELIGIBLE_ID, leaderId,
                "the ineligible node should never be elected, even with the highest priority");
        assertTrue(leaderId.equals(ELIGIBLE_1_ID) || leaderId.equals(ELIGIBLE_2_ID),
                "the leader should be one of the eligible nodes, was: " + leaderId);

        // awaitClusterConsensus already guarantees agreement; reinforced here node by node.
        assertEquals(leaderId, eligible2.coordinator().leaderInfo().map(NodeInfo::nodeId).orElse(null));
        assertEquals(leaderId, ineligible.coordinator().leaderInfo().map(NodeInfo::nodeId).orElse(null));
    }

    @Test
    void ineligibleNodeDoesNotSelfElectWhenTheEligibleNodesGoDown() throws Exception {
        ClusterTestUtils.awaitClusterConsensus(eligible1, eligible2, ineligible);

        eligible1.close();
        eligible2.close();

        // With no eligible member active, the ineligible node must never self-elect, even alone and
        // with the highest configured priority. Observed over a reasonable period rather than a
        // single check, to catch both an immediate self-election and a delayed one (log/flap loop).
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(ineligible.coordinator().leaderInfo()
                            .map(NodeInfo::nodeId)
                            .filter(INELIGIBLE_ID::equals)
                            .isEmpty(),
                    "the ineligible node should never self-elect as leader");
            Thread.sleep(200);
        }
        assertTrue(ineligible.coordinator().leaderInfo().isEmpty(),
                "with no eligible members active, the cluster should end up leaderless");
    }

    private void closeQuietly(NGridNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (IOException ignored) {
            // best-effort cleanup
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
