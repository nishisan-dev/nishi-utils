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

package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * I2 — a client node that joins via {@code NGridNodeBuilder.seed(...)} with {@code roles(...)} and
 * {@code priority(...)} must have both fields correctly propagated to the seed by the real
 * {@code TcpTransport} handshake (not just present in the local config): the seed must never elect
 * the ineligible client as leader, even with a higher priority, and must see the client's correct
 * roles in {@code activeMembers()} after the connection.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SeedClientRolePropagationIntegrationTest {

    private NGridNode seed;
    private NGridNode client;

    @AfterEach
    void tearDown() {
        closeQuietly(client);
        closeQuietly(seed);
    }

    @Test
    void seedNeverElectsIneligibleClientEvenWithHigherPriorityAndSeesItsRolesAfterHandshake(
            @TempDir Path tempDir) throws Exception {
        int seedPort = allocateFreeLocalPort();

        seed = NGrid.node("127.0.0.1", seedPort)
                .id("seed")
                .priority(10)
                .dataDir(tempDir.resolve("seed"))
                .start();

        client = NGrid.node("127.0.0.1", 0)
                .id("client")
                .seed("127.0.0.1:" + seedPort)
                .roles(NodeInfo.ROLE_LEADER_INELIGIBLE)
                .priority(100)
                .dataDir(tempDir.resolve("client"))
                .start();

        awaitClusterFormed();

        // The seed (priority 10, eligible) remains leader even though the client advertises
        // priority 100 — because the client is leader-ineligible.
        Optional<NodeInfo> seedLeader = seed.coordinator().leaderInfo();
        assertTrue(seedLeader.isPresent(), "a leader should be present");
        assertEquals(seed.transport().local().nodeId(), seedLeader.get().nodeId(),
                "the seed should remain leader — the client is ineligible despite its higher priority");

        // The seed sees the client's NodeInfo, UPDATED by the real handshake (not a placeholder
        // without roles/priority), in activeMembers().
        NodeInfo clientAsSeenBySeed = seed.coordinator().activeMembers().stream()
                .filter(n -> n.nodeId().equals(client.transport().local().nodeId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the seed should see the client in activeMembers()"));

        assertEquals(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), clientAsSeenBySeed.roles(),
                "the seed should see the client's real roles after the handshake");
        assertEquals(100, clientAsSeenBySeed.priority(),
                "the seed should see the client's real priority after the handshake");
    }

    private void awaitClusterFormed() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean seedReady = seed.coordinator().activeMembers().size() == 2
                    && seed.coordinator().leaderInfo().isPresent();
            boolean clientReady = client.coordinator().activeMembers().size() == 2
                    && client.coordinator().leaderInfo().isPresent();
            if (seedReady && clientReady) {
                return;
            }
            Thread.sleep(200);
        }
        fail("seed+client cluster did not converge in time (seed activeMembers="
                + seed.coordinator().activeMembers().size()
                + ", client activeMembers=" + client.coordinator().activeMembers().size() + ")");
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
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }
}
