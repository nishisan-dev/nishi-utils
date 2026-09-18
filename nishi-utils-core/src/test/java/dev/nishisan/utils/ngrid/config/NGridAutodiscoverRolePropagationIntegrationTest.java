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

package dev.nishisan.utils.ngrid.config;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * M0 (item 1, coverage) — a client node bootstrapped via the raw-socket autodiscover handshake
 * ({@link NGridNode} {@code performAutodiscover}, not the {@code NGridNodeBuilder.seed(...)} path)
 * must have its {@code roles}/{@code priority} carried by that very first handshake, so the seed's
 * {@link dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator} learns them from the
 * start. Mirrors the setup of {@link NGridAutodiscoverIntegrationTest}, but the client's YAML sets
 * {@code node.roles: [leader-ineligible]} and {@code node.priority: 100} while the seed has
 * priority 10 — the seed must remain leader and see the client's real roles/priority.
 */
class NGridAutodiscoverRolePropagationIntegrationTest {

    @TempDir
    Path tempDir;

    private NGridNode seedNode;
    private NGridNode clientNode;
    private Path clientConfigFile;
    private int seedPort;

    @BeforeEach
    void setUp() throws IOException {
        seedPort = allocateFreeLocalPort();
        int clientPort = allocateFreeLocalPort(Set.of(seedPort));

        // === Seed node (programmatic YAML, priority 10, eligible) ===
        Path seedConfigFile = tempDir.resolve("seed.yaml");
        NGridYamlConfig seedConfig = new NGridYamlConfig();

        NodeIdentityConfig seedIdentity = new NodeIdentityConfig();
        seedIdentity.setId("seed");
        seedIdentity.setHost("127.0.0.1");
        seedIdentity.setPort(seedPort);
        seedIdentity.setPriority(10);
        NodeIdentityConfig.DirsConfig seedDirs = new NodeIdentityConfig.DirsConfig();
        seedDirs.setBase(tempDir.resolve("seed-data").toString());
        seedIdentity.setDirs(seedDirs);
        seedConfig.setNode(seedIdentity);

        AutodiscoverConfig seedAuto = new AutodiscoverConfig();
        seedAuto.setSecret("role-propagation-secret");
        seedConfig.setAutodiscover(seedAuto);

        ClusterPolicyConfig clusterPolicy = new ClusterPolicyConfig();
        clusterPolicy.setName("role-propagation-cluster");
        ClusterPolicyConfig.ReplicationConfig rep = new ClusterPolicyConfig.ReplicationConfig();
        rep.setFactor(2);
        rep.setStrict(true);
        clusterPolicy.setReplication(rep);
        clusterPolicy.setSeeds(List.of("127.0.0.1:" + seedPort));
        seedConfig.setCluster(clusterPolicy);

        QueuePolicyConfig queuePolicy = new QueuePolicyConfig();
        queuePolicy.setName("role-propagation-queue");
        QueuePolicyConfig.RetentionConfig ret = new QueuePolicyConfig.RetentionConfig();
        ret.setPolicy("TIME_BASED");
        ret.setDuration("1h");
        queuePolicy.setRetention(ret);
        seedConfig.setQueue(queuePolicy);

        NGridConfigLoader.save(seedConfigFile, seedConfig);

        seedNode = new NGridNode(seedConfigFile);
        seedNode.start();

        // === Client node (YAML with roles + priority, autodiscover pointed at the seed) ===
        clientConfigFile = tempDir.resolve("client.yaml");
        NGridYamlConfig clientConfig = new NGridYamlConfig();

        NodeIdentityConfig clientIdentity = new NodeIdentityConfig();
        clientIdentity.setId("client");
        clientIdentity.setHost("127.0.0.1");
        clientIdentity.setPort(clientPort);
        clientIdentity.setPriority(100); // higher than the seed's, but leader-ineligible below
        clientIdentity.setRoles(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE));
        NodeIdentityConfig.DirsConfig clientDirs = new NodeIdentityConfig.DirsConfig();
        clientDirs.setBase(tempDir.resolve("client-data").toString());
        clientIdentity.setDirs(clientDirs);
        clientConfig.setNode(clientIdentity);

        AutodiscoverConfig clientAuto = new AutodiscoverConfig();
        clientAuto.setEnabled(true);
        clientAuto.setSecret("role-propagation-secret");
        clientAuto.setSeed("127.0.0.1:" + seedPort);
        clientConfig.setAutodiscover(clientAuto);

        NGridConfigLoader.save(clientConfigFile, clientConfig);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (clientNode != null) {
            clientNode.close();
        }
        if (seedNode != null) {
            seedNode.close();
        }
    }

    @Test
    @Timeout(30)
    void seedSeesClientRealRolesAndPriorityAfterAutodiscoverAndKeepsLeadership() throws Exception {
        // A single start() call runs performAutodiscover() (raw-socket handshake + config fetch)
        // and then immediately starts the real TcpTransport-based join in the same call.
        clientNode = new NGridNode(clientConfigFile);
        clientNode.start();

        awaitClusterFormed();

        NodeInfo seedLeader = seedNode.coordinator().leaderInfo()
                .orElseThrow(() -> new AssertionError("seed should have elected a leader"));
        assertEquals(seedNode.transport().local().nodeId(), seedLeader.nodeId(),
                "the seed (priority 10, eligible) must remain leader — the client is leader-ineligible"
                        + " despite its higher priority");

        NodeInfo clientAsSeenBySeed = seedNode.coordinator().activeMembers().stream()
                .filter(n -> n.nodeId().equals(clientNode.transport().local().nodeId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("seed should see the client in activeMembers()"));

        assertEquals(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), clientAsSeenBySeed.roles(),
                "the seed should see the client's real roles after the autodiscover handshake");
        assertEquals(100, clientAsSeenBySeed.priority(),
                "the seed should see the client's real priority after the autodiscover handshake");
    }

    private void awaitClusterFormed() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            boolean seedReady = seedNode.coordinator().activeMembers().size() == 2
                    && seedNode.coordinator().leaderInfo().isPresent();
            boolean clientReady = clientNode.coordinator().activeMembers().size() == 2
                    && clientNode.coordinator().leaderInfo().isPresent();
            if (seedReady && clientReady) {
                return;
            }
            Thread.sleep(200);
        }
        fail("seed+client cluster did not converge in time (seed activeMembers="
                + seedNode.coordinator().activeMembers().size()
                + ", client activeMembers=" + clientNode.coordinator().activeMembers().size() + ")");
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
