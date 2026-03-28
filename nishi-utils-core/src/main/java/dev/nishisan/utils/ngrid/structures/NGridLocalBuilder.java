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

package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Builder for creating a local NGrid cluster for dev/test usage.
 * <p>
 * All nodes bind to {@code 127.0.0.1} with ephemeral ports, use temporary
 * data directories, and form a full-mesh cluster automatically.
 * <p>
 * Created via {@link NGrid#local(int)}.
 *
 * @since 3.3.0
 */
public final class NGridLocalBuilder {

    private static final Duration DEFAULT_CONSENSUS_TIMEOUT = Duration.ofSeconds(30);

    private final int nodeCount;
    private final List<String> queueNames = new ArrayList<>();
    private final List<String> mapNames = new ArrayList<>();
    private Integer replicationFactor;
    private Path dataDir;
    private boolean strictConsistency = false;

    NGridLocalBuilder(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    /**
     * Adds a distributed queue to all nodes in the cluster.
     *
     * @param name the queue name
     * @return this builder
     */
    public NGridLocalBuilder queue(String name) {
        queueNames.add(Objects.requireNonNull(name, "queue name"));
        return this;
    }

    /**
     * Adds a distributed map to all nodes in the cluster.
     *
     * @param name the map name
     * @return this builder
     */
    public NGridLocalBuilder map(String name) {
        mapNames.add(Objects.requireNonNull(name, "map name"));
        return this;
    }

    /**
     * Sets the replication factor. Defaults to the number of nodes.
     *
     * @param factor the replication factor (must be ≥ 1)
     * @return this builder
     */
    public NGridLocalBuilder replication(int factor) {
        if (factor < 1) {
            throw new IllegalArgumentException("replication factor must be >= 1, got: " + factor);
        }
        this.replicationFactor = factor;
        return this;
    }

    /**
     * Sets the base data directory. Each node will get a sub-directory.
     * Defaults to a temporary directory.
     *
     * @param dir the base data directory
     * @return this builder
     */
    public NGridLocalBuilder dataDir(Path dir) {
        this.dataDir = Objects.requireNonNull(dir, "dataDir");
        return this;
    }

    /**
     * Sets the consistency mode.
     * <p>
     * Defaults to {@code false} (AP mode) for local clusters.
     *
     * @param strict {@code true} for CP mode, {@code false} for AP mode
     * @return this builder
     */
    public NGridLocalBuilder strictConsistency(boolean strict) {
        this.strictConsistency = strict;
        return this;
    }

    /**
     * Builds and starts all nodes, forming a local cluster.
     * <p>
     * This method allocates ephemeral ports, creates data directories,
     * starts all nodes, and waits for cluster consensus before returning.
     *
     * @return a running {@link NGridCluster}
     * @throws IOException          if data directories cannot be created
     * @throws InterruptedException if interrupted while waiting for consensus
     */
    public NGridCluster start() throws IOException, InterruptedException {
        int effectiveReplication = replicationFactor != null ? replicationFactor : nodeCount;

        // Allocate ephemeral ports
        Set<Integer> usedPorts = new HashSet<>();
        int[] ports = new int[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            ports[i] = allocateFreeLocalPort(usedPorts);
            usedPorts.add(ports[i]);
        }

        // Create NodeInfos
        NodeInfo[] nodeInfos = new NodeInfo[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            nodeInfos[i] = new NodeInfo(NodeId.of("local-" + (i + 1)), "127.0.0.1", ports[i]);
        }

        // Create data directories
        Path baseDir = dataDir != null ? dataDir : Files.createTempDirectory("ngrid-local");
        Path[] dataDirs = new Path[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            dataDirs[i] = Files.createDirectories(baseDir.resolve("local-" + (i + 1)));
        }

        // Build queue/map configs
        List<QueueConfig> queueConfigs = new ArrayList<>();
        for (String name : queueNames) {
            queueConfigs.add(QueueConfig.builder(name).build());
        }
        List<MapConfig> mapConfigs = new ArrayList<>();
        for (String name : mapNames) {
            mapConfigs.add(MapConfig.builder(name).build());
        }

        // Ensure at least one queue exists (required by NGridNode)
        if (queueConfigs.isEmpty()) {
            queueConfigs.add(QueueConfig.builder("default").build());
        }

        // Build and start nodes
        List<NGridNode> nodes = new ArrayList<>();
        try {
            for (int i = 0; i < nodeCount; i++) {
                NGridConfig.Builder builder = NGridConfig.builder(nodeInfos[i])
                        .dataDirectory(dataDirs[i])
                        .replicationFactor(effectiveReplication)
                        .strictConsistency(strictConsistency);

                // Add all other nodes as peers (full mesh)
                for (int j = 0; j < nodeCount; j++) {
                    if (j != i) {
                        builder.addPeer(nodeInfos[j]);
                    }
                }

                // Add queues and maps
                for (QueueConfig qc : queueConfigs) {
                    builder.addQueue(qc);
                }
                for (MapConfig mc : mapConfigs) {
                    builder.addMap(mc);
                }

                NGridNode node = new NGridNode(builder.build());
                node.start();
                nodes.add(node);
            }

            // Wait for cluster consensus
            awaitConsensus(nodes, DEFAULT_CONSENSUS_TIMEOUT);

            return new NGridCluster(nodes);
        } catch (Exception e) {
            // Cleanup on failure
            for (NGridNode node : nodes) {
                try {
                    node.close();
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            throw new IOException("Failed to start local cluster", e);
        }
    }

    /**
     * Waits for all nodes to agree on a leader and see all members.
     */
    private static void awaitConsensus(List<NGridNode> nodes, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int expectedMembers = nodes.size();

        while (System.currentTimeMillis() < deadline) {
            if (hasConsensus(nodes, expectedMembers)) {
                return;
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException("Local cluster did not reach consensus within " + timeout);
    }

    private static boolean hasConsensus(List<NGridNode> nodes, int expectedMembers) {
        // All nodes must agree on leader
        Optional<NodeInfo> firstLeader = nodes.get(0).coordinator().leaderInfo();
        if (firstLeader.isEmpty()) {
            return false;
        }
        for (NGridNode node : nodes) {
            if (!firstLeader.equals(node.coordinator().leaderInfo())) {
                return false;
            }
        }

        // All nodes must see all members
        for (NGridNode node : nodes) {
            if (node.coordinator().activeMembers().size() != expectedMembers) {
                return false;
            }
        }

        // All nodes must have transport connectivity to all others
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = 0; j < nodes.size(); j++) {
                if (i == j) continue;
                NodeId otherId = nodes.get(j).transport().local().nodeId();
                if (!nodes.get(i).transport().isConnected(otherId)) {
                    return false;
                }
            }
        }

        // Leader must not be syncing (otherwise writes will fail with
        // "Leader sync in progress")
        for (NGridNode node : nodes) {
            if (node.replicationManager() != null && node.replicationManager().isLeaderSyncing()) {
                return false;
            }
        }

        return true;
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
