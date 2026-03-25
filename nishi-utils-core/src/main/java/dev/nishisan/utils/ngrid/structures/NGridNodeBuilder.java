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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builder for creating a single NGrid node for production usage.
 * <p>
 * Provides a simplified API for configuring a node with seed-based
 * autodiscovery or explicit peer lists. The gossip protocol in
 * {@code TcpTransport} ensures full cluster membership propagation
 * even when only a subset of peers is specified.
 * <p>
 * Created via {@link NGrid#node(String)} or {@link NGrid#node(String, int)}.
 *
 * @since 3.3.0
 */
public final class NGridNodeBuilder {

    private final String host;
    private final int port;
    private String nodeId;
    private String seedAddress;
    private final List<String> peerAddresses = new ArrayList<>();
    private final List<String> queueNames = new ArrayList<>();
    private final List<String> mapNames = new ArrayList<>();
    private Integer replicationFactor;
    private Path dataDir;
    private boolean strictConsistency = true;

    NGridNodeBuilder(String host, int port) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    /**
     * Sets a custom NodeId. If not set, the NodeId is derived from
     * {@code host:port}.
     *
     * @param id the custom node identifier
     * @return this builder
     */
    public NGridNodeBuilder id(String id) {
        this.nodeId = Objects.requireNonNull(id, "nodeId");
        return this;
    }

    /**
     * Sets the seed node address for autodiscovery.
     * <p>
     * The node will connect to the seed during startup to fetch
     * the full cluster configuration. The gossip protocol ensures
     * all cluster members are discovered even if only one seed is
     * provided.
     *
     * @param hostPort the seed address in {@code "host:port"} format
     * @return this builder
     */
    public NGridNodeBuilder seed(String hostPort) {
        this.seedAddress = Objects.requireNonNull(hostPort, "seed address");
        return this;
    }

    /**
     * Adds explicit peer addresses. The gossip protocol in
     * {@code TcpTransport} will propagate any additional peers
     * discovered via handshake.
     *
     * @param hostPorts peer addresses in {@code "host:port"} format
     * @return this builder
     */
    public NGridNodeBuilder peers(String... hostPorts) {
        for (String hp : hostPorts) {
            peerAddresses.add(Objects.requireNonNull(hp, "peer address"));
        }
        return this;
    }

    /**
     * Adds a distributed queue.
     *
     * @param name the queue name
     * @return this builder
     */
    public NGridNodeBuilder queue(String name) {
        queueNames.add(Objects.requireNonNull(name, "queue name"));
        return this;
    }

    /**
     * Adds a distributed map.
     *
     * @param name the map name
     * @return this builder
     */
    public NGridNodeBuilder map(String name) {
        mapNames.add(Objects.requireNonNull(name, "map name"));
        return this;
    }

    /**
     * Sets the replication factor. Defaults to 2.
     *
     * @param factor the replication factor (must be ≥ 1)
     * @return this builder
     */
    public NGridNodeBuilder replication(int factor) {
        if (factor < 1) {
            throw new IllegalArgumentException("replication factor must be >= 1, got: " + factor);
        }
        this.replicationFactor = factor;
        return this;
    }

    /**
     * Sets the data directory for queues, maps, and replication state.
     *
     * @param dir the data directory
     * @return this builder
     */
    public NGridNodeBuilder dataDir(Path dir) {
        this.dataDir = Objects.requireNonNull(dir, "dataDir");
        return this;
    }

    /**
     * Sets the consistency mode. Defaults to {@code true} (CP mode)
     * for production nodes.
     *
     * @param strict {@code true} for CP mode, {@code false} for AP mode
     * @return this builder
     */
    public NGridNodeBuilder strictConsistency(boolean strict) {
        this.strictConsistency = strict;
        return this;
    }

    /**
     * Builds and starts the node.
     * <p>
     * If no data directory is specified, the build will fail for
     * production nodes. Use {@link #dataDir(Path)} to set it.
     *
     * @return a running {@link NGridNode}
     * @throws IOException if the node cannot be started
     */
    public NGridNode start() throws IOException {
        // Resolve port (ephemeral if 0)
        int effectivePort = port > 0 ? port : allocateFreeLocalPort();

        // Generate NodeId if not set
        String effectiveNodeId = nodeId != null ? nodeId : host + ":" + effectivePort;

        NodeInfo localInfo = new NodeInfo(NodeId.of(effectiveNodeId), host, effectivePort);

        NGridConfig.Builder builder = NGridConfig.builder(localInfo)
                .strictConsistency(strictConsistency);

        if (dataDir != null) {
            builder.dataDirectory(dataDir);
        }

        if (replicationFactor != null) {
            builder.replicationFactor(replicationFactor);
        }

        // Add peers
        for (String peerAddress : peerAddresses) {
            NodeInfo peerInfo = parseHostPort(peerAddress);
            builder.addPeer(peerInfo);
        }

        // If seed is provided, add it as a peer so the transport connects
        // to it. The gossip protocol will propagate the rest of the cluster.
        if (seedAddress != null) {
            NodeInfo seedInfo = parseHostPort(seedAddress);
            builder.addPeer(seedInfo);
        }

        // Add queues
        for (String name : queueNames) {
            builder.addQueue(QueueConfig.builder(name).build());
        }

        // Add maps
        for (String name : mapNames) {
            builder.addMap(MapConfig.builder(name).build());
        }

        // Ensure at least one queue exists (required by NGridNode)
        if (queueNames.isEmpty()) {
            builder.addQueue(QueueConfig.builder("default").build());
        }

        NGridNode node = new NGridNode(builder.build());
        node.start();
        return node;
    }

    /**
     * Parses a {@code "host:port"} string into a {@link NodeInfo}.
     * The NodeId is derived from the address itself.
     */
    private static NodeInfo parseHostPort(String hostPort) {
        Objects.requireNonNull(hostPort, "hostPort");
        int colonIndex = hostPort.lastIndexOf(':');
        if (colonIndex <= 0 || colonIndex == hostPort.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid address format '" + hostPort + "'. Expected 'host:port'.");
        }
        String h = hostPort.substring(0, colonIndex);
        int p;
        try {
            p = Integer.parseInt(hostPort.substring(colonIndex + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid port in address '" + hostPort + "'.", e);
        }
        return new NodeInfo(NodeId.of(hostPort), h, p);
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("0.0.0.0", 0));
            return socket.getLocalPort();
        }
    }
}
