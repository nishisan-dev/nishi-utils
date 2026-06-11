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
import dev.nishisan.utils.ngrid.replication.FollowerIngestMode;
import dev.nishisan.utils.ngrid.replication.RelayDurability;

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
    private final List<MapConfig> mapConfigs = new ArrayList<>();
    private Integer replicationFactor;
    private Path dataDir;
    private boolean strictConsistency = true;
    private FollowerIngestMode followerIngestMode = FollowerIngestMode.RELAY_STREAM;
    private RelayDurability relayDurability = RelayDurability.OS_MANAGED;
    private boolean persistentResendLog = false;
    private int relayApplyBatchSize = 256;
    private boolean leaderPauseOnJoin = false;
    private boolean leaderPauseOnReclaim = false;
    private int priority = 0;

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
     * Sets this node's leadership priority (affinity). Higher values are preferred when electing a
     * leader; ties are broken deterministically by node id. Defaults to {@code 0}.
     *
     * @param priority the leadership priority (higher = preferred leader)
     * @return this builder
     */
    public NGridNodeBuilder priority(int priority) {
        this.priority = priority;
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
        mapConfigs.add(MapConfig.builder(Objects.requireNonNull(name, "map name")).build());
        return this;
    }

    /**
     * Adds a distributed map with leader-local by-reference mode set.
     * <p>
     * When {@code leaderLocalByReference} is {@code true}, the leader keeps the
     * original value instance in its local state ({@code put(k, v)} then
     * {@code get(k) == v}); followers still receive the serialized copy.
     *
     * @param name                   the map name
     * @param leaderLocalByReference whether to enable by-reference mode
     * @return this builder
     */
    public NGridNodeBuilder map(String name, boolean leaderLocalByReference) {
        mapConfigs.add(MapConfig.builder(Objects.requireNonNull(name, "map name"))
                .leaderLocalByReference(leaderLocalByReference)
                .build());
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
     * Sets how this node ingests replication when acting as a follower. Since 5.0.0 the only mode is
     * {@link FollowerIngestMode#RELAY_STREAM} (the default): the follower PULLS the leader's durable
     * op-log as a sequential stream and applies it at its own pace.
     *
     * @param mode the follower ingest mode (must not be {@code null})
     * @return this builder
     */
    public NGridNodeBuilder followerIngestMode(FollowerIngestMode mode) {
        this.followerIngestMode = Objects.requireNonNull(mode, "followerIngestMode");
        return this;
    }

    /**
     * Sets the relay-log tail durability policy (#124), analogous to MySQL's
     * {@code sync_relay_log}. Defaults to {@link RelayDurability#OS_MANAGED}.
     *
     * @param relayDurability the durability policy (must not be {@code null})
     * @return this builder
     */
    public NGridNodeBuilder relayDurability(RelayDurability relayDurability) {
        this.relayDurability = Objects.requireNonNull(relayDurability, "relayDurability");
        return this;
    }

    /**
     * Enables the disk-backed resend op-log (#127), keeping the deep backlog window off-heap so a
     * large {@link NGridConfig.Builder#replicationLogRetentionTime(java.time.Duration) temporal
     * window} does not pressure the JVM heap. Defaults to {@code false}.
     *
     * @param persistentResendLog {@code true} to enable the on-disk resend op-log
     * @return this builder
     */
    public NGridNodeBuilder persistentResendLog(boolean persistentResendLog) {
        this.persistentResendLog = persistentResendLog;
        return this;
    }

    /**
     * Sets how many relay-log entries the follower apply consumer drains per batch (#128), raising
     * apply throughput under burst while keeping strict in-order application. Defaults to 256.
     *
     * @param batchSize the relay apply batch size (must be >= 1)
     * @return this builder
     */
    public NGridNodeBuilder relayApplyBatchSize(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("relayApplyBatchSize must be >= 1");
        }
        this.relayApplyBatchSize = batchSize;
        return this;
    }

    /**
     * Enables leader-pause-on-join (#129): the leader pauses production while a not-caught-up follower
     * joins, generalizing the failover drain-gate to the join path for deterministic bootstrap
     * convergence. Bounded and released on catch-up/disconnect/timeout. Defaults to {@code false}.
     *
     * @param leaderPauseOnJoin {@code true} to pause production while a behind follower joins
     * @return this builder
     */
    public NGridNodeBuilder leaderPauseOnJoin(boolean leaderPauseOnJoin) {
        this.leaderPauseOnJoin = leaderPauseOnJoin;
        return this;
    }

    /**
     * Enables the quiesce-assisted reclaim (issue tems#9, D10b): when a higher-affinity candidate
     * approaches the leader's watermark, the incumbent pauses production so the candidate pairs up
     * exactly and the affinity handoff completes coordinately. Bounded and cooldown-protected.
     * Defaults to {@code false}. Recommended for HA pairs with distinct priorities.
     *
     * @param leaderPauseOnReclaim {@code true} to pause production for an approaching reclaim
     *                             candidate
     * @return this builder
     */
    public NGridNodeBuilder leaderPauseOnReclaim(boolean leaderPauseOnReclaim) {
        this.leaderPauseOnReclaim = leaderPauseOnReclaim;
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

        NodeInfo localInfo = new NodeInfo(NodeId.of(effectiveNodeId), host, effectivePort,
                java.util.Collections.emptySet(), priority);

        NGridConfig.Builder builder = NGridConfig.builder(localInfo)
                .strictConsistency(strictConsistency)
                .followerIngestMode(followerIngestMode)
                .relayDurability(relayDurability)
                .persistentResendLog(persistentResendLog)
                .relayApplyBatchSize(relayApplyBatchSize)
                .leaderPauseOnJoin(leaderPauseOnJoin)
                .leaderPauseOnReclaim(leaderPauseOnReclaim);

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
        for (MapConfig mc : mapConfigs) {
            builder.addMap(mc);
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
