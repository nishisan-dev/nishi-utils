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
 *  along with this program.  Ref: <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;

import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransportConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.ConfigFetchRequestPayload;
import dev.nishisan.utils.ngrid.common.ConfigFetchResponsePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.config.NGridConfigLoader;
import dev.nishisan.utils.ngrid.config.NGridYamlConfig;
import dev.nishisan.utils.ngrid.config.QueuePolicyConfig;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.map.MapPersistenceConfig;
import dev.nishisan.utils.ngrid.map.MapPersistenceMode;
import dev.nishisan.utils.ngrid.metrics.LeaderReelectionService;
import dev.nishisan.utils.ngrid.metrics.NGridMetrics;
import dev.nishisan.utils.ngrid.metrics.NGridAlertEngine;
import dev.nishisan.utils.ngrid.metrics.NGridAlertListener;
import dev.nishisan.utils.ngrid.metrics.NGridDashboardReporter;
import dev.nishisan.utils.ngrid.metrics.NGridOperationalSnapshot;
import dev.nishisan.utils.ngrid.metrics.NGridStatsSnapshot;
import dev.nishisan.utils.ngrid.metrics.RttMonitor;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
import dev.nishisan.utils.ngrid.queue.DistributedOffsetStore;
import dev.nishisan.utils.ngrid.queue.OffsetStore;
import dev.nishisan.utils.ngrid.replication.ReplicationConfig;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.queue.NQueue;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * High level component that ties together transport, coordination, replication
 * and data
 * structures for a single node instance.
 */
public final class NGridNode implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(NGridNode.class.getName());
    private NGridConfig config;
    private Path configFile;
    private NGridYamlConfig yamlConfig;

    private Transport transport;
    private ClusterCoordinator coordinator;
    private ReplicationManager replicationManager;
    private DistributedQueueConfig defaultQueueConfig;
    private final Map<String, QueueClusterService<Serializable>> queueServices = new ConcurrentHashMap<>();
    private final Map<String, DistributedQueue<Serializable>> queues = new ConcurrentHashMap<>();
    private DistributedQueue<Serializable> queue;
    private final Map<String, MapClusterService<Serializable, Serializable>> mapServices = new ConcurrentHashMap<>();
    private final Map<String, DistributedMap<Serializable, Serializable>> maps = new ConcurrentHashMap<>();
    private DistributedMap<Serializable, Serializable> map;
    private ScheduledExecutorService coordinatorScheduler;
    private ScheduledExecutorService metricsScheduler;
    private RttMonitor rttMonitor;
    private LeaderReelectionService leaderReelectionService;
    private NGridAlertEngine alertEngine;
    private NGridDashboardReporter dashboardReporter;
    private final StatsUtils stats = new StatsUtils();
    private final AtomicBoolean started = new AtomicBoolean();
    private final List<Runnable> resourceListeners = new ArrayList<>();

    public NGridNode(NGridConfig config) {
        this.config = config;
    }

    public NGridNode(Path configFile) throws IOException {
        this.configFile = Objects.requireNonNull(configFile, "configFile");
        this.yamlConfig = NGridConfigLoader.load(configFile);
        this.config = NGridConfigLoader.convertToDomain(this.yamlConfig);
    }

    public void addResourceListener(Runnable listener) {
        synchronized (resourceListeners) {
            resourceListeners.add(Objects.requireNonNull(listener));
        }
    }

    private void notifyResourceListeners() {
        List<Runnable> copy;
        synchronized (resourceListeners) {
            copy = new ArrayList<>(resourceListeners);
        }
        copy.forEach(Runnable::run);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        if (yamlConfig != null && yamlConfig.getAutodiscover() != null && yamlConfig.getAutodiscover().isEnabled()) {
            try {
                performAutodiscover();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Autodiscover failed", e);
                throw new IllegalStateException("Autodiscover failed", e);
            }
        }

        startServices();
    }

    private void performAutodiscover() throws Exception {

        String seedAddress = yamlConfig.getAutodiscover().getSeed();

        LOGGER.info(() -> "Starting autodiscover (Socket Mode) via seed: " + seedAddress);

        if (seedAddress == null || seedAddress.isBlank()) {

            throw new IllegalStateException("Autodiscover enabled but no seed provided");

        }

        String[] parts = seedAddress.split(":");

        if (parts.length != 2)
            throw new IllegalArgumentException("Invalid seed address format");

        String host = parts[0];

        int port = Integer.parseInt(parts[1]);

        NodeId localId = config.local().nodeId();

        try (java.net.Socket socket = new java.net.Socket()) {

            socket.connect(new java.net.InetSocketAddress(host, port), 5000);

            socket.setTcpNoDelay(true);

            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(socket.getOutputStream());

            oos.flush();

            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(socket.getInputStream());

            // 1. Send Handshake (required by TcpTransport on the seed)

            int advertisedPort = config.local().port() > 0 ? config.local().port() : 0;
            dev.nishisan.utils.ngrid.common.HandshakePayload handshake = new dev.nishisan.utils.ngrid.common.HandshakePayload(

                    new NodeInfo(localId, config.local().host(), advertisedPort),

                    java.util.Collections.emptySet()

            );

            ClusterMessage hsMsg = ClusterMessage.request(MessageType.HANDSHAKE, "hello", localId, null, handshake);

            oos.writeObject(hsMsg);

            oos.flush();

            // 2. Send Config Request

            ConfigFetchRequestPayload requestPayload = new ConfigFetchRequestPayload(
                    yamlConfig.getAutodiscover().getSecret());

            ClusterMessage request = ClusterMessage.request(MessageType.CONFIG_FETCH_REQUEST, "config", localId, null,
                    requestPayload);

            oos.writeObject(request);

            oos.flush();

            // 3. Await Response

            long deadline = System.currentTimeMillis() + 15000;

            while (System.currentTimeMillis() < deadline) {

                Object received = ois.readObject();

                if (received instanceof ClusterMessage msg) {

                    if (msg.type() == MessageType.CONFIG_FETCH_RESPONSE) {

                        ConfigFetchResponsePayload responsePayload = msg.payload(ConfigFetchResponsePayload.class);

                        // Merge config

                        yamlConfig.setCluster(responsePayload.cluster());

                        yamlConfig.setQueue(responsePayload.queue());
                        if (responsePayload.queues() != null && !responsePayload.queues().isEmpty()) {
                            yamlConfig.setQueues(responsePayload.queues());
                            if (responsePayload.queue() == null) {
                                yamlConfig.setQueue(responsePayload.queues().get(0));
                            }
                        }

                        yamlConfig.setMaps(responsePayload.maps());

                        if (responsePayload.seedInfo() != null && yamlConfig.getCluster() != null) {
                            dev.nishisan.utils.ngrid.config.ClusterPolicyConfig.SeedNodeConfig seedNode = new dev.nishisan.utils.ngrid.config.ClusterPolicyConfig.SeedNodeConfig();
                            seedNode.setId(responsePayload.seedInfo().nodeId().value());
                            seedNode.setHost(responsePayload.seedInfo().host());
                            seedNode.setPort(responsePayload.seedInfo().port());
                            yamlConfig.getCluster().setSeedNodes(java.util.List.of(seedNode));
                        }

                        yamlConfig.getAutodiscover().setEnabled(false);

                        if (configFile != null) {

                            NGridConfigLoader.save(configFile, yamlConfig);

                            LOGGER.info(
                                    () -> "Cluster configuration updated via raw socket and saved to " + configFile);

                        }

                        this.config = NGridConfigLoader.convertToDomain(yamlConfig, responsePayload.seedInfo());

                        return; // Success!

                    }

                }

            }

            throw new IOException("Timeout waiting for config response from seed");

        }

    }

    private void startServices() {
        Duration requestTimeout = config.requestTimeout();
        Duration replicationTimeout = config.replicationOperationTimeout();
        if (replicationTimeout == null) {
            replicationTimeout = Duration.ofSeconds(30);
        }
        if (requestTimeout != null && replicationTimeout.compareTo(requestTimeout) > 0) {
            requestTimeout = replicationTimeout.plusSeconds(5);
        }
        TcpTransportConfig.Builder transportBuilder = TcpTransportConfig.builder(config.local())
                .connectTimeout(config.connectTimeout())
                .reconnectInterval(config.reconnectInterval())
                .requestTimeout(requestTimeout)
                .workerThreads(config.transportWorkerThreads());
        config.peers().forEach(transportBuilder::addPeer);
        transport = new TcpTransport(transportBuilder.build(), stats);

        if (yamlConfig != null) {
            LOGGER.fine(() -> "Registering ConfigHandler on node " + config.local().nodeId());
            transport.addListener(new ConfigHandler());
        }

        transport.start();

        coordinatorScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ngrid-coordinator");
            t.setDaemon(true);
            return t;
        });
        ClusterCoordinatorConfig coordinatorConfig = ClusterCoordinatorConfig.of(
                config.heartbeatInterval(),
                Duration.ofSeconds(5),
                config.leaseTimeout(),
                1,
                null);
        coordinator = new ClusterCoordinator(transport, coordinatorConfig, coordinatorScheduler);
        coordinator.start();

        metricsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ngrid-metrics");
            t.setDaemon(true);
            return t;
        });
        rttMonitor = new RttMonitor(transport, coordinator, stats, metricsScheduler, config.rttProbeInterval());
        rttMonitor.start();
        if (config.leaderReelectionEnabled()) {
            leaderReelectionService = new LeaderReelectionService(
                    transport,
                    coordinator,
                    stats,
                    metricsScheduler,
                    config.leaderReelectionInterval(),
                    config.leaderReelectionCooldown(),
                    config.leaderReelectionSuggestionTtl(),
                    config.leaderReelectionMinDelta());
            leaderReelectionService.start();
        }

        ReplicationConfig.Builder replicationBuilder = ReplicationConfig.builder(config.replicationFactor());
        if (config.replicationOperationTimeout() != null) {
            replicationBuilder.operationTimeout(config.replicationOperationTimeout());
        }
        replicationBuilder.strictConsistency(config.strictConsistency());

        // Determine replication data directory
        Path replicationDataDir;
        if (config.dataDirectory() != null) {
            replicationDataDir = config.dataDirectory().resolve("replication");
        } else if (config.queueDirectory() != null) {
            // Fallback for legacy config
            replicationDataDir = config.queueDirectory().getParent().resolve("replication");
        } else {
            throw new IllegalStateException("Neither dataDirectory nor queueDirectory is configured");
        }
        replicationBuilder.dataDirectory(replicationDataDir);

        replicationManager = new ReplicationManager(transport, coordinator, replicationBuilder.build());
        replicationManager.start();

        coordinator.setLeaderHighWatermarkSupplier(() -> coordinator.isLeader() ? replicationManager.getGlobalSequence()
                : replicationManager.getLastAppliedSequence());

        NQueue.Options queueOptions = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
        defaultQueueConfig = DistributedQueueConfig.builder(config.queueName())
                .queueOptions(queueOptions)
                .replicationFactor(config.replicationFactor())
                .build();

        // Only create default queue if no queues are explicitly configured
        // This maintains backward compatibility with single-queue API
        if (config.queues() == null || config.queues().isEmpty()) {
            queue = getQueue(config.queueName(), defaultQueueConfig, Serializable.class);
        } else {
            // For multi-queue setup, eagerly register all configured queues so leaders can
            // serve requests.
            queue = null;
            for (QueueConfig queueConfig : config.queues()) {
                DistributedQueueConfig effectiveConfig = toDistributedQueueConfig(queueConfig);
                DistributedQueue<Serializable> created = getQueue(queueConfig.name(), effectiveConfig,
                        Serializable.class);
                if (queue == null && config.queues().size() == 1) {
                    queue = created;
                }
            }
        }

        String defaultMapName = config.mapName();
        map = maps.computeIfAbsent(defaultMapName, this::createDistributedMap);

        // ── Observability: Alert Engine + Dashboard Reporter ──
        alertEngine = NGridAlertEngine.builder(this::operationalSnapshot, metricsScheduler)
                .build();
        alertEngine.start();

        Path dashboardPath = null;
        if (config.dataDirectory() != null) {
            dashboardPath = config.dataDirectory().resolve("dashboard.yaml");
        } else if (config.queueDirectory() != null) {
            dashboardPath = config.queueDirectory().getParent().resolve("dashboard.yaml");
        }
        if (dashboardPath != null) {
            dashboardReporter = new NGridDashboardReporter(
                    this::operationalSnapshot, metricsScheduler,
                    dashboardPath, Duration.ofSeconds(10));
            dashboardReporter.start();
        }
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> queue(Class<T> type) {
        if (queue == null) {
            throw new IllegalStateException("Multiple queues configured; use getQueue(name, type) instead");
        }
        return (DistributedQueue<T>) queue;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> getQueue(String name, Class<T> type) {
        return (DistributedQueue<T>) getQueue(name, null, type);
    }

    /**
     * Type-safe queue accessor using {@link TypedQueue}.
     * <p>
     * Provides compile-time type safety by packaging queue name and type together.
     * 
     * @param typedQueue queue descriptor with name and type
     * @return distributed queue instance
     * @since 2.1.0
     */
    public <T extends Serializable> DistributedQueue<T> getQueue(TypedQueue<T> typedQueue) {
        Objects.requireNonNull(typedQueue, "typedQueue cannot be null");
        return getQueue(typedQueue.name(), typedQueue.type());
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> getQueue(String name, DistributedQueueConfig queueConfig,
            Class<T> type) {
        Objects.requireNonNull(name, "queue name cannot be null");
        if (!started.get()) {
            throw new IllegalStateException("Node not started");
        }
        DistributedQueueConfig effectiveConfig = buildQueueConfig(name, queueConfig);
        return (DistributedQueue<T>) queues.computeIfAbsent(name, key -> createDistributedQueue(key, effectiveConfig));
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> map(Class<K> keyType,
            Class<V> valueType) {
        return (DistributedMap<K, V>) map;
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> getMap(String name, Class<K> keyType,
            Class<V> valueType) {
        Objects.requireNonNull(name, "map name cannot be null");
        if (!started.get()) {
            throw new IllegalStateException("Node not started");
        }
        return (DistributedMap<K, V>) maps.computeIfAbsent(name, this::createDistributedMap);
    }

    public Set<String> getQueueNames() {
        return queues.keySet();
    }

    public Set<String> getMapNames() {
        return maps.keySet();
    }

    public NGridConfig config() {
        return config;
    }

    public Transport transport() {
        return transport;
    }

    public void join(NodeInfo peer) {
        Objects.requireNonNull(peer, "peer");
        if (transport == null) {
            throw new IllegalStateException("Transport not initialized");
        }
        transport.addPeer(peer);
    }

    public ClusterCoordinator coordinator() {
        return coordinator;
    }

    public ReplicationManager replicationManager() {
        return replicationManager;
    }

    /**
     * Returns the alert engine for this node. May be null if the node has not
     * started.
     */
    public NGridAlertEngine alertEngine() {
        return alertEngine;
    }

    /**
     * Registers an alert listener on the embedded alert engine.
     *
     * @throws IllegalStateException if the node has not started
     */
    public void addAlertListener(NGridAlertListener listener) {
        if (alertEngine == null) {
            throw new IllegalStateException("Node not started; alert engine not available");
        }
        alertEngine.addListener(listener);
    }

    public StatsUtils stats() {
        return stats;
    }

    public NGridStatsSnapshot metricsSnapshot() {
        List<String> nodeIds = new ArrayList<>();
        if (coordinator != null) {
            coordinator.activeMembers().forEach(member -> nodeIds.add(member.nodeId().value()));
        }
        List<String> mapNames = new ArrayList<>(mapServices.keySet());
        Map<String, Long> writesByNode = new HashMap<>();
        Map<String, Long> ingressWritesByNode = new HashMap<>();
        Map<String, Long> queueOffersByNode = new HashMap<>();
        Map<String, Long> queuePollsByNode = new HashMap<>();
        Map<String, Map<String, Long>> mapPutsByName = new HashMap<>();
        Map<String, Map<String, Long>> mapRemovesByName = new HashMap<>();
        Map<String, Double> rttMsByNode = new HashMap<>();
        Map<String, Long> rttFailuresByNode = new HashMap<>();

        for (String nodeId : nodeIds) {
            Long writes = stats.getCounterValueOrNull(NGridMetrics.writeNode(NodeId.of(nodeId)));
            if (writes != null) {
                writesByNode.put(nodeId, writes);
            }
            Long ingressWrites = stats.getCounterValueOrNull(NGridMetrics.ingressWrite(NodeId.of(nodeId)));
            if (ingressWrites != null) {
                ingressWritesByNode.put(nodeId, ingressWrites);
            }
            Long offers = stats.getCounterValueOrNull(NGridMetrics.queueOffer(NodeId.of(nodeId)));
            if (offers != null) {
                queueOffersByNode.put(nodeId, offers);
            }
            Long polls = stats.getCounterValueOrNull(NGridMetrics.queuePoll(NodeId.of(nodeId)));
            if (polls != null) {
                queuePollsByNode.put(nodeId, polls);
            }
            Double rtt = stats.getAverageOrNull(NGridMetrics.rttMs(NodeId.of(nodeId)));
            if (rtt != null) {
                rttMsByNode.put(nodeId, rtt);
            }
            Long rttFail = stats.getCounterValueOrNull(NGridMetrics.rttFailure(NodeId.of(nodeId)));
            if (rttFail != null) {
                rttFailuresByNode.put(nodeId, rttFail);
            }
        }

        for (String mapName : mapNames) {
            Map<String, Long> putByNode = new HashMap<>();
            Map<String, Long> removeByNode = new HashMap<>();
            for (String nodeId : nodeIds) {
                NodeId id = NodeId.of(nodeId);
                Long put = stats.getCounterValueOrNull(NGridMetrics.mapPut(mapName, id));
                if (put != null) {
                    putByNode.put(nodeId, put);
                }
                Long remove = stats.getCounterValueOrNull(NGridMetrics.mapRemove(mapName, id));
                if (remove != null) {
                    removeByNode.put(nodeId, remove);
                }
            }
            if (!putByNode.isEmpty()) {
                mapPutsByName.put(mapName, putByNode);
            }
            if (!removeByNode.isEmpty()) {
                mapRemovesByName.put(mapName, removeByNode);
            }
        }

        return new NGridStatsSnapshot(
                Instant.now(),
                writesByNode,
                ingressWritesByNode,
                queueOffersByNode,
                queuePollsByNode,
                mapPutsByName,
                mapRemovesByName,
                rttMsByNode,
                rttFailuresByNode);
    }

    /**
     * Captures a comprehensive operational snapshot of this node, aggregating
     * cluster state, replication health, and I/O metrics into a single immutable
     * object suitable for dashboards and alerting engines.
     *
     * @return an operational snapshot of the current node state
     * @since 2.1.0
     */
    public NGridOperationalSnapshot operationalSnapshot() {
        String localId = config.local().nodeId().value();
        String leaderId = coordinator.leaderInfo()
                .map(info -> info.nodeId().value())
                .orElse("unknown");
        long leaderEpoch = coordinator.getLeaderEpoch();
        long trackedLeaderEpoch = coordinator.getTrackedLeaderEpoch();
        int activeMembersCount = coordinator.getActiveMembersCount();
        boolean isLeader = coordinator.isLeader();
        boolean hasValidLease = coordinator.hasValidLease();
        long trackedHighWatermark = coordinator.getTrackedLeaderHighWatermark();

        long globalSeq = replicationManager.getGlobalSequence();
        long lastApplied = replicationManager.getLastAppliedSequence();
        long lag = Math.max(0, trackedHighWatermark - lastApplied);
        long gaps = replicationManager.getGapsDetected();
        long resendSuccess = replicationManager.getResendSuccessCount();
        long snapshotFallback = replicationManager.getSnapshotFallbackCount();
        double avgConvergence = replicationManager.getAverageConvergenceTimeMs();
        int pendingOps = replicationManager.getPendingOperationsCount();

        // Count reachable nodes from the expected cluster membership (configured peers
        // + local), so quorum reflects partitions even when membership views shrink.
        int totalNodes = config.peers().size() + 1;
        int reachable = 1; // local node is always reachable
        for (NodeInfo peer : config.peers()) {
            if (transport.isReachable(peer.nodeId())) {
                reachable++;
            }
        }

        NGridStatsSnapshot ioStats = metricsSnapshot();

        return new NGridOperationalSnapshot(
                localId,
                leaderId,
                leaderEpoch,
                trackedLeaderEpoch,
                activeMembersCount,
                isLeader,
                hasValidLease,
                trackedHighWatermark,
                globalSeq,
                lastApplied,
                lag,
                gaps,
                resendSuccess,
                snapshotFallback,
                avgConvergence,
                pendingOps,
                reachable,
                totalNodes,
                ioStats,
                Instant.now());
    }

    @Override
    public void close() throws IOException {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        IOException first = null;
        try {
            for (DistributedQueue<Serializable> q : queues.values()) {
                q.close();
            }
        } catch (IOException e) {
            first = e;
        }
        for (DistributedMap<Serializable, Serializable> m : maps.values()) {
            try {
                m.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        for (MapClusterService<Serializable, Serializable> service : mapServices.values()) {
            try {
                service.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        try {
            if (replicationManager != null) {
                replicationManager.close();
            }
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        try {
            if (rttMonitor != null) {
                rttMonitor.close();
            }
        } catch (RuntimeException e) {
            if (first == null) {
                first = new IOException("Failed to stop RTT monitor", e);
            }
        }
        try {
            if (leaderReelectionService != null) {
                leaderReelectionService.close();
            }
        } catch (RuntimeException e) {
            if (first == null) {
                first = new IOException("Failed to stop leader reelection service", e);
            }
        }
        try {
            if (alertEngine != null) {
                alertEngine.close();
            }
        } catch (RuntimeException e) {
            // best-effort shutdown
        }
        try {
            if (dashboardReporter != null) {
                dashboardReporter.close();
            }
        } catch (RuntimeException e) {
            // best-effort shutdown
        }
        try {
            if (coordinator != null) {
                coordinator.close();
            }
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        try {
            if (coordinatorScheduler != null) {
                coordinatorScheduler.shutdownNow();
            }
        } catch (RuntimeException e) {
            // best-effort shutdown
        }
        try {
            if (metricsScheduler != null) {
                metricsScheduler.shutdownNow();
            }
        } catch (RuntimeException e) {
            // best-effort shutdown
        }
        try {
            if (transport != null) {
                transport.close();
            }
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private DistributedQueue<Serializable> createDistributedQueue(String queueName,
            DistributedQueueConfig queueConfig) {
        QueueClusterService<Serializable> service = queueServices.computeIfAbsent(queueName,
                key -> createQueueService(key, queueConfig));
        DistributedQueue<Serializable> q = new DistributedQueue<>(transport, coordinator, service, queueName, stats);
        notifyResourceListeners();
        return q;
    }

    private QueueClusterService<Serializable> createQueueService(String queueName, DistributedQueueConfig queueConfig) {
        NQueue.Options options = queueConfig.queueOptions();
        if (options == null) {
            options = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
        }
        int replicationFactor = queueConfig.replicationFactor() != null ? queueConfig.replicationFactor()
                : config.replicationFactor();
        OffsetStore offsetStore = null;
        if (config.queueDirectory() == null) {
            DistributedMap<String, Long> offsets = getMap("_ngrid-queue-offsets", String.class, Long.class);
            offsetStore = new DistributedOffsetStore(offsets, queueName);
        }

        // Determine queue directory
        // For new API: dataDirectory/queues/{queueName}
        // For legacy API: queueDirectory (deprecated)
        Path queueDir;
        if (config.dataDirectory() != null) {
            queueDir = config.dataDirectory().resolve("queues").resolve(queueName);
        } else if (config.queueDirectory() != null) {
            queueDir = config.queueDirectory();
        } else {
            throw new IllegalStateException("Neither dataDirectory nor queueDirectory is configured");
        }

        return new QueueClusterService<>(queueDir, queueName, replicationManager, options,
                replicationFactor, offsetStore);
    }

    private DistributedQueueConfig buildQueueConfig(String queueName, DistributedQueueConfig queueConfig) {
        if (queueConfig == null) {
            if (defaultQueueConfig != null && defaultQueueConfig.name().equals(queueName)) {
                return defaultQueueConfig;
            }
            NQueue.Options options = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
            return DistributedQueueConfig.builder(queueName)
                    .replicationFactor(config.replicationFactor())
                    .queueOptions(options)
                    .build();
        }
        if (!queueName.equals(queueConfig.name())) {
            NQueue.Options options = queueConfig.queueOptions();
            if (options == null) {
                options = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
            }
            return DistributedQueueConfig.builder(queueName)
                    .replicationFactor(queueConfig.replicationFactor() != null ? queueConfig.replicationFactor()
                            : config.replicationFactor())
                    .queueOptions(options)
                    .build();
        }
        return queueConfig;
    }

    private DistributedQueueConfig toDistributedQueueConfig(QueueConfig queueConfig) {
        NQueue.Options options = queueConfig.nqueueOptions();
        if (options == null) {
            options = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
        }
        options = options.copy();
        if (config.queueDirectory() == null) {
            QueueConfig.RetentionPolicy retention = queueConfig.retention();
            Duration retentionTime = retention != null ? retention.duration() : Duration.ofDays(7);
            options.withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                    .withRetentionTime(retentionTime);
        }
        return DistributedQueueConfig.builder(queueConfig.name())
                .replicationFactor(config.replicationFactor())
                .queueOptions(options)
                .build();
    }

    private DistributedMap<Serializable, Serializable> createDistributedMap(String mapName) {
        MapClusterService<Serializable, Serializable> service = mapServices.computeIfAbsent(mapName,
                this::createMapService);
        DistributedMap<Serializable, Serializable> m = new DistributedMap<>(transport, coordinator, service, mapName,
                stats, replicationManager);
        notifyResourceListeners();
        return m;
    }

    private MapClusterService<Serializable, Serializable> createMapService(String mapName) {
        // Infrastructure maps (like offset tracking) always persist regardless of user
        // configuration
        MapPersistenceMode effectiveMode = config.mapPersistenceMode();
        if ("_ngrid-queue-offsets".equals(mapName)
                && (effectiveMode == null || effectiveMode == MapPersistenceMode.DISABLED)) {
            effectiveMode = MapPersistenceMode.ASYNC_WITH_FSYNC;
        }
        if (effectiveMode != null && effectiveMode != MapPersistenceMode.DISABLED) {
            MapPersistenceConfig persistenceConfig = MapPersistenceConfig.defaults(
                    config.mapDirectory(),
                    mapName,
                    effectiveMode);
            MapClusterService<Serializable, Serializable> service = new MapClusterService<>(
                    replicationManager,
                    MapClusterService.topicFor(mapName),
                    persistenceConfig);
            service.loadFromDisk();
            return service;
        }
        return new MapClusterService<>(replicationManager, MapClusterService.topicFor(mapName), null);
    }

    private class ConfigHandler implements TransportListener {
        @Override
        public void onMessage(ClusterMessage message) {
            if (message.type() != MessageType.CONFIG_FETCH_REQUEST)
                return;

            LOGGER.info(() -> "ConfigHandler received request from " + message.source());
            ConfigFetchRequestPayload payload = message.payload(ConfigFetchRequestPayload.class);
            String secret = yamlConfig != null && yamlConfig.getAutodiscover() != null
                    ? yamlConfig.getAutodiscover().getSecret()
                    : null;

            if (secret != null && secret.equals(payload.secret())) {
                LOGGER.info(() -> "Autodiscover secret valid for " + message.source() + ". Sending configuration...");
                QueuePolicyConfig queuePolicy = yamlConfig.getQueue();
                java.util.List<QueuePolicyConfig> queuePolicies = yamlConfig.getQueues();
                if (queuePolicy == null && queuePolicies != null && !queuePolicies.isEmpty()) {
                    queuePolicy = queuePolicies.get(0);
                }
                ConfigFetchResponsePayload responsePayload = new ConfigFetchResponsePayload(
                        yamlConfig.getCluster(),
                        queuePolicy,
                        queuePolicies != null ? java.util.List.copyOf(queuePolicies) : java.util.List.of(),
                        yamlConfig.getMaps(),
                        config.local());

                ClusterMessage response = new ClusterMessage(
                        java.util.UUID.randomUUID(),
                        message.messageId(),
                        MessageType.CONFIG_FETCH_RESPONSE,
                        message.qualifier(),
                        config.local().nodeId(),
                        message.source(),
                        responsePayload);

                transport.send(response);
            } else {
                LOGGER.warning(() -> "Autodiscover secret INVALID from " + message.source());
            }
        }

        @Override
        public void onPeerConnected(NodeInfo peer) {
        }

        @Override
        public void onPeerDisconnected(NodeId peerId) {
        }
    }
}
