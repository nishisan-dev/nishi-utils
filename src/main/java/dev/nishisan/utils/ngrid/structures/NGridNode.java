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
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
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
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.map.MapPersistenceConfig;
import dev.nishisan.utils.ngrid.map.MapPersistenceMode;
import dev.nishisan.utils.ngrid.metrics.LeaderReelectionService;
import dev.nishisan.utils.ngrid.metrics.NGridMetrics;
import dev.nishisan.utils.ngrid.metrics.NGridStatsSnapshot;
import dev.nishisan.utils.ngrid.metrics.RttMonitor;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * High level component that ties together transport, coordination, replication and data
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

            if (parts.length != 2) throw new IllegalArgumentException("Invalid seed address format");

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

                ConfigFetchRequestPayload requestPayload = new ConfigFetchRequestPayload(yamlConfig.getAutodiscover().getSecret());

                ClusterMessage request = ClusterMessage.request(MessageType.CONFIG_FETCH_REQUEST, "config", localId, null, requestPayload);

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

                            yamlConfig.setMaps(responsePayload.maps());

                            if (responsePayload.seedInfo() != null && yamlConfig.getCluster() != null) {
                                dev.nishisan.utils.ngrid.config.ClusterPolicyConfig.SeedNodeConfig seedNode =
                                        new dev.nishisan.utils.ngrid.config.ClusterPolicyConfig.SeedNodeConfig();
                                seedNode.setId(responsePayload.seedInfo().nodeId().value());
                                seedNode.setHost(responsePayload.seedInfo().host());
                                seedNode.setPort(responsePayload.seedInfo().port());
                                yamlConfig.getCluster().setSeedNodes(java.util.List.of(seedNode));
                            }

                            yamlConfig.getAutodiscover().setEnabled(false); 

                            

                            if (configFile != null) {

                                NGridConfigLoader.save(configFile, yamlConfig);

                                LOGGER.info(() -> "Cluster configuration updated via raw socket and saved to " + configFile);

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
                Duration.ofSeconds(5)
        );
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
                    config.leaderReelectionMinDelta()
            );
            leaderReelectionService.start();
        }

        ReplicationConfig.Builder replicationBuilder = ReplicationConfig.builder(config.replicationFactor());
        if (config.replicationOperationTimeout() != null) {
            replicationBuilder.operationTimeout(config.replicationOperationTimeout());
        }
        replicationBuilder.strictConsistency(config.strictConsistency());
        replicationManager = new ReplicationManager(transport, coordinator, replicationBuilder.build());
        replicationManager.start();
        
        coordinator.setLeaderHighWatermarkSupplier(() -> coordinator.isLeader() ? replicationManager.getGlobalSequence() : replicationManager.getLastAppliedSequence());

        NQueue.Options queueOptions = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
        defaultQueueConfig = DistributedQueueConfig.builder(config.queueName())
                .queueOptions(queueOptions)
                .replicationFactor(config.replicationFactor())
                .build();
        queue = getQueue(config.queueName(), defaultQueueConfig, Serializable.class);

        String defaultMapName = config.mapName();
        map = maps.computeIfAbsent(defaultMapName, this::createDistributedMap);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> queue(Class<T> type) {
        return (DistributedQueue<T>) queue;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> getQueue(String name, Class<T> type) {
        return (DistributedQueue<T>) getQueue(name, null, type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> getQueue(String name, DistributedQueueConfig queueConfig, Class<T> type) {
        Objects.requireNonNull(name, "queue name cannot be null");
        if (!started.get()) {
            throw new IllegalStateException("Node not started");
        }
        DistributedQueueConfig effectiveConfig = buildQueueConfig(name, queueConfig);
        return (DistributedQueue<T>) queues.computeIfAbsent(name, key -> createDistributedQueue(key, effectiveConfig));
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> map(Class<K> keyType, Class<V> valueType) {
        return (DistributedMap<K, V>) map;
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> getMap(String name, Class<K> keyType, Class<V> valueType) {
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
                rttFailuresByNode
        );
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

    private DistributedQueue<Serializable> createDistributedQueue(String queueName, DistributedQueueConfig queueConfig) {
        QueueClusterService<Serializable> service = queueServices.computeIfAbsent(queueName, key -> createQueueService(key, queueConfig));
        DistributedQueue<Serializable> q = new DistributedQueue<>(transport, coordinator, service, stats);
        notifyResourceListeners();
        return q;
    }

    private QueueClusterService<Serializable> createQueueService(String queueName, DistributedQueueConfig queueConfig) {
        NQueue.Options options = queueConfig.queueOptions();
        if (options == null) {
            options = config.queueOptions() != null ? config.queueOptions() : NQueue.Options.defaults();
        }
        int replicationFactor = queueConfig.replicationFactor() != null ? queueConfig.replicationFactor() : config.replicationFactor();
        return new QueueClusterService<>(config.queueDirectory(), queueName, replicationManager, options, replicationFactor);
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
                    .replicationFactor(queueConfig.replicationFactor() != null ? queueConfig.replicationFactor() : config.replicationFactor())
                    .queueOptions(options)
                    .build();
        }
        return queueConfig;
    }

    private DistributedMap<Serializable, Serializable> createDistributedMap(String mapName) {
        MapClusterService<Serializable, Serializable> service = mapServices.computeIfAbsent(mapName, this::createMapService);
        DistributedMap<Serializable, Serializable> m = new DistributedMap<>(transport, coordinator, service, mapName, stats, replicationManager);
        notifyResourceListeners();
        return m;
    }

    private MapClusterService<Serializable, Serializable> createMapService(String mapName) {
        if (config.mapPersistenceMode() != null && config.mapPersistenceMode() != MapPersistenceMode.DISABLED) {
            MapPersistenceConfig persistenceConfig = MapPersistenceConfig.defaults(
                    config.mapDirectory(),
                    mapName,
                    config.mapPersistenceMode()
            );
            MapClusterService<Serializable, Serializable> service = new MapClusterService<>(
                    replicationManager,
                    MapClusterService.topicFor(mapName),
                    persistenceConfig
            );
            service.loadFromDisk();
            return service;
        }
        return new MapClusterService<>(replicationManager, MapClusterService.topicFor(mapName), null);
    }

    private class ConfigHandler implements TransportListener {
        @Override
        public void onMessage(ClusterMessage message) {
            if (message.type() != MessageType.CONFIG_FETCH_REQUEST) return;
            
            LOGGER.info(() -> "ConfigHandler received request from " + message.source());
            ConfigFetchRequestPayload payload = message.payload(ConfigFetchRequestPayload.class);
            String secret = yamlConfig != null && yamlConfig.getAutodiscover() != null ? yamlConfig.getAutodiscover().getSecret() : null;
            
            if (secret != null && secret.equals(payload.secret())) {
                LOGGER.info(() -> "Autodiscover secret valid for " + message.source() + ". Sending configuration...");
                ConfigFetchResponsePayload responsePayload = new ConfigFetchResponsePayload(
                        yamlConfig.getCluster(),
                        yamlConfig.getQueue(),
                        yamlConfig.getMaps(),
                        config.local()
                );
                
                ClusterMessage response = new ClusterMessage(
                        java.util.UUID.randomUUID(),
                        message.messageId(),
                        MessageType.CONFIG_FETCH_RESPONSE,
                        message.qualifier(),
                        config.local().nodeId(),
                        message.source(),
                        responsePayload
                );
                
                transport.send(response);
            } else {
                LOGGER.warning(() -> "Autodiscover secret INVALID from " + message.source());
            }
        }

        @Override
        public void onPeerConnected(NodeInfo peer) {}

        @Override
        public void onPeerDisconnected(NodeId peerId) {}
    }
}
