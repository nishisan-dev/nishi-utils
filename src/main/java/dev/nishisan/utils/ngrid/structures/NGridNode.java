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

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransportConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.map.MapPersistenceConfig;
import dev.nishisan.utils.ngrid.map.MapPersistenceMode;
import dev.nishisan.utils.ngrid.queue.QueueClusterService;
import dev.nishisan.utils.ngrid.replication.ReplicationConfig;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High level component that ties together transport, coordination, replication and data
 * structures for a single node instance.
 */
public final class NGridNode implements Closeable {
    private final NGridConfig config;

    private Transport transport;
    private ClusterCoordinator coordinator;
    private ReplicationManager replicationManager;
    private QueueClusterService<Serializable> queueService;
    private DistributedQueue<Serializable> queue;
    private final Map<String, MapClusterService<Serializable, Serializable>> mapServices = new ConcurrentHashMap<>();
    private final Map<String, DistributedMap<Serializable, Serializable>> maps = new ConcurrentHashMap<>();
    private DistributedMap<Serializable, Serializable> map;
    private ScheduledExecutorService coordinatorScheduler;
    private final AtomicBoolean started = new AtomicBoolean();

    public NGridNode(NGridConfig config) {
        this.config = config;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        TcpTransportConfig.Builder transportBuilder = TcpTransportConfig.builder(config.local())
                .connectTimeout(Duration.ofSeconds(5))
                .reconnectInterval(Duration.ofSeconds(2));
        config.peers().forEach(transportBuilder::addPeer);
        transport = new TcpTransport(transportBuilder.build());
        transport.start();

        coordinatorScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ngrid-coordinator");
            t.setDaemon(true);
            return t;
        });
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), coordinatorScheduler);
        coordinator.start();

        replicationManager = new ReplicationManager(transport, coordinator, ReplicationConfig.of(config.replicationQuorum()));
        replicationManager.start();

        queueService = new QueueClusterService<>(config.queueDirectory(), config.queueName(), replicationManager);
        queue = new DistributedQueue<>(transport, coordinator, queueService);

        // Default map for backward-compatible API (node.map(...))
        String defaultMapName = config.mapName();
        map = maps.computeIfAbsent(defaultMapName, this::createDistributedMap);
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DistributedQueue<T> queue(Class<T> type) {
        return (DistributedQueue<T>) queue;
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> map(Class<K> keyType, Class<V> valueType) {
        return (DistributedMap<K, V>) map;
    }

    @SuppressWarnings("unchecked")
    public <K extends Serializable, V extends Serializable> DistributedMap<K, V> getMap(String name, Class<K> keyType, Class<V> valueType) {
        Objects.requireNonNull(name, "name");
        if (!started.get()) {
            throw new IllegalStateException("Node not started");
        }
        return (DistributedMap<K, V>) maps.computeIfAbsent(name, this::createDistributedMap);
    }

    public Transport transport() {
        return transport;
    }

    public ClusterCoordinator coordinator() {
        return coordinator;
    }

    public ReplicationManager replicationManager() {
        return replicationManager;
    }

    @Override
    public void close() throws IOException {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        IOException first = null;
        try {
            if (queue != null) {
                queue.close();
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

    private DistributedMap<Serializable, Serializable> createDistributedMap(String mapName) {
        MapClusterService<Serializable, Serializable> service = mapServices.computeIfAbsent(mapName, this::createMapService);
        return new DistributedMap<>(transport, coordinator, service, mapName);
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
}
