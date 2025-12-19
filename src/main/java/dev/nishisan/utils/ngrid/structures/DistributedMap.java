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
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.metrics.NGridMetrics;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed map facade exposing a subset of {@link java.util.Map} operations with
 * leader based replication.
 */
public final class DistributedMap<K extends Serializable, V extends Serializable> implements TransportListener, Closeable {
    private static final String COMMAND_PREFIX_PUT = "map.put:";
    private static final String COMMAND_PREFIX_REMOVE = "map.remove:";
    private static final String COMMAND_PREFIX_GET = "map.get:";

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final MapClusterService<K, V> mapService;
    private final String mapName;
    private final String mapPutCommand;
    private final String mapRemoveCommand;
    private final String mapGetCommand;
    private final StatsUtils stats;
    private final NodeId localNodeId;

    public DistributedMap(Transport transport, ClusterCoordinator coordinator, MapClusterService<K, V> mapService) {
        this(transport, coordinator, mapService, MapClusterService.DEFAULT_MAP_NAME, null);
    }

    public DistributedMap(Transport transport, ClusterCoordinator coordinator, MapClusterService<K, V> mapService, String mapName) {
        this(transport, coordinator, mapService, mapName, null);
    }

    public DistributedMap(Transport transport, ClusterCoordinator coordinator, MapClusterService<K, V> mapService, String mapName, StatsUtils stats) {
        this.transport = transport;
        this.coordinator = coordinator;
        this.mapService = mapService;
        this.mapName = mapName;
        this.stats = stats;
        this.localNodeId = transport.local().nodeId();
        if (this.mapName == null || this.mapName.isBlank()) {
            throw new IllegalArgumentException("mapName cannot be blank");
        }
        this.mapPutCommand = COMMAND_PREFIX_PUT + this.mapName;
        this.mapRemoveCommand = COMMAND_PREFIX_REMOVE + this.mapName;
        this.mapGetCommand = COMMAND_PREFIX_GET + this.mapName;
        transport.addListener(this);
    }

    @SuppressWarnings("unchecked")
    public Optional<V> put(K key, V value) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            recordMapPut();
            return mapService.put(key, value);
        }
        Serializable body = new MapEntry<>(key, value);
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(mapPutCommand, body);
        return result.toOptional();
    }

    @SuppressWarnings("unchecked")
    public Optional<V> remove(K key) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            recordMapRemove();
            return mapService.remove(key);
        }
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(mapRemoveCommand, key);
        return result.toOptional();
    }

    @SuppressWarnings("unchecked")
    public Optional<V> get(K key) {
        if (coordinator.isLeader()) {
            return mapService.get(key);
        }
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(mapGetCommand, key);
        return result.toOptional();
    }

    private Serializable invokeLeader(String command, Serializable body) {
        NodeInfo leaderInfo = coordinator.leaderInfo().orElseThrow(() -> new IllegalStateException("No leader available"));
        if (leaderInfo.nodeId().equals(transport.local().nodeId())) {
            return executeLocal(command, body);
        }
        ClientRequestPayload payload = new ClientRequestPayload(UUID.randomUUID(), command, body);
        ClusterMessage request = ClusterMessage.request(MessageType.CLIENT_REQUEST,
                command,
                transport.local().nodeId(),
                leaderInfo.nodeId(),
                payload);
        CompletableFuture<ClusterMessage> future = transport.sendAndAwait(request);
        ClusterMessage response = future.join();
        ClientResponsePayload responsePayload = response.payload(ClientResponsePayload.class);
        if (!responsePayload.success()) {
            throw new IllegalStateException(responsePayload.error());
        }
        return responsePayload.body();
    }

    @SuppressWarnings("unchecked")
    private Serializable executeLocal(String command, Serializable body) {
        if (command.equals(mapPutCommand)) {
            MapEntry<K, V> entry = (MapEntry<K, V>) body;
            recordMapPut();
            return mapService.put(entry.key(), entry.value())
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        if (command.equals(mapRemoveCommand)) {
            recordMapRemove();
            return mapService.remove((K) body)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        if (command.equals(mapGetCommand)) {
            return mapService.get((K) body)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        throw new IllegalArgumentException("Unknown command: " + command);
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        // no-op
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        // no-op
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() != MessageType.CLIENT_REQUEST) {
            return;
        }
        ClientRequestPayload payload = message.payload(ClientRequestPayload.class);
        if (!Set.of(mapPutCommand, mapRemoveCommand, mapGetCommand).contains(payload.command())) {
            return;
        }
        ClientResponsePayload responsePayload;
        if (!coordinator.isLeader()) {
            responsePayload = new ClientResponsePayload(payload.requestId(), false, null, "Not the leader");
        } else {
            Serializable result = executeLocal(payload.command(), payload.body());
            responsePayload = new ClientResponsePayload(payload.requestId(), true, result, null);
        }
        ClusterMessage response = ClusterMessage.response(message, responsePayload);
        transport.send(response);
    }

    @Override
    public void close() throws IOException {
        transport.removeListener(this);
        // Note: mapService is NOT closed here because it may be shared by multiple DistributedMap instances.
        // The owner (NGridNode) is responsible for closing MapClusterService instances.
    }

    private void recordMapPut() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.writeNode(localNodeId));
        stats.notifyHitCounter(NGridMetrics.mapPut(mapName, localNodeId));
    }

    private void recordMapRemove() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.writeNode(localNodeId));
        stats.notifyHitCounter(NGridMetrics.mapRemove(mapName, localNodeId));
    }

    private void recordIngressWrite() {
        if (stats == null) {
            return;
        }
        stats.notifyHitCounter(NGridMetrics.ingressWrite(localNodeId));
    }

    private record MapEntry<K extends Serializable, V extends Serializable>(K key, V value) implements Serializable {
    }
}
