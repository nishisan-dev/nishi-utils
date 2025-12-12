/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
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
    private static final String MAP_PUT = "map.put";
    private static final String MAP_REMOVE = "map.remove";
    private static final String MAP_GET = "map.get";

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final MapClusterService<K, V> mapService;

    public DistributedMap(Transport transport, ClusterCoordinator coordinator, MapClusterService<K, V> mapService) {
        this.transport = transport;
        this.coordinator = coordinator;
        this.mapService = mapService;
        transport.addListener(this);
    }

    @SuppressWarnings("unchecked")
    public Optional<V> put(K key, V value) {
        if (coordinator.isLeader()) {
            return mapService.put(key, value);
        }
        Serializable body = new MapEntry<>(key, value);
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(MAP_PUT, body);
        return result.toOptional();
    }

    @SuppressWarnings("unchecked")
    public Optional<V> remove(K key) {
        if (coordinator.isLeader()) {
            return mapService.remove(key);
        }
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(MAP_REMOVE, key);
        return result.toOptional();
    }

    @SuppressWarnings("unchecked")
    public Optional<V> get(K key) {
        if (coordinator.isLeader()) {
            return mapService.get(key);
        }
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(MAP_GET, key);
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
        return switch (command) {
            case MAP_PUT -> {
                MapEntry<K, V> entry = (MapEntry<K, V>) body;
                yield mapService.put(entry.key(), entry.value())
                        .map(SerializableOptional::of)
                        .orElseGet(SerializableOptional::empty);
            }
            case MAP_REMOVE -> mapService.remove((K) body)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
            case MAP_GET -> mapService.get((K) body)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        };
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
        if (!Set.of(MAP_PUT, MAP_REMOVE, MAP_GET).contains(payload.command())) {
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
        mapService.close();
    }

    private record MapEntry<K extends Serializable, V extends Serializable>(K key, V value) implements Serializable {
    }
}
