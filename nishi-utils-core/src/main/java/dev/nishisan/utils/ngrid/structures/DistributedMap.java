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
import dev.nishisan.utils.ngrid.map.EncodedCommand;
import dev.nishisan.utils.ngrid.map.MapReplicationCodec;
import dev.nishisan.utils.ngrid.map.MapReplicationCommand;
import dev.nishisan.utils.ngrid.metrics.NGridMetrics;
import dev.nishisan.utils.stats.StatsUtils;

import java.io.Closeable;
import java.io.IOException;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed map facade exposing a subset of {@link java.util.Map} operations
 * with
 * leader based replication.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class DistributedMap<K, V>
        implements TransportListener, Closeable {
    private static final String COMMAND_PREFIX_PUT = "map.put:";
    private static final String COMMAND_PREFIX_REMOVE = "map.remove:";
    private static final String COMMAND_PREFIX_GET = "map.get:";
    private static final String COMMAND_PREFIX_DESTROY = "map.destroy:";

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final MapClusterService<K, V> mapService;
    private final String mapName;
    private final String mapPutCommand;
    private final String mapRemoveCommand;
    private final String mapGetCommand;
    private final String mapDestroyCommand;
    private final StatsUtils stats;
    private final dev.nishisan.utils.ngrid.replication.ReplicationManager replicationManager;
    private final NodeId localNodeId;

    /**
     * Creates a distributed map with default name and no stats or replication
     * manager.
     *
     * @param transport   the cluster transport
     * @param coordinator the cluster coordinator
     * @param mapService  the underlying map cluster service
     */
    public DistributedMap(Transport transport, ClusterCoordinator coordinator, MapClusterService<K, V> mapService) {
        this(transport, coordinator, mapService, MapClusterService.DEFAULT_MAP_NAME, null, null);
    }

    /**
     * Creates a distributed map with a custom name and no stats or replication
     * manager.
     *
     * @param transport   the cluster transport
     * @param coordinator the cluster coordinator
     * @param mapService  the underlying map cluster service
     * @param mapName     the map name
     */
    public DistributedMap(Transport transport, ClusterCoordinator coordinator, MapClusterService<K, V> mapService,
            String mapName) {
        this(transport, coordinator, mapService, mapName, null, null);
    }

    /**
     * Creates a distributed map with optional stats.
     *
     * @param transport   the cluster transport
     * @param coordinator the cluster coordinator
     * @param mapService  the underlying map cluster service
     * @param mapName     the map name
     * @param stats       stats utility, or {@code null}
     */
    public DistributedMap(Transport transport,
            ClusterCoordinator coordinator,
            MapClusterService<K, V> mapService,
            String mapName,
            StatsUtils stats) {
        this(transport, coordinator, mapService, mapName, stats, null);
    }

    /**
     * Creates a distributed map with all configuration options.
     *
     * @param transport          the cluster transport
     * @param coordinator        the cluster coordinator
     * @param mapService         the underlying map cluster service
     * @param mapName            the map name
     * @param stats              stats utility, or {@code null}
     * @param replicationManager the replication manager for bounded reads, or
     *                           {@code null}
     */
    public DistributedMap(Transport transport,
            ClusterCoordinator coordinator,
            MapClusterService<K, V> mapService,
            String mapName,
            StatsUtils stats,
            dev.nishisan.utils.ngrid.replication.ReplicationManager replicationManager) {
        this.transport = transport;
        this.coordinator = coordinator;
        this.mapService = mapService;
        this.mapName = mapName;
        this.stats = stats;
        this.replicationManager = replicationManager;
        this.localNodeId = transport.local().nodeId();
        if (this.mapName == null || this.mapName.isBlank()) {
            throw new IllegalArgumentException("mapName cannot be blank");
        }
        this.mapPutCommand = COMMAND_PREFIX_PUT + this.mapName;
        this.mapRemoveCommand = COMMAND_PREFIX_REMOVE + this.mapName;
        this.mapGetCommand = COMMAND_PREFIX_GET + this.mapName;
        this.mapDestroyCommand = COMMAND_PREFIX_DESTROY + this.mapName;
        transport.addListener(this);
    }

    /**
     * Puts a key-value pair into the distributed map.
     *
     * @param key   the key
     * @param value the value
     * @return the previous value, or empty
     */
    @SuppressWarnings("unchecked")
    public Optional<V> put(K key, V value) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            if (!coordinator.hasValidLease()) {
                throw new IllegalStateException("Leader lease expired, cannot accept writes");
            }
            recordMapPut();
            return mapService.put(key, value);
        }
        // Follower path: encode the command via MapReplicationCodec (preserves concrete
        // POJO types) and wrap in EncodedCommand so that Jackson does not flatten the
        // byte[] to a Base64 String. @JsonTypeInfo(CLASS) on ClientRequestPayload.body
        // writes the @class discriminator, allowing the leader to deserialize the body
        // back as EncodedCommand instead of String.
        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.put(key, value));
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(mapPutCommand, new EncodedCommand(encoded));
        return result.toOptional();
    }

    /**
     * Removes the entry for a key from the distributed map.
     *
     * @param key the key to remove
     * @return the previous value, or empty
     */
    @SuppressWarnings("unchecked")
    public Optional<V> remove(K key) {
        recordIngressWrite();
        if (coordinator.isLeader()) {
            if (!coordinator.hasValidLease()) {
                throw new IllegalStateException("Leader lease expired, cannot accept writes");
            }
            recordMapRemove();
            return mapService.remove(key);
        }
        // Follower path: same rationale as put() — wrap encoded bytes in EncodedCommand.
        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.remove(key));
        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(mapRemoveCommand, new EncodedCommand(encoded));
        return result.toOptional();
    }

    /**
     * Removes all entries whose string key starts with {@code prefix} from the
     * local in-memory state, persisting tombstones to disk if enabled.
     * No Raft replication is triggered — safe to call during snapshot install.
     *
     * <p>
     * Used by {@link dev.nishisan.utils.ngrid.queue.DistributedOffsetStore#reset()}
     * to clear stale consumer offsets after a queue snapshot install.
     *
     * @param prefix the key prefix (must not be null)
     */
    public void removeByPrefix(String prefix) {
        mapService.clearLocalByPrefix(prefix);
    }

    /**
     * Retrieves the value for a key using {@link Consistency#STRONG}.
     *
     * @param key the key
     * @return the value, or empty
     */
    @SuppressWarnings("unchecked")
    public Optional<V> get(K key) {
        return get(key, Consistency.STRONG);
    }

    /**
     * Returns an unmodifiable view of the keys in the local replica.
     * This is an eventually-consistent snapshot — no replication or
     * leader routing is involved.
     *
     * @return an unmodifiable set of keys
     */
    public Set<K> keySet() {
        return mapService.keySet();
    }

    /**
     * Returns {@code true} if the local replica contains a mapping for the
     * specified key. This is an eventually-consistent check — no replication
     * or leader routing is involved.
     *
     * @param key the key to check
     * @return {@code true} if the key is present in the local replica
     */
    public boolean containsKey(K key) {
        return mapService.get(key).isPresent();
    }

    /**
     * Returns the number of entries in the local replica.
     * This is an eventually-consistent value — no replication or
     * leader routing is involved.
     *
     * @return the number of entries
     */
    public int size() {
        return mapService.size();
    }

    /**
     * Returns {@code true} if the local replica contains no entries.
     * This is an eventually-consistent value — no replication or
     * leader routing is involved.
     *
     * @return {@code true} if empty
     */
    public boolean isEmpty() {
        return mapService.size() == 0;
    }

    /**
     * Puts all entries from the given map into the distributed map,
     * replicating each entry individually. Each put is a separate
     * replicated operation.
     *
     * @param entries the entries to put
     */
    public void putAll(java.util.Map<K, V> entries) {
        java.util.Objects.requireNonNull(entries, "entries");
        for (java.util.Map.Entry<K, V> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Retrieves the value for a key with the specified consistency level.
     *
     * @param key         the key
     * @param consistency the desired consistency
     * @return the value, or empty
     */
    public Optional<V> get(K key, Consistency consistency) {
        if (coordinator.isLeader()) {
            return mapService.get(key);
        }

        boolean canReadLocally = false;
        if (consistency.level() == ConsistencyLevel.EVENTUAL) {
            canReadLocally = true;
        } else if (consistency.level() == ConsistencyLevel.BOUNDED) {
            long leaderWatermark = coordinator.getTrackedLeaderHighWatermark();
            if (leaderWatermark < 0) {
                // Leader watermark unknown, conservatively route to leader or fail?
                // For now, fallback to leader.
                canReadLocally = false;
            } else {
                long localSequence = replicationManager != null ? replicationManager.getLastAppliedSequence() : 0;
                long lag = leaderWatermark - localSequence;
                if (lag <= consistency.maxLag()) {
                    canReadLocally = true;
                }
            }
        }

        if (canReadLocally) {
            return mapService.get(key);
        }

        SerializableOptional<V> result = (SerializableOptional<V>) invokeLeader(mapGetCommand, key);
        return result.toOptional();
    }

    private Object invokeLeader(String command, Object body) {
        int maxAttempts = 5;
        long backoffMs = 200;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            NodeInfo leaderInfo = coordinator.leaderInfo().orElse(null);
            if (leaderInfo == null) {
                if (attempt >= maxAttempts) {
                    throw new IllegalStateException("No leader available after " + maxAttempts + " attempts");
                }
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 2000);
                continue;
            }
            if (leaderInfo.nodeId().equals(transport.local().nodeId())) {
                return executeLocal(command, body);
            }
            ClientRequestPayload payload = new ClientRequestPayload(UUID.randomUUID(), command, body);
            ClusterMessage request = ClusterMessage.request(MessageType.CLIENT_REQUEST,
                    command,
                    transport.local().nodeId(),
                    leaderInfo.nodeId(),
                    payload);
            try {
                CompletableFuture<ClusterMessage> future = transport.sendAndAwait(request);
                ClusterMessage response = future.join();
                ClientResponsePayload responsePayload = response.payload(ClientResponsePayload.class);
                if (!responsePayload.success()) {
                    if ("Not the leader".equals(responsePayload.error()) && attempt < maxAttempts) {
                        sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, 2000);
                        continue;
                    }
                    throw new IllegalStateException(responsePayload.error());
                }
                return responsePayload.body();
            } catch (Exception e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 2000);
            }
        }
        throw new IllegalStateException("Failed to invoke leader after retries");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during retry backoff", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Object executeLocal(String command, Object body) {
        if (command.equals(mapPutCommand)) {
            // When routed via the network (follower → leader) the body arrives as
            // EncodedCommand — a POJO wrapper around the MapReplicationCodec bytes.
            // When the leader calls executeLocal() directly (leader path), the body
            // is still a raw MapEntry. Decode accordingly.
            K key;
            V value;
            if (body instanceof EncodedCommand ec) {
                MapReplicationCommand cmd = MapReplicationCodec.decode(ec.payload());
                key   = (K) cmd.key();
                value = (V) cmd.value();
            } else {
                MapEntry<K, V> entry = (MapEntry<K, V>) body;
                key   = entry.key();
                value = entry.value();
            }
            recordMapPut();
            return mapService.put(key, value)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        if (command.equals(mapRemoveCommand)) {
            K key;
            if (body instanceof EncodedCommand ec) {
                MapReplicationCommand cmd = MapReplicationCodec.decode(ec.payload());
                key = (K) cmd.key();
            } else {
                key = (K) body;
            }
            recordMapRemove();
            return mapService.remove(key)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        if (command.equals(mapGetCommand)) {
            return mapService.get((K) body)
                    .map(SerializableOptional::of)
                    .orElseGet(SerializableOptional::empty);
        }
        if (command.equals(mapDestroyCommand)) {
            mapService.destroyReplicated();
            return Boolean.TRUE;
        }
        throw new IllegalArgumentException("Unknown command: " + command);
    }

    /** {@inheritDoc} */
    @Override
    public void onPeerConnected(NodeInfo peer) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override
    public void onPeerDisconnected(NodeId peerId) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() != MessageType.CLIENT_REQUEST) {
            return;
        }
        ClientRequestPayload payload = message.payload(ClientRequestPayload.class);
        if (!Set.of(mapPutCommand, mapRemoveCommand, mapGetCommand, mapDestroyCommand).contains(payload.command())) {
            return;
        }
        ClientResponsePayload responsePayload;
        if (!coordinator.isLeader()) {
            responsePayload = new ClientResponsePayload(payload.requestId(), false, null, "Not the leader");
        } else {
            try {
                Object result = executeLocal(payload.command(), payload.body());
                responsePayload = new ClientResponsePayload(payload.requestId(), true, result, null);
            } catch (RuntimeException e) {
                String messageText = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                responsePayload = new ClientResponsePayload(payload.requestId(), false, null, messageText);
            }
        }
        ClusterMessage response = ClusterMessage.response(message, responsePayload);
        transport.send(response);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        transport.removeListener(this);
        // Note: mapService is NOT closed here because it may be shared by multiple
        // DistributedMap instances.
        // The owner (NGridNode) is responsible for closing MapClusterService instances.
    }

    /**
     * Destrói o mapa distribuído replicando o comando DESTROY para todos os nós
     * do cluster via pipeline de replicação leader-based.
     *
     * Fluxo Negocial
     * 1. Se o nó local é o líder com lease válido, invoca
     *    {@link MapClusterService#destroyReplicated()} diretamente.
     * 2. Se o nó local é follower, encaminha a requisição ao líder via
     *    {@code CLIENT_REQUEST} (mesmo mecanismo de {@code put}/{@code remove}).
     * 3. O líder replica o comando DESTROY para todos os nós — cada nó, ao
     *    aplicar, limpa os dados em memória e apaga os arquivos de persistência.
     * 4. Após a confirmação, remove o listener de transporte via {@link #close()}.
     *
     * @throws IOException se ocorrer erro de I/O ao remover o listener de transporte
     */
    public void destroy() throws IOException {
        if (coordinator.isLeader()) {
            if (!coordinator.hasValidLease()) {
                throw new IllegalStateException("Leader lease expired, cannot accept writes");
            }
            mapService.destroyReplicated();
        } else {
            byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.destroy());
            invokeLeader(mapDestroyCommand, new EncodedCommand(encoded));
        }
        close();
    }

    /**
     * Destrói o mapa apenas no nó local, sem replicação para o cluster.
     *
     * Remove todos os dados em memória e apaga os arquivos de persistência
     * do nó local. Útil em cenários de shutdown ou cleanup onde a replicação
     * não é possível ou desejada.
     *
     * @throws IOException se ocorrer erro de I/O ao remover arquivos de persistência
     */
    public void destroyLocal() throws IOException {
        mapService.destroy();
        close();
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

    private record MapEntry<K, V>(K key, V value) {
    }
}
