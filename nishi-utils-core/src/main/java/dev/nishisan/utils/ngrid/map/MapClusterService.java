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

package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.map.NMapConfig;
import dev.nishisan.utils.map.NMapOperationType;
import dev.nishisan.utils.map.NMapPersistence;
import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.replication.QuorumUnreachableException;
import dev.nishisan.utils.ngrid.replication.ReplicationHandler;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.replication.ReplicationResult;

import java.io.Closeable;
import java.io.IOException;

import java.util.Collections;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple distributed map that relies on the replication layer to keep replicas
 * aligned. Persistence is delegated to the standalone {@link NMapPersistence}
 * engine.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class MapClusterService<K, V>
        implements Closeable, ReplicationHandler {
    /** Topic prefix for map replication messages. */
    public static final String TOPIC_PREFIX = "map:";
    /** Default map name when none is specified. */
    public static final String DEFAULT_MAP_NAME = "default-map";

    private static final Logger LOGGER = Logger.getLogger(MapClusterService.class.getName());

    private final ConcurrentMap<K, V> data = new ConcurrentHashMap<>();
    private final ReplicationManager replicationManager;
    private final NMapPersistence<K, V> persistence;
    private final String topic;

    /**
     * Creates a map cluster service backed by the given replication manager
     * with no persistence.
     *
     * @param replicationManager the replication manager
     */
    public MapClusterService(ReplicationManager replicationManager) {
        this(replicationManager, topicFor(DEFAULT_MAP_NAME), null);
    }

    /**
     * Creates a map cluster service with explicit topic and no persistence.
     *
     * @param replicationManager the replication manager
     * @param topic              the replication topic
     * @param unused             kept for backward compatibility (pass {@code null})
     */
    public MapClusterService(ReplicationManager replicationManager, String topic,
            @SuppressWarnings("unused") Void unused) {
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.topic = Objects.requireNonNull(topic, "topic");
        if (this.topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be blank");
        }
        this.replicationManager.registerHandler(this.topic, this);
        this.persistence = null;
    }

    /**
     * Creates a map cluster service with persistence backed by the standalone
     * {@link NMapPersistence} engine.
     *
     * @param replicationManager the replication manager
     * @param topic              the replication topic
     * @param baseDir            base directory for map persistence files
     * @param mapName            logical map name (used for subdirectory)
     * @param nmapConfig         persistence configuration
     */
    public MapClusterService(ReplicationManager replicationManager, String topic,
            Path baseDir, String mapName, NMapConfig nmapConfig) {
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.topic = Objects.requireNonNull(topic, "topic");
        if (this.topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be blank");
        }
        this.replicationManager.registerHandler(this.topic, this);
        if (nmapConfig != null && nmapConfig.mode() != NMapPersistenceMode.DISABLED) {
            this.persistence = new NMapPersistence<>(nmapConfig, data, baseDir, mapName);
        } else {
            this.persistence = null;
        }
    }

    /**
     * Puts a key-value pair into the map, replicating the operation to the cluster.
     *
     * @param key   the key
     * @param value the value
     * @return the previous value associated with the key, or empty
     */
    public Optional<V> put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        V previous = data.get(key);
        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.put(key, value));
        waitForReplication(replicationManager.replicate(topic, encoded));
        return Optional.ofNullable(previous);
    }

    /**
     * Removes the mapping for a key, replicating the operation to the cluster.
     *
     * @param key the key to remove
     * @return the previous value, or empty if not present
     */
    public Optional<V> remove(K key) {
        Objects.requireNonNull(key, "key");
        V previous = data.get(key);
        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.remove(key));
        waitForReplication(replicationManager.replicate(topic, encoded));
        return Optional.ofNullable(previous);
    }

    /**
     * Replica um comando DESTROY para todos os nós do cluster, limpando os dados
     * em memória e removendo os arquivos de persistência em cada réplica.
     *
     * Fluxo Negocial
     * 1. Codifica um {@link MapReplicationCommand} do tipo {@code DESTROY}.
     * 2. Envia o comando pelo pipeline de replicação e aguarda quorum.
     * 3. Cada nó, ao aplicar o comando em {@link #apply}, limpa o
     *    {@code ConcurrentMap} e invoca {@code persistence.destroy()}.
     */
    public void destroyReplicated() {
        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.destroy());
        waitForReplication(replicationManager.replicate(topic, encoded));
    }

    /**
     * Removes all entries whose string key starts with {@code prefix} from the
     * <em>local</em> in-memory state only, writing tombstone entries to the
     * persistence layer if enabled. No Raft replication is triggered.
     *
     * <p>
     * This is intentionally <em>local-only</em> so it can be called safely
     * during {@link #resetState()}, which runs while the cluster is installing a
     * snapshot and may not yet have quorum for write operations.
     *
     * @param prefix the key prefix (must not be null)
     */
    @SuppressWarnings("unchecked")
    public void clearLocalByPrefix(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        for (K key : new java.util.ArrayList<>(data.keySet())) {
            if (key instanceof String s && s.startsWith(prefix)) {
                data.remove(key);
                if (persistence != null) {
                    persistence.appendSync(NMapOperationType.REMOVE, key, null);
                }
            }
        }
    }

    /**
     * Retrieves the value associated with a key from the local replica.
     *
     * @param key the key to look up
     * @return an optional containing the value, or empty if not present
     */
    public Optional<V> get(K key) {
        return Optional.ofNullable(data.get(key));
    }

    /**
     * Returns an unmodifiable view of the keys in the local replica.
     * This is an eventually-consistent snapshot — no replication or
     * leader routing is involved.
     *
     * @return an unmodifiable set of keys
     */
    public java.util.Set<K> keySet() {
        return Collections.unmodifiableSet(data.keySet());
    }

    /**
     * Returns the number of entries in the local replica.
     * This is an eventually-consistent value — no replication or
     * leader routing is involved.
     *
     * @return the number of entries
     */
    public int size() {
        return data.size();
    }

    /**
     * Loads map state from disk (snapshot + WAL) and starts the persistence
     * background writer when enabled.
     * This is a no-op when persistence is disabled.
     */
    public void loadFromDisk() {
        if (persistence == null) {
            return;
        }
        persistence.load();
        persistence.start();
    }

    private void waitForReplication(CompletableFuture<ReplicationResult> future) {
        try {
            long timeoutMs = Math.max(1L, replicationManager.operationTimeout().toMillis());
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new IllegalStateException("Replication operation timed out", cause);
            } else if (cause instanceof QuorumUnreachableException) {
                throw new IllegalStateException("Quorum unreachable for replication operation", cause);
            } else {
                throw new IllegalStateException("Replication operation failed", cause != null ? cause : e);
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("Replication operation timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Replication operation interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new IllegalStateException("Replication operation timed out", cause);
            } else if (cause instanceof QuorumUnreachableException) {
                throw new IllegalStateException("Quorum unreachable for replication operation", cause);
            } else {
                throw new IllegalStateException("Replication operation failed", cause != null ? cause : e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void apply(UUID operationId, Object payload) {
        // The payload is transported as byte[] to preserve concrete POJO types across
        // the Jackson serialization boundary (see MapReplicationCodec for rationale).
        MapReplicationCommand command = MapReplicationCodec.decode((byte[]) payload);
        switch (command.type()) {
            case PUT -> {
                // For offset maps, apply monotonic semantics: only accept higher values
                if (topic.equals("map:_ngrid-queue-offsets") && command.value() instanceof Long newValue) {
                    data.compute((K) command.key(), (k, currentValue) -> {
                        if (currentValue instanceof Long currentLong) {
                            if (newValue > currentLong) {
                                return (V) newValue;
                            }
                            // Ignore regression
                            return currentValue;
                        }
                        return (V) newValue;
                    });
                } else {
                    data.put((K) command.key(), (V) command.value());
                }
            }
            case REMOVE -> data.remove((K) command.key());
            case DESTROY -> {
                data.clear();
                if (persistence != null) {
                    try {
                        persistence.destroy();
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "Failed to destroy persistence files for topic: " + topic, e);
                    }
                }
                return; // skip normal persistence append below
            }
        }
        if (persistence != null) {
            NMapOperationType opType = command.type();
            // Persist locally on every node (leader and followers) when applying the
            // replicated command.
            if (topic.equals("map:_ngrid-queue-offsets")) {
                // Offsets must survive hard crashes to avoid duplicate delivery.
                persistence.appendSync(opType, command.key(), command.value());
            } else {
                persistence.appendAsync(opType, command.key(), command.value());
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public SnapshotChunk getSnapshotChunk(int chunkIndex) {
        int chunkSize = 1000;
        List<Map.Entry<K, V>> entries = new ArrayList<>(data.entrySet());
        int start = chunkIndex * chunkSize;
        if (start >= entries.size()) {
            return new SnapshotChunk(MapReplicationCodec.encodeSnapshot(new HashMap<>()), false);
        }
        int end = Math.min(start + chunkSize, entries.size());
        Map<K, V> chunk = new HashMap<>();
        for (int i = start; i < end; i++) {
            Map.Entry<K, V> e = entries.get(i);
            chunk.put(e.getKey(), e.getValue());
        }
        // Encode to byte[] so that the transport layer preserves concrete POJO types
        // inside the snapshot values (see MapReplicationCodec for rationale).
        return new SnapshotChunk(MapReplicationCodec.encodeSnapshot(chunk), end < entries.size());
    }

    /** {@inheritDoc} */
    @Override
    public Object getSnapshot() {
        return MapReplicationCodec.encodeSnapshot(new HashMap<>(data));
    }

    /** {@inheritDoc} */
    @Override
    public void resetState() {
        data.clear();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void installSnapshot(Object snapshot) {
        // Snapshots are transported as byte[] (produced by MapReplicationCodec.encodeSnapshot)
        // to preserve the concrete types of POJO values across the Jackson boundary.
        Map<Object, Object> newMap = MapReplicationCodec.decodeSnapshot((byte[]) snapshot);
        // For offset maps, apply monotonic semantics during snapshot install
        if (topic.equals("map:_ngrid-queue-offsets")) {
            for (Map.Entry<?, ?> entry : newMap.entrySet()) {
                K key = (K) entry.getKey();
                V newValue = (V) entry.getValue();
                if (newValue instanceof Long newLong) {
                    data.compute(key, (k, currentValue) -> {
                        if (currentValue instanceof Long currentLong) {
                            if (newLong > currentLong) {
                                return (V) newLong;
                            }
                            return currentValue;
                        }
                        return newValue;
                    });
                } else {
                    data.put(key, newValue);
                }
            }
        } else {
            data.putAll((Map<? extends K, ? extends V>) newMap);
        }
        if (persistence != null) {
            persistence.maybeSnapshot();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        if (persistence != null) {
            persistence.close();
        }
    }

    /**
     * Limpa todos os dados em memória e remove os arquivos de persistência
     * associados a este serviço de mapa no nó local.
     *
     * Operação exclusivamente local — não dispara replicação para o cluster.
     * Cada nó participante deve invocar este método individualmente para
     * destruir seus próprios arquivos.
     *
     * Fluxo Negocial
     * 1. Limpa o {@code ConcurrentMap} local com todos os dados do mapa.
     * 2. Se a persistência estiver habilitada, invoca {@code destroy()} na
     *    engine {@link NMapPersistence}, removendo WAL, snapshot e metadados.
     *
     * @throws IOException se ocorrer erro de I/O ao remover arquivos de persistência
     */
    public void destroy() throws IOException {
        data.clear();
        if (persistence != null) {
            persistence.destroy();
        }
    }

    /**
     * Returns {@code true} if no persistence failures have occurred.
     * This is always {@code true} when persistence is disabled.
     *
     * @return {@code true} if healthy
     */
    public boolean isHealthy() {
        return persistence == null || persistence.failureCount() == 0;
    }

    /**
     * Returns the number of persistence failures since this service was created.
     * Returns 0 when persistence is disabled.
     *
     * @return the failure count
     */
    public long persistenceFailureCount() {
        return persistence != null ? persistence.failureCount() : 0;
    }

    /**
     * Returns the replication topic used by this service.
     *
     * @return the topic string
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the replication topic for the given map name.
     *
     * @param mapName the map name
     * @return the topic string prefixed with {@value TOPIC_PREFIX}
     */
    public static String topicFor(String mapName) {
        Objects.requireNonNull(mapName, "mapName");
        if (mapName.isBlank()) {
            throw new IllegalArgumentException("mapName cannot be blank");
        }
        return TOPIC_PREFIX + mapName;
    }
}
