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

import dev.nishisan.utils.ngrid.replication.QuorumUnreachableException;
import dev.nishisan.utils.ngrid.replication.ReplicationHandler;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.replication.ReplicationResult;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
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

/**
 * Simple distributed map that relies on the replication layer to keep replicas
 * aligned.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class MapClusterService<K extends Serializable, V extends Serializable>
        implements Closeable, ReplicationHandler {
    /** Topic prefix for map replication messages. */
    public static final String TOPIC_PREFIX = "map:";
    /** Default map name when none is specified. */
    public static final String DEFAULT_MAP_NAME = "default-map";

    private final ConcurrentMap<K, V> data = new ConcurrentHashMap<>();
    private final ReplicationManager replicationManager;
    private final MapPersistence<K, V> persistence;
    private final String topic;

    /**
     * Creates a map cluster service backed by the given replication manager
     * with no persistence.
     *
     * @param replicationManager the replication manager
     */
    public MapClusterService(ReplicationManager replicationManager) {
        this(replicationManager, null);
    }

    /**
     * Creates a map cluster service with optional persistence configuration.
     *
     * @param replicationManager the replication manager
     * @param persistenceConfig  persistence configuration, or {@code null} to
     *                           disable
     */
    public MapClusterService(ReplicationManager replicationManager, MapPersistenceConfig persistenceConfig) {
        this(replicationManager,
                topicFor(persistenceConfig != null ? persistenceConfig.mapName() : DEFAULT_MAP_NAME),
                persistenceConfig);
    }

    /**
     * Creates a map cluster service with explicit topic and persistence
     * configuration.
     *
     * @param replicationManager the replication manager
     * @param topic              the replication topic
     * @param persistenceConfig  persistence configuration, or {@code null} to
     *                           disable
     */
    public MapClusterService(ReplicationManager replicationManager, String topic,
            MapPersistenceConfig persistenceConfig) {
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.topic = Objects.requireNonNull(topic, "topic");
        if (this.topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be blank");
        }
        this.replicationManager.registerHandler(this.topic, this);
        if (persistenceConfig != null && persistenceConfig.mode() != MapPersistenceMode.DISABLED) {
            this.persistence = new MapPersistence<>(persistenceConfig, data);
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
        MapReplicationCommand command = MapReplicationCommand.put(key, value);
        waitForReplication(replicationManager.replicate(topic, command));
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
        MapReplicationCommand command = MapReplicationCommand.remove(key);
        waitForReplication(replicationManager.replicate(topic, command));
        return Optional.ofNullable(previous);
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
    public void apply(UUID operationId, Serializable payload) {
        MapReplicationCommand command = (MapReplicationCommand) payload;
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
        }
        if (persistence != null) {
            // Persist locally on every node (leader and followers) when applying the
            // replicated command.
            if (topic.equals("map:_ngrid-queue-offsets")) {
                // Offsets must survive hard crashes to avoid duplicate delivery.
                persistence.appendSync(command.type(), command.key(), command.value());
            } else {
                persistence.appendAsync(command.type(), command.key(), command.value());
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
            return new SnapshotChunk(new HashMap<>(), false);
        }
        int end = Math.min(start + chunkSize, entries.size());
        Map<K, V> chunk = new HashMap<>();
        for (int i = start; i < end; i++) {
            Map.Entry<K, V> e = entries.get(i);
            chunk.put(e.getKey(), e.getValue());
        }
        return new SnapshotChunk((Serializable) chunk, end < entries.size());
    }

    /** {@inheritDoc} */
    @Override
    public Serializable getSnapshot() {
        return new HashMap<>(data);
    }

    /** {@inheritDoc} */
    @Override
    public void resetState() {
        data.clear();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void installSnapshot(Serializable snapshot) {
        if (snapshot instanceof Map<?, ?> newMap) {
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
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        if (persistence != null) {
            persistence.close();
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
