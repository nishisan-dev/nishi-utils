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
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.replication.ReplicationResult;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

/**
 * Simple distributed map that relies on the replication layer to keep replicas aligned.
 */
public final class MapClusterService<K extends Serializable, V extends Serializable> implements Closeable {
    public static final String TOPIC_PREFIX = "map:";
    public static final String DEFAULT_MAP_NAME = "default-map";

    private final ConcurrentMap<K, V> data = new ConcurrentHashMap<>();
    private final ReplicationManager replicationManager;
    private final MapPersistence<K, V> persistence;
    private final String topic;

    public MapClusterService(ReplicationManager replicationManager) {
        this(replicationManager, null);
    }

    public MapClusterService(ReplicationManager replicationManager, MapPersistenceConfig persistenceConfig) {
        this(replicationManager,
                topicFor(persistenceConfig != null ? persistenceConfig.mapName() : DEFAULT_MAP_NAME),
                persistenceConfig);
    }

    public MapClusterService(ReplicationManager replicationManager, String topic, MapPersistenceConfig persistenceConfig) {
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.topic = Objects.requireNonNull(topic, "topic");
        if (this.topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be blank");
        }
        this.replicationManager.registerHandler(this.topic, this::applyReplication);
        if (persistenceConfig != null && persistenceConfig.mode() != MapPersistenceMode.DISABLED) {
            this.persistence = new MapPersistence<>(persistenceConfig, data);
        } else {
            this.persistence = null;
        }
    }

    public Optional<V> put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        V previous = data.get(key);
        MapReplicationCommand command = MapReplicationCommand.put(key, value);
        waitForReplication(replicationManager.replicate(topic, command));
        return Optional.ofNullable(previous);
    }

    public Optional<V> remove(K key) {
        Objects.requireNonNull(key, "key");
        V previous = data.get(key);
        MapReplicationCommand command = MapReplicationCommand.remove(key);
        waitForReplication(replicationManager.replicate(topic, command));
        return Optional.ofNullable(previous);
    }

    public Optional<V> get(K key) {
        return Optional.ofNullable(data.get(key));
    }

    /**
     * Loads map state from disk (snapshot + WAL) and starts the persistence background writer when enabled.
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
            future.join();
        } catch (CompletionException e) {
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

    @SuppressWarnings("unchecked")
    private void applyReplication(UUID operationId, Serializable payload) {
        MapReplicationCommand command = (MapReplicationCommand) payload;
        switch (command.type()) {
            case PUT -> data.put((K) command.key(), (V) command.value());
            case REMOVE -> data.remove((K) command.key());
        }
        if (persistence != null) {
            // Persist locally on every node (leader and followers) when applying the replicated command.
            persistence.appendAsync(command.type(), command.key(), command.value());
        }
    }

    @Override
    public void close() throws IOException {
        if (persistence != null) {
            persistence.close();
        }
    }

    public String topic() {
        return topic;
    }

    public static String topicFor(String mapName) {
        Objects.requireNonNull(mapName, "mapName");
        if (mapName.isBlank()) {
            throw new IllegalArgumentException("mapName cannot be blank");
        }
        return TOPIC_PREFIX + mapName;
    }
}
