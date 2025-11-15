package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.replication.ReplicationResult;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple distributed map that relies on the replication layer to keep replicas aligned.
 */
public final class MapClusterService<K extends Serializable, V extends Serializable> {
    public static final String TOPIC = "map";

    private final ConcurrentMap<K, V> data = new ConcurrentHashMap<>();
    private final ReplicationManager replicationManager;

    public MapClusterService(ReplicationManager replicationManager) {
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.replicationManager.registerHandler(TOPIC, this::applyReplication);
    }

    public Optional<V> put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        V previous = data.get(key);
        MapReplicationCommand command = MapReplicationCommand.put(key, value);
        waitForReplication(replicationManager.replicate(TOPIC, command));
        return Optional.ofNullable(previous);
    }

    public Optional<V> remove(K key) {
        Objects.requireNonNull(key, "key");
        V previous = data.get(key);
        MapReplicationCommand command = MapReplicationCommand.remove(key);
        waitForReplication(replicationManager.replicate(TOPIC, command));
        return Optional.ofNullable(previous);
    }

    public Optional<V> get(K key) {
        return Optional.ofNullable(data.get(key));
    }

    private void waitForReplication(CompletableFuture<ReplicationResult> future) {
        future.join();
    }

    @SuppressWarnings("unchecked")
    private void applyReplication(UUID operationId, Serializable payload) {
        MapReplicationCommand command = (MapReplicationCommand) payload;
        switch (command.type()) {
            case PUT -> data.put((K) command.key(), (V) command.value());
            case REMOVE -> data.remove((K) command.key());
        }
    }
}
