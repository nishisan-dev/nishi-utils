package dev.nishisan.utils.ngrid.map;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Serializable replication command for distributed map operations.
 */
public final class MapReplicationCommand implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final MapReplicationCommandType type;
    private final Serializable key;
    private final Serializable value;

    private MapReplicationCommand(MapReplicationCommandType type, Serializable key, Serializable value) {
        this.type = Objects.requireNonNull(type, "type");
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
    }

    public static MapReplicationCommand put(Serializable key, Serializable value) {
        return new MapReplicationCommand(MapReplicationCommandType.PUT, key, value);
    }

    public static MapReplicationCommand remove(Serializable key) {
        return new MapReplicationCommand(MapReplicationCommandType.REMOVE, key, null);
    }

    public MapReplicationCommandType type() {
        return type;
    }

    public Serializable key() {
        return key;
    }

    public Serializable value() {
        return value;
    }
}
