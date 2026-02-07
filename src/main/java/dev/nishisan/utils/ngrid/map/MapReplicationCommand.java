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

    /**
     * Creates a PUT replication command.
     *
     * @param key   the key
     * @param value the value
     * @return the command
     */
    public static MapReplicationCommand put(Serializable key, Serializable value) {
        return new MapReplicationCommand(MapReplicationCommandType.PUT, key, value);
    }

    /**
     * Creates a REMOVE replication command.
     *
     * @param key the key
     * @return the command
     */
    public static MapReplicationCommand remove(Serializable key) {
        return new MapReplicationCommand(MapReplicationCommandType.REMOVE, key, null);
    }

    /**
     * Returns the command type.
     * 
     * @return the type
     */
    public MapReplicationCommandType type() {
        return type;
    }

    /**
     * Returns the key.
     * 
     * @return the key
     */
    public Serializable key() {
        return key;
    }

    /**
     * Returns the value (may be {@code null} for REMOVE).
     * 
     * @return the value
     */
    public Serializable value() {
        return value;
    }
}
