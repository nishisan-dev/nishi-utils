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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.nishisan.utils.map.NMapOperationType;

import java.util.Objects;

/**
 * Serializable replication command for distributed map operations.
 * Uses {@link NMapOperationType} to indicate the operation type.
 */
public final class MapReplicationCommand  {

    private final NMapOperationType type;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object key;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object value;

    @JsonCreator
    private MapReplicationCommand(
            @JsonProperty("type") NMapOperationType type,
            @JsonProperty("key") Object key,
            @JsonProperty("value") Object value) {
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
    public static MapReplicationCommand put(Object key, Object value) {
        return new MapReplicationCommand(NMapOperationType.PUT, key, value);
    }

    /**
     * Creates a REMOVE replication command.
     *
     * @param key the key
     * @return the command
     */
    public static MapReplicationCommand remove(Object key) {
        return new MapReplicationCommand(NMapOperationType.REMOVE, key, null);
    }

    /**
     * Returns the command type.
     * 
     * @return the type
     */
    public NMapOperationType type() {
        return type;
    }

    /**
     * Returns the key.
     * 
     * @return the key
     */
    public Object key() {
        return key;
    }

    /**
     * Returns the value (may be {@code null} for REMOVE).
     * 
     * @return the value
     */
    public Object value() {
        return value;
    }
}
