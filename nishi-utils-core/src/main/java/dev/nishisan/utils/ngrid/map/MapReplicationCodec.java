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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

import java.io.IOException;
import java.util.Map;

/**
 * Dedicated codec for serializing and deserializing {@link MapReplicationCommand}
 * and map snapshots with full polymorphic type fidelity.
 *
 * <p>The standard {@code JacksonMessageCodec} used by the transport layer does not
 * enable {@code defaultTyping}, which means that POJO values stored in a
 * {@code DistributedMap<K, V>} are not annotated with type information when nested
 * inside a {@code Map} container. Upon deserialization on followers or during
 * snapshot sync, Jackson falls back to {@code LinkedHashMap}, causing a
 * {@code ClassCastException} at the call site.
 *
 * <p>This codec solves the problem by using a separate {@link ObjectMapper} configured
 * with {@code activateDefaultTyping}, scoped only to the map replication pipeline.
 * The command is serialized to {@code byte[]} <em>before</em> entering the
 * {@link dev.nishisan.utils.ngrid.common.ReplicationPayload}, and deserialized back
 * only inside {@link MapClusterService#apply}, ensuring the concrete POJO type is
 * always preserved end-to-end without requiring any Jackson annotations on consumer
 * domain classes.
 *
 * This codec is used within the map replication subsystem and by {@link
 * dev.nishisan.utils.ngrid.structures.DistributedMap} for the CLIENT_REQUEST
 * forwarding path (follower → leader).
 */
public final class MapReplicationCodec {

    /**
     * Allows any non-final class to carry type metadata.
     * We scope to {@code NON_FINAL} (instead of {@code EVERYTHING}) to avoid
     * interfering with JDK primitives and standard collections subclasses while
     * still covering arbitrary user-defined POJOs.
     */
    private static final ObjectMapper MAPPER = buildMapper();

    private MapReplicationCodec() {
        // utility class
    }

    // -------------------------------------------------------------------------
    // MapReplicationCommand round-trip
    // -------------------------------------------------------------------------

    /**
     * Serializes a {@link MapReplicationCommand} to a JSON byte array with full
     * type metadata embedded for all nested objects.
     *
     * @param command the command to serialize (must not be {@code null})
     * @return the JSON bytes
     * @throws MapReplicationCodecException if serialization fails
     */
    public static byte[] encode(MapReplicationCommand command) {
        try {
            return MAPPER.writeValueAsBytes(command);
        } catch (IOException e) {
            throw new MapReplicationCodecException("Failed to serialize MapReplicationCommand", e);
        }
    }

    /**
     * Deserializes a {@link MapReplicationCommand} from JSON bytes, restoring the
     * original concrete types for {@code key} and {@code value}.
     *
     * @param bytes the JSON bytes produced by {@link #encode}
     * @return the deserialized command
     * @throws MapReplicationCodecException if deserialization fails
     */
    public static MapReplicationCommand decode(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, MapReplicationCommand.class);
        } catch (IOException e) {
            throw new MapReplicationCodecException("Failed to deserialize MapReplicationCommand", e);
        }
    }

    // -------------------------------------------------------------------------
    // Snapshot (Map<K,V>) round-trip
    // -------------------------------------------------------------------------

    /**
     * Serializes a map snapshot chunk to JSON bytes. Each value in the map will
     * carry its concrete type so that followers can reconstruct the original POJOs.
     *
     * @param snapshot the map chunk to serialize (must not be {@code null})
     * @return the JSON bytes
     * @throws MapReplicationCodecException if serialization fails
     */
    public static byte[] encodeSnapshot(Map<?, ?> snapshot) {
        try {
            return MAPPER.writeValueAsBytes(snapshot);
        } catch (IOException e) {
            throw new MapReplicationCodecException("Failed to serialize map snapshot", e);
        }
    }

    /**
     * Deserializes a map snapshot chunk from JSON bytes. Keys and values are
     * restored to their original concrete types.
     *
     * @param bytes the JSON bytes produced by {@link #encodeSnapshot}
     * @return the deserialized map
     * @throws MapReplicationCodecException if deserialization fails
     */
    @SuppressWarnings("unchecked")
    public static Map<Object, Object> decodeSnapshot(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, Map.class);
        } catch (IOException e) {
            throw new MapReplicationCodecException("Failed to deserialize map snapshot", e);
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Field access — domain classes typically use final fields without setters
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);

        // Forward compatibility: ignore unknown fields from future versions
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        // Enable polymorphic type information for all NON_FINAL types.
        // This is the key configuration that distinguishes this mapper from the
        // transport-level JacksonMessageCodec and allows arbitrary POJOs to survive
        // the serialization round-trip without any Jackson annotations on the POJO itself.
        mapper.activateDefaultTyping(
                BasicPolymorphicTypeValidator.builder()
                        .allowIfBaseType(Object.class)
                        .build(),
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.PROPERTY
        );

        return mapper;
    }

    /**
     * Unchecked exception thrown when codec operations fail.
     */
    static final class MapReplicationCodecException extends RuntimeException {
        MapReplicationCodecException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
