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
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

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
 *
 * <p><strong>Extension point (#110):</strong> the underlying {@link ObjectMapper} can be
 * customized at bootstrap via {@link #registerModule}, {@link #addMixIn} or
 * {@link #registerCustomizer}, composing with (not replacing) the default typing. This is
 * <em>global to the codec</em> (process-wide) and applies symmetrically to serialization
 * and deserialization — e.g. attaching {@code @JsonIdentityInfo} to a self-referential DTO
 * via a mixin to break cycles without annotating the POJO globally.
 */
public final class MapReplicationCodec {

    /**
     * Customizers applied to the codec's {@link ObjectMapper} <em>after</em> the base
     * configuration (visibility + default typing), in registration order. Used to add
     * Jackson {@link Module}s or Mixins without touching the base default-typing setup.
     *
     * <p><strong>Scope:</strong> global to the codec (process-wide), since the codec is
     * stateless and shared by all distributed maps. Register at bootstrap, before any
     * replication traffic. The same configured mapper is used for serialization
     * (put/snapshot) and deserialization, so customizations apply symmetrically.
     */
    private static final List<Consumer<ObjectMapper>> CUSTOMIZERS = new CopyOnWriteArrayList<>();

    private static volatile ObjectMapper MAPPER = buildMapper();

    private MapReplicationCodec() {
        // utility class
    }

    // -------------------------------------------------------------------------
    // Extension point — customizing the codec's ObjectMapper (#110)
    // -------------------------------------------------------------------------

    /**
     * Registers a Jackson {@link Module} on the codec's {@link ObjectMapper}. The module
     * <em>composes</em> with the existing default-typing configuration (it is not
     * replaced) and is applied symmetrically to serialization and deserialization.
     *
     * <p>Global to the codec; call at bootstrap before any replication traffic.
     *
     * @param module the Jackson module to register (must not be {@code null})
     */
    public static void registerModule(Module module) {
        Objects.requireNonNull(module, "module");
        registerCustomizer(m -> m.registerModule(module));
    }

    /**
     * Registers a Jackson <em>mixin</em> on the codec's {@link ObjectMapper}, associating
     * {@code mixinSource}'s annotations with {@code target} <em>only</em> within the
     * replication codec — without annotating the target POJO globally. Useful, e.g., to
     * attach {@code @JsonIdentityInfo} to a self-referential DTO to break cycles and
     * deduplicate by id during the round-trip.
     *
     * <p>Composes with default typing and applies symmetrically. Global to the codec;
     * call at bootstrap before any replication traffic.
     *
     * @param target      the class whose serialization to customize (must not be {@code null})
     * @param mixinSource the class carrying the Jackson annotations (must not be {@code null})
     */
    public static void addMixIn(Class<?> target, Class<?> mixinSource) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(mixinSource, "mixinSource");
        registerCustomizer(m -> m.addMixIn(target, mixinSource));
    }

    /**
     * Registers an arbitrary customizer applied to the codec's {@link ObjectMapper} after
     * the base configuration. Escape hatch for customizations not covered by
     * {@link #registerModule} / {@link #addMixIn}.
     *
     * @param customizer the customizer to apply (must not be {@code null})
     */
    public static synchronized void registerCustomizer(Consumer<ObjectMapper> customizer) {
        Objects.requireNonNull(customizer, "customizer");
        CUSTOMIZERS.add(customizer);
        MAPPER = buildMapper();
    }

    /**
     * Clears all registered customizers and rebuilds the base mapper, restoring the
     * default behavior. Intended for test isolation.
     */
    static synchronized void resetCustomizers() {
        CUSTOMIZERS.clear();
        MAPPER = buildMapper();
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

        // Apply registered customizers last so they compose with (and can refine) the
        // base configuration above (#110). With no customizers this is a no-op (RF6).
        for (Consumer<ObjectMapper> customizer : CUSTOMIZERS) {
            customizer.accept(mapper);
        }

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
