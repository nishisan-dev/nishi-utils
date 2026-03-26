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

package dev.nishisan.utils.ngrid.cluster.transport.codec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.nishisan.utils.ngrid.common.ClusterMessage;

import java.io.IOException;

/**
 * Jackson-based implementation of {@link MessageCodec}.
 * <p>
 * Serializes {@link ClusterMessage} instances to JSON bytes using a pre-configured
 * {@link ObjectMapper}. This codec is thread-safe and designed to be shared across
 * all connections in a transport.
 * <p>
 * The {@code ObjectMapper} is configured to:
 * <ul>
 *   <li>Detect fields directly (no getters/setters required)</li>
 *   <li>Ignore unknown properties during deserialization (forward compatibility)</li>
 *   <li>Support polymorphic payload types via {@code @JsonTypeInfo} on ClusterMessage</li>
 * </ul>
 */
public final class JacksonMessageCodec implements MessageCodec {

    private final ObjectMapper mapper;

    /**
     * Creates a codec with the default ObjectMapper configuration.
     */
    public JacksonMessageCodec() {
        this(createDefaultMapper());
    }

    /**
     * Creates a codec with a custom ObjectMapper.
     *
     * @param mapper the ObjectMapper to use
     */
    public JacksonMessageCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] encode(ClusterMessage message) throws IOException {
        try {
            return mapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to encode ClusterMessage", e);
        }
    }

    @Override
    public ClusterMessage decode(byte[] data) throws IOException {
        try {
            return mapper.readValue(data, ClusterMessage.class);
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to decode ClusterMessage", e);
        }
    }

    /**
     * Returns the underlying ObjectMapper for testing or extension.
     *
     * @return the ObjectMapper
     */
    public ObjectMapper mapper() {
        return mapper;
    }

    /**
     * Creates a pre-configured ObjectMapper suitable for NGrid message serialization.
     *
     * @return a new ObjectMapper
     */
    public static ObjectMapper createDefaultMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Use field access — our domain classes use final fields without setters
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
        // Forward compatibility: ignore fields from newer protocol versions
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Don't fail on empty beans (some payloads have very few fields)
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }
}
