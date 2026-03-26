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

package dev.nishisan.utils.ngrid.cluster.transport;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;

import dev.nishisan.utils.ngrid.common.ClusterMessage;

import java.io.IOException;

/**
 * Jackson-based {@link MessageCodec} implementation.
 * <p>
 * Uses field-access, ignores unknown properties for forward-compatibility,
 * and activates default typing so polymorphic payloads (Object fields annotated
 * with {@code @JsonTypeInfo}) are correctly round-tripped.
 * </p>
 * <p>
 * This implementation is <strong>thread-safe</strong> — the underlying
 * {@link ObjectMapper} is immutable after construction.
 * </p>
 */
public final class JacksonMessageCodec implements MessageCodec {

    private final ObjectMapper mapper;

    public JacksonMessageCodec() {
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();

        this.mapper = new ObjectMapper()
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);
    }

    @Override
    public byte[] encode(ClusterMessage message) throws IOException {
        return mapper.writeValueAsBytes(message);
    }

    @Override
    public ClusterMessage decode(byte[] data) throws IOException {
        return mapper.readValue(data, ClusterMessage.class);
    }
}
