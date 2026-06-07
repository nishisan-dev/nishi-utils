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

package dev.nishisan.utils.ngrid.queue;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

import java.io.IOException;

/**
 * Dedicated codec for serializing a {@link QueueReplicationCommand} to {@code byte[]}
 * with full polymorphic type fidelity, used to persist queue operations durably in a
 * follower's relay-log (#124).
 *
 * <p>The transport-level {@code JacksonMessageCodec} does not enable {@code defaultTyping},
 * so the concrete type of {@link QueueReplicationCommand#value()} (an arbitrary consumer
 * POJO) is lost when the command is rebuilt from the wire — a {@code LinkedHashMap} would
 * surface at {@link QueueClusterService#apply} and fail the cast. This codec uses a separate
 * {@link ObjectMapper} with {@code activateDefaultTyping}, scoped to the relay pipeline, so
 * the original type survives the relay round-trip without annotating consumer classes.
 *
 * <p>Symmetric to {@code MapReplicationCodec}.
 */
public final class QueueReplicationCodec {

    private static final ObjectMapper MAPPER = buildMapper();

    private QueueReplicationCodec() {
        // utility class
    }

    /**
     * Serializes a {@link QueueReplicationCommand} to JSON bytes with embedded type
     * metadata for the nested {@code value}.
     *
     * @param command the command to serialize (must not be {@code null})
     * @return the JSON bytes
     * @throws QueueReplicationCodecException if serialization fails
     */
    public static byte[] encode(QueueReplicationCommand command) {
        try {
            return MAPPER.writeValueAsBytes(command);
        } catch (IOException e) {
            throw new QueueReplicationCodecException("Failed to serialize QueueReplicationCommand", e);
        }
    }

    /**
     * Deserializes a {@link QueueReplicationCommand} from JSON bytes, restoring the
     * concrete type of {@code value}.
     *
     * @param bytes the JSON bytes produced by {@link #encode}
     * @return the deserialized command
     * @throws QueueReplicationCodecException if deserialization fails
     */
    public static QueueReplicationCommand decode(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, QueueReplicationCommand.class);
        } catch (IOException e) {
            throw new QueueReplicationCodecException("Failed to deserialize QueueReplicationCommand", e);
        }
    }

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.activateDefaultTyping(
                BasicPolymorphicTypeValidator.builder()
                        .allowIfBaseType(Object.class)
                        .build(),
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.PROPERTY);
        return mapper;
    }

    /**
     * Unchecked exception thrown when codec operations fail.
     */
    public static final class QueueReplicationCodecException extends RuntimeException {
        QueueReplicationCodecException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
