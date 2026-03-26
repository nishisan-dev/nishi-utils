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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.nishisan.utils.queue.NQueueHeaders;

import java.util.Objects;

/**
 * Command replicated across the cluster for queue operations.
 * Carries the optional routing key and headers introduced by the V3 record
 * format.
 */
public final class QueueReplicationCommand {

    private final QueueReplicationCommandType type;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private final Object value;
    private final byte[] key; // nullable, routing/partitioning key
    private final NQueueHeaders headers; // never null

    @JsonCreator
    private QueueReplicationCommand(
            @JsonProperty("type") QueueReplicationCommandType type,
            @JsonProperty("value") Object value,
            @JsonProperty("key") byte[] key,
            @JsonProperty("headers") NQueueHeaders headers) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
        this.key = key;
        this.headers = headers != null ? headers : NQueueHeaders.empty();
    }

    /**
     * Creates an OFFER command without key or headers (simple queue use-case).
     */
    public static QueueReplicationCommand offer(Object value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.OFFER, value, null, NQueueHeaders.empty());
    }

    /**
     * Creates an OFFER command with key and headers (Kafka-like use-case).
     */
    public static QueueReplicationCommand offer(byte[] key, NQueueHeaders headers, Object value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.OFFER, value, key, headers);
    }

    public static QueueReplicationCommand poll(Object value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.POLL, value, null, NQueueHeaders.empty());
    }

    public QueueReplicationCommandType type() {
        return type;
    }

    public Object value() {
        return value;
    }

    /** Returns the routing/partitioning key, or {@code null} if absent. */
    public byte[] key() {
        return key;
    }

    /** Returns the record headers; never {@code null}. */
    public NQueueHeaders headers() {
        return headers;
    }
}
