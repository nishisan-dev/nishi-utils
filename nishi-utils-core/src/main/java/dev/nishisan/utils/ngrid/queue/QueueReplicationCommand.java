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

import dev.nishisan.utils.queue.NQueueHeaders;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Serializable command replicated across the cluster for queue operations.
 * Carries the optional routing key and headers introduced by the V3 record
 * format.
 */
public final class QueueReplicationCommand implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    private final QueueReplicationCommandType type;
    private final Serializable value;
    private final byte[] key; // nullable, routing/partitioning key
    private final NQueueHeaders headers; // never null

    private QueueReplicationCommand(QueueReplicationCommandType type, Serializable value,
            byte[] key, NQueueHeaders headers) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
        this.key = key;
        this.headers = headers != null ? headers : NQueueHeaders.empty();
    }

    /**
     * Creates an OFFER command without key or headers (simple queue use-case).
     */
    public static QueueReplicationCommand offer(Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.OFFER, value, null, NQueueHeaders.empty());
    }

    /**
     * Creates an OFFER command with key and headers (Kafka-like use-case).
     */
    public static QueueReplicationCommand offer(byte[] key, NQueueHeaders headers, Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.OFFER, value, key, headers);
    }

    public static QueueReplicationCommand poll(Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.POLL, value, null, NQueueHeaders.empty());
    }

    public QueueReplicationCommandType type() {
        return type;
    }

    public Serializable value() {
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
