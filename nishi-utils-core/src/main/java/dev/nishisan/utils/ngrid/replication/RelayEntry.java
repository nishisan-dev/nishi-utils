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

package dev.nishisan.utils.ngrid.replication;

import java.util.Objects;
import java.util.UUID;

/**
 * One entry of a follower's relay-log (#124). It carries the ordering/fencing
 * metadata ({@code epoch}, {@code sequence}) and identity ({@code operationId},
 * {@code topic}) of a replicated operation, plus the already-encoded operation
 * payload ({@code payloadBytes}, produced by the topic's
 * {@link ReplicationHandler#encodePayload(Object)}).
 *
 * <p>The relay stores these as a compact binary frame (see {@link RelayEntryCodec})
 * inside a {@code NQueue<byte[]>}, so the apply consumer can fence on
 * {@code epoch}/{@code sequence} without decoding the payload.
 *
 * @param epoch         the leader epoch that produced the operation (fencing)
 * @param sequence      the per-topic sequence number (ordering)
 * @param topic         the replication topic
 * @param operationId   the operation identifier (idempotency/dedup)
 * @param payloadBytes  the encoded operation payload (never {@code null})
 */
record RelayEntry(long epoch, long sequence, String topic, UUID operationId, byte[] payloadBytes) {

    RelayEntry {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(payloadBytes, "payloadBytes");
    }
}
