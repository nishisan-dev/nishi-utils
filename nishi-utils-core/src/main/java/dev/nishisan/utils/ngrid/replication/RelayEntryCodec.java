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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Compact, self-describing binary frame for a {@link RelayEntry}, stored as the
 * element of a follower's relay {@code NQueue<byte[]>}.
 *
 * <p>Layout (big-endian):
 * <pre>
 *   version(1) · epoch(8) · sequence(8) · uuidMsb(8) · uuidLsb(8)
 *   · topicLen(4) · topic(UTF-8) · payloadLen(4) · payload
 * </pre>
 * The frame is preferred over Java-serializing the record because the queue's
 * {@code ObjectOutputStream} codec would repeat the full class descriptor on every
 * element — wasteful for a high-volume relay.
 */
final class RelayEntryCodec {

    private static final byte VERSION = 1;
    /** version(1) + epoch(8) + sequence(8) + uuid(16) + topicLen(4) + payloadLen(4). */
    private static final int FIXED_OVERHEAD = 1 + 8 + 8 + 16 + 4 + 4;

    private RelayEntryCodec() {
    }

    static byte[] encode(RelayEntry entry) {
        byte[] topicBytes = entry.topic().getBytes(StandardCharsets.UTF_8);
        byte[] payload = entry.payloadBytes();
        ByteBuffer buf = ByteBuffer.allocate(FIXED_OVERHEAD + topicBytes.length + payload.length);
        buf.put(VERSION);
        buf.putLong(entry.epoch());
        buf.putLong(entry.sequence());
        buf.putLong(entry.operationId().getMostSignificantBits());
        buf.putLong(entry.operationId().getLeastSignificantBits());
        buf.putInt(topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(payload.length);
        buf.put(payload);
        return buf.array();
    }

    static RelayEntry decode(byte[] frame) {
        ByteBuffer buf = ByteBuffer.wrap(frame);
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported relay frame version: " + version);
        }
        long epoch = buf.getLong();
        long sequence = buf.getLong();
        long msb = buf.getLong();
        long lsb = buf.getLong();
        UUID operationId = new UUID(msb, lsb);
        byte[] topicBytes = new byte[buf.getInt()];
        buf.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);
        byte[] payload = new byte[buf.getInt()];
        buf.get(payload);
        return new RelayEntry(epoch, sequence, topic, operationId, payload);
    }
}
