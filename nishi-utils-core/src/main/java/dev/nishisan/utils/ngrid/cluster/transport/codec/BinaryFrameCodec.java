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

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Binary codec optimized for high-frequency, lightweight messages
 * ({@link MessageType#HEARTBEAT} and {@link MessageType#PING}).
 * <p>
 * These messages are serialized as compact binary frames instead of JSON,
 * reducing CPU overhead from serialization and eliminating unnecessary
 * object graph traversal.
 * <p>
 * Frame layout:
 * <pre>
 * | offset   | size     | field                                   |
 * |----------|----------|-----------------------------------------|
 * | 0        | 1 byte   | messageType (0x01=HEARTBEAT, 0x02=PING) |
 * | 1        | 2 bytes  | sourceNodeId length (unsigned short)    |
 * | 3        | N bytes  | sourceNodeId (UTF-8)                    |
 * | 3+N      | 8 bytes  | epochMilli                              |
 * | 11+N     | 8 bytes  | leaderHighWatermark                     |
 * | 19+N     | 8 bytes  | leaderEpoch                             |
 * | 27+N     | 1 byte   | leader flag (0/1) — OPTIONAL trailing   |
 * | 28+N     | 2 bytes  | topic count (unsigned short) — OPTIONAL |
 * | 30+N     | ...      | count × (u16 len, UTF-8 topic, i64 frontier) |
 * </pre>
 * <p>
 * The trailing leader flag (issue tems#9, D10c) is wire-compatible in both directions: an old
 * decoder stops after the three longs and ignores trailing bytes; a new decoder reads the flag
 * only when present ({@code remaining() > 0}) and defaults to {@code false} for old frames.
 * <p>
 * The trailing per-topic frontier section (issue #178) follows the same rule: it is emitted only
 * when the payload carries a non-empty vector, a decoder that predates it ignores the trailing
 * bytes, and a new decoder reads it only when at least the count is present ({@code remaining() >= 2}),
 * decoding an empty map for older frames. At most {@link #MAX_TOPICS_PER_FRAME} topics are encoded
 * (sorted by name; the sender logs once when it has to truncate).
 * <p>
 * This codec is thread-safe.
 *
 * @see CompositeMessageCodec
 */
public final class BinaryFrameCodec {

    /** Magic byte identifying a HEARTBEAT frame. */
    public static final byte HEARTBEAT_MARKER = 0x01;

    /** Magic byte identifying a PING frame. */
    public static final byte PING_MARKER = 0x02;

    /** Sentinel UUID used for lightweight messages that don't need correlation. */
    static final UUID ZERO_UUID = new UUID(0, 0);

    /** Upper bound of per-topic frontiers carried in one heartbeat frame (issue #178). */
    public static final int MAX_TOPICS_PER_FRAME = 255;

    private static final java.util.concurrent.atomic.AtomicBoolean TRUNCATION_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean();
    private static final java.util.logging.Logger LOGGER =
            java.util.logging.Logger.getLogger(BinaryFrameCodec.class.getName());

    /**
     * Returns {@code true} if the given message type can be encoded by this codec.
     *
     * @param type the message type to check
     * @return {@code true} for HEARTBEAT and PING
     */
    public boolean supports(MessageType type) {
        return type == MessageType.HEARTBEAT || type == MessageType.PING;
    }

    /**
     * Returns {@code true} if the first byte of the frame indicates a binary frame.
     *
     * @param marker the first byte of the frame
     * @return {@code true} if it is a known binary marker
     */
    public static boolean isBinaryFrame(byte marker) {
        return marker == HEARTBEAT_MARKER || marker == PING_MARKER;
    }

    /**
     * Encodes a HEARTBEAT or PING {@link ClusterMessage} into a compact binary frame.
     *
     * @param message the message to encode (must be HEARTBEAT or PING with a HeartbeatPayload)
     * @return the encoded bytes
     * @throws IOException if the message type is unsupported
     */
    public byte[] encode(ClusterMessage message) throws IOException {
        byte marker = switch (message.type()) {
            case HEARTBEAT -> HEARTBEAT_MARKER;
            case PING -> PING_MARKER;
            default -> throw new IOException("BinaryFrameCodec does not support type: " + message.type());
        };

        HeartbeatPayload payload = message.payload(HeartbeatPayload.class);
        byte[] sourceBytes = message.source().value().getBytes(StandardCharsets.UTF_8);

        // Per-topic frontier section (issue #178): only for HEARTBEAT with a non-empty vector.
        java.util.List<byte[]> topicNames = new java.util.ArrayList<>();
        java.util.List<Long> frontiers = new java.util.ArrayList<>();
        int sectionBytes = 0;
        if (marker == HEARTBEAT_MARKER && !payload.topicFrontiers().isEmpty()) {
            int count = 0;
            for (java.util.Map.Entry<String, Long> e : payload.topicFrontiers().entrySet()) {
                if (count == MAX_TOPICS_PER_FRAME) {
                    if (TRUNCATION_WARNED.compareAndSet(false, true)) {
                        LOGGER.warning(() -> "Heartbeat carries more than " + MAX_TOPICS_PER_FRAME
                                + " replicated topics; only the first " + MAX_TOPICS_PER_FRAME
                                + " (by name) are advertised in the frontier vector");
                    }
                    break;
                }
                byte[] name = e.getKey().getBytes(StandardCharsets.UTF_8);
                if (name.length > 0xFFFF) {
                    continue;
                }
                topicNames.add(name);
                frontiers.add(e.getValue());
                sectionBytes += 2 + name.length + 8;
                count++;
            }
            sectionBytes += 2; // count
        }

        // 1 (marker) + 2 (source length) + N (source) + 24 (3 longs) + 1 (leader flag) + section
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + sourceBytes.length + 24 + 1 + sectionBytes);
        buffer.put(marker);
        buffer.putShort((short) sourceBytes.length);
        buffer.put(sourceBytes);
        buffer.putLong(payload.epochMilli());
        buffer.putLong(payload.leaderHighWatermark());
        buffer.putLong(payload.leaderEpoch());
        buffer.put(payload.leader() ? (byte) 1 : (byte) 0);
        if (sectionBytes > 0) {
            buffer.putShort((short) topicNames.size());
            for (int i = 0; i < topicNames.size(); i++) {
                byte[] name = topicNames.get(i);
                buffer.putShort((short) name.length);
                buffer.put(name);
                buffer.putLong(frontiers.get(i));
            }
        }

        return buffer.array();
    }

    /**
     * Decodes a binary frame back into a {@link ClusterMessage} with a {@link HeartbeatPayload}.
     * <p>
     * The reconstructed message uses a {@link #ZERO_UUID} as messageId since these
     * messages are fire-and-forget (heartbeats) or use external correlation (pings).
     *
     * @param data the binary frame bytes (including the marker byte)
     * @return the decoded ClusterMessage
     * @throws IOException if the frame is malformed or uses an unknown marker
     */
    public ClusterMessage decode(byte[] data) throws IOException {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte marker = buffer.get();

            MessageType type = switch (marker) {
                case HEARTBEAT_MARKER -> MessageType.HEARTBEAT;
                case PING_MARKER -> MessageType.PING;
                default -> throw new IOException("Unknown binary frame marker: 0x"
                        + Integer.toHexString(marker & 0xFF));
            };

            short sourceLength = buffer.getShort();
            byte[] sourceBytes = new byte[sourceLength & 0xFFFF]; // unsigned
            buffer.get(sourceBytes);
            NodeId source = NodeId.of(new String(sourceBytes, StandardCharsets.UTF_8));

            long epochMilli = buffer.getLong();
            long leaderHighWatermark = buffer.getLong();
            long leaderEpoch = buffer.getLong();
            // Optional trailing leader flag (issue tems#9, D10c): absent in frames from older
            // peers — decode as false (a node that cannot assert is never treated as a dual leader).
            boolean leader = buffer.remaining() > 0 && buffer.get() != 0;
            // Optional trailing per-topic frontier section (issue #178): absent in frames from older
            // peers — decode as an empty vector (consumers fall back to the scalar watermark).
            java.util.Map<String, Long> topicFrontiers = java.util.Map.of();
            if (buffer.remaining() >= 2) {
                int count = buffer.getShort() & 0xFFFF;
                java.util.Map<String, Long> decoded = new java.util.LinkedHashMap<>();
                for (int i = 0; i < count; i++) {
                    int len = buffer.getShort() & 0xFFFF;
                    byte[] name = new byte[len];
                    buffer.get(name);
                    decoded.put(new String(name, StandardCharsets.UTF_8), buffer.getLong());
                }
                topicFrontiers = decoded;
            }

            HeartbeatPayload payload = new HeartbeatPayload(epochMilli, leaderHighWatermark, leaderEpoch, leader,
                    topicFrontiers);

            String qualifier = type == MessageType.HEARTBEAT ? "hb" : "rtt";
            return new ClusterMessage(ZERO_UUID, null, type, qualifier, source, null, payload, 1);
        } catch (BufferUnderflowException e) {
            throw new IOException("Truncated binary frame", e);
        }
    }
}
