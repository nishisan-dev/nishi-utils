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
 * </pre>
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

        // 1 (marker) + 2 (source length) + N (source) + 24 (3 longs)
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + sourceBytes.length + 24);
        buffer.put(marker);
        buffer.putShort((short) sourceBytes.length);
        buffer.put(sourceBytes);
        buffer.putLong(payload.epochMilli());
        buffer.putLong(payload.leaderHighWatermark());
        buffer.putLong(payload.leaderEpoch());

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

            HeartbeatPayload payload = new HeartbeatPayload(epochMilli, leaderHighWatermark, leaderEpoch);

            String qualifier = type == MessageType.HEARTBEAT ? "hb" : "rtt";
            return new ClusterMessage(ZERO_UUID, null, type, qualifier, source, null, payload, 1);
        } catch (BufferUnderflowException e) {
            throw new IOException("Truncated binary frame", e);
        }
    }
}
