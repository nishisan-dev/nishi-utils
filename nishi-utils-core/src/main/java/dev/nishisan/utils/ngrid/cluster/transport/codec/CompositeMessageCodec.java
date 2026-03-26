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

import java.io.IOException;
import java.util.Arrays;

/**
 * Composite codec that dispatches between {@link BinaryFrameCodec} and
 * {@link JacksonMessageCodec} based on the message type (for encoding) or
 * the first byte of the frame (for decoding).
 * <p>
 * <strong>Encoding rules:</strong>
 * <ul>
 *   <li>HEARTBEAT and PING messages → {@link BinaryFrameCodec}</li>
 *   <li>All other messages → marker byte {@code 0x00} + JSON bytes via {@link JacksonMessageCodec}</li>
 * </ul>
 * <p>
 * <strong>Decoding rules:</strong>
 * <ul>
 *   <li>First byte {@code 0x01} or {@code 0x02} → {@link BinaryFrameCodec}</li>
 *   <li>First byte {@code 0x00} → strip marker, decode JSON via {@link JacksonMessageCodec}</li>
 *   <li>First byte {@code 0x7B} ('{') → legacy JSON without marker (backward compat)</li>
 * </ul>
 * <p>
 * This codec is thread-safe.
 */
public final class CompositeMessageCodec implements MessageCodec {

    /** Marker byte prepended to JSON frames to distinguish from binary frames. */
    static final byte JSON_MARKER = 0x00;

    /** First byte of a raw JSON object ('{' character). */
    private static final byte JSON_BRACE = 0x7B;

    private final BinaryFrameCodec binaryCodec;
    private final JacksonMessageCodec jacksonCodec;

    /**
     * Creates a composite codec with default sub-codecs.
     */
    public CompositeMessageCodec() {
        this(new BinaryFrameCodec(), new JacksonMessageCodec());
    }

    /**
     * Creates a composite codec with the given sub-codecs.
     *
     * @param binaryCodec  the binary frame codec for HEARTBEAT/PING
     * @param jacksonCodec the Jackson codec for all other message types
     */
    public CompositeMessageCodec(BinaryFrameCodec binaryCodec, JacksonMessageCodec jacksonCodec) {
        this.binaryCodec = binaryCodec;
        this.jacksonCodec = jacksonCodec;
    }

    @Override
    public byte[] encode(ClusterMessage message) throws IOException {
        if (binaryCodec.supports(message.type())) {
            return binaryCodec.encode(message);
        }

        byte[] jsonBytes = jacksonCodec.encode(message);
        byte[] framed = new byte[1 + jsonBytes.length];
        framed[0] = JSON_MARKER;
        System.arraycopy(jsonBytes, 0, framed, 1, jsonBytes.length);
        return framed;
    }

    @Override
    public ClusterMessage decode(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            throw new IOException("Empty frame");
        }

        byte firstByte = data[0];

        // Binary frame (HEARTBEAT or PING)
        if (BinaryFrameCodec.isBinaryFrame(firstByte)) {
            return binaryCodec.decode(data);
        }

        // JSON with marker byte
        if (firstByte == JSON_MARKER) {
            return jacksonCodec.decode(Arrays.copyOfRange(data, 1, data.length));
        }

        // Legacy JSON without marker (starts with '{')
        if (firstByte == JSON_BRACE) {
            return jacksonCodec.decode(data);
        }

        throw new IOException("Unknown frame type marker: 0x" + Integer.toHexString(firstByte & 0xFF));
    }
}
