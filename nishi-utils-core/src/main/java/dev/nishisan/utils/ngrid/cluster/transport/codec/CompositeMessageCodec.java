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
 *   <li>HEARTBEAT and PING messages → {@link BinaryFrameCodec} (never compressed)</li>
 *   <li>All other messages → JSON via {@link JacksonMessageCodec}, then either:
 *     <ul>
 *       <li>marker {@code 0x10} + LZ4-compressed JSON, when {@link #setCompressOutput(boolean)}
 *           is enabled, the JSON is at least {@code compressionMinSize} bytes, and the
 *           compressed frame is actually smaller; otherwise</li>
 *       <li>marker {@code 0x00} + raw JSON bytes.</li>
 *     </ul>
 *   </li>
 * </ul>
 * <p>
 * <strong>Decoding rules</strong> (always capable, independent of the outbound flag):
 * <ul>
 *   <li>First byte {@code 0x01} or {@code 0x02} → {@link BinaryFrameCodec}</li>
 *   <li>First byte {@code 0x10} → LZ4-decompress, then decode JSON via {@link JacksonMessageCodec}</li>
 *   <li>First byte {@code 0x00} → strip marker, decode JSON via {@link JacksonMessageCodec}</li>
 *   <li>First byte {@code 0x7B} ('{') → legacy JSON without marker (backward compat)</li>
 * </ul>
 * <p>
 * Outbound compression is negotiated per-peer in the handshake, so a {@code CompositeMessageCodec}
 * instance is created per connection and its {@link #setCompressOutput(boolean)} flag is toggled
 * once the remote node advertises support. Decoding is always able to handle every marker above,
 * so a node never fails to read a frame it could legitimately receive.
 * <p>
 * This codec is thread-safe: {@code compressOutput} is {@code volatile}, the sub-codecs and the
 * {@link Lz4FrameCompressor} are stateless/thread-safe.
 */
public final class CompositeMessageCodec implements MessageCodec {

    /** Marker byte prepended to JSON frames to distinguish from binary frames. */
    static final byte JSON_MARKER = 0x00;

    /** First byte of a raw JSON object ('{' character). */
    private static final byte JSON_BRACE = 0x7B;

    /** Default minimum JSON size (bytes) below which compression is skipped. */
    public static final int DEFAULT_COMPRESSION_MIN_SIZE = 512;

    private final BinaryFrameCodec binaryCodec;
    private final JacksonMessageCodec jacksonCodec;
    private final int compressionMinSize;
    private volatile boolean compressOutput;

    /**
     * Creates a composite codec with default sub-codecs and the default compression threshold.
     * Outbound compression starts disabled.
     */
    public CompositeMessageCodec() {
        this(new BinaryFrameCodec(), new JacksonMessageCodec(), DEFAULT_COMPRESSION_MIN_SIZE);
    }

    /**
     * Creates a composite codec with default sub-codecs and the given compression threshold.
     * Outbound compression starts disabled.
     *
     * @param compressionMinSize minimum JSON size (bytes) below which compression is skipped
     */
    public CompositeMessageCodec(int compressionMinSize) {
        this(new BinaryFrameCodec(), new JacksonMessageCodec(), compressionMinSize);
    }

    /**
     * Creates a composite codec with the given sub-codecs and the default compression threshold.
     *
     * @param binaryCodec  the binary frame codec for HEARTBEAT/PING
     * @param jacksonCodec the Jackson codec for all other message types
     */
    public CompositeMessageCodec(BinaryFrameCodec binaryCodec, JacksonMessageCodec jacksonCodec) {
        this(binaryCodec, jacksonCodec, DEFAULT_COMPRESSION_MIN_SIZE);
    }

    /**
     * Creates a composite codec with the given sub-codecs and compression threshold.
     *
     * @param binaryCodec        the binary frame codec for HEARTBEAT/PING
     * @param jacksonCodec       the Jackson codec for all other message types
     * @param compressionMinSize minimum JSON size (bytes) below which compression is skipped
     */
    public CompositeMessageCodec(BinaryFrameCodec binaryCodec, JacksonMessageCodec jacksonCodec,
            int compressionMinSize) {
        this.binaryCodec = binaryCodec;
        this.jacksonCodec = jacksonCodec;
        this.compressionMinSize = Math.max(0, compressionMinSize);
    }

    /**
     * Enables or disables LZ4 compression of outbound JSON frames. Decoding is unaffected
     * (compressed frames are always decodable). Set {@code true} only after the peer has
     * advertised compression support in the handshake.
     *
     * @param compressOutput whether to compress eligible outbound JSON frames
     */
    public void setCompressOutput(boolean compressOutput) {
        this.compressOutput = compressOutput;
    }

    /**
     * Returns whether outbound JSON compression is currently enabled for this codec.
     *
     * @return {@code true} if eligible outbound JSON frames are compressed
     */
    public boolean isCompressOutput() {
        return compressOutput;
    }

    @Override
    public byte[] encode(ClusterMessage message) throws IOException {
        if (binaryCodec.supports(message.type())) {
            return binaryCodec.encode(message);
        }

        byte[] jsonBytes = jacksonCodec.encode(message);

        if (compressOutput && jsonBytes.length >= compressionMinSize) {
            byte[] compressed = Lz4FrameCompressor.wrap(jsonBytes);
            // Only ship the compressed frame if it is actually smaller than the plain
            // 0x00 + JSON frame; otherwise fall through to the uncompressed path so an
            // incompressible payload never inflates on the wire.
            if (compressed.length < jsonBytes.length + 1) {
                return compressed;
            }
        }

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

        // LZ4-compressed JSON
        if (firstByte == Lz4FrameCompressor.LZ4_JSON_MARKER) {
            return jacksonCodec.decode(Lz4FrameCompressor.unwrap(data));
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
