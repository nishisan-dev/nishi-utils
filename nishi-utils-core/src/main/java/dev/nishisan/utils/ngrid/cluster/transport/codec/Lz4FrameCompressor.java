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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Utility that wraps and unwraps payloads using LZ4 <em>block</em> compression.
 * <p>
 * LZ4 block format does not store the decompressed size, so the produced frame carries
 * it explicitly in a fixed header. The frame layout is:
 * <pre>
 * | offset | size  | field                                       |
 * |--------|-------|---------------------------------------------|
 * | 0      | 1 B   | marker = 0x10 (LZ4_JSON_MARKER)            |
 * | 1      | 4 B   | originalLength (int, decompressed size)    |
 * | 5      | N B   | LZ4-compressed bytes                       |
 * </pre>
 * <p>
 * The {@link LZ4Factory} and the (de)compressor instances are thread-safe and shared as
 * static singletons; only this class' static methods are exposed (it is never instantiated).
 *
 * @see CompositeMessageCodec
 */
public final class Lz4FrameCompressor {

    /** Marker byte identifying an LZ4-compressed JSON frame. */
    public static final byte LZ4_JSON_MARKER = 0x10;

    /** Frame header length: 1 marker byte + 4 bytes original (decompressed) length. */
    static final int HEADER_LENGTH = 5;

    /**
     * Upper bound on the decompressed size accepted by {@link #unwrap(byte[])}, guarding
     * against an OOM from a corrupted or hostile {@code originalLength}. 256 MiB is far above
     * any legitimate (byte-sliced) snapshot chunk while keeping a single allocation bounded.
     */
    static final int MAX_DECOMPRESSED_SIZE = 256 * 1024 * 1024;

    private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
    private static final LZ4Compressor COMPRESSOR = FACTORY.fastCompressor();
    private static final LZ4FastDecompressor DECOMPRESSOR = FACTORY.fastDecompressor();

    private Lz4FrameCompressor() {
    }

    /**
     * Wraps the given payload into an LZ4 frame ({@code [0x10][originalLength][lz4 bytes]}).
     *
     * @param payload the raw bytes to compress (e.g. JSON)
     * @return the compressed frame, including the marker and length header
     */
    public static byte[] wrap(byte[] payload) {
        int maxCompressed = COMPRESSOR.maxCompressedLength(payload.length);
        byte[] scratch = new byte[HEADER_LENGTH + maxCompressed];
        ByteBuffer header = ByteBuffer.wrap(scratch, 0, HEADER_LENGTH);
        header.put(LZ4_JSON_MARKER);
        header.putInt(payload.length);
        int compressedLength = COMPRESSOR.compress(
                payload, 0, payload.length, scratch, HEADER_LENGTH, maxCompressed);
        // maxCompressedLength is an upper bound; trim the scratch buffer to the actual size.
        byte[] frame = new byte[HEADER_LENGTH + compressedLength];
        System.arraycopy(scratch, 0, frame, 0, frame.length);
        return frame;
    }

    /**
     * Reverses {@link #wrap(byte[])}: validates the marker and declared length, then
     * decompresses back to the original payload.
     *
     * @param frame the LZ4 frame produced by {@link #wrap(byte[])}
     * @return the original (decompressed) bytes
     * @throws IOException if the frame is truncated, carries a wrong marker, or declares a
     *                     length outside {@code [0, MAX_DECOMPRESSED_SIZE]}
     */
    public static byte[] unwrap(byte[] frame) throws IOException {
        if (frame.length < HEADER_LENGTH) {
            throw new IOException("Truncated LZ4 frame: " + frame.length + " bytes");
        }
        ByteBuffer header = ByteBuffer.wrap(frame, 0, HEADER_LENGTH);
        byte marker = header.get();
        if (marker != LZ4_JSON_MARKER) {
            throw new IOException("Not an LZ4 frame, marker: 0x" + Integer.toHexString(marker & 0xFF));
        }
        int originalLength = header.getInt();
        if (originalLength < 0 || originalLength > MAX_DECOMPRESSED_SIZE) {
            throw new IOException("Invalid LZ4 decompressed length: " + originalLength);
        }
        byte[] restored = new byte[originalLength];
        if (originalLength > 0) {
            DECOMPRESSOR.decompress(frame, HEADER_LENGTH, restored, 0, originalLength);
        }
        return restored;
    }
}
