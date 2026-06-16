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

package dev.nishisan.utils.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * Binary codec for a single record in a {@link SegmentOffloadStrategy} segment
 * log. Records are framed with a magic number, version, flags, type, explicit
 * length prefixes and a trailing CRC32, mirroring the tolerant, length-prefixed
 * framing used by {@code NMapPersistence}.
 *
 * <pre>
 * | offset | size   | field                                              |
 * |--------|--------|----------------------------------------------------|
 * | 0      | 4 B    | MAGIC = 0x4E4D5347 ("NMSG")                        |
 * | 4      | 1 B    | version                                            |
 * | 5      | 1 B    | flags (bit0 = value is LZ4-compressed)            |
 * | 6      | 1 B    | type (0 = PUT, 1 = TOMBSTONE)                      |
 * | 7      | 4 B    | keyLen                                              |
 * | 11     | 4 B    | valLen (0 for TOMBSTONE)                           |
 * | 15     | keyLen | key bytes (Java serialization, never compressed)  |
 * | ...    | valLen | value bytes (Java serialization; LZ4 if flagged)  |
 * | end    | 4 B    | CRC32 over every byte above                         |
 * </pre>
 *
 * The key is always stored uncompressed so the index can be rebuilt at startup
 * without decompressing values.
 */
final class SegmentRecord {

    /** Record magic — ASCII "NMSG". */
    static final int MAGIC = 0x4E4D5347;
    /** Current record format version. */
    static final byte VERSION = 1;
    /** Flag bit: the value bytes are LZ4-compressed. */
    static final int FLAG_VALUE_LZ4 = 0x01;
    /** Record type: insert/update. */
    static final byte TYPE_PUT = 0;
    /** Record type: logical delete. */
    static final byte TYPE_TOMBSTONE = 1;

    /** Fixed header length: magic(4) + version(1) + flags(1) + type(1) + keyLen(4) + valLen(4). */
    static final int HEADER_LEN = 15;
    /** Trailing CRC32 length. */
    static final int TRAILER_LEN = 4;
    /** Upper bound on a single key/value field, guarding against corrupt lengths. */
    static final int MAX_FIELD_LEN = 256 * 1024 * 1024;

    private SegmentRecord() {
    }

    /**
     * Decoded view of a record. {@code value} is the raw stored value (still
     * LZ4-compressed when {@link #isValueCompressed()} is {@code true}).
     */
    record Decoded(byte type, int flags, byte[] key, byte[] value, int recordLength) {
        boolean isTombstone() {
            return type == TYPE_TOMBSTONE;
        }

        boolean isValueCompressed() {
            return (flags & FLAG_VALUE_LZ4) != 0;
        }
    }

    /**
     * Encodes a record into a self-contained byte array (header + key + value + CRC).
     *
     * @param type  {@link #TYPE_PUT} or {@link #TYPE_TOMBSTONE}
     * @param flags flag bits (e.g. {@link #FLAG_VALUE_LZ4})
     * @param key   the serialized key (never {@code null})
     * @param value the serialized (possibly compressed) value, or {@code null} for a tombstone
     * @return the encoded record
     */
    static byte[] encode(byte type, int flags, byte[] key, byte[] value) {
        int keyLen = key.length;
        int valLen = value == null ? 0 : value.length;
        int total = HEADER_LEN + keyLen + valLen + TRAILER_LEN;
        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.putInt(MAGIC);
        buf.put(VERSION);
        buf.put((byte) flags);
        buf.put(type);
        buf.putInt(keyLen);
        buf.putInt(valLen);
        buf.put(key);
        if (valLen > 0) {
            buf.put(value);
        }
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, HEADER_LEN + keyLen + valLen);
        buf.putInt((int) crc.getValue());
        return buf.array();
    }

    /**
     * Reads and validates the record starting at {@code pos}. Returns
     * {@code null} when the record is truncated or fails validation (bad magic,
     * version, lengths, or CRC) — the caller should treat this as end-of-log and
     * truncate the tail.
     *
     * @param ch       the segment channel
     * @param pos      the byte offset of the record
     * @param fileSize the current channel size
     * @return the decoded record, or {@code null} if invalid/truncated
     * @throws IOException on an I/O failure
     */
    static Decoded readAt(FileChannel ch, long pos, long fileSize) throws IOException {
        if (pos + HEADER_LEN > fileSize) {
            return null;
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_LEN);
        if (!readFully(ch, header, pos)) {
            return null;
        }
        header.flip();
        int magic = header.getInt();
        byte version = header.get();
        byte flags = header.get();
        byte type = header.get();
        int keyLen = header.getInt();
        int valLen = header.getInt();
        if (magic != MAGIC || version != VERSION) {
            return null;
        }
        if (keyLen <= 0 || keyLen > MAX_FIELD_LEN || valLen < 0 || valLen > MAX_FIELD_LEN) {
            return null;
        }
        if (type != TYPE_PUT && type != TYPE_TOMBSTONE) {
            return null;
        }
        long recordLength = (long) HEADER_LEN + keyLen + valLen + TRAILER_LEN;
        if (pos + recordLength > fileSize) {
            return null;
        }
        ByteBuffer record = ByteBuffer.allocate((int) recordLength);
        if (!readFully(ch, record, pos)) {
            return null;
        }
        return decodeValidated(record.array(), flags, type, keyLen, valLen, (int) recordLength);
    }

    /**
     * Decodes a record from a fully-read byte array of exactly its length.
     * Returns {@code null} if the buffer is not a valid record (bad magic,
     * version, lengths, or CRC).
     *
     * @param record the record bytes (length == record length)
     * @return the decoded record, or {@code null} if invalid
     */
    static Decoded decode(byte[] record) {
        if (record.length < HEADER_LEN + TRAILER_LEN) {
            return null;
        }
        ByteBuffer header = ByteBuffer.wrap(record, 0, HEADER_LEN);
        int magic = header.getInt();
        byte version = header.get();
        byte flags = header.get();
        byte type = header.get();
        int keyLen = header.getInt();
        int valLen = header.getInt();
        if (magic != MAGIC || version != VERSION) {
            return null;
        }
        if (keyLen <= 0 || valLen < 0
                || (long) HEADER_LEN + keyLen + valLen + TRAILER_LEN != record.length) {
            return null;
        }
        return decodeValidated(record, flags, type, keyLen, valLen, record.length);
    }

    private static Decoded decodeValidated(byte[] record, byte flags, byte type,
            int keyLen, int valLen, int recordLength) {
        CRC32 crc = new CRC32();
        crc.update(record, 0, recordLength - TRAILER_LEN);
        int storedCrc = ByteBuffer.wrap(record, recordLength - TRAILER_LEN, TRAILER_LEN).getInt();
        if ((int) crc.getValue() != storedCrc) {
            return null;
        }
        byte[] key = new byte[keyLen];
        System.arraycopy(record, HEADER_LEN, key, 0, keyLen);
        byte[] value = null;
        if (valLen > 0) {
            value = new byte[valLen];
            System.arraycopy(record, HEADER_LEN + keyLen, value, 0, valLen);
        }
        return new Decoded(type, flags & 0xFF, key, value, recordLength);
    }

    private static boolean readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) {
                return false;
            }
            p += n;
        }
        return true;
    }
}
