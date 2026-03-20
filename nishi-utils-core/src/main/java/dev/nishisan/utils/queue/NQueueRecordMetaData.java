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

package dev.nishisan.utils.queue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * Represents the on-disk metadata header for an NQueue record (Version 3).
 *
 * <h2>Binary layout</h2>
 * 
 * <pre>
 * ┌─────────── Fixed Prefix (9 bytes) ───────────┐
 * │ MAGIC(4) │ VER(1)=0x03 │ HEADER_LEN(4)       │
 * ├─────────── Variable Header ───────────────────┤
 * │ INDEX(8) │ TIMESTAMP(8)                       │
 * │ KEY_LEN(4) │ KEY(KEY_LEN bytes)               │   ← KEY_LEN=0 means null key
 * │ [NQueueHeaders binary block]                  │   ← starts with HEADERS_COUNT(2)
 * │ PAYLOAD_LEN(4) │ CLASSNAME_LEN(2) │ CLASS(N) │
 * ├─────────── Payload (separate) ───────────────┤
 * │ PAYLOAD(PAYLOAD_LEN bytes)                    │
 * └───────────────────────────────────────────────┘
 * </pre>
 *
 * <p>
 * Versions 1 and 2 are no longer supported. Existing data stores
 * using those formats must be rebuilt.
 *
 * @since 3.0.0
 */
public class NQueueRecordMetaData {

    // ──────────────────────────── Constants ──────────────────────────────────

    /** Wire magic: four bytes 'NQMD'. */
    public static final int MAGIC = 0x4E_51_4D_44;

    /** Current on-disk format version. */
    public static final byte VERSION_3 = 0x03;
    public static final byte CURRENT_VERSION = VERSION_3;

    /** Maximum key size in bytes. */
    public static final int MAX_KEY_BYTES = 65_535;

    private static final int MAX_CLASSNAME_LEN = 1024;

    /**
     * Minimum variable header size when key is empty and no headers:
     * INDEX(8) + TIMESTAMP(8) + KEY_LEN(4) + HEADERS_COUNT(2) + PAYLOAD_LEN(4) +
     * CLASSNAME_LEN(2) + 1 byte class
     */
    private static final int MIN_HEADER_LEN = 8 + 8 + 4 + 2 + 4 + 2 + 1;

    /** Sane upper bound to detect corrupt data (4 MiB). */
    private static final int MAX_HEADER_LEN = 4 * 1024 * 1024;

    // ──────────────────────────── Fields ─────────────────────────────────────

    private int headerLen; // size of variable header block (after HEADER_LEN field)
    private long index; // monotonically increasing record index
    private long timestamp; // epoch millis at offer time
    private byte[] key; // nullable routing/partitioning key
    private NQueueHeaders headers; // arbitrary metadata headers
    private int payloadLen; // byte length of the serialised payload
    private String className; // canonical class name for deserialization
    private int classNameLen; // cached UTF-8 byte length

    // ──────────────────────────── Constructors ────────────────────────────────

    /**
     * Full constructor used when producing a new record.
     *
     * @param index      monotonic sequence number
     * @param timestamp  epoch millis (typically {@code System.currentTimeMillis()})
     * @param key        optional routing key; {@code null} or zero-length means
     *                   absent
     * @param headers    record headers; use {@link NQueueHeaders#empty()} when none
     * @param payloadLen serialized payload byte length
     * @param className  canonical name of the record's value type
     */
    public NQueueRecordMetaData(long index, long timestamp, byte[] key, NQueueHeaders headers,
            int payloadLen, String className) {
        this.index = index;
        this.timestamp = timestamp;
        this.key = (key != null && key.length > 0) ? key.clone() : new byte[0];
        if (this.key.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key length exceeds limit: " + this.key.length);
        }
        this.headers = headers != null ? headers : NQueueHeaders.empty();
        this.payloadLen = payloadLen;
        this.className = className;
        this.classNameLen = className.getBytes(StandardCharsets.UTF_8).length;
        if (this.classNameLen <= 0 || this.classNameLen > MAX_CLASSNAME_LEN) {
            throw new IllegalArgumentException("className length out of bounds: " + className);
        }
        // headerLen = total bytes of the variable block
        this.headerLen = computeHeaderLen(this.key, this.headers, this.classNameLen);
        if (this.headerLen < MIN_HEADER_LEN || this.headerLen > MAX_HEADER_LEN) {
            throw new IllegalArgumentException("computed header length out of bounds: " + this.headerLen);
        }
    }

    /**
     * Convenience constructor without key or headers (simple queue use-case).
     */
    public NQueueRecordMetaData(long index, long timestamp, int payloadLen, String className) {
        this(index, timestamp, null, NQueueHeaders.empty(), payloadLen, className);
    }

    /** Private no-arg constructor used during deserialization only. */
    private NQueueRecordMetaData() {
    }

    // ──────────────────────────── Sizing helpers ─────────────────────────────

    /** Size of the fixed prefix that precedes the variable header. */
    public static int fixedPrefixSize() {
        // MAGIC(4) + VER(1) + HEADER_LEN(4)
        return 4 + 1 + 4;
    }

    /** Total on-disk size of the header block (prefix + variable). */
    public int totalHeaderSize() {
        return fixedPrefixSize() + headerLen;
    }

    private static int computeHeaderLen(byte[] key, NQueueHeaders headers, int classNameBytes) {
        // INDEX(8) + TIMESTAMP(8) + KEY_LEN(4) + KEY(N)
        // + headers.serializedSize()
        // + PAYLOAD_LEN(4) + CLASSNAME_LEN(2) + CLASSNAME(classNameBytes)
        return 8 + 8 + 4 + key.length
                + headers.serializedSize()
                + 4 + 2 + classNameBytes;
    }

    // ──────────────────────────── Serialization ────────────────────────────

    /**
     * Serialises the complete header (prefix + variable block) into a new
     * ByteBuffer.
     *
     * @return buffer flipped and ready to be written to a channel
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(totalHeaderSize());
        // ── Fixed prefix ──
        buf.putInt(MAGIC);
        buf.put(CURRENT_VERSION);
        buf.putInt(headerLen);
        // ── Variable block ──
        buf.putLong(index);
        buf.putLong(timestamp);
        // key
        buf.putInt(key.length);
        if (key.length > 0) {
            buf.put(key);
        }
        // headers
        headers.writeTo(buf);
        // payload length + class name
        buf.putInt(payloadLen);
        buf.putShort((short) classNameLen);
        buf.put(className.getBytes(StandardCharsets.UTF_8));
        buf.flip();
        return buf;
    }

    // ──────────────────────────── Deserialization ─────────────────────────

    /**
     * Reads the fixed 9-byte prefix at the given file offset.
     * Validates MAGIC and enforces VERSION_3.
     *
     * @param ch     open FileChannel
     * @param offset absolute file position
     * @return parsed prefix (version + headerLen)
     * @throws IOException if data is corrupt, truncated, or an unsupported version
     */
    public static HeaderPrefix readPrefix(FileChannel ch, long offset) throws IOException {
        ByteBuffer prefix = ByteBuffer.allocate(fixedPrefixSize());
        int r = ch.read(prefix, offset);
        if (r < fixedPrefixSize()) {
            throw new EOFException("Incomplete record prefix at offset " + offset);
        }
        prefix.flip();
        int magic = prefix.getInt();
        if (magic != MAGIC) {
            throw new IOException("Invalid MAGIC at offset " + offset + ": 0x" + Integer.toHexString(magic));
        }
        byte ver = prefix.get();
        if (ver != VERSION_3) {
            throw new IOException("Unsupported NQueue record format version " + (ver & 0xFF)
                    + " at offset " + offset + ". Only V3 is supported. Rebuild required.");
        }
        int headerLen = prefix.getInt();
        if (headerLen < MIN_HEADER_LEN || headerLen > MAX_HEADER_LEN) {
            throw new IOException("Invalid HEADER_LEN at offset " + offset + ": " + headerLen);
        }
        return new HeaderPrefix(ver, headerLen);
    }

    /**
     * Reads and fully parses the variable header block at the given file offset.
     * Callers must have already validated the prefix via {@link #readPrefix}.
     *
     * @param ch        open FileChannel
     * @param offset    absolute start of the full record (including prefix)
     * @param headerLen variable header length reported by the prefix
     * @return parsed metadata
     * @throws IOException if data is truncated or structurally invalid
     */
    public static NQueueRecordMetaData fromBuffer(FileChannel ch, long offset, int headerLen) throws IOException {
        long varStart = offset + fixedPrefixSize();
        ByteBuffer hb = ByteBuffer.allocate(headerLen);
        int r = ch.read(hb, varStart);
        if (r < headerLen) {
            throw new EOFException("Incomplete variable header at offset " + offset);
        }
        hb.flip();

        NQueueRecordMetaData m = new NQueueRecordMetaData();
        m.headerLen = headerLen;

        m.index = hb.getLong();
        m.timestamp = hb.getLong();

        // key
        int keyLen = hb.getInt();
        if (keyLen < 0 || keyLen > MAX_KEY_BYTES) {
            throw new IOException("Invalid KEY_LEN: " + keyLen);
        }
        if (keyLen > 0) {
            m.key = new byte[keyLen];
            hb.get(m.key);
        } else {
            m.key = new byte[0];
        }

        // headers
        m.headers = NQueueHeaders.readFrom(hb);

        // payload length
        m.payloadLen = hb.getInt();

        // class name
        int nameLen = Short.toUnsignedInt(hb.getShort());
        if (nameLen <= 0 || nameLen > MAX_CLASSNAME_LEN) {
            throw new IOException("Invalid CLASSNAME_LEN: " + nameLen);
        }
        byte[] nameBytes = new byte[nameLen];
        hb.get(nameBytes);
        m.className = new String(nameBytes, StandardCharsets.UTF_8);
        m.classNameLen = nameLen;

        return m;
    }

    // ──────────────────────────── Accessors ──────────────────────────────────

    /** Monotonic record index within the queue. */
    public long getIndex() {
        return index;
    }

    /** Sets the record index (used internally during recovery). */
    public void setIndex(long index) {
        this.index = index;
    }

    /** Creation time in epoch millis. */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the routing/partitioning key, or an empty array if absent.
     * Never {@code null}.
     *
     * @return key bytes (defensive copy)
     */
    public byte[] getKey() {
        return key.clone();
    }

    /** Returns {@code true} if this record carries a non-empty key. */
    public boolean hasKey() {
        return key.length > 0;
    }

    /** Returns the headers attached to this record. Never {@code null}. */
    public NQueueHeaders getHeaders() {
        return headers;
    }

    /** Byte length of the serialised payload. */
    public int getPayloadLen() {
        return payloadLen;
    }

    /** Canonical class name used for deserialization. */
    public String getClassName() {
        return className;
    }

    /** Cached UTF-8 byte length of {@link #getClassName()}. */
    public int getClassNameLen() {
        return classNameLen;
    }

    /** Byte length of the variable header block (after the fixed prefix). */
    public int getHeaderLen() {
        return headerLen;
    }

    // ──────────────────────────── Inner types ───────────────────────────────

    /**
     * Parsed fixed prefix; carries the version byte and variable header length.
     */
    public static final class HeaderPrefix {
        public final byte version;
        public final int headerLen;

        public HeaderPrefix(byte version, int headerLen) {
            this.version = version;
            this.headerLen = headerLen;
        }
    }
}
