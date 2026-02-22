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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable collection of key-value headers for an NQueue record.
 * <p>
 * Header keys are UTF-8 strings; values are raw byte arrays, keeping NQueue
 * agnostic to any higher-level serialization format.
 * <p>
 * Common use-cases include carrying tracing metadata (e.g. correlationId),
 * content-type hints, or routing keys without embedding them in the payload.
 *
 * <pre>
 * Binary layout written/read by {@link #writeTo}/{@link #readFrom}:
 *   HEADERS_COUNT(2) │ for each: KEY_LEN(2) │ KEY(N) │ VAL_LEN(4) │ VAL(N)
 * </pre>
 *
 * @since 3.0.0
 */
public final class NQueueHeaders implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Maximum number of headers per record. */
    public static final int MAX_HEADERS = 256;

    /** Maximum length of a single header key (bytes, UTF-8). */
    public static final int MAX_KEY_BYTES = 256;

    /** Maximum length of a single header value (bytes). */
    public static final int MAX_VALUE_BYTES = 65_535;

    private static final NQueueHeaders EMPTY = new NQueueHeaders(Collections.emptyMap());

    private final Map<String, byte[]> entries;

    private NQueueHeaders(Map<String, byte[]> entries) {
        this.entries = entries;
    }

    // ──────────────────────────── Factory ────────────────────────────────────

    /**
     * Returns a shared empty headers instance. No allocation.
     *
     * @return empty headers singleton
     */
    public static NQueueHeaders empty() {
        return EMPTY;
    }

    /**
     * Creates a headers instance with a single entry.
     *
     * @param key   header key, must not be null or blank
     * @param value header value, must not be null
     * @return new headers with one entry
     */
    public static NQueueHeaders of(String key, byte[] value) {
        return empty().add(key, value);
    }

    // ──────────────────────────── Mutation (returns new instance) ───────────

    /**
     * Returns a new {@code NQueueHeaders} with the given entry added.
     * If the key already exists, its value is replaced.
     *
     * @param key   header key, not null or blank
     * @param value header value, not null
     * @return new immutable headers
     * @throws IllegalArgumentException if key or value violates size limits
     */
    public NQueueHeaders add(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length == 0 || keyBytes.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("header key length out of bounds [1," + MAX_KEY_BYTES + "]: " + key);
        }
        if (value.length > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("header value length exceeds limit " + MAX_VALUE_BYTES);
        }
        if (entries.size() >= MAX_HEADERS && !entries.containsKey(key)) {
            throw new IllegalArgumentException("max headers exceeded: " + MAX_HEADERS);
        }
        Map<String, byte[]> copy = new LinkedHashMap<>(entries);
        copy.put(key, value.clone());
        return new NQueueHeaders(Collections.unmodifiableMap(copy));
    }

    // ──────────────────────────── Accessors ──────────────────────────────────

    /**
     * Returns the value for the given header key, or empty if absent.
     *
     * @param key header key
     * @return optional byte array value
     */
    public Optional<byte[]> get(String key) {
        byte[] v = entries.get(key);
        return v == null ? Optional.empty() : Optional.of(v.clone());
    }

    /**
     * Returns an unmodifiable view of all header entries.
     *
     * @return header map (values are defensive copies are NOT guaranteed here;
     *         callers must not mutate the returned arrays)
     */
    public Map<String, byte[]> asMap() {
        return entries;
    }

    /**
     * Returns {@code true} if this instance carries no headers.
     *
     * @return true when empty
     */
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Returns the number of headers in this instance.
     *
     * @return count of entries
     */
    public int size() {
        return entries.size();
    }

    // ──────────────────────────── Binary serialization ───────────────────────

    /**
     * Computes the number of bytes this instance occupies when written via
     * {@link #writeTo(ByteBuffer)}.
     * 
     * <pre>
     *   2 (HEADERS_COUNT)
     *   + for each entry: 2 (KEY_LEN) + keyBytes + 4 (VAL_LEN) + valBytes
     * </pre>
     *
     * @return byte count
     */
    public int serializedSize() {
        int size = 2; // HEADERS_COUNT (short)
        for (Map.Entry<String, byte[]> e : entries.entrySet()) {
            int kl = e.getKey().getBytes(StandardCharsets.UTF_8).length;
            size += 2 + kl + 4 + e.getValue().length;
        }
        return size;
    }

    /**
     * Writes this headers instance into the given {@link ByteBuffer}.
     * The buffer must have at least {@link #serializedSize()} bytes remaining.
     *
     * @param buf target buffer (must be write-mode and have enough capacity)
     */
    public void writeTo(ByteBuffer buf) {
        buf.putShort((short) entries.size());
        for (Map.Entry<String, byte[]> e : entries.entrySet()) {
            byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) keyBytes.length);
            buf.put(keyBytes);
            buf.putInt(e.getValue().length);
            buf.put(e.getValue());
        }
    }

    /**
     * Reads a headers instance from the given {@link ByteBuffer}.
     * The buffer position must be at the start of the headers block.
     *
     * @param buf source buffer in read-mode
     * @return deserialized headers
     * @throws java.io.IOException if the data is malformed
     */
    public static NQueueHeaders readFrom(ByteBuffer buf) throws java.io.IOException {
        int count = Short.toUnsignedInt(buf.getShort());
        if (count == 0) {
            return EMPTY;
        }
        if (count > MAX_HEADERS) {
            throw new java.io.IOException("Headers count exceeds limit: " + count);
        }
        Map<String, byte[]> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            int kLen = Short.toUnsignedInt(buf.getShort());
            if (kLen == 0 || kLen > MAX_KEY_BYTES) {
                throw new java.io.IOException("Header key length invalid: " + kLen);
            }
            byte[] keyBytes = new byte[kLen];
            buf.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            int vLen = buf.getInt();
            if (vLen < 0 || vLen > MAX_VALUE_BYTES) {
                throw new java.io.IOException("Header value length invalid: " + vLen);
            }
            byte[] val = new byte[vLen];
            buf.get(val);
            map.put(key, val);
        }
        return new NQueueHeaders(Collections.unmodifiableMap(map));
    }

    // ──────────────────────────── Object ─────────────────────────────────────

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof NQueueHeaders that))
            return false;
        if (entries.size() != that.entries.size())
            return false;
        for (Map.Entry<String, byte[]> e : entries.entrySet()) {
            byte[] other = that.entries.get(e.getKey());
            if (!java.util.Arrays.equals(e.getValue(), other))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (Map.Entry<String, byte[]> e : entries.entrySet()) {
            h += e.getKey().hashCode() ^ java.util.Arrays.hashCode(e.getValue());
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NQueueHeaders{");
        entries.forEach((k, v) -> sb.append(k).append("=").append(v.length).append("b, "));
        if (!entries.isEmpty())
            sb.setLength(sb.length() - 2);
        sb.append("}");
        return sb.toString();
    }
}
