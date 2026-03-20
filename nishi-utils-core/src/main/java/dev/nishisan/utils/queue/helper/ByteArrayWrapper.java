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

package dev.nishisan.utils.queue.helper;

/**
 * A wrapper class for a byte array. This class encapsulates a byte array
 * to provide additional functionality and abstraction.
 */
public class ByteArrayWrapper {
    private final byte[] bytes;

    /**
     * Creates a new wrapper for the given byte array.
     *
     * @param bytes the byte array to wrap
     */
    public ByteArrayWrapper(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Returns the wrapped byte array.
     *
     * @return the byte array
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Creates a new instance of ByteArrayWrapper that encapsulates the provided
     * byte array.
     *
     * @param bytes the byte array to be wrapped in a ByteArrayWrapper
     * @return a new instance of ByteArrayWrapper containing the provided byte array
     */
    public static ByteArrayWrapper of(byte[] bytes) {
        return new ByteArrayWrapper(bytes);
    }
}
