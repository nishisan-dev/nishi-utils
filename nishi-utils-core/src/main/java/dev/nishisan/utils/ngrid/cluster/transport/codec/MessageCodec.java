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

/**
 * Codec responsible for encoding and decoding {@link ClusterMessage} instances
 * to and from byte arrays for network transport.
 * <p>
 * Implementations must be thread-safe as a single codec instance is shared
 * across all connections in a {@code TcpTransport}.
 */
public interface MessageCodec {

    /**
     * Encodes a cluster message into a byte array.
     *
     * @param message the message to encode
     * @return the encoded bytes
     * @throws IOException if encoding fails
     */
    byte[] encode(ClusterMessage message) throws IOException;

    /**
     * Decodes a byte array back into a cluster message.
     *
     * @param data the encoded bytes
     * @return the decoded message
     * @throws IOException if decoding fails
     */
    ClusterMessage decode(byte[] data) throws IOException;
}
