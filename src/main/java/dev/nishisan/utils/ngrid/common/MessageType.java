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

package dev.nishisan.utils.ngrid.common;

/**
 * Enumerates the built-in message types exchanged between transport and higher-level
 * components. Additional application specific commands can be encoded using the
 * {@link ClusterMessage#qualifier()} field.
 */
public enum MessageType {
    HANDSHAKE,
    PEER_UPDATE,
    HEARTBEAT,
    REPLICATION_REQUEST,
    REPLICATION_ACK,
    CLIENT_REQUEST,
    CLIENT_RESPONSE
}
