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
 * Enumerates the built-in message types exchanged between transport and
 * higher-level
 * components. Additional application specific commands can be encoded using the
 * {@link ClusterMessage#qualifier()} field.
 */
public enum MessageType {
    /** Initial handshake between peers. */
    HANDSHAKE,
    /** Peer membership update. */
    PEER_UPDATE,
    /** Periodic liveness heartbeat. */
    HEARTBEAT,
    /** Network latency probe. */
    PING,
    /** Leader write-rate score broadcast. */
    LEADER_SCORE,
    /** Suggestion to change the current leader. */
    LEADER_SUGGESTION,
    /** Replication request from leader to followers. */
    REPLICATION_REQUEST,
    /** Acknowledgement of a replication request. */
    REPLICATION_ACK,
    /** Client-originated request routed through the cluster. */
    CLIENT_REQUEST,
    /** Response to a client request. */
    CLIENT_RESPONSE,
    /** Full-state synchronisation request. */
    SYNC_REQUEST,
    /** Full-state synchronisation response. */
    SYNC_RESPONSE,
    /** Configuration fetch request. */
    CONFIG_FETCH_REQUEST,
    /** Configuration fetch response. */
    CONFIG_FETCH_RESPONSE,
    /** Request to resend a sequence range. */
    SEQUENCE_RESEND_REQUEST,
    /** Response carrying resent sequence data. */
    SEQUENCE_RESEND_RESPONSE,
    /** Subscribe to a distributed queue topic. */
    QUEUE_SUBSCRIBE,
    /** Unsubscribe from a distributed queue topic. */
    QUEUE_UNSUBSCRIBE,
    /** Notification of new items in a queue. */
    QUEUE_NOTIFY
}
