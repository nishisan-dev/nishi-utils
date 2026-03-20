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

import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;

/**
 * Envelope exchanged between nodes. It includes the type, optional qualifier to
 * distinguish
 * operations, an optional destination and a payload that must be
 * {@link Serializable}.
 */
public final class ClusterMessage implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    private final UUID messageId;
    private final UUID correlationId;
    private final MessageType type;
    private final String qualifier;
    private final NodeId source;
    private final NodeId destination;
    private final Serializable payload;
    private final int ttl;

    /**
     * Creates a new cluster message with all fields specified.
     *
     * @param messageId     unique message identifier; auto-generated if
     *                      {@code null}
     * @param correlationId optional correlation ID for request-response pairing
     * @param type          the message type
     * @param qualifier     additional qualifier for routing
     * @param source        the originating node
     * @param destination   the target node, or {@code null} for broadcast
     * @param payload       the serializable message payload
     * @param ttl           time-to-live hop counter
     */
    public ClusterMessage(UUID messageId,
            UUID correlationId,
            MessageType type,
            String qualifier,
            NodeId source,
            NodeId destination,
            Serializable payload,
            int ttl) {
        this.messageId = messageId == null ? UUID.randomUUID() : messageId;
        this.correlationId = correlationId;
        this.type = type;
        this.qualifier = qualifier;
        this.source = source;
        this.destination = destination;
        this.payload = payload;
        this.ttl = ttl;
    }

    /**
     * Creates a new cluster message with a default TTL of 5.
     *
     * @param messageId     unique message identifier
     * @param correlationId optional correlation ID
     * @param type          the message type
     * @param qualifier     additional qualifier for routing
     * @param source        the originating node
     * @param destination   the target node
     * @param payload       the serializable message payload
     */
    public ClusterMessage(UUID messageId,
            UUID correlationId,
            MessageType type,
            String qualifier,
            NodeId source,
            NodeId destination,
            Serializable payload) {
        this(messageId, correlationId, type, qualifier, source, destination, payload, 5);
    }

    /**
     * Creates a new request message with an auto-generated ID and default TTL.
     *
     * @param type        the message type
     * @param qualifier   routing qualifier
     * @param source      the originating node
     * @param destination the target node, or {@code null} for broadcast
     * @param payload     the request payload
     * @return a new request message
     */
    public static ClusterMessage request(MessageType type, String qualifier, NodeId source, NodeId destination,
            Serializable payload) {
        return new ClusterMessage(UUID.randomUUID(), null, type, qualifier, source, destination, payload, 5);
    }

    /**
     * Creates a response message correlated to the given request.
     *
     * @param request the original request message
     * @param payload the response payload
     * @return a new response message
     */
    public static ClusterMessage response(ClusterMessage request, Serializable payload) {
        return new ClusterMessage(UUID.randomUUID(), request.messageId, MessageType.CLIENT_RESPONSE, request.qualifier,
                request.destination(), request.source(), payload, 5);
    }

    /**
     * Creates a copy of this message with the TTL decremented by one.
     *
     * @return a new message with reduced TTL
     * @throws IllegalStateException if the TTL has already expired
     */
    public ClusterMessage nextHop() {
        if (ttl <= 0) {
            throw new IllegalStateException("Message TTL expired");
        }
        return new ClusterMessage(messageId, correlationId, type, qualifier, source, destination, payload, ttl - 1);
    }

    /**
     * Returns the remaining time-to-live hop count.
     *
     * @return the TTL
     */
    public int ttl() {
        return ttl;
    }

    /**
     * Returns the unique message identifier.
     *
     * @return the message ID
     */
    public UUID messageId() {
        return messageId;
    }

    /**
     * Returns the optional correlation identifier for request-response pairing.
     *
     * @return an optional containing the correlation ID, or empty
     */
    public Optional<UUID> correlationId() {
        return Optional.ofNullable(correlationId);
    }

    /**
     * Returns the message type.
     *
     * @return the message type
     */
    public MessageType type() {
        return type;
    }

    /**
     * Returns the routing qualifier.
     *
     * @return the qualifier
     */
    public String qualifier() {
        return qualifier;
    }

    /**
     * Returns the source node identifier.
     *
     * @return the source node ID
     */
    public NodeId source() {
        return source;
    }

    /**
     * Returns the destination node identifier.
     *
     * @return the destination node ID, or {@code null} for broadcast
     */
    public NodeId destination() {
        return destination;
    }

    /**
     * Returns the message payload cast to the expected type.
     *
     * @param <T>  the expected payload type
     * @param type the class to cast the payload to
     * @return the typed payload
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> T payload(Class<T> type) {
        return (T) payload;
    }

    /**
     * Returns the raw message payload.
     *
     * @return the payload
     */
    public Serializable payload() {
        return payload;
    }
}
