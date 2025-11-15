package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;

/**
 * Envelope exchanged between nodes. It includes the type, optional qualifier to distinguish
 * operations, an optional destination and a payload that must be {@link Serializable}.
 */
public final class ClusterMessage implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID messageId;
    private final UUID correlationId;
    private final MessageType type;
    private final String qualifier;
    private final NodeId source;
    private final NodeId destination;
    private final Serializable payload;

    public ClusterMessage(UUID messageId,
                          UUID correlationId,
                          MessageType type,
                          String qualifier,
                          NodeId source,
                          NodeId destination,
                          Serializable payload) {
        this.messageId = messageId == null ? UUID.randomUUID() : messageId;
        this.correlationId = correlationId;
        this.type = type;
        this.qualifier = qualifier;
        this.source = source;
        this.destination = destination;
        this.payload = payload;
    }

    public static ClusterMessage request(MessageType type, String qualifier, NodeId source, NodeId destination, Serializable payload) {
        return new ClusterMessage(UUID.randomUUID(), null, type, qualifier, source, destination, payload);
    }

    public static ClusterMessage response(ClusterMessage request, Serializable payload) {
        return new ClusterMessage(UUID.randomUUID(), request.messageId, MessageType.CLIENT_RESPONSE, request.qualifier, request.destination(), request.source(), payload);
    }

    public UUID messageId() {
        return messageId;
    }

    public Optional<UUID> correlationId() {
        return Optional.ofNullable(correlationId);
    }

    public MessageType type() {
        return type;
    }

    public String qualifier() {
        return qualifier;
    }

    public NodeId source() {
        return source;
    }

    public NodeId destination() {
        return destination;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T payload(Class<T> type) {
        return (T) payload;
    }

    public Serializable payload() {
        return payload;
    }
}
