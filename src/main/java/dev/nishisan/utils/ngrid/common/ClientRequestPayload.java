package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Simple RPC style payload for client requests routed to the cluster leader.
 */
public final class ClientRequestPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID requestId;
    private final String command;
    private final Serializable body;

    public ClientRequestPayload(UUID requestId, String command, Serializable body) {
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.command = Objects.requireNonNull(command, "command");
        this.body = body;
    }

    public UUID requestId() {
        return requestId;
    }

    public String command() {
        return command;
    }

    public Serializable body() {
        return body;
    }
}
