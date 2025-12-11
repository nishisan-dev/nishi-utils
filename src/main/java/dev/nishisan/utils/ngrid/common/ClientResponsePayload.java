package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Response payload for RPC style commands executed by the leader.
 */
public final class ClientResponsePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID requestId;
    private final boolean success;
    private final Serializable body;
    private final String error;

    public ClientResponsePayload(UUID requestId, boolean success, Serializable body, String error) {
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.success = success;
        this.body = body;
        this.error = error;
    }

    public UUID requestId() {
        return requestId;
    }

    public boolean success() {
        return success;
    }

    public Serializable body() {
        return body;
    }

    public String error() {
        return error;
    }
}
