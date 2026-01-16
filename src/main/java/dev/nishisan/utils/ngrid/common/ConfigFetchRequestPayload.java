package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Payload sent by a new node to request configuration from a seed node.
 */
public final class ConfigFetchRequestPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String secret;

    public ConfigFetchRequestPayload(String secret) {
        this.secret = Objects.requireNonNull(secret, "secret");
    }

    public String secret() {
        return secret;
    }
}
