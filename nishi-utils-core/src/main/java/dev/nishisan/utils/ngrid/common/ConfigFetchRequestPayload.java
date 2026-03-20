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

    /**
     * Creates a config fetch request with the given secret.
     *
     * @param secret the shared authentication secret
     */
    public ConfigFetchRequestPayload(String secret) {
        this.secret = Objects.requireNonNull(secret, "secret");
    }

    /**
     * Returns the authentication secret.
     *
     * @return the secret
     */
    public String secret() {
        return secret;
    }
}
