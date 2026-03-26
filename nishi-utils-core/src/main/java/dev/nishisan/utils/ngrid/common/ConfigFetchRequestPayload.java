package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Payload sent by a new node to request configuration from a seed node.
 */
public final class ConfigFetchRequestPayload {

    private final String secret;

    @JsonCreator
    public ConfigFetchRequestPayload(@JsonProperty("secret") String secret) {
        this.secret = Objects.requireNonNull(secret, "secret");
    }

    public String secret() {
        return secret;
    }
}
