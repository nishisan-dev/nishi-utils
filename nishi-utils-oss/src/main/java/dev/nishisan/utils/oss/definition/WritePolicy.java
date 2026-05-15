package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.WriteMode;

/**
 * Política de escrita: modo (append-only) e regras de idempotência.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WritePolicy(
        @JsonProperty("mode") WriteMode mode,
        @JsonProperty("idempotency") IdempotencyDef idempotency
) {
}
