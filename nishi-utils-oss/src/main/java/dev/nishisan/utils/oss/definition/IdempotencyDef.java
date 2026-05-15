package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.ConflictResolution;

/**
 * Configuração de idempotência: template da chave determinística e estratégia
 * de resolução de conflito quando o objeto já existe.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IdempotencyDef(
        @JsonProperty("key") String key,
        @JsonProperty("onConflict") ConflictResolution onConflict
) {
}
