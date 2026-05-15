package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.ManifestUpdateMode;

/**
 * Política de atualização do manifesto.
 *
 * @param updateMode  estratégia (atualmente {@code PERIODIC_SNAPSHOT})
 * @param intervalSec intervalo entre snapshots em segundos
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ManifestPolicy(
        @JsonProperty("updateMode") ManifestUpdateMode updateMode,
        @JsonProperty("intervalSec") int intervalSec
) {
}
