package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.LateSampleAction;

/**
 * Tolerância e ação para amostras que chegam fora do step esperado.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record LateSamplePolicy(
        @JsonProperty("maxLatenessSec") int maxLatenessSec,
        @JsonProperty("onLate") LateSampleAction onLate
) {
}
