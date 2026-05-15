package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Política de detecção de counter reset.
 *
 * @param detectCounterReset  liga/desliga a heurística
 * @param maxResetDeltaRatio  razão {@code delta/prev} a partir da qual a queda
 *                            é interpretada como reset (ex.: {@code 0.90}
 *                            = queda &gt; 90% do valor anterior)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ResetPolicyDef(
        @JsonProperty("detectCounterReset") boolean detectCounterReset,
        @JsonProperty("maxResetDeltaRatio") double maxResetDeltaRatio
) {
}
