package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.ResetBehavior;
import dev.nishisan.utils.oss.api.WrapBehavior;

/**
 * Definição do DS derivado: nome de saída, unidade, fórmula e tratamento de
 * casos especiais (reset, wrap, deltas negativos).
 *
 * @param name                 nome do DS derivado (ex.: {@code in_bps})
 * @param unit                 unidade textual (ex.: {@code "bit/s"})
 * @param formula              expressão de derivação suportada pela engine
 * @param clampNegativeToZero  se {@code true}, deltas negativos viram 0
 * @param onReset              comportamento ao detectar counter reset
 * @param onWrap               comportamento ao detectar wrap (overflow)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DeriveOutputDef(
        @JsonProperty("name") String name,
        @JsonProperty("unit") String unit,
        @JsonProperty("formula") String formula,
        @JsonProperty("clampNegativeToZero") boolean clampNegativeToZero,
        @JsonProperty("onReset") ResetBehavior onReset,
        @JsonProperty("onWrap") WrapBehavior onWrap
) {
}
