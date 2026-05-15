package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.ConsolidationFunction;

import java.util.List;
import java.util.Objects;

/**
 * Preset de visualização (daily, weekly, etc.).
 *
 * @param name           identificador legível
 * @param window         janela total no formato ISO-8601 (ex.: {@code P1D}, {@code P7D})
 * @param targetStepSec  granularidade desejada — direciona o {@code BestFitSelector}
 * @param cf             função de consolidação preferida
 * @param maxPoints      limite máximo de pontos retornados (downsample uniforme)
 * @param series         lista de DS (ou derivados) a renderizar
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PresetDef(
        @JsonProperty("name") String name,
        @JsonProperty("window") String window,
        @JsonProperty("targetStepSec") int targetStepSec,
        @JsonProperty("cf") ConsolidationFunction cf,
        @JsonProperty("maxPoints") int maxPoints,
        @JsonProperty("series") List<String> series
) {
    public PresetDef {
        series = Objects.requireNonNullElse(series, List.of());
    }
}
