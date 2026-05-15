package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.ConsolidationFunction;

import java.util.List;
import java.util.Objects;

/**
 * Round-Robin Archive: arquivo finito de pontos consolidados.
 *
 * @param name     identificador lógico (ex.: {@code rra_5m_30d})
 * @param stepSec  granularidade do arquivo em segundos; múltiplo do baseStep
 * @param rows     quantidade de pontos retidos (define a janela total)
 * @param cf       funções de consolidação aplicadas (AVERAGE, MAX, MIN, LAST)
 * @param xff      fração máxima de PDPs missing dentro de um CDP antes do CDP
 *                 virar NaN — em [0.0, 1.0)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RraDef(
        @JsonProperty("name") String name,
        @JsonProperty("stepSec") int stepSec,
        @JsonProperty("rows") int rows,
        @JsonProperty("cf") List<ConsolidationFunction> cf,
        @JsonProperty("xff") double xff
) {
    public RraDef {
        cf = Objects.requireNonNullElse(cf, List.of());
    }
}
