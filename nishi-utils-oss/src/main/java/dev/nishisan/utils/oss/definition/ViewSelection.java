package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.FallbackSource;
import dev.nishisan.utils.oss.api.SelectionStrategy;

import java.util.List;
import java.util.Objects;

/**
 * Configuração de seleção de RRA na leitura.
 *
 * @param strategy         estratégia de seleção (atualmente apenas BEST_FIT)
 * @param maxPointsDefault limite default de pontos retornados quando o preset
 *                         não define {@code maxPoints} próprio
 * @param fallbackOrder    ordem de fallback quando o RRA preferido não cobre
 *                         (raw → agg ou vice-versa)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ViewSelection(
        @JsonProperty("strategy") SelectionStrategy strategy,
        @JsonProperty("maxPointsDefault") int maxPointsDefault,
        @JsonProperty("fallbackOrder") List<FallbackSource> fallbackOrder
) {
    public ViewSelection {
        fallbackOrder = Objects.requireNonNullElse(fallbackOrder, List.of());
    }
}
