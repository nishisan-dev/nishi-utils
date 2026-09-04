package dev.nishisan.utils.oss.api;

import java.util.List;
import java.util.Objects;

/**
 * Resultado de uma leitura: lista de pontos consolidados com metadados sobre
 * qual RRA foi escolhido e a função de consolidação aplicada.
 *
 * @param stepSec espaçamento real dos pontos em {@code points}. Igual ao step do
 *                RRA quando o range coube em {@code maxPoints}; o step médio do
 *                bucket quando houve agregação. Não é o step do RRA por definição
 *                — quem precisa dele tem o {@code rraName}.
 */
public record SeriesResult(
        String dsName,
        String rraName,
        ConsolidationFunction cf,
        int stepSec,
        List<DataPoint> points
) {
    public SeriesResult {
        points = List.copyOf(Objects.requireNonNullElse(points, List.of()));
    }
}
