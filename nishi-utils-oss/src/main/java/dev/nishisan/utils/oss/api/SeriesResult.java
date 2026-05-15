package dev.nishisan.utils.oss.api;

import java.util.List;
import java.util.Objects;

/**
 * Resultado de uma leitura: lista de pontos consolidados com metadados sobre
 * qual RRA foi escolhido e a função de consolidação aplicada.
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
