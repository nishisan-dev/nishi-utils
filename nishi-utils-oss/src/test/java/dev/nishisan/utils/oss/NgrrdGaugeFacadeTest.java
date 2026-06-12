package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Paridade RRD fim-a-fim: uma série criada do zero com um DS {@code GAUGE} sem
 * bloco {@code derive} persiste e lê o valor as-is.
 */
class NgrrdGaugeFacadeTest {

    private static final long START_MS = 1_747_339_200_000L;
    private static final int STEP_MS = 300_000;

    private static final String GAUGE_ONLY = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: gauge-only
            spec:
              time:
                baseStepSec: 300
              identity:
                seriesKeyTemplate: "sensor:{id}"
                tags:
                  - name: id
              dataSources:
                - name: temperature
                  type: GAUGE
                  heartbeatSec: 900
              archives:
                rras:
                  - {name: rra_5m, stepSec: 300, rows: 8, cf: [AVERAGE], xff: 0.5}
              storage:
                backend: localDisk
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    @Test
    void gaugeSemDerivePersisteELeAsIs(@TempDir Path dir) {
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(dir);
        ViewQuery query = new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 1000);

        try (NgrrdHandle h = Ngrrd.fromYaml(GAUGE_ONLY, bindings, Map.of("id", "s1"), null,
                Ngrrd.OpenOptions.defaults())) {
            h.write("temperature", new Sample(START_MS, 20.0));
            h.write("temperature", new Sample(START_MS + STEP_MS, 25.0));
            h.write("temperature", new Sample(START_MS + 2L * STEP_MS, 30.0));
            h.checkpoint();

            SeriesResult result = h.read("temperature", query, START_MS + 3L * STEP_MS);
            List<Double> values = result.points().stream().map(p -> p.value())
                    .filter(v -> !Double.isNaN(v)).toList();
            assertFalse(values.isEmpty(), "gauge sem derive deveria persistir valores");
            // Valores as-is (sem derivação): pelo menos um dos valores escritos aparece.
            assertTrue(values.contains(20.0) || values.contains(25.0) || values.contains(30.0), values.toString());
        }
    }
}
