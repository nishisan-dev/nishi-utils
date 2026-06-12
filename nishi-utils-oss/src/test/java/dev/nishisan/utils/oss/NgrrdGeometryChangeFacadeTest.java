package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.migration.NgrrdGeometryChangeException;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o tratamento de mudança de geometria na façade {@link Ngrrd}: FAIL como
 * default fail-safe, gate por {@code schemaRevision}, RECREATE e MIGRATE
 * preservando a história.
 */
class NgrrdGeometryChangeFacadeTest {

    private static final long START_MS = 1_747_339_200_000L;
    private static final int STEP_MS = 300_000;
    private static final Map<String, String> TAGS = Map.of("deviceId", "r1");
    private static final ViewQuery QUERY =
            new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 1000);

    private static final String V1 = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: geom-change
              schemaRevision: 1
            spec:
              time:
                baseStepSec: 300
              identity:
                seriesKeyTemplate: "dev:{deviceId}"
                tags:
                  - name: deviceId
              dataSources:
                - name: in_octets
                  type: COUNTER
                  counterBits: 64
                  heartbeatSec: 900
                  derive:
                    output: {name: in_bps, formula: "delta * 8 / deltaT"}
              archives:
                rras:
                  - {name: rra_5m, stepSec: 300, rows: 8, cf: [AVERAGE], xff: 0.5}
              storage:
                backend: localDisk
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    // V1 + 1 GAUGE sem derive e schemaRevision incrementado para 2.
    private static final String V2_GAUGE = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: geom-change
              schemaRevision: 2
            spec:
              time:
                baseStepSec: 300
              identity:
                seriesKeyTemplate: "dev:{deviceId}"
                tags:
                  - name: deviceId
              dataSources:
                - name: in_octets
                  type: COUNTER
                  counterBits: 64
                  heartbeatSec: 900
                  derive:
                    output: {name: in_bps, formula: "delta * 8 / deltaT"}
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

    private NgrrdHandle open(String yaml, Path dir, OnGeometryChange policy) {
        return Ngrrd.fromYaml(yaml, StorageFactory.StorageBindings.forLocalDisk(dir), TAGS, null,
                policy == null ? Ngrrd.OpenOptions.defaults() : Ngrrd.OpenOptions.onGeometryChange(policy));
    }

    private void seedV1(Path dir) {
        try (NgrrdHandle h = open(V1, dir, null)) {
            h.write("in_octets", new Sample(START_MS, 1_000_000L));
            h.write("in_octets", new Sample(START_MS + STEP_MS, 1_300_000L));
            h.write("in_octets", new Sample(START_MS + 2L * STEP_MS, 1_600_000L));
            h.checkpoint();
        }
    }

    private static List<Double> nonNan(SeriesResult result) {
        return result.points().stream().map(p -> p.value()).filter(v -> !Double.isNaN(v)).toList();
    }

    @Test
    void defaultFailAbortaSemTocarNaSerie(@TempDir Path dir) {
        seedV1(dir);
        List<Double> before;
        try (NgrrdHandle h = open(V1, dir, null)) {
            before = nonNan(h.read("in_bps", QUERY, START_MS + 3L * STEP_MS));
        }
        assertFalse(before.isEmpty());

        // Abrir com a geometria nova e default (FAIL) deve lançar e não tocar na série.
        assertThrows(NgrrdGeometryChangeException.class, () -> open(V2_GAUGE, dir, null).close());

        try (NgrrdHandle h = open(V1, dir, null)) {
            assertEquals(before, nonNan(h.read("in_bps", QUERY, START_MS + 3L * STEP_MS)));
        }
    }

    @Test
    void geometriaSemBumpDeRevisaoEhBloqueada(@TempDir Path dir) {
        seedV1(dir);
        // V2 com gauge mas sem incrementar schemaRevision (continua 1).
        String v2NoBump = V2_GAUGE.replace("schemaRevision: 2", "schemaRevision: 1");
        NgrrdGeometryChangeException ex = assertThrows(NgrrdGeometryChangeException.class,
                () -> open(v2NoBump, dir, OnGeometryChange.MIGRATE).close());
        assertTrue(ex.getMessage().contains("schemaRevision"), ex.getMessage());
    }

    @Test
    void recreateDescartaHistoria(@TempDir Path dir) {
        seedV1(dir);
        try (NgrrdHandle h = open(V2_GAUGE, dir, OnGeometryChange.RECREATE)) {
            assertTrue(nonNan(h.read("in_bps", QUERY, START_MS + 3L * STEP_MS)).isEmpty());
        }
    }

    @Test
    void migratePreservaHistoria(@TempDir Path dir) {
        seedV1(dir);
        List<Double> before;
        try (NgrrdHandle h = open(V1, dir, null)) {
            before = nonNan(h.read("in_bps", QUERY, START_MS + 3L * STEP_MS));
        }
        assertFalse(before.isEmpty());

        try (NgrrdHandle h = open(V2_GAUGE, dir, OnGeometryChange.MIGRATE)) {
            // História da coluna preexistente preservada.
            assertEquals(before, nonNan(h.read("in_bps", QUERY, START_MS + 3L * STEP_MS)));
            // Coluna nova (GAUGE) é gravável e legível as-is.
            h.write("temperature", new Sample(START_MS + 3L * STEP_MS, 42.0));
            h.checkpoint();
            assertTrue(nonNan(h.read("temperature", QUERY, START_MS + 4L * STEP_MS)).contains(42.0));
        }
    }
}
