package dev.nishisan.utils.oss.cli;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NgrrdMigrateCliTest {

    private static final long START_MS = 1_747_339_200_000L;
    private static final int STEP_MS = 300_000;
    private static final long END = START_MS + 3L * STEP_MS;
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

    private NgrrdHandle open(String yaml, Path dir, String device, OnGeometryChange policy) {
        return Ngrrd.fromYaml(yaml, StorageFactory.StorageBindings.forLocalDisk(dir),
                Map.of("deviceId", device), null,
                policy == null ? Ngrrd.OpenOptions.defaults() : Ngrrd.OpenOptions.onGeometryChange(policy));
    }

    private void seed(Path dir, String device) {
        try (NgrrdHandle h = open(V1, dir, device, null)) {
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
    void dryRunRelataMigraveisSemEscrever(@TempDir Path dir) throws Exception {
        seed(dir, "r1");
        seed(dir, "r2");
        Path yamlFile = dir.resolve("v2.yaml");
        Files.writeString(yamlFile, V2_GAUGE);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout, true, StandardCharsets.UTF_8);
        int code = new NgrrdMigrateCli().run(
                new String[]{yamlFile.toString(), dir.toString(), "--dry-run"}, out, out);

        assertEquals(0, code);
        String report = bout.toString(StandardCharsets.UTF_8);
        assertTrue(report.contains("séries inspecionadas: 2"), report);
        assertTrue(report.contains("migráveis (estrutural): 2"), report);

        // Dry-run não escreve: a série antiga continua íntegra (mesma geometria v1).
        try (NgrrdHandle h = open(V1, dir, "r1", null)) {
            assertFalse(nonNan(h.read("in_bps", QUERY, END)).isEmpty());
        }
    }

    @Test
    void execucaoMigraTodasAsSeriesPreservandoHistoria(@TempDir Path dir) throws Exception {
        seed(dir, "r1");
        seed(dir, "r2");
        List<Double> before;
        try (NgrrdHandle h = open(V1, dir, "r1", null)) {
            before = nonNan(h.read("in_bps", QUERY, END));
        }
        assertFalse(before.isEmpty());

        Path yamlFile = dir.resolve("v2.yaml");
        Files.writeString(yamlFile, V2_GAUGE);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout, true, StandardCharsets.UTF_8);
        int code = new NgrrdMigrateCli().run(new String[]{yamlFile.toString(), dir.toString()}, out, out);

        assertEquals(0, code);
        assertTrue(bout.toString(StandardCharsets.UTF_8).contains("2 migrada(s)"));

        // Após a migração, a geometria já é v2 (reconcile no-op) e a história foi preservada.
        try (NgrrdHandle h = open(V2_GAUGE, dir, "r1", null)) {
            assertEquals(before, nonNan(h.read("in_bps", QUERY, END)));
        }
        try (NgrrdHandle h = open(V2_GAUGE, dir, "r2", null)) {
            assertFalse(nonNan(h.read("in_bps", QUERY, END)).isEmpty());
        }
    }
}
