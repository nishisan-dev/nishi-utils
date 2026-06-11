package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Valida a retenção como ring buffer (paridade com o RRDtool): cada RRA mantém
 * exatamente {@code rows} CDPs; janelas mais antigas são sobrescritas in-place,
 * sem deletar arquivos. Não há mais blocos nem manifesto — a verificação é feita
 * lendo a série de volta pelo {@link NgrrdReader}.
 */
class NgrrdRetentionTest {

    /**
     * YAML mínimo desenhado para tornar o ring observável:
     *
     * <ul>
     *     <li>baseStepSec = 60 s (1 min)</li>
     *     <li>RRA com stepSec=120 (2 PDPs/CDP) e rows=3 → janela = 360 s</li>
     * </ul>
     *
     * Ingerindo 5 janelas de 2 min, esperamos ler de volta só as 3 mais novas.
     */
    private static final String TINY_YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: retention-test
            spec:
              time:
                baseStepSec: 60
              identity:
                seriesKeyTemplate: "s:{id}"
                tags:
                  - name: id
              dataSources:
                - name: cpu
                  type: GAUGE
                  heartbeatSec: 120
                  derive:
                    output:
                      name: cpu_pct
                      unit: "%"
                      formula: "delta"
                      clampNegativeToZero: false
                      onReset: unknown
                      onWrap: auto
              archives:
                appliesTo:
                  include: [cpu_pct]
                rras:
                  - name: rra_2m_3
                    stepSec: 120
                    rows: 3
                    cf: [AVERAGE]
                    xff: 0.5
              views:
                selection:
                  strategy: best_fit
                  maxPointsDefault: 100
                  fallbackOrder: [raw, agg]
                presets: []
              storage:
                backend: localDisk
                objectNaming:
                  scheme: deterministic
                  seriesPrefix: series
                  schemaPrefix: schema
                writePolicy:
                  mode: append_only
                  idempotency:
                    key: "{seriesKey}"
                    onConflict: verify_or_replace_if_identical
              quality:
                emitMetrics: []
            """;

    private static final long BASE_SEC = 1_747_339_200L;
    private static final long BASE_MS = BASE_SEC * 1000L;
    private static final int WINDOW_SEC = 120;

    private static void ingestWindows(NgrrdWriter writer, int windows) {
        for (int w = 0; w < windows; w++) {
            long slotA = BASE_MS + (long) w * WINDOW_SEC * 1000L;
            long slotB = slotA + 60_000L;
            writer.write("cpu", new Sample(slotA, 10.0 + w)); // slot par
            writer.write("cpu", new Sample(slotB, 20.0 + w)); // slot ímpar → CDP AVG = 15 + w
        }
        writer.flush();
    }

    private static List<Double> readNonNaN(NgrrdDefinition def, NgrrdStorage storage, long endMs) {
        NgrrdReader reader = new NgrrdReader(def, storage, "s:r1");
        ViewQuery query = new ViewQuery(Duration.ofHours(1), 120, ConsolidationFunction.AVERAGE, 100);
        SeriesResult result = reader.read("cpu_pct", query, endMs);
        return result.points().stream().map(p -> p.value())
                .filter(v -> !Double.isNaN(v)).toList();
    }

    @Test
    void janelasForaDoRingSaoSobrescritasMantendoApenasAsMaisRecentes(@TempDir Path tempDir) {
        NgrrdDefinition def = NgrrdYamlLoader.parse(TINY_YAML, k -> null);
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "s:r1")) {
            ingestWindows(writer, 5); // janelas 0..4 (valores 15..19)
        }

        long endMs = BASE_MS + 5L * WINDOW_SEC * 1000L;
        List<Double> values = readNonNaN(def, storage, endMs);
        // rows=3 → sobrevivem as 3 janelas mais novas: 17, 18, 19.
        assertEquals(List.of(17.0, 18.0, 19.0), values,
                "ring deveria reter só as 3 janelas mais recentes, obteve " + values);
    }

    @Test
    void semSobrescritaQuandoVolumeCabeNoRing(@TempDir Path tempDir) {
        NgrrdDefinition def = NgrrdYamlLoader.parse(TINY_YAML, k -> null);
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "s:r1")) {
            ingestWindows(writer, 2); // janelas 0..1 (valores 15, 16) — abaixo de rows=3
        }

        long endMs = BASE_MS + 2L * WINDOW_SEC * 1000L;
        List<Double> values = readNonNaN(def, storage, endMs);
        assertEquals(List.of(15.0, 16.0), values, "esperava 2 janelas retidas, obteve " + values);
    }
}
