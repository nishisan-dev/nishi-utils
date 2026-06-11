package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.config.NgrrdDefinitionValidator;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import dev.nishisan.utils.oss.format.NgrrdManifest;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a persistência incremental (rrdtool-like): reabertura reidrata o
 * counterPrev e os acumuladores, o {@code checkpoint()} materializa um bloco
 * parcial legível antes do rollover, e não há perda de dados ao reabrir o handle
 * no meio de um bloco.
 */
class NgrrdWriterIncrementalTest {

    private static final long BLOCK_START_SEC = 1_747_339_200L; // alinhado a 6h
    private static final long BLOCK_START_MS = BLOCK_START_SEC * 1000L;
    private static final int STEP_MS = 300_000;
    private static final double EXPECTED_BPS = 8 * 100_000.0 / 300.0; // 2666.67 bit/s
    private static final String SERIES_KEY = "device:r1/iface:eth0";

    private NgrrdDefinition incrementalDefinition() throws Exception {
        String yaml;
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-errors-v1.yaml")) {
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        yaml = yaml.replace("mode: append_only",
                "mode: append_only\n      persistenceMode: incremental\n      persistLastValue: true");
        NgrrdDefinition def = NgrrdYamlLoader.parse(yaml, k -> null);
        NgrrdDefinitionValidator.validate(def);
        return def;
    }

    @Test
    void seriesStateCodecRoundTrip() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.add(10.0);
        pdp.add(20.0);
        pdp.add(Double.NaN); // missing
        Map<String, PrimaryDataPoint.Memento[]> buckets = new LinkedHashMap<>();
        buckets.put("in_bps", new PrimaryDataPoint.Memento[]{pdp.snapshot(), new PrimaryDataPoint().snapshot()});
        Map<String, BlockWindow.CounterPrev> cp = new LinkedHashMap<>();
        cp.put("in_octets", new BlockWindow.CounterPrev(123_456.0, BLOCK_START_MS));

        SeriesWriterState state = new SeriesWriterState(BLOCK_START_SEC, 300, 600, cp, buckets);
        SeriesWriterState back = SeriesStateCodec.decode(SeriesStateCodec.encode(state));

        assertEquals(BLOCK_START_SEC, back.blockStartEpochSec());
        assertEquals(300, back.baseStepSec());
        assertEquals(123_456.0, back.counterPrev().get("in_octets").value());
        assertEquals(BLOCK_START_MS, back.counterPrev().get("in_octets").tsEpochMs());
        PrimaryDataPoint.Memento slot0 = back.buckets().get("in_bps")[0];
        assertEquals(30.0, slot0.sum());
        assertEquals(2, slot0.count());
        assertEquals(1, slot0.missing());
        assertEquals(15.0, PrimaryDataPoint.restore(slot0).consolidate(ConsolidationFunction.AVERAGE));
    }

    @Test
    void reaberturaReidrataCounterPrevEDerivaNaPrimeiraAmostra(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = incrementalDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        // Writer A: 1ª amostra (FIRST_SAMPLE → NaN) mas persiste o counterPrev no estado.
        try (NgrrdWriter a = new NgrrdWriter(def, storage, SERIES_KEY)) {
            a.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            a.checkpoint();
        }

        // Writer B: reabre, reidrata o counterPrev → 2ª amostra deriva (não NaN).
        try (NgrrdWriter b = new NgrrdWriter(def, storage, SERIES_KEY)) {
            b.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 1_100_000L));
            b.checkpoint();
            assertFalse(b.persistedBlocks().isEmpty(), "checkpoint devia ter materializado bloco parcial");
            NgrrdManifest snap = new ManifestUpdater(b, storage, "h", 60, true).writeSnapshot();
            assertFalse(snap.rras().isEmpty(), "manifesto não deveria ter rras vazio após checkpoint");
        }

        NgrrdReader reader = new NgrrdReader(def, storage, SERIES_KEY);
        ViewQuery query = new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);
        SeriesResult result = reader.read("in_bps", query, BLOCK_START_MS + 2L * STEP_MS);
        long nonNaN = result.points().stream().filter(p -> !Double.isNaN(p.value())).count();
        assertTrue(nonNaN >= 1, "esperava ao menos 1 ponto derivado após a reabertura, obteve " + result.points());
        double v = result.points().stream().filter(p -> !Double.isNaN(p.value())).findFirst().orElseThrow().value();
        assertEquals(EXPECTED_BPS, v, 1e-6);
    }

    @Test
    void checkpointMaterializaBlocoNgrrdAntesDoRollover(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = incrementalDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        try (NgrrdWriter w = new NgrrdWriter(def, storage, SERIES_KEY)) {
            w.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            w.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 1_100_000L));
            w.checkpoint(); // NÃO houve rollover (bloco é de 6h)
            assertTrue(temBlocoNgrrd(tempDir), "esperava bloco .ngrrd em disco após checkpoint, antes do rollover");
        }
    }

    @Test
    void naoPerdeDadosAoReabrirNoMeioDoBloco(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = incrementalDefinition();
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        // Writer A: preenche slot1 (a 1ª amostra é FIRST_SAMPLE).
        try (NgrrdWriter a = new NgrrdWriter(def, storage, SERIES_KEY)) {
            a.write("in_octets", new Sample(BLOCK_START_MS, 1_000_000L));
            a.write("in_octets", new Sample(BLOCK_START_MS + STEP_MS, 1_100_000L)); // slot1 = 2666.67
            a.checkpoint();
        }

        // Writer B: reabre no MESMO bloco e preenche slot2. Sem reidratação, o
        // overwrite do bloco apagaria o slot1 — o reload garante a janela completa.
        try (NgrrdWriter b = new NgrrdWriter(def, storage, SERIES_KEY)) {
            b.write("in_octets", new Sample(BLOCK_START_MS + 2L * STEP_MS, 1_200_000L)); // slot2 = 2666.67
            b.checkpoint();
            new ManifestUpdater(b, storage, "h", 60, true).writeSnapshot();
        }

        NgrrdReader reader = new NgrrdReader(def, storage, SERIES_KEY);
        ViewQuery query = new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);
        SeriesResult result = reader.read("in_bps", query, BLOCK_START_MS + 3L * STEP_MS);
        List<Double> nonNaN = result.points().stream().map(p -> p.value())
                .filter(v -> !Double.isNaN(v)).toList();
        assertEquals(2, nonNaN.size(),
                "esperava 2 pontos (slot1 do writer A + slot2 do writer B), sem perda no overwrite: " + result.points());
        nonNaN.forEach(v -> assertEquals(EXPECTED_BPS, v, 1e-6));
    }

    private static boolean temBlocoNgrrd(Path root) throws Exception {
        try (var files = Files.walk(root)) {
            return files.anyMatch(p -> p.getFileName().toString().endsWith(".ngrrd"));
        }
    }
}
