package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.metrics.NgrrdMetrics;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke completo do ngrrd usando o YAML do enunciado
 * ({@code iface-traffic-errors-v1.yaml}). Ingere 30 dias × 288 amostras/dia
 * para os 4 DS COUNTER (in_octets, out_octets, in_errors, out_errors) com um
 * reset proposital injetado e valida os 5 presets declarados no YAML.
 *
 * <p>Roda apenas no profile {@code ngrrd-integration} (Failsafe) — o tamanho
 * total de samples (4 × 8640 = 34.560) torna o tempo de execução incompatível
 * com a suite unitária default.</p>
 */
class IfaceTrafficSmokeIT {

    private static final int DAY_SECONDS = 86_400;
    private static final int DAYS = 30;
    private static final int STEP_SECONDS = 300;
    private static final int SAMPLES_PER_DAY = DAY_SECONDS / STEP_SECONDS; // 288
    private static final int TOTAL_SAMPLES = DAYS * SAMPLES_PER_DAY;       // 8640

    private static final long START_EPOCH_SEC = 1_747_339_200L; // 2025-05-15 16:00 UTC, alinhado
    private static final long START_EPOCH_MS = START_EPOCH_SEC * 1000L;
    private static final long END_EPOCH_MS = START_EPOCH_MS + (long) TOTAL_SAMPLES * STEP_SECONDS * 1000L;

    private static String loadYaml() throws Exception {
        try (InputStream in = IfaceTrafficSmokeIT.class
                .getResourceAsStream("/iface-traffic-errors-v1.yaml")) {
            return new String(in.readAllBytes());
        }
    }

    private static Map<String, String> tags() {
        return Map.of(
                "deviceId", "r1",
                "interfaceId", "eth0",
                "region", "br-sp",
                "vendor", "x",
                "role", "core");
    }

    @Test
    void ingestaoDeTrintaDiasValidaCincoPresetsEmDisco(@TempDir Path tempDir) throws Exception {
        // O YAML padrão usa backend objectStorage; reescreve para localDisk no smoke.
        String yaml = loadYaml().replaceFirst("backend: objectStorage", "backend: localDisk");

        NgrrdMetrics metrics = new NgrrdMetrics();
        long[] eventResets = {0};
        NgrrdMetricsListener watcher = new NgrrdMetricsListener() {
            @Override
            public void onCounterReset(String dsName) {
                eventResets[0]++;
            }
        };
        NgrrdMetrics chained = new NgrrdMetrics(watcher);
        NgrrdMetricsListener fanout = combine(metrics, chained);

        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);
        long resetIndex = TOTAL_SAMPLES / 2; // reset no meio da série em in_octets

        try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags(), fanout)) {
            long inOctets = 0L;
            long outOctets = 0L;
            long inErrors = 0L;
            long outErrors = 0L;
            for (long i = 0; i < TOTAL_SAMPLES; i++) {
                long tsMs = START_EPOCH_MS + i * STEP_SECONDS * 1000L;
                inOctets += 50_000L + (i % 100);
                outOctets += 60_000L + (i % 80);
                inErrors += i % 7;
                outErrors += i % 5;

                long inObserved = (i == resetIndex) ? 1L : inOctets; // reset grande
                handle.write("in_octets", new Sample(tsMs, inObserved));
                handle.write("out_octets", new Sample(tsMs, outOctets));
                handle.write("in_errors", new Sample(tsMs, inErrors));
                handle.write("out_errors", new Sample(tsMs, outErrors));
            }
            handle.flush();

            assertValidatesPreset(handle, "daily", "in_bps", 280);
            assertValidatesPreset(handle, "weekly", "in_bps", 160);
            assertValidatesPreset(handle, "monthly", "in_bps", 700);
            assertValidatesPreset(handle, "errors_daily", "in_eps", 280);
            // yearly tem window P365D mas só temos 30 dias de dados; o downsample
            // limita a maxPoints=5000 mas a contagem real depende do RRA escolhido.
            Map<String, SeriesResult> yearly = handle.read("yearly");
            assertNotNull(yearly.get("in_bps"));
            assertTrue(yearly.get("in_bps").points().size() > 0,
                    "yearly deveria retornar pontos mesmo com dataset menor que a janela");
        }

        assertEquals(1, metrics.counterResetCount(),
                "esperava exatamente 1 counter_reset_count");
        assertEquals(1, eventResets[0]);
    }

    private static void assertValidatesPreset(NgrrdHandle handle, String preset,
                                              String ds, int minimumPoints) {
        // Os presets fazem read da janela [now-window, now). Como nosso dataset
        // termina em END_EPOCH_MS, usamos o ViewExecutor através do reader já
        // anexado (via tags do handle) — mas precisamos do endExclusive específico.
        SeriesResult result = readPreset(handle, preset, ds);
        assertNotNull(result, "Preset " + preset + " não devolveu resultado para " + ds);
        assertTrue(result.points().size() >= minimumPoints,
                "Preset " + preset + " devolveu " + result.points().size()
                        + " pontos (mínimo esperado " + minimumPoints + ")");
        long nan = result.points().stream().filter(p -> Double.isNaN(p.value())).count();
        long total = result.points().size();
        assertFalse(nan == total, "Preset " + preset + " devolveu apenas NaNs para " + ds);
    }

    private static SeriesResult readPreset(NgrrdHandle handle, String preset, String ds) {
        return handle.read(preset, END_EPOCH_MS).get(ds);
    }

    private static NgrrdMetricsListener combine(NgrrdMetricsListener a, NgrrdMetricsListener b) {
        return new NgrrdMetricsListener() {
            @Override public void onLateSample(String dsName, long latenessSec) {
                a.onLateSample(dsName, latenessSec); b.onLateSample(dsName, latenessSec);
            }
            @Override public void onCounterReset(String dsName) {
                a.onCounterReset(dsName); b.onCounterReset(dsName);
            }
            @Override public void onWrapDetected(String dsName) {
                a.onWrapDetected(dsName); b.onWrapDetected(dsName);
            }
            @Override public void onIngestLag(String dsName, long lagSec) {
                a.onIngestLag(dsName, lagSec); b.onIngestLag(dsName, lagSec);
            }
            @Override public void onBlockClosed(String rraName, String dsName, double missingRatio) {
                a.onBlockClosed(rraName, dsName, missingRatio); b.onBlockClosed(rraName, dsName, missingRatio);
            }
        };
    }
}
