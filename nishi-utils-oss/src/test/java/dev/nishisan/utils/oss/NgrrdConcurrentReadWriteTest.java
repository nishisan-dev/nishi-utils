package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stress do contrato de concorrência do {@link NgrrdHandle}: 1 writer lógico +
 * N readers no mesmo handle, sem serialização externa (issue #146).
 *
 * <p>A série é um GAUGE passthrough com {@code value == tsSec} da amostra —
 * qualquer leitura rasgada (live-state antigo interpretando células novas do
 * ring) desloca timestamps e quebra a invariante
 * {@code point.value() == point.tsEpochMs() / 1000}, tornando a corrida
 * detectável de forma determinística.</p>
 */
class NgrrdConcurrentReadWriteTest {

    private static final int STEP_SEC = 300;
    private static final int ROWS = 48;
    private static final long T0_SEC = 1_700_000_400L; // alinhado a 300s
    private static final int SLOTS = 2_000;
    private static final int READERS = 3;

    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: concurrent-read-write
              description: GAUGE deterministico (value == tsSec) para stress de concorrencia.

            spec:
              time:
                baseStepSec: 300
                timestampAlignment: epoch
                lateSamplePolicy:
                  maxLatenessSec: 600
                  onLate: "bucket_if_possible"
                missingValue: "NaN"

              identity:
                seriesKeyTemplate: "sensor:{sensorId}"
                tags:
                  - name: sensorId

              dataSources:
                - name: level
                  type: GAUGE
                  heartbeatSec: 900
                  derive:
                    output:
                      name: level_out
                      unit: "u"
                      formula: "delta"

              archives:
                appliesTo:
                  include:
                    - level_out
                  exclude: []
                rras:
                  - name: rra_5m
                    stepSec: 300
                    rows: 48
                    cf:
                      - AVERAGE
                    xff: 0.50

              views:
                selection:
                  strategy: best_fit
                  maxPointsDefault: 2000
                  fallbackOrder: [raw, agg]
                presets:
                  - name: full
                    window: "PT4H"
                    targetStepSec: 300
                    cf: AVERAGE
                    maxPoints: 2000
                    series:
                      - level_out

              storage:
                backend: localDisk
                objectNaming:
                  scheme: deterministic
                  seriesPrefix: "series"
                  schemaPrefix: "schema"
                writePolicy:
                  mode: append_only
                  idempotency:
                    key: "{seriesKey}"
                    onConflict: "verify_or_replace_if_identical"
            """;

    @Test
    void umWriterENReadersConcorrentesNoMesmoHandle(@TempDir Path tempDir) throws Exception {
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tempDir);

        try (NgrrdHandle handle = Ngrrd.fromYaml(YAML, bindings, Map.of("sensorId", "s1"), null)) {
            AtomicLong lastWrittenMs = new AtomicLong(T0_SEC * 1000L);
            AtomicBoolean writerDone = new AtomicBoolean(false);
            AtomicLong readsExecuted = new AtomicLong();
            List<String> violations = Collections.synchronizedList(new ArrayList<>());

            // Janela cobrindo o ring inteiro: força leitura das linhas mais
            // antigas, onde uma corrida live-state x ring deslocaria timestamps.
            ViewQuery fullRing = new ViewQuery(Duration.ofSeconds((long) ROWS * STEP_SEC),
                    STEP_SEC, ConsolidationFunction.AVERAGE, 10_000);

            ExecutorService pool = Executors.newFixedThreadPool(READERS + 1);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();

                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < SLOTS; i++) {
                        long tsSec = T0_SEC + (long) i * STEP_SEC;
                        handle.write("level", new Sample(tsSec * 1000L, (double) tsSec));
                        lastWrittenMs.set(tsSec * 1000L);
                        if (i % 10 == 0) {
                            handle.checkpoint();
                        }
                    }
                    handle.checkpoint();
                    writerDone.set(true);
                    return null;
                }));

                for (int r = 0; r < READERS; r++) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        do {
                            long endExclusiveMs = lastWrittenMs.get() + STEP_SEC * 1000L;
                            SeriesResult res = handle.read("level_out", fullRing, endExclusiveMs);
                            for (DataPoint p : res.points()) {
                                double v = p.value();
                                if (!Double.isNaN(v) && v != (double) (p.tsEpochMs() / 1000L)) {
                                    violations.add("ponto inconsistente: ts=" + p.tsEpochMs()
                                            + " valor=" + v);
                                }
                            }
                            readsExecuted.incrementAndGet();
                        } while (!writerDone.get());
                        return null;
                    }));
                }

                start.countDown();
                for (Future<?> f : futures) {
                    f.get(120, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertTrue(violations.isEmpty(),
                    "leituras concorrentes observaram pontos inconsistentes ("
                            + violations.size() + "): " + violations.stream().limit(5).toList());
            assertTrue(readsExecuted.get() >= READERS,
                    "esperava ao menos uma leitura por reader, houve " + readsExecuted.get());

            // Sanidade pós-corrida: a leitura final enxerga o ring cheio e íntegro.
            SeriesResult fin = handle.read("level_out", fullRing, lastWrittenMs.get() + STEP_SEC * 1000L);
            long nonNan = fin.points().stream().filter(p -> !Double.isNaN(p.value())).count();
            assertTrue(nonNan >= ROWS - 1,
                    "esperava o ring praticamente cheio após a corrida, pontos válidos=" + nonNan);
        }
    }
}
