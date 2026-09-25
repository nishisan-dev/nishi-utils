/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Medição (não regressão): custo em latência de {@code open} no cliente e em confirmações extras no
 * líder ({@code leaderConfirmations}, introduzidas pela correção da issue #174 — ver Javadoc de
 * {@code StorageRequestHandler#confirmOwnerWithLeader}) ao criar um lote grande de séries NOVAS via
 * {@code createIfMissing=true} (o padrão).
 *
 * <p>{@code N} e a concorrência são configuráveis via propriedades de sistema
 * ({@value #SERIES_PROPERTY} / {@value #CONCURRENCY_PROPERTY}) — o default aqui é deliberadamente
 * pequeno (seguro para rodar em qualquer máquina de desenvolvimento ou CI local sob o profile
 * {@code ngrrd-cluster}); a comparação A/B contra a linha de base pré-#174 (commit {@code a3f8e1a}) usa
 * um N maior explícito — resultados em {@code doc/oss/ngrrd-cluster-operacao.md}, seção "Confirmação do
 * dono antes de criar (issue #174)". Cada série ocupa poucas centenas de KB reais em disco
 * — os shards do volume ({@code BlobVolume}) são arquivos esparsos, então o N grande não implica o N ×
 * capacidade nominal do shard em uso real de disco (verificado empiricamente antes de escrever este
 * teste).</p>
 *
 * <p>A linha {@code NGRRD_BULK_CREATE_COST} é o produto principal deste teste — as asserções são só de
 * sanidade (todas as séries abertas com sucesso e existentes no catálogo; confirmações no líder não
 * excedem N).</p>
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BulkCreateLeaderCostClusterTest {

    private static final Logger LOGGER = Logger.getLogger(BulkCreateLeaderCostClusterTest.class.getName());

    private static final String SERIES_PROPERTY = "ngrrd.bulkCreate.series";
    private static final String CONCURRENCY_PROPERTY = "ngrrd.bulkCreate.concurrency";
    private static final int DEFAULT_SERIES_COUNT = 200;
    private static final int DEFAULT_CONCURRENCY = 16;

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void aberturaEmLoteDeSeriesNovasMedeCustoDeConfirmacaoNoLider(@TempDir Path base) throws Exception {
        int seriesCount = Integer.getInteger(SERIES_PROPERTY, DEFAULT_SERIES_COUNT);
        int concurrency = Integer.getInteger(CONCURRENCY_PROPERTY, DEFAULT_CONCURRENCY);
        assertTrue(seriesCount > 0, SERIES_PROPERTY + " deve ser > 0: " + seriesCount);
        assertTrue(concurrency > 0, CONCURRENCY_PROPERTY + " deve ser > 0: " + concurrency);

        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));

        harness = NgrrdClusterTestHarness.start(base, 3, builder -> builder.rebalanceEnabled(false));
        harness.awaitNodeStatuses(3);
        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(10))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(30)));

        long[] openLatencyNanos = new long[seriesCount];
        String[] seriesKeys = new String[seriesCount];
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        try {
            List<Future<?>> futures = new ArrayList<>(seriesCount);
            long startNanos = System.nanoTime();
            for (int i = 0; i < seriesCount; i++) {
                int index = i;
                futures.add(pool.submit(() -> {
                    Map<String, String> tags = Map.of("deviceId", "bulk" + index, "interfaceId", "eth0",
                            "region", "br-sp", "vendor", "x", "role", "core");
                    long t0 = System.nanoTime();
                    NgrrdHandle handle = client.open(yaml, tags);
                    long elapsed = System.nanoTime() - t0;
                    openLatencyNanos[index] = elapsed;
                    seriesKeys[index] = handle.seriesKey();
                    handle.close();
                }));
            }
            for (Future<?> future : futures) {
                future.get(600, TimeUnit.SECONDS);
            }
            long totalNanos = System.nanoTime() - startNanos;

            // Sanidade: todas as N séries foram criadas com sucesso e existem no catálogo.
            List<String> keys = Arrays.asList(seriesKeys);
            Map<String, Boolean> existence = client.exists(keys);
            assertEquals(seriesCount, existence.size());
            long missing = existence.values().stream().filter(present -> !present).count();
            assertEquals(0L, missing, "todas as " + seriesCount + " séries deveriam existir após o open");

            long[] sortedLatencyUs = Arrays.stream(openLatencyNanos).map(n -> n / 1_000L).sorted().toArray();
            long openP50Us = percentile(sortedLatencyUs, 50);
            long openP99Us = percentile(sortedLatencyUs, 99);
            long openMaxUs = sortedLatencyUs[sortedLatencyUs.length - 1];
            double totalSeconds = totalNanos / 1_000_000_000.0;
            double throughputPerSecond = seriesCount / totalSeconds;
            long placeCount = client.metrics().placeCount();

            long leaderConfirmations = 0L;
            StringBuilder perNodeConfirmationLatency = new StringBuilder();
            for (NgrrdStorageNode node : harness.nodes()) {
                NodeMetricsSnapshot snapshot = node.metricsSnapshot();
                leaderConfirmations += snapshot.leaderConfirmations();
                LatencySnapshot latency = snapshot.leaderConfirmationLatency();
                if (perNodeConfirmationLatency.length() > 0) {
                    perNodeConfirmationLatency.append(',');
                }
                perNodeConfirmationLatency.append(node.nodeId()).append(":count=").append(snapshot.leaderConfirmations())
                        .append(",p50Us=").append(latency.p50Micros())
                        .append(",p99Us=").append(latency.p99Micros())
                        .append(",maxUs=").append(latency.maxMicros());
            }
            double leaderConfirmationsPerSeries = leaderConfirmations / (double) seriesCount;

            // Sanidade: a confirmação extra no líder nunca é chamada mais de uma vez por série criada.
            assertTrue(leaderConfirmations <= seriesCount, "leaderConfirmations (" + leaderConfirmations
                    + ") não deveria exceder o número de séries criadas (" + seriesCount + ")");

            LOGGER.info("NGRRD_BULK_CREATE_COST n=" + seriesCount + " concurrency=" + concurrency
                    + " totalMs=" + (totalNanos / 1_000_000L)
                    + " throughputPerSec=" + String.format(Locale.ROOT, "%.2f", throughputPerSecond)
                    + " openP50Us=" + openP50Us + " openP99Us=" + openP99Us + " openMaxUs=" + openMaxUs
                    + " placeCount=" + placeCount
                    + " retriesByStatus=" + client.metrics().retriesByStatus()
                    + " leaderConfirmations=" + leaderConfirmations
                    + " leaderConfirmationsPerSeries=" + String.format(Locale.ROOT, "%.4f", leaderConfirmationsPerSeries)
                    + " leaderConfirmationLatencyByNode=[" + perNodeConfirmationLatency + "]");
        } finally {
            pool.shutdownNow();
        }

        client.close();
    }

    /** Percentil aproximado por índice sobre um array já ordenado (mesmo método usado nos relatórios A/B). */
    private static long percentile(long[] sortedValues, int p) {
        if (sortedValues.length == 0) {
            return 0L;
        }
        int index = (int) Math.ceil(p / 100.0 * sortedValues.length) - 1;
        index = Math.max(0, Math.min(index, sortedValues.length - 1));
        return sortedValues[index];
    }
}
