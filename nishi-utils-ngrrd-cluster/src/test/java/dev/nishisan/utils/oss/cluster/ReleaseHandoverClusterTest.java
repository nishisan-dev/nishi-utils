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
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.ReleaseOptions;
import dev.nishisan.utils.oss.cluster.api.ReleaseResult;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Entrega de séries entre dois clientes com {@code release(seriesKeys)}: 3 storages, o cliente A escreve, libera as
 * séries e o cliente B as abre e continua a escrita. Confere que nenhuma amostra se perde na entrega, que o padrão
 * ({@code closeOnNode=false}) não gera {@code NOT_OPEN} no nó e que A fica sem rota de escrita. A variação com
 * {@code closeOnNode=true} mostra o custo que motivou o padrão: o {@code CLOSE} descarta a definição no nó, e B paga um
 * {@code NOT_OPEN} (absorvido por reabertura, sem falha de escrita).
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ReleaseHandoverClusterTest {

    private static final long BASE_STEP_MS = 300_000L;
    /** Início alinhado ao passo; o contador de cada série cresce {@code 300 × i} no passo {@code i} (taxa {@code 8 × i}). */
    private static final long T0 = 1_700_000_100_000L - (1_700_000_100_000L % BASE_STEP_MS);
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(90);
    private static final int OPEN_CONCURRENCY = 16;

    private NgrrdClusterTestHarness harness;
    private String yaml;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void releaseEntregaAsSeriesAoOutroClienteSemPerdaNemNotOpen(@TempDir Path base) throws Exception {
        int seriesCount = 2_000;
        int stepsPerClient = 10;
        startCluster(base);
        NgrrdClusterClient clientA = connect();
        NgrrdClusterClient clientB = connect();

        Map<String, NgrrdHandle> handlesA = openAll(clientA, seriesCount);
        List<String> keys = List.copyOf(handlesA.keySet());
        long notOpenBefore = notOpenOnNodes(clientA);

        // A escreve continuamente (sem flush) e, ao parar, libera as séries com as escritas ainda em voo.
        CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
            for (int step = 0; step < stepsPerClient; step++) {
                for (NgrrdHandle handle : handlesA.values()) {
                    writeStep(handle, step);
                }
            }
        });
        writer.get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        ReleaseResult released = clientA.release(keys).join();

        assertTrue(released.allReleased(), "não liberadas: " + released.notReleased());
        assertEquals(seriesCount, released.series().size());
        assertEquals(0, clientA.metrics().routes(), "A fica sem rota de escrita");
        assertEquals(0L, clientA.metrics().samplesFailed());

        // B assume as séries e continua do passo seguinte.
        Map<String, NgrrdHandle> handlesB = openAll(clientB, seriesCount);
        for (int step = stepsPerClient; step < 2 * stepsPerClient; step++) {
            for (NgrrdHandle handle : handlesB.values()) {
                writeStep(handle, step);
            }
        }
        clientB.flushAll();

        assertEquals(0L, clientB.metrics().samplesFailed());
        assertEquals(0L, notOpenOnNodes(clientB) - notOpenBefore, "o padrão não manda CLOSE: nenhum NOT_OPEN no nó");
        assertEquals(0L, clientB.metrics().retriesByStatus().getOrDefault(SeriesStatus.NOT_OPEN, 0L));
        assertNoSampleLost(clientB, handlesB, 2 * stepsPerClient);
    }

    @Test
    void releaseComCloseOnNodeCustaUmNotOpenAoClienteQueJaAbriu(@TempDir Path base) throws Exception {
        int seriesCount = 20;
        int stepsByA = 5;
        startCluster(base);
        NgrrdClusterClient clientA = connect();
        NgrrdClusterClient clientB = connect();

        Map<String, NgrrdHandle> handlesA = openAll(clientA, seriesCount);
        for (int step = 0; step < stepsByA; step++) {
            for (NgrrdHandle handle : handlesA.values()) {
                writeStep(handle, step);
            }
        }
        clientA.flushAll();

        // B abre e escreve uma amostra antes do release de A.
        Map<String, NgrrdHandle> handlesB = openAll(clientB, seriesCount);
        for (NgrrdHandle handle : handlesB.values()) {
            writeStep(handle, stepsByA);
        }
        clientB.flushAll();

        ReleaseResult released = clientA.release(List.copyOf(handlesA.keySet()),
                ReleaseOptions.defaults().withCloseOnNode(true)).join();
        assertTrue(released.allReleased(), "não liberadas: " + released.notReleased());

        // O CLOSE descartou a definição no nó: a escrita seguinte de B recebe NOT_OPEN e reabre.
        for (NgrrdHandle handle : handlesB.values()) {
            writeStep(handle, stepsByA + 1);
        }
        clientB.flushAll();

        assertTrue(clientB.metrics().retriesByStatus().getOrDefault(SeriesStatus.NOT_OPEN, 0L) >= 1,
                "retentativas de B: " + clientB.metrics().retriesByStatus());
        assertEquals(0L, clientB.metrics().samplesFailed());
        assertNoSampleLost(clientB, handlesB, stepsByA + 2);
    }

    private void startCluster(Path base) throws Exception {
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        harness = NgrrdClusterTestHarness.start(base, 3, builder -> builder.rebalanceEnabled(false));
        harness.awaitLeader();
        harness.awaitNodeStatuses(3);
    }

    private NgrrdClusterClient connect() {
        return harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5))
                .closeTimeout(Duration.ofSeconds(60)));
    }

    /** Abre as séries em paralelo ({@value #OPEN_CONCURRENCY} por vez), na ordem dos índices. */
    private Map<String, NgrrdHandle> openAll(NgrrdClusterClient client, int seriesCount) throws Exception {
        List<Future<NgrrdHandle>> opened = new ArrayList<>(seriesCount);
        ExecutorService pool = Executors.newFixedThreadPool(OPEN_CONCURRENCY);
        try {
            for (int i = 0; i < seriesCount; i++) {
                Map<String, String> tags = Map.of("deviceId", "r" + i, "interfaceId", "eth0",
                        "region", "br-sp", "vendor", "x", "role", "core");
                opened.add(pool.submit(() -> retryUntilSuccess(() -> client.open(yaml, tags))));
            }
            Map<String, NgrrdHandle> handles = new LinkedHashMap<>();
            for (Future<NgrrdHandle> future : opened) {
                NgrrdHandle handle = future.get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                handles.put(handle.seriesKey(), handle);
            }
            assertEquals(seriesCount, handles.size());
            return handles;
        } finally {
            pool.shutdownNow();
        }
    }

    /** Amostra do passo {@code step}: contador {@code 300 × step(step+1)/2}, taxa {@code 8 × step} desde o passo anterior. */
    private static void writeStep(NgrrdHandle handle, int step) {
        long ts = T0 + step * BASE_STEP_MS;
        double octets = 300d * step * (step + 1) / 2d;
        handle.write("in_octets", new Sample(ts, octets));
        handle.write("out_octets", new Sample(ts, octets));
    }

    /**
     * Confere as {@code steps} amostras de cada série (passos {@code 0 .. steps-1}), em paralelo. Antes, escreve uma
     * amostra sentinela no passo {@code steps}: a engine só publica a taxa do último passo quando chega a amostra
     * seguinte.
     */
    private static void assertNoSampleLost(NgrrdClusterClient client, Map<String, NgrrdHandle> handles, int steps)
            throws Exception {
        for (NgrrdHandle handle : handles.values()) {
            writeStep(handle, steps);
        }
        client.flushAll();
        ExecutorService pool = Executors.newFixedThreadPool(OPEN_CONCURRENCY);
        try {
            List<Future<?>> reads = new ArrayList<>(handles.size());
            handles.forEach((key, handle) -> reads.add(pool.submit(() -> assertNoSampleLost(key, handle, steps))));
            for (Future<?> read : reads) {
                try {
                    read.get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof AssertionError failure) {
                        throw failure;
                    }
                    throw e;
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Uma amostra perdida juntaria dois passos numa taxa média, então a leitura tem de trazer exatamente as taxas
     * {@code 8 × 1 .. 8 × (steps-1)}, uma por passo — mais, se já publicada, a da sentinela ({@code 8 × steps}).
     */
    private static void assertNoSampleLost(String seriesKey, NgrrdHandle handle, int steps) {
        long endExclusive = T0 + (steps + 2) * BASE_STEP_MS;
        SeriesResult result = handle.read("in_bps",
                new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 500), endExclusive);
        List<Double> rates = new ArrayList<>();
        for (DataPoint point : result.points()) {
            if (!Double.isNaN(point.value())) {
                rates.add(point.value());
            }
        }
        rates.sort(Double::compare);
        if (!rates.isEmpty() && Math.abs(rates.getLast() - 8d * steps) < 1e-6) {
            rates.removeLast();
        }
        List<Double> expected = new ArrayList<>();
        for (int step = 1; step < steps; step++) {
            expected.add(8d * step);
        }
        assertEquals(expected.size(), rates.size(), seriesKey + ": taxas lidas " + rates);
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), rates.get(i), 1e-6, seriesKey + ": taxas lidas " + rates);
        }
    }

    /** Soma de {@code NOT_OPEN} respondidos pelos storages desde que subiram. */
    private long notOpenOnNodes(NgrrdClusterClient client) {
        long total = 0L;
        for (NgrrdStorageNode node : harness.nodes()) {
            total += client.nodeMetrics(node.nodeId()).errorsByStatus().getOrDefault(SeriesStatus.NOT_OPEN, 0L);
        }
        return total;
    }

    /** Absorve o churn de liderança do bootstrap no {@code open}; relança a última falha se o prazo esgotar. */
    private static <T> T retryUntilSuccess(Supplier<T> action) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        NgrrdClusterException lastFailure;
        do {
            try {
                return action.get();
            } catch (NgrrdClusterException e) {
                lastFailure = e;
                Thread.sleep(200L);
            }
        } while (System.currentTimeMillis() < deadline);
        throw lastFailure;
    }
}
