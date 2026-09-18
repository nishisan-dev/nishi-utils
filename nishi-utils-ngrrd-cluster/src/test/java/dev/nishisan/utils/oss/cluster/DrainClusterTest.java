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
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Seção 5 da spec do M4: {@code drainNode}/{@code activateNode} fim a fim — 3 storage nodes, cliente
 * escrevendo continuamente. {@code drainNode(alvo)} esvazia o nó em até 90 s (0 séries, {@code DRAINED}),
 * com as imagens preservadas byte a byte (SHA-256) nos novos donos e nenhuma exceção do lado do cliente;
 * {@code activateNode(alvo)} + {@code rebalanceNow()} devolve séries ao nó reativado.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DrainClusterTest {

    private static final int STORAGE_NODE_COUNT = 3;
    private static final int SERIES_COUNT = 24;
    private static final int SETUP_SAMPLES = 3;
    private static final long BASE_STEP_MS = 300_000L;
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration REBALANCE_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration RETRIGGER_INTERVAL = Duration.ofSeconds(5);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void drainEsvaziaONoPreservaImagensEDepoisDeAtivadoVoltaAReceberSeries(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);

        harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT,
                builder -> builder.rebalanceEnabled(false).rebalanceMinDelta(1L));
        harness.awaitLeader();
        harness.awaitNodeStatuses(STORAGE_NODE_COUNT);

        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(20)));

        Map<String, NgrrdHandle> handlesBySeriesKey = new LinkedHashMap<>();
        Map<String, Integer> sampleCountBySeriesKey = new LinkedHashMap<>();
        // Metade "congelada" (comparação de SHA antes/depois válida) e metade escrita continuamente
        // durante a drenagem (mesma estratégia do RebalanceClusterTest — as duas exigências da spec só
        // são verificáveis ao mesmo tempo em conjuntos disjuntos de séries).
        Set<String> frozenSeriesKeys = new LinkedHashSet<>();
        for (int i = 0; i < SERIES_COUNT; i++) {
            Map<String, String> tags = Map.of("deviceId", "dr" + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
            handlesBySeriesKey.put(handle.seriesKey(), handle);
            if (i % 2 == 0) {
                frozenSeriesKeys.add(handle.seriesKey());
            }
            long t0 = alignedBase(handle.seriesKey());
            for (int sample = 0; sample < SETUP_SAMPLES; sample++) {
                long ts = t0 + sample * BASE_STEP_MS;
                handle.write("in_octets", new Sample(ts, 1_000d + sample * 1_000d));
                handle.write("out_octets", new Sample(ts, 500d + sample * 500d));
            }
            handle.checkpoint();
            sampleCountBySeriesKey.put(handle.seriesKey(), SETUP_SAMPLES);
        }
        assertEquals(SERIES_COUNT, handlesBySeriesKey.size(), "seriesKey deveria ser único por conjunto de tags");
        harness.awaitPlacements(SERIES_COUNT);

        String targetNodeId = harness.nodes().get(1).nodeId();

        // Séries do alvo ANTES da drenagem, com o SHA-256 das congeladas.
        Set<String> targetSeriesKeysBefore = new LinkedHashSet<>();
        Map<String, String> shaBeforeBySeriesKey = new LinkedHashMap<>();
        for (String seriesKey : handlesBySeriesKey.keySet()) {
            if (targetNodeId.equals(ownerOf(seriesKey))) {
                targetSeriesKeysBefore.add(seriesKey);
                if (frozenSeriesKeys.contains(seriesKey)) {
                    byte[] image = imageAt(targetNodeId, seriesKey)
                            .orElseThrow(() -> new AssertionError("imagem ausente no dono original de " + seriesKey));
                    shaBeforeBySeriesKey.put(seriesKey, sha256Hex(image));
                }
            }
        }
        assertTrue(targetSeriesKeysBefore.size() > 0, "o nó alvo deveria possuir ao menos uma série antes da drenagem");

        AtomicBoolean stopWriting = new AtomicBoolean(false);
        List<Throwable> writerErrors = new CopyOnWriteArrayList<>();
        Thread continuousWriter = new Thread(() -> {
            int extraSample = SETUP_SAMPLES;
            while (!stopWriting.get()) {
                for (Map.Entry<String, NgrrdHandle> entry : handlesBySeriesKey.entrySet()) {
                    if (frozenSeriesKeys.contains(entry.getKey())) {
                        continue;
                    }
                    try {
                        long ts = alignedBase(entry.getKey()) + extraSample * BASE_STEP_MS;
                        entry.getValue().write("in_octets", new Sample(ts, 2_000d + extraSample));
                        entry.getValue().write("out_octets", new Sample(ts, 1_000d + extraSample));
                        sampleCountBySeriesKey.merge(entry.getKey(), 1, Integer::sum);
                    } catch (Throwable t) {
                        writerErrors.add(t);
                    }
                }
                extraSample++;
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "drain-continuous-writer");
        continuousWriter.start();

        // drainNode é idempotente: redispara periodicamente para cobrir um ciclo que morreu com uma
        // troca de liderança (mesmo tratamento de RebalanceClusterTest#awaitTrueRetriggering).
        awaitTrueRetriggeringDrain(targetNodeId, client);

        stopWriting.set(true);
        continuousWriter.join(REBALANCE_TIMEOUT.toMillis());
        assertTrue(writerErrors.isEmpty(), "escritas contínuas não deveriam lançar durante a drenagem: " + writerErrors);

        // Confirma o essencial: 0 séries no catálogo, estado DRAINED.
        assertEquals(0, countOwnedBy(targetNodeId), "nó drenado não deveria possuir série alguma");
        assertEquals(NodeState.DRAINED, harness.nodes().get(0).catalog().nodeStatusLocal(targetNodeId)
                .orElseThrow().state());

        int checkedFrozen = 0;
        for (String seriesKey : targetSeriesKeysBefore) {
            String ownerAfter = ownerOf(seriesKey);
            assertTrue(!targetNodeId.equals(ownerAfter), "série " + seriesKey + " deveria ter sido movida para fora do nó drenado");
            byte[] imageAfter = imageAt(ownerAfter, seriesKey)
                    .orElseThrow(() -> new AssertionError("imagem ausente no novo dono de " + seriesKey));
            if (frozenSeriesKeys.contains(seriesKey)) {
                checkedFrozen++;
                assertEquals(shaBeforeBySeriesKey.get(seriesKey), sha256Hex(imageAfter),
                        "SHA-256 da imagem de " + seriesKey + " deveria sobreviver à drenagem");
            }
            awaitTrue("imagem de " + seriesKey + " apagada no nó drenado " + targetNodeId, () ->
                    imageAt(targetNodeId, seriesKey).isEmpty());
        }
        assertTrue(checkedFrozen > 0, "ao menos uma série congelada do nó alvo deveria ter sido verificada por SHA-256");

        // Reativa o nó e força um rebalanceamento: ele volta a receber séries.
        client.activateNode(targetNodeId);
        awaitTrueRetriggeringRebalance(() -> countOwnedBy(targetNodeId) > 0, client);

        client.close();
    }

    private String ownerOf(String seriesKey) {
        SeriesPlacement placement = harness.nodes().get(0).catalog().placementStrong(seriesKey)
                .orElseThrow(() -> new AssertionError("série sem placement: " + seriesKey));
        return placement.ownerNodeId();
    }

    private Optional<byte[]> imageAt(String nodeId, String seriesKey) {
        return harness.nodes().stream()
                .filter(node -> node.nodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("nó desconhecido: " + nodeId))
                .volume().storage().get(objectKey(seriesKey));
    }

    private long countOwnedBy(String nodeId) {
        return harness.nodes().get(0).catalog().placementsLocal().values().stream()
                .filter(p -> p.ownerNodeId().equals(nodeId))
                .count();
    }

    private static String objectKey(String seriesKey) {
        return "series/" + seriesKey + ".ngrr";
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long alignedBase(String seriesKey) {
        long base = 1_700_000_000_000L + Math.floorMod(seriesKey.hashCode(), 1_000) * BASE_STEP_MS;
        return base - (base % BASE_STEP_MS);
    }

    private static <T> T retryUntilSuccess(Supplier<T> action) throws InterruptedException {
        long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT.toMillis();
        NgrrdClusterException lastFailure = null;
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

    /** Redispara {@code drainNode(targetNodeId)} (idempotente) a cada {@link #RETRIGGER_INTERVAL} até 0 séries + DRAINED. */
    private void awaitTrueRetriggeringDrain(String targetNodeId, NgrrdClusterClient client) throws InterruptedException {
        long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT.toMillis();
        long nextTrigger = 0L;
        while (System.currentTimeMillis() < deadline) {
            boolean drained = countOwnedBy(targetNodeId) == 0
                    && harness.nodes().get(0).catalog().nodeStatusLocal(targetNodeId)
                            .map(status -> status.state() == NodeState.DRAINED).orElse(false);
            if (drained) {
                return;
            }
            if (System.currentTimeMillis() >= nextTrigger) {
                try {
                    client.drainNode(targetNodeId);
                } catch (NgrrdClusterException e) {
                    // líder em transição: a próxima volta redispara.
                }
                nextTrigger = System.currentTimeMillis() + RETRIGGER_INTERVAL.toMillis();
            }
            Thread.sleep(150L);
        }
        fail("nó " + targetNodeId + " não chegou a DRAINED com 0 séries em " + DRAIN_TIMEOUT);
    }

    /** Redispara {@code rebalanceNow()} a cada {@link #RETRIGGER_INTERVAL} até {@code condition} valer. */
    private void awaitTrueRetriggeringRebalance(BooleanSupplier condition, NgrrdClusterClient client)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + REBALANCE_TIMEOUT.toMillis();
        long nextTrigger = 0L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            if (System.currentTimeMillis() >= nextTrigger) {
                try {
                    client.rebalanceNow();
                } catch (NgrrdClusterException e) {
                    // líder em transição: a próxima volta redispara.
                }
                nextTrigger = System.currentTimeMillis() + RETRIGGER_INTERVAL.toMillis();
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição de rebalanceamento pós-ativação não satisfeita em " + REBALANCE_TIMEOUT);
        }
    }

    private void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + DRAIN_TIMEOUT + "): " + description);
        }
    }
}
