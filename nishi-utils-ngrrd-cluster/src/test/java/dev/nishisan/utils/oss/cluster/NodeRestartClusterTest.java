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
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regressão enxuta do F1 (achado do Debugger): reinicia o storage node dono de
 * uma série, com o MESMO {@code nodeId}/porta/diretórios, e confirma que o
 * catálogo persistido converge e que a primeira escrita+checkpoint pós-restart
 * termina rápido — nunca mais os vários minutos observados antes das
 * correções F1.1–F1.3 (catálogo sem persistência + {@code WRONG_OWNER} sem
 * dono virando loop silencioso).
 */
// SEPARATE_THREAD (O4): o modo padrão do @Timeout só mede o tempo depois que o método retorna — não
// preempta uma chamada bloqueada de verdade (foi exatamente por isso que os testes deste módulo
// chegaram a travar por vários minutos apesar do @Timeout "de 120s" antes das correções do F1/F2).
@Timeout(value = 90, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NodeRestartClusterTest {

    private static final long BASE_STEP_MS = 300_000L;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration RECOVERY_BUDGET = Duration.ofSeconds(5);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void reinicioDoDonoComMesmoIdPortaEDiretoriosRecuperaRapido(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);

        harness = NgrrdClusterTestHarness.start(base, 2, builder -> { });
        harness.awaitLeader();
        harness.awaitNodeStatuses(2);

        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(3))
                .closeTimeout(Duration.ofSeconds(5)));

        Map<String, NgrrdHandle> handlesBySeriesKey = new LinkedHashMap<>();
        for (int i = 0; i < 2; i++) {
            Map<String, String> tags = Map.of("deviceId", "r" + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(AWAIT_TIMEOUT, () -> client.open(yaml, tags));
            handlesBySeriesKey.put(handle.seriesKey(), handle);
        }

        for (Map.Entry<String, NgrrdHandle> entry : handlesBySeriesKey.entrySet()) {
            long t0 = alignedBase(entry.getKey());
            entry.getValue().write("in_octets", new Sample(t0, 1_000d));
            entry.getValue().write("out_octets", new Sample(t0, 1_000d));
            entry.getValue().checkpoint();
        }

        harness.awaitPlacements(2);
        CatalogService leaderViewCatalog = harness.nodes().get(0).catalog();
        Map<String, SeriesPlacement> placements = leaderViewCatalog.placementsLocal();
        String restartedNodeId = placements.values().iterator().next().ownerNodeId();
        int restartedIndex = Integer.parseInt(restartedNodeId.substring("storage-".length()));
        int survivorIndex = 1 - restartedIndex;

        harness.nodes().get(restartedIndex).close();
        awaitTrue(restartedNodeId + " detectado como caído", AWAIT_TIMEOUT, () ->
                harness.nodes().get(survivorIndex).node().coordinator().activeMembers().size() <= 2);

        harness.restartStorageNode(restartedIndex);
        awaitTrue(restartedNodeId + " reconectado", AWAIT_TIMEOUT, () ->
                harness.nodes().get(survivorIndex).node().coordinator().activeMembers().size() == 3);

        // B1 (achado do Refuter): a causa raiz do catálogo local do nó reiniciado nunca reconvergir era
        // a ausência de Serializable em SeriesPlacement/StorageNodeStatus — todo append no WAL do
        // NMapPersistence falhava silenciosamente com NotSerializableException, então o catálogo
        // persistente (F1.3) nunca persistia de fato. Corrigido isso, a réplica local do nó reiniciado
        // DEVE reconvergir às 2 séries rapidamente; esta verificação agora é exigida, não tolerada.
        awaitTrue(restartedNodeId + " catálogo local reconvergiu às 2 séries", Duration.ofSeconds(15), () ->
                harness.nodes().get(restartedIndex).catalog().placementsLocal().size() == 2);

        // F1.1/F1.2: a primeira escrita+checkpoint em CADA série deste dono deve terminar rápido — sem
        // as correções, isso ficava preso por vários minutos (WRONG_OWNER sem dono virando loop
        // silencioso, ou o dono correto sendo rejeitado por engano por causa do catálogo vazio).
        for (Map.Entry<String, NgrrdHandle> entry : handlesBySeriesKey.entrySet()) {
            String seriesKey = entry.getKey();
            if (!restartedNodeId.equals(placements.get(seriesKey).ownerNodeId())) {
                continue;
            }
            NgrrdHandle handle = entry.getValue();
            long newSampleTs = alignedBase(seriesKey) + BASE_STEP_MS;
            long startedAt = System.currentTimeMillis();
            handle.write("in_octets", new Sample(newSampleTs, 2_000d));
            handle.write("out_octets", new Sample(newSampleTs, 2_000d));
            handle.checkpoint();
            long elapsedMs = System.currentTimeMillis() - startedAt;
            assertTrue(elapsedMs < RECOVERY_BUDGET.toMillis(),
                    "escrita+checkpoint pós-restart para " + seriesKey + " levou " + elapsedMs
                            + " ms, esperava < " + RECOVERY_BUDGET.toMillis() + " ms");
        }

        client.close();
    }

    private static long alignedBase(String seriesKey) {
        long base = 1_700_000_000_000L + Math.floorMod(seriesKey.hashCode(), 1000) * BASE_STEP_MS;
        return base - (base % BASE_STEP_MS);
    }

    private static <T> T retryUntilSuccess(Duration timeout, Supplier<T> action) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        NgrrdClusterException lastFailure = null;
        do {
            try {
                return action.get();
            } catch (NgrrdClusterException e) {
                lastFailure = e;
                Thread.sleep(200L);
            }
        } while (System.currentTimeMillis() < deadline);
        // item 9 (achado do Refuter): sempre há ao menos uma tentativa antes de checar o prazo, então
        // lastFailure nunca é nulo aqui — sem isso, um deadline já vencido no momento da chamada
        // lançava NullPointerException em vez de propagar a falha real do último "action.get()".
        throw lastFailure;
    }

    private static void awaitTrue(String description, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + timeout + "): " + description);
        }
    }
}
