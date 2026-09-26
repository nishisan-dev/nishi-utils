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
import dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.RebalanceTrigger;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #167 (item 3), cota dura por nó fim a fim — 3 storage nodes, {@code storage-1} com
 * {@code ngrrd.quota.maxSeries=2}. Cobre: o placement de 12 séries deixa {@code storage-1} com 2 (a cota é
 * um limite duro; um placement que escape pela janela entre o {@code PLACE} e o próximo status é corrigido
 * pelo rebalance, que trata a fonte acima da cota como quem sempre cede); o drain de {@code storage-2} não
 * empurra nada para {@code storage-1} (tudo vai para {@code storage-0}); {@code clusterStatus()} e a CLI
 * mostram a cota; e {@code triggerRebalance()} lista {@code storage-1(quota_series…)} entre os destinos
 * excluídos.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class QuotaClusterTest {

    private static final int STORAGE_NODE_COUNT = 3;
    private static final int SERIES_COUNT = 12;
    private static final long QUOTA_MAX_SERIES = 2L;
    private static final long BASE_STEP_MS = 300_000L;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration RETRIGGER_INTERVAL = Duration.ofSeconds(5);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void cotaDeSeriesLimitaPlacementDrainERebalanceEApareceNoStatus(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);

        harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT, (builder, index) -> {
            builder.rebalanceEnabled(false).rebalanceMinDelta(1L);
            if (index == 1) {
                builder.quotaMaxSeries(QUOTA_MAX_SERIES);
            }
        });
        harness.awaitLeader();
        harness.awaitNodeStatuses(STORAGE_NODE_COUNT);
        String quotaNodeId = harness.nodes().get(1).nodeId();
        String drainNodeId = harness.nodes().get(2).nodeId();
        String freeNodeId = harness.nodes().get(0).nodeId();

        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(20)));

        Map<String, NgrrdHandle> handlesBySeriesKey = new LinkedHashMap<>();
        for (int i = 0; i < SERIES_COUNT; i++) {
            Map<String, String> tags = Map.of("deviceId", "q" + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
            handlesBySeriesKey.put(handle.seriesKey(), handle);
            long t0 = alignedBase(handle.seriesKey());
            for (int sample = 0; sample < 3; sample++) {
                handle.write("in_octets", new Sample(t0 + sample * BASE_STEP_MS, 1_000d + sample));
            }
            handle.checkpoint();
        }
        assertEquals(SERIES_COUNT, handlesBySeriesKey.size());
        harness.awaitPlacements(SERIES_COUNT);

        // Placement: storage-1 fica na cota. O rebalance é redisparado só para cobrir a janela em que um
        // status novo zera as pendências do líder antes de refletir os placements recém-feitos.
        long placedAtQuotaNode = countOwnedBy(quotaNodeId);
        assertTrue(placedAtQuotaNode <= QUOTA_MAX_SERIES + 1,
                "o placement nunca deveria passar da cota por mais que a janela de um status: " + placedAtQuotaNode);
        awaitTrueRetriggeringRebalance(() -> countOwnedBy(quotaNodeId) == QUOTA_MAX_SERIES, client);
        assertEquals(SERIES_COUNT - QUOTA_MAX_SERIES, countOwnedBy(freeNodeId) + countOwnedBy(drainNodeId));

        // Status admin e CLI mostram a cota do nó.
        AdminStatusResponse status = client.clusterStatus();
        Map<String, NodeStatusView> byNode = new LinkedHashMap<>();
        status.nodes().forEach(view -> byNode.put(view.status().nodeId(), view));
        assertEquals(QUOTA_MAX_SERIES, byNode.get(quotaNodeId).status().quotaMaxSeries());
        assertEquals(0L, byNode.get(quotaNodeId).status().quotaMaxBytes());
        assertEquals(0L, byNode.get(freeNodeId).status().quotaMaxSeries());
        String cliOut = runCli("--seed", seedAddress(), "status");
        String quotaLine = cliOut.lines().filter(line -> line.startsWith(quotaNodeId + " ")).findFirst()
                .orElseThrow(() -> new AssertionError("linha de " + quotaNodeId + " ausente em:\n" + cliOut));
        assertTrue(quotaLine.contains(" " + QUOTA_MAX_SERIES + "/- "), "coluna QUOTA de " + quotaNodeId + ": " + quotaLine);

        // triggerRebalance lista o nó na cota como destino excluído (um ciclo anterior ainda em curso
        // devolve 0/0 sem exclusões — retenta).
        awaitTrue("triggerRebalance() lista " + quotaNodeId + "(quota_series…)", () -> {
            try {
                RebalanceTrigger trigger = client.triggerRebalance();
                String reason = trigger.excludedDestinations().get(quotaNodeId);
                return reason != null && reason.startsWith("quota_series(");
            } catch (NgrrdClusterException e) {
                return false;
            }
        });

        // Drain de storage-2: nada vai para storage-1 (na cota); storage-0 absorve tudo.
        awaitTrueRetriggeringDrain(drainNodeId, client);
        assertEquals(0, countOwnedBy(drainNodeId));
        assertEquals(QUOTA_MAX_SERIES, countOwnedBy(quotaNodeId), "o nó na cota não recebe séries do drain");
        assertEquals(SERIES_COUNT - QUOTA_MAX_SERIES, countOwnedBy(freeNodeId));
        assertEquals(NodeState.DRAINED, harness.nodes().get(0).catalog().nodeStatusLocal(drainNodeId)
                .orElseThrow().state());

        client.close();
    }

    private long countOwnedBy(String nodeId) {
        return harness.nodes().get(0).catalog().placementsLocal().values().stream()
                .filter(p -> p.ownerNodeId().equals(nodeId))
                .count();
    }

    private String seedAddress() {
        NgrrdStorageNode first = harness.nodes().get(0);
        return first.config().host() + ":" + first.config().port();
    }

    private static String runCli(String... args) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)) {
            exitCode = new NgrrdClusterAdminCli().run(args, out, err);
        }
        assertEquals(0, exitCode, errBytes.toString(StandardCharsets.UTF_8));
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private static long alignedBase(String seriesKey) {
        long base = 1_700_000_000_000L + Math.floorMod(seriesKey.hashCode(), 1_000) * BASE_STEP_MS;
        return base - (base % BASE_STEP_MS);
    }

    private static <T> T retryUntilSuccess(Supplier<T> action) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
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
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        long nextTrigger = 0L;
        while (System.currentTimeMillis() < deadline) {
            boolean drained = countOwnedBy(targetNodeId) == 0
                    && harness.nodes().get(0).catalog().nodeStatusLocal(targetNodeId)
                            .map(s -> s.state() == NodeState.DRAINED).orElse(false);
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
        fail("nó " + targetNodeId + " não chegou a DRAINED com 0 séries em " + AWAIT_TIMEOUT);
    }

    /** Redispara {@code rebalanceNow()} a cada {@link #RETRIGGER_INTERVAL} até {@code condition} valer. */
    private void awaitTrueRetriggeringRebalance(BooleanSupplier condition, NgrrdClusterClient client)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
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
            fail("Condição de rebalanceamento não satisfeita em " + AWAIT_TIMEOUT);
        }
    }

    private void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(250L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
        }
    }
}
