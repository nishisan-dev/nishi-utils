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
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.placement.DestinationEligibility;
import dev.nishisan.utils.oss.cluster.placement.PlacementRule;
import dev.nishisan.utils.oss.cluster.placement.PlacementRules;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #167 (item 3), regras de placement fim a fim — 3 storage nodes com as mesmas regras (e um
 * {@code storage-2} com uma regra extra inócua, só para divergir no fingerprint):
 * <ul>
 *   <li>{@code tems-core}: definição {@code iface-traffic-blob} fixada em {@code {storage-1, storage-2}} —
 *       nenhuma série dessa definição cai em {@code storage-0};</li>
 *   <li>{@code no-lab-on-2}: prefixo {@code device:lab} excluído de {@code storage-2};</li>
 *   <li>{@code solo-1}: prefixo {@code device:solo} fixado só em {@code storage-1} — o drain de
 *       {@code storage-1} move o resto, mas essas ficam ({@code NGRRD_DRAIN_PENDING … rulesSkipped=3}) e o nó
 *       permanece {@code DRAINING}.</li>
 * </ul>
 * O status admin leva o fingerprint do líder; a CLI marca com {@code !} os nós cujo fingerprint difere; o
 * líder loga {@code NGRRD_PLACEMENT_RULES divergent} ao colocar séries.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class PlacementRulesClusterTest {

    private static final int STORAGE_NODE_COUNT = 3;
    private static final int CORE_SERIES = 6;
    private static final int LAB_SERIES = 6;
    private static final int SOLO_SERIES = 3;
    private static final long BASE_STEP_MS = 300_000L;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration RETRIGGER_INTERVAL = Duration.ofSeconds(5);

    private static final List<PlacementRule> COMMON_RULES = List.of(
            new PlacementRule("tems-core", "iface-traffic-blob", null, Set.of("storage-1", "storage-2"), null),
            new PlacementRule("no-lab-on-2", null, "device:lab", null, Set.of("storage-2")),
            new PlacementRule("solo-1", null, "device:solo", Set.of("storage-1"), null));
    /** Regra que não casa série alguma: só muda o fingerprint de storage-2. */
    private static final PlacementRule NOOP_RULE =
            new PlacementRule("noop", null, "zzz/", null, Set.of("storage-0"));

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void pinEExcludeRestringemOPlacementODrainFicaPendenteEADivergenciaAparece(@TempDir Path base) throws Exception {
        String coreYaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        assertTrue(coreYaml.contains("name: iface-traffic-blob"), "fixture com metadata.name esperado");
        String labYaml = coreYaml.replace("name: iface-traffic-blob", "name: iface-traffic-lab");

        List<LogRecord> divergenceWarnings = new CopyOnWriteArrayList<>();
        List<LogRecord> drainPending = new CopyOnWriteArrayList<>();
        Handler eligibilityCapture = capturing("NGRRD_PLACEMENT_RULES divergent", divergenceWarnings);
        Handler rebalancerCapture = capturing("NGRRD_DRAIN_PENDING", drainPending);
        Logger eligibilityLogger = Logger.getLogger(DestinationEligibility.class.getName());
        Logger rebalancerLogger = Logger.getLogger(Rebalancer.class.getName());
        eligibilityLogger.addHandler(eligibilityCapture);
        rebalancerLogger.addHandler(rebalancerCapture);
        try {
            harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT, (builder, index) -> {
                builder.rebalanceEnabled(false).rebalanceMinDelta(1L);
                List<PlacementRule> rules = new ArrayList<>(COMMON_RULES);
                if (index == 2) {
                    rules.add(NOOP_RULE);
                }
                builder.placementRules(PlacementRules.of(rules));
            });
            harness.awaitLeader();
            harness.awaitNodeStatuses(STORAGE_NODE_COUNT);

            NgrrdClusterClient client = harness.connectClient(builder -> builder
                    .requestTimeout(Duration.ofSeconds(5))
                    .retryTimeout(Duration.ofSeconds(30))
                    .closeTimeout(Duration.ofSeconds(20)));

            List<String> coreKeys = openSeries(client, coreYaml, "a", CORE_SERIES);
            List<String> labKeys = openSeries(client, labYaml, "lab", LAB_SERIES);
            List<String> soloKeys = openSeries(client, labYaml, "solo", SOLO_SERIES);
            labKeys.forEach(key -> assertTrue(key.startsWith("device:lab"), key));
            soloKeys.forEach(key -> assertTrue(key.startsWith("device:solo"), key));
            harness.awaitPlacements(CORE_SERIES + LAB_SERIES + SOLO_SERIES);

            // Placement respeita pin/exclude e a série "lab" (outra definição) não casa a regra tems-core.
            for (String key : coreKeys) {
                assertNotEquals("storage-0", ownerOf(key), "série da definição fixada nunca cai em storage-0: " + key);
                assertEquals("iface-traffic-blob", placementOf(key).definitionName());
            }
            for (String key : labKeys) {
                assertNotEquals("storage-2", ownerOf(key), "série lab nunca cai em storage-2: " + key);
                assertEquals("iface-traffic-lab", placementOf(key).definitionName());
            }
            for (String key : soloKeys) {
                assertEquals("storage-1", ownerOf(key), "série solo só vive em storage-1: " + key);
            }

            // Status admin: fingerprint do líder; storage-2 diverge dos outros dois.
            String leaderHash = harness.leaderNode().config().placementRules().fingerprint();
            AdminStatusResponse status = client.clusterStatus();
            assertEquals(leaderHash, status.placementRulesHash());
            assertEquals(harness.leaderNode().config().placementRules().size(), status.placementRulesCount());
            Map<String, NodeStatusView> byNode = new LinkedHashMap<>();
            status.nodes().forEach(view -> byNode.put(view.status().nodeId(), view));
            String hash0 = byNode.get("storage-0").status().placementRulesHash();
            assertEquals(PlacementRules.of(COMMON_RULES).fingerprint(), hash0);
            assertEquals(hash0, byNode.get("storage-1").status().placementRulesHash());
            assertNotEquals(hash0, byNode.get("storage-2").status().placementRulesHash());

            // CLI: "!" exatamente nos nós cujo fingerprint difere do líder.
            String cliOut = runCli("--seed", seedAddress(), "status");
            assertTrue(cliOut.contains("REGRAS: " + leaderHash + " ("), cliOut);
            for (NgrrdStorageNode node : harness.nodes()) {
                String rulesColumn = rulesColumnOf(cliOut, node.nodeId());
                boolean divergent = !leaderHash.equals(node.config().placementRules().fingerprint());
                assertEquals(divergent, rulesColumn.endsWith("!"), node.nodeId() + " RULES=" + rulesColumn + "\n" + cliOut);
                assertTrue(rulesColumn.startsWith(node.config().placementRules().fingerprint().substring(0, 8)),
                        node.nodeId() + " RULES=" + rulesColumn);
            }

            // O líder avisou a divergência ao colocar séries (uma vez por mudança do conjunto).
            assertFalse(divergenceWarnings.isEmpty(), "esperava NGRRD_PLACEMENT_RULES divergent no líder");
            assertTrue(divergenceWarnings.stream().allMatch(r -> r.getLevel() == Level.WARNING));
            assertTrue(divergenceWarnings.get(0).getMessage().startsWith("NGRRD_PLACEMENT_RULES divergent leader=" + leaderHash),
                    divergenceWarnings.get(0).getMessage());

            // Drain de storage-1: séries core/lab saem, as "solo" (fixadas só nele) ficam e o drain fica pendente.
            awaitTrueRetriggeringDrain("storage-1", () ->
                    countOwnedBy("storage-1") == SOLO_SERIES
                            && drainPending.stream().anyMatch(r -> r.getMessage().contains("rulesSkipped=" + SOLO_SERIES)),
                    client);
            for (String key : soloKeys) {
                assertEquals("storage-1", ownerOf(key), "série solo não transborda no drain: " + key);
            }
            for (String key : coreKeys) {
                assertEquals("storage-2", ownerOf(key), "série core drenada só pode ir para storage-2: " + key);
            }
            for (String key : labKeys) {
                assertEquals("storage-0", ownerOf(key), "série lab drenada só pode ir para storage-0: " + key);
            }
            assertEquals(NodeState.DRAINING, harness.nodes().get(0).catalog().nodeStatusLocal("storage-1")
                    .orElseThrow().state(), "com séries presas pela regra o nó nunca chega a DRAINED");
            assertTrue(drainPending.stream().anyMatch(r -> r.getMessage().equals(
                    "NGRRD_DRAIN_PENDING reason=no_admissible_destination_or_confirmed_geometry_or_quota_or_rules"
                            + " rulesSkipped=" + SOLO_SERIES)), drainPending.stream().map(LogRecord::getMessage).toList().toString());

            client.close();
        } finally {
            eligibilityLogger.removeHandler(eligibilityCapture);
            rebalancerLogger.removeHandler(rebalancerCapture);
        }
    }

    private List<String> openSeries(NgrrdClusterClient client, String yaml, String devicePrefix, int count)
            throws InterruptedException {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, String> tags = Map.of("deviceId", devicePrefix + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
            long t0 = alignedBase(handle.seriesKey());
            for (int sample = 0; sample < 3; sample++) {
                handle.write("in_octets", new Sample(t0 + sample * BASE_STEP_MS, 1_000d + sample));
            }
            handle.checkpoint();
            keys.add(handle.seriesKey());
        }
        return keys;
    }

    private SeriesPlacement placementOf(String seriesKey) {
        return harness.nodes().get(0).catalog().placementStrong(seriesKey)
                .orElseThrow(() -> new AssertionError("série sem placement: " + seriesKey));
    }

    private String ownerOf(String seriesKey) {
        return placementOf(seriesKey).ownerNodeId();
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

    /** Coluna RULES (antepenúltima: RULES, CAT_LAG, CAPABILITIES) da linha do nó na saída de {@code status}. */
    private static String rulesColumnOf(String output, String nodeId) {
        String line = output.lines().filter(l -> l.startsWith(nodeId + " ")).findFirst()
                .orElseThrow(() -> new AssertionError("linha de " + nodeId + " ausente em:\n" + output));
        String[] columns = line.trim().split("\\s+");
        return columns[columns.length - 3];
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

    private static Handler capturing(String prefix, List<LogRecord> sink) {
        return new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null && record.getMessage().startsWith(prefix)) {
                    sink.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
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

    /** Redispara {@code drainNode(targetNodeId)} (idempotente) a cada {@link #RETRIGGER_INTERVAL} até {@code condition}. */
    private void awaitTrueRetriggeringDrain(String targetNodeId, BooleanSupplier condition, NgrrdClusterClient client)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        long nextTrigger = 0L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
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
        fail("drain de " + targetNodeId + " não chegou à condição esperada em " + AWAIT_TIMEOUT
                + " (séries em " + targetNodeId + ": " + countOwnedBy(targetNodeId) + ")");
    }
}
