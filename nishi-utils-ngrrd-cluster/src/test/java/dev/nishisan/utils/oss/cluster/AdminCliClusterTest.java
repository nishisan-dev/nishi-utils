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
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Seção 5 da spec do M4: {@link NgrrdClusterAdminCli#run} contra um cluster real de 3 storage nodes —
 * {@code status} e {@code drain} entram na malha como um cliente comum e a saída reflete o estado
 * verdadeiro do cluster.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AdminCliClusterTest {

    private static final int STORAGE_NODE_COUNT = 3;
    private static final int SERIES_COUNT = 9;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(90);
    private static final String ADMIN_ID_PREFIX = "ngrrd-cluster-admin-leave-";
    private static final int ADMIN_RUNS = 3;
    private static final String NGRID_LOGGER_NAME = "dev.nishisan.utils.ngrid";
    /** Um LEAVE de membro efêmero é esquecido na hora; o gatilho lento (≥ 1 min) não pode ser o caminho. */
    private static final Duration ADMIN_FORGET_TIMEOUT = Duration.ofSeconds(10);
    /** Cobre vários ciclos de reconexão, heartbeat e gossip entre os storages. */
    private static final Duration ADMIN_QUIET_WINDOW = Duration.ofSeconds(8);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void statusEDrainViaCliRefletemOEstadoRealDoCluster(@TempDir Path base) throws Exception {
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
        for (int i = 0; i < SERIES_COUNT; i++) {
            Map<String, String> tags = Map.of("deviceId", "cli" + i, "interfaceId", "eth0",
                    "region", "br-sp", "vendor", "x", "role", "core");
            NgrrdHandle handle = retryUntilSuccess(() -> client.open(yaml, tags));
            handlesBySeriesKey.put(handle.seriesKey(), handle);
            handle.write("in_octets", new Sample(1_700_000_000_000L, 1_000d));
            handle.write("out_octets", new Sample(1_700_000_000_000L, 500d));
        }
        for (NgrrdHandle handle : handlesBySeriesKey.values()) {
            handle.checkpoint();
        }
        harness.awaitPlacements(SERIES_COUNT);

        String seed = seedAddress();
        NgrrdClusterAdminCli cli = new NgrrdClusterAdminCli();

        // status: a saída deve conter os 3 nós, com contagem de séries refletindo o catálogo real.
        Capture statusCapture = run(cli, "--seed", seed, "status");
        assertEquals(0, statusCapture.exitCode, statusCapture.err);
        for (NgrrdStorageNode node : harness.nodes()) {
            assertTrue(statusCapture.out.contains(node.nodeId()), "saída do status deveria conter " + node.nodeId()
                    + ": " + statusCapture.out);
        }
        assertTrue(statusCapture.out.contains("ACTIVE"), statusCapture.out);
        assertTrue(statusCapture.out.contains("MIGRACOES"), statusCapture.out);

        // drain: nó alvo passa a DRAINING (ou já DRAINED, se o ciclo disparado foi rápido o bastante).
        String targetNodeId = harness.nodes().get(1).nodeId();
        Capture drainCapture = run(cli, "--seed", seed, "drain", targetNodeId);
        assertEquals(0, drainCapture.exitCode, drainCapture.err);
        assertTrue(drainCapture.out.contains("drain OK"), drainCapture.out);
        assertTrue(drainCapture.out.contains(targetNodeId), drainCapture.out);

        awaitTrue(targetNodeId + " deveria estar DRAINING ou DRAINED após o drain via CLI", () ->
                harness.nodes().get(0).catalog().nodeStatusLocal(targetNodeId)
                        .map(status -> status.state() == NodeState.DRAINING || status.state() == NodeState.DRAINED)
                        .orElse(false));

        client.close();
    }

    /**
     * O CLI administrativo entra na malha como membro efêmero (cliente, inelegível a líder) e sai no fim
     * de cada comando. Os storages não podem guardá-lo como peer nem seguir discando para ele: antes do
     * LEAVE, cada execução do CLI deixava um id {@code ngrrd-cluster-admin-*} conhecido para sempre,
     * com "No connection available" a cada heartbeat.
     */
    @Test
    void cliAdministrativoNaoFicaComoMembroDepoisDeSair(@TempDir Path base) throws Exception {
        harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT, builder -> builder.rebalanceEnabled(false));
        harness.awaitLeader();
        harness.awaitNodeStatuses(STORAGE_NODE_COUNT);
        harness.awaitMeshStable();

        Logger ngridLogger = Logger.getLogger(NGRID_LOGGER_NAME);
        WarningCapture warnings = new WarningCapture(ADMIN_ID_PREFIX);
        ngridLogger.addHandler(warnings);
        try {
            String seed = seedAddress();
            NgrrdClusterAdminCli cli = new NgrrdClusterAdminCli();
            List<String> adminIds = new ArrayList<>();
            for (int i = 0; i < ADMIN_RUNS; i++) {
                String adminId = ADMIN_ID_PREFIX + i;
                adminIds.add(adminId);
                Capture capture = run(cli, "--seed", seed, "--client-id", adminId, "status");
                assertEquals(0, capture.exitCode, capture.err);
            }

            long forgetDeadline = System.currentTimeMillis() + ADMIN_FORGET_TIMEOUT.toMillis();
            while (!describeAdminLeftovers(adminIds).isEmpty() && System.currentTimeMillis() < forgetDeadline) {
                Thread.sleep(150L);
            }
            assertEquals("", describeAdminLeftovers(adminIds),
                    "storages ainda conhecem o CLI administrativo " + ADMIN_FORGET_TIMEOUT + " depois que ele saiu");

            // Depois da carência, nenhum storage volta a aprender (gossip) nem a discar os ids que saíram.
            warnings.clear();
            Thread.sleep(ADMIN_QUIET_WINDOW.toMillis());
            assertEquals("", describeAdminLeftovers(adminIds), "CLI administrativo voltou a ser conhecido");
            assertTrue(warnings.messages().isEmpty(),
                    "storages seguem tentando falar com o CLI que saiu: " + warnings.messages());
        } finally {
            ngridLogger.removeHandler(warnings);
        }
    }

    /** Ids administrativos ainda vistos como peer do transporte ou membro ativo, por storage (vazio = nenhum). */
    private String describeAdminLeftovers(List<String> adminIds) {
        StringBuilder leftovers = new StringBuilder();
        for (NgrrdStorageNode node : harness.nodes()) {
            List<String> seen = new ArrayList<>();
            node.node().transport().peers().stream()
                    .map(peer -> peer.nodeId().value())
                    .filter(adminIds::contains)
                    .forEach(id -> seen.add("peer:" + id));
            node.node().coordinator().activeMembers().stream()
                    .map(member -> member.nodeId().value())
                    .filter(adminIds::contains)
                    .forEach(id -> seen.add("member:" + id));
            if (!seen.isEmpty()) {
                leftovers.append(node.nodeId()).append(seen).append(' ');
            }
        }
        return leftovers.toString().trim();
    }

    /** Guarda as mensagens (formatadas) de WARNING ou acima que citam o CLI administrativo. */
    private static final class WarningCapture extends Handler {
        private final String marker;
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final SimpleFormatter formatter = new SimpleFormatter();

        private WarningCapture(String marker) {
            this.marker = marker;
            setLevel(Level.WARNING);
        }

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() < Level.WARNING.intValue()) {
                return;
            }
            String message = formatter.formatMessage(record);
            if (message != null && message.contains(marker)) {
                messages.add(message);
            }
        }

        void clear() {
            messages.clear();
        }

        List<String> messages() {
            return List.copyOf(messages);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    private String seedAddress() {
        NgrrdStorageNode first = harness.nodes().get(0);
        return first.config().host() + ":" + first.config().port();
    }

    private static Capture run(NgrrdClusterAdminCli cli, String... args) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)) {
            exitCode = cli.run(args, out, err);
        }
        return new Capture(exitCode, outBytes.toString(StandardCharsets.UTF_8), errBytes.toString(StandardCharsets.UTF_8));
    }

    private record Capture(int exitCode, String out, String err) {
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

    private void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
        }
    }
}
