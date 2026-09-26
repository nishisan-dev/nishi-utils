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
import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.node.NodeStatusReporter;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * M2: {@code ngrrd.admin.status}/{@code ngrrd.admin.metrics} fim a fim — 2 storage nodes + 1
 * cliente, 20 séries escritas e com checkpoint. Cobre: {@code clusterStatus()} listando os 2 nós
 * alcançáveis com a contagem de séries correta, {@code nodeMetrics(ownerId)} refletindo operações
 * reais (amostras, latência de checkpoint, handles abertos), a marcação {@code reachable=false} de
 * um nó derrubado dentro de 10 s, e o log marker {@code NGRRD_NODE_STATUS}. Desde a issue #177, também
 * o estado da réplica local do catálogo publicado por cada nó ({@link CatalogReplicaStatus}).
 */
// SEPARATE_THREAD (mesmo motivo de DistributedWriteReadClusterTest): o modo padrão do @Timeout só
// mede o tempo depois que o método retorna — não preempta uma chamada bloqueada de verdade.
@Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AdminStatusClusterTest {

    private static final int SERIES_COUNT = 20;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(60);
    // Refuter r2 (MÉDIO): 10s era curto demais para a detecção de queda convergir numa malha de 4
    // (3 storage + cliente) com roteamento por proxy — medições reais levaram mais que isso.
    private static final Duration REACHABILITY_TIMEOUT = Duration.ofSeconds(30);

    /** Mínimo para tolerar a queda de um nó sem perder o líder (maioria de votantes). */
    private static final int STORAGE_NODE_COUNT = 3;
    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void statusEMetricasDoClusterRefletemEscritasENoDerrubado(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);

        // Captura NGRRD_NODE_STATUS antes de subir os nós — o primeiro tick de cada
        // NodeStatusReporter dispara imediatamente no start().
        LogCapture logCapture = new LogCapture();
        Logger statusLogger = Logger.getLogger(NodeStatusReporter.class.getName());
        // B2 (achado do Refuter): Logger é um singleton por nome no JUL — sem restaurar o nível
        // original no finally, este teste alteraria permanentemente (para o resto da JVM) o nível do
        // logger de NodeStatusReporter, mudando o comportamento de log de qualquer teste seguinte que
        // rode no mesmo processo.
        Level originalLevel = statusLogger.getLevel();
        statusLogger.addHandler(logCapture);
        statusLogger.setLevel(Level.ALL);

        try {
            // 3 storage nodes: tolerar a queda de UM nó exige maioria de votantes (ver Javadoc de
            // StorageNodeConfig) — com 2, derrubar um deixa o cluster sem líder e os comandos de líder
            // (clusterStatus/nodeMetrics) param, o que não é o que este teste verifica.
            harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT, builder -> { });
            harness.awaitLeader();
            harness.awaitNodeStatuses(STORAGE_NODE_COUNT);

            NgrrdClusterClient client = harness.connectClient(builder -> builder
                    // curtos de propósito (cluster local): depois de derrubar um nó, tanto o
                    // clusterStatus()/nodeMetrics() do teste quanto o client.close() final não podem
                    // ficar presos nele. requestTimeout governa CADA tentativa de RPC — uma conexão TCP
                    // que fica aberta com o processo morto (sem RST/FIN imediato) só falha depois desse
                    // prazo; retryTimeout é o teto de quantas tentativas cabem depois disso
                    // (callWithTransportRetry só reavalia "exhausted" DEPOIS de uma tentativa completa,
                    // então o prazo real por handle é ~requestTimeout, não retryTimeout, quando
                    // retryTimeout < requestTimeout — daí os dois precisam ser pequenos, não só um).
                    .requestTimeout(Duration.ofSeconds(2))
                    .retryTimeout(Duration.ofSeconds(2))
                    .closeTimeout(Duration.ofSeconds(15)));

            Map<String, NgrrdHandle> handlesBySeriesKey = new LinkedHashMap<>();
            for (int i = 0; i < SERIES_COUNT; i++) {
                Map<String, String> tags = Map.of("deviceId", "r" + i, "interfaceId", "eth0",
                        "region", "br-sp", "vendor", "x", "role", "core");
                NgrrdHandle handle = retryUntilSuccess(AWAIT_TIMEOUT, () -> client.open(yaml, tags));
                handlesBySeriesKey.put(handle.seriesKey(), handle);
            }
            assertEquals(SERIES_COUNT, handlesBySeriesKey.size(), "seriesKey deveria ser único por conjunto de tags");

            long ts = 1_700_000_000_000L;
            for (NgrrdHandle handle : handlesBySeriesKey.values()) {
                handle.write("in_octets", new Sample(ts, 1_000d));
                handle.write("out_octets", new Sample(ts, 1_000d));
            }
            for (NgrrdHandle handle : handlesBySeriesKey.values()) {
                handle.checkpoint();
            }
            harness.awaitPlacements(SERIES_COUNT);

            // B2 (achado do Refuter): harness.awaitPlacements acima só confirma a réplica LOCAL do
            // catálogo no nó 0 (ver Javadoc de NgrrdClusterTestHarness#awaitPlacements) — clusterStatus()
            // usa a réplica local do LÍDER (que pode ser outro nó), lida com Consistency.EVENTUAL. Uma
            // asserção única logo em seguida corria o risco de pegar o líder um instante antes da
            // réplica dele convergir para as 20 séries recém-colocadas; poll com prazo em vez disso.
            AtomicReference<AdminStatusResponse> statusRef = new AtomicReference<>();
            awaitTrue("clusterStatus() reflete as " + SERIES_COUNT + " séries no líder", AWAIT_TIMEOUT, () -> {
                try {
                    AdminStatusResponse response = client.clusterStatus();
                    if (response.status() != SeriesStatus.OK) {
                        return false;
                    }
                    long total = response.seriesCountByNode().values().stream().mapToLong(Long::longValue).sum();
                    if (total != SERIES_COUNT) {
                        return false;
                    }
                    statusRef.set(response);
                    return true;
                } catch (NgrrdClusterException e) {
                    return false;
                }
            });
            AdminStatusResponse status = statusRef.get();
            assertEquals(STORAGE_NODE_COUNT, status.nodes().size());
            for (NodeStatusView view : status.nodes()) {
                assertTrue(view.reachable(), view.status().nodeId() + " deveria estar alcançável antes de derrubar nó algum");
            }

            // Issue #177: todo nó publica o estado da réplica local do catálogo; só o líder se declara líder e
            // os seguidores já conhecem o high-watermark dele (houve escrita no catálogo: os 20 placements). O
            // status de boot de um seguidor, publicado antes dessa escrita, ainda não conhece o HWM — por isso
            // o poll até o próximo relatório.
            AtomicReference<AdminStatusResponse> replicaStatusRef = new AtomicReference<>();
            awaitTrue("clusterStatus() reporta a réplica do catálogo de todos os nós", AWAIT_TIMEOUT, () -> {
                try {
                    AdminStatusResponse response = client.clusterStatus();
                    replicaStatusRef.set(response);
                    return response.status() == SeriesStatus.OK && catalogReplicasReported(response);
                } catch (NgrrdClusterException e) {
                    return false;
                }
            }, () -> "último status: " + describeCatalogReplicas(replicaStatusRef.get()));
            AdminStatusResponse replicaStatus = replicaStatusRef.get();
            assertEquals(STORAGE_NODE_COUNT, replicaStatus.nodes().size(), describeCatalogReplicas(replicaStatus));
            List<String> leadersByReplica = replicaStatus.nodes().stream()
                    .filter(view -> view.status().catalogReplica().leader())
                    .map(view -> view.status().nodeId())
                    .toList();
            assertEquals(1, leadersByReplica.size(), describeCatalogReplicas(replicaStatus));
            for (NodeStatusView view : replicaStatus.nodes()) {
                CatalogReplicaStatus replica = view.status().catalogReplica();
                assertNotNull(replica, view.status().nodeId() + " sem catalogReplica");
                assertTrue(replica.lagKnown(),
                        view.status().nodeId() + " deveria conhecer o HWM do líder: " + replica);
            }

            // nodeMetrics(ownerId): dono real (o que recebeu mais séries) reflete operações de verdade.
            String ownerId = status.seriesCountByNode().entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElseThrow(() -> new AssertionError("nenhum nó possui série alguma"));
            NodeMetricsSnapshot ownerMetrics = client.nodeMetrics(ownerId);
            assertEquals(ownerId, ownerMetrics.nodeId());
            assertTrue(ownerMetrics.samplesWritten() > 0, "samplesWritten deveria refletir as escritas reais");
            assertTrue(ownerMetrics.checkpointLatency().count() > 0, "checkpointLatency deveria refletir os checkpoints reais");
            assertTrue(ownerMetrics.openHandles() > 0, "openHandles deveria refletir os handles abertos pelas escritas");

            // ADMIN_METRICS encaminhado: consulta ao nó QUE NÃO é o dono ainda funciona (um salto).
            String otherNodeId = harness.nodes().stream()
                    .map(NgrrdStorageNode::nodeId)
                    .filter(id -> !id.equals(ownerId))
                    .findFirst()
                    .orElseThrow();
            NodeMetricsSnapshot otherMetrics = client.nodeMetrics(otherNodeId);
            assertEquals(otherNodeId, otherMetrics.nodeId());

            // Derruba um storage node que NÃO é o líder atual (evita a complicação de re-resolver o
            // líder em pleno handoff — fora do escopo deste teste) e confirma reachable=false em até 10s.
            NgrrdStorageNode leader = harness.leaderNode();
            NgrrdStorageNode victim = harness.nodes().stream()
                    .filter(node -> !node.nodeId().equals(leader.nodeId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("cluster de " + STORAGE_NODE_COUNT
                            + " nós deveria ter um não-líder"));
            String victimId = victim.nodeId();
            victim.close();

            awaitTrue(victimId + " marcado reachable=false pelo clusterStatus()", REACHABILITY_TIMEOUT, () -> {
                AdminStatusResponse afterFailure = client.clusterStatus();
                return afterFailure.status() == SeriesStatus.OK && afterFailure.nodes().stream()
                        .filter(view -> view.status().nodeId().equals(victimId))
                        .anyMatch(view -> !view.reachable());
            });

            assertTrue(logCapture.containsMarker(), "log de algum storage node deveria conter NGRRD_NODE_STATUS");

            client.close();
        } finally {
            statusLogger.removeHandler(logCapture);
            statusLogger.setLevel(originalLevel);
        }
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
        throw lastFailure;
    }

    private static void awaitTrue(String description, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        awaitTrue(description, timeout, condition, () -> "");
    }

    private static void awaitTrue(String description, Duration timeout, BooleanSupplier condition,
            Supplier<String> diagnostics) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + timeout + "): " + description + " " + diagnostics.get());
        }
    }

    /**
     * Se todo nó do status traz {@code catalogReplica}, exatamente um se declara líder e todo seguidor já
     * conhece o high-watermark do líder ({@link CatalogReplicaStatus#lagKnown()}).
     */
    private static boolean catalogReplicasReported(AdminStatusResponse response) {
        if (response.nodes().size() != STORAGE_NODE_COUNT) {
            return false;
        }
        long leaders = 0;
        for (NodeStatusView view : response.nodes()) {
            CatalogReplicaStatus replica = view.status().catalogReplica();
            if (replica == null || !replica.lagKnown()) {
                return false;
            }
            if (replica.leader()) {
                leaders++;
            }
        }
        return leaders == 1;
    }

    private static String describeCatalogReplicas(AdminStatusResponse response) {
        if (response == null) {
            return "<nenhum>";
        }
        StringBuilder sb = new StringBuilder();
        for (NodeStatusView view : response.nodes()) {
            sb.append(view.status().nodeId()).append('=').append(view.status().catalogReplica()).append(' ');
        }
        return sb.toString();
    }

    /** {@link Handler} de JUL que só guarda as mensagens, para verificar o marcador {@code NGRRD_NODE_STATUS}. */
    private static final class LogCapture extends Handler {
        private final List<LogRecord> records = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        boolean containsMarker() {
            return records.stream().anyMatch(r -> String.valueOf(r.getMessage()).contains("NGRRD_NODE_STATUS"));
        }
    }
}
