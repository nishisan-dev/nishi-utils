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

package dev.nishisan.utils.oss.cluster.admin;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.cluster.api.ClientMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.SeriesInfo;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.BlobVolumeSummary;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link NgrrdClusterAdminCli#run} sem cluster real: um {@link NgrrdClusterClient} fake substitui
 * a conexão de verdade ({@link NgrrdClusterAdminCli} nunca fala com {@code ClusterRpc} diretamente — só
 * através do cliente transparente, que é o que o seam de teste injeta).
 */
class NgrrdClusterAdminCliTest {

    private final NgrrdClusterAdminCli cli = new NgrrdClusterAdminCli();

    @Test
    void semSeedFalhaComCodigoUmESemConectar() {
        AtomicInteger connectCalls = new AtomicInteger();
        Capture capture = run(new String[] {"status"}, cfg -> {
            connectCalls.incrementAndGet();
            return new ClientFake();
        });

        assertEquals(1, capture.exitCode);
        assertTrue(capture.err.contains("--seed"), capture.err);
        assertEquals(0, connectCalls.get(), "não deveria tentar conectar sem --seed");
    }

    @Test
    void semComandoFalhaComCodigoUm() {
        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000"}, cfg -> new ClientFake());

        assertEquals(1, capture.exitCode);
        assertTrue(capture.err.contains("comando"), capture.err);
    }

    @Test
    void metricsSemNodeIdFalhaComCodigoUm() {
        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "metrics"}, cfg -> new ClientFake());

        assertEquals(1, capture.exitCode);
        assertTrue(capture.err.contains("nodeId"), capture.err);
    }

    @Test
    void statusImprimeLiderNosEMigracoes() {
        ClientFake client = new ClientFake();
        client.statusResponse = new AdminStatusResponse(
                dev.nishisan.utils.oss.cluster.protocol.SeriesStatus.OK, "storage-0",
                List.of(
                        new NodeStatusView(new StorageNodeStatus("storage-0", NodeState.ACTIVE, 5, 1_000, 10_000, 1L), true),
                        new NodeStatusView(new StorageNodeStatus("storage-1", NodeState.DRAINING, 2, 500, 10_000, 1L), false)),
                0, Map.of("storage-0", 5L, "storage-1", 2L));

        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "status"}, cfg -> client);

        assertEquals(0, capture.exitCode);
        assertTrue(capture.out.contains("storage-0"), capture.out);
        assertTrue(capture.out.contains("storage-1"), capture.out);
        assertTrue(capture.out.contains("ACTIVE"), capture.out);
        assertTrue(capture.out.contains("DRAINING"), capture.out);
        assertTrue(capture.out.contains("MIGRACOES"), capture.out);
        assertTrue(client.closed.get(), "o cliente deveria ser fechado ao final");
    }

    @Test
    void metricsImprimeSnapshotDoNoPedido() {
        ClientFake client = new ClientFake();
        client.metricsResponse = fixedSnapshot("storage-7");

        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "metrics", "storage-7"}, cfg -> client);

        assertEquals(0, capture.exitCode);
        assertEquals("storage-7", client.metricsRequestedNodeId);
        assertTrue(capture.out.contains("storage-7"), capture.out);
        assertTrue(capture.out.contains("SERIES: 5"), capture.out);
    }

    @Test
    void drainImprimeNodeIdEEstadoResultante() {
        ClientFake client = new ClientFake();
        client.drainResponse = new StorageNodeStatus("storage-2", NodeState.DRAINING, 3, 100, 1_000, 1L);

        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "drain", "storage-2"}, cfg -> client);

        assertEquals(0, capture.exitCode);
        assertEquals("storage-2", client.drainRequestedNodeId);
        assertTrue(capture.out.contains("drain OK"), capture.out);
        assertTrue(capture.out.contains("storage-2"), capture.out);
        assertTrue(capture.out.contains("DRAINING"), capture.out);
    }

    @Test
    void activateImprimeNodeIdEEstadoResultante() {
        ClientFake client = new ClientFake();
        client.activateResponse = new StorageNodeStatus("storage-2", NodeState.ACTIVE, 3, 100, 1_000, 1L);

        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "activate", "storage-2"}, cfg -> client);

        assertEquals(0, capture.exitCode);
        assertEquals("storage-2", client.activateRequestedNodeId);
        assertTrue(capture.out.contains("activate OK"), capture.out);
        assertTrue(capture.out.contains("ACTIVE"), capture.out);
    }

    @Test
    void rebalanceDisparaEImprimeConfirmacao() {
        ClientFake client = new ClientFake();

        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "rebalance"}, cfg -> client);

        assertEquals(0, capture.exitCode);
        assertTrue(client.rebalanceCalled.get());
        assertTrue(capture.out.toLowerCase().contains("rebalance"), capture.out);
    }

    @Test
    void comandoDesconhecidoFalhaComCodigoUm() {
        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "chute"}, cfg -> new ClientFake());

        assertEquals(1, capture.exitCode);
        assertFalse(capture.err.isBlank());
    }

    @Test
    void erroRemotoDoClienteVirraCodigoUmComMensagem() {
        ClientFake client = new ClientFake();
        client.drainFailure = new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "nó desconhecido pelo catálogo: storage-x");

        Capture capture = run(new String[] {"--seed", "127.0.0.1:9000", "drain", "storage-x"}, cfg -> client);

        assertEquals(1, capture.exitCode);
        assertTrue(capture.err.contains("storage-x"), capture.err);
        assertTrue(client.closed.get(), "o cliente deveria ser fechado mesmo em erro");
    }

    private static NodeMetricsSnapshot fixedSnapshot(String nodeId) {
        return new NodeMetricsSnapshot(nodeId, 1_000L, true, 5L, 100L, 1_000L, 2, 3L, 30L, 0L, 1L, 0L, 4L,
                LatencySnapshot.EMPTY, LatencySnapshot.EMPTY, LatencySnapshot.EMPTY, Map.of(),
                new BlobVolumeSummary(1, 100L, 1_000L, 0.1, 5, 0L), 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    private Capture run(String[] args, Function<NgrrdClusterConfig, NgrrdClusterClient> factory) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)) {
            exitCode = cli.run(args, out, err, factory);
        }
        return new Capture(exitCode, outBytes.toString(StandardCharsets.UTF_8), errBytes.toString(StandardCharsets.UTF_8));
    }

    private record Capture(int exitCode, String out, String err) {
    }

    /** {@link NgrrdClusterClient} fake: cada método devolve/lança o que o teste configurou. */
    private static final class ClientFake implements NgrrdClusterClient {
        AdminStatusResponse statusResponse;
        NodeMetricsSnapshot metricsResponse;
        StorageNodeStatus drainResponse;
        StorageNodeStatus activateResponse;
        RuntimeException drainFailure;
        String metricsRequestedNodeId;
        String drainRequestedNodeId;
        String activateRequestedNodeId;
        final AtomicBoolean rebalanceCalled = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public NgrrdHandle open(String yaml, Map<String, String> tags) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NgrrdHandle open(String yaml, Map<String, String> tags, Ngrrd.OpenOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NgrrdHandle open(Path yamlFile, Map<String, String> tags) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(String seriesKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Boolean> exists(Collection<String> seriesKeys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SeriesInfo> find(String seriesKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flushAll() {
        }

        @Override
        public ClientMetricsSnapshot metrics() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AdminStatusResponse clusterStatus() {
            return statusResponse;
        }

        @Override
        public NodeMetricsSnapshot nodeMetrics(String nodeId) {
            metricsRequestedNodeId = nodeId;
            return metricsResponse;
        }

        @Override
        public void rebalanceNow() {
            rebalanceCalled.set(true);
        }

        @Override
        public StorageNodeStatus drainNode(String nodeId) {
            drainRequestedNodeId = nodeId;
            if (drainFailure != null) {
                throw drainFailure;
            }
            return drainResponse;
        }

        @Override
        public StorageNodeStatus activateNode(String nodeId) {
            activateRequestedNodeId = nodeId;
            return activateResponse;
        }

        @Override
        public NodeId clientNodeId() {
            return NodeId.of("cli-fake");
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
