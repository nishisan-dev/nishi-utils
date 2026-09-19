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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cobre {@link RemoteSeriesHandle} com {@link RecordingClusterRpc} fake e um
 * {@link WriteBuffer}/{@link PlacementLookup} fakes locais — sem
 * {@code WriteDispatcher} nem cluster real.
 */
class RemoteSeriesHandleTest {

    private static final String SERIES_KEY = "device:r1/iface:eth0";
    private static final NodeId OWNER_A = NodeId.of("storage-a");
    private static final NodeId OWNER_B = NodeId.of("storage-b");

    private RecordingClusterRpc rpc;
    private FakePlacementLookup resolver;
    private NoOpWriteBuffer dispatcher;

    private RemoteSeriesHandle newHandle() {
        return newHandle(Duration.ofSeconds(2));
    }

    private RemoteSeriesHandle newHandle(Duration retryTimeout) {
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        resolver = new FakePlacementLookup(OWNER_A.value());
        dispatcher = new NoOpWriteBuffer();
        RetryPolicy retry = new RetryPolicy(retryTimeout, Duration.ofMillis(5), Duration.ofMillis(50));
        return new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(), Ngrrd.OpenOptions.defaults(),
                resolver, rpc, dispatcher, retry, Duration.ofSeconds(5), Duration.ofSeconds(5), Clock.systemUTC(),
                key -> { });
    }

    @Test
    void openComWrongOwnerNaPrimeiraTentativaAbreNoSegundoDono() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, OWNER_B.value(), null));
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_B.value(), null));

        handle.open();

        assertEquals(2, rpc.calls().size());
        assertEquals(OWNER_A, rpc.calls().get(0).target());
        assertEquals(OWNER_B, rpc.calls().get(1).target());
        assertEquals(Commands.OPEN, rpc.calls().get(1).command());
    }

    @Test
    void checkpointComNotOpenReabreERepeteUmaVez() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();

        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.NOT_OPEN, null, null));
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null)); // reopen
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null)); // checkpoint

        handle.checkpoint();

        List<String> commands = rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList();
        assertEquals(List.of(Commands.OPEN, Commands.CHECKPOINT, Commands.OPEN, Commands.CHECKPOINT), commands);
    }

    @Test
    void migratingAlemDoPrazoLancaExcecaoMigrating() {
        RemoteSeriesHandle handle = newHandle(Duration.ofMillis(150));
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();

        rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.MIGRATING, OWNER_A.value(), null));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class, handle::checkpoint);
        assertEquals(ErrorCode.MIGRATING, ex.code());
    }

    @Test
    void handleFechadoLancaClosedAoEscrever() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();

        rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.close();

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> handle.write("in_octets", new Sample(1L, 1.0)));
        assertEquals(ErrorCode.CLOSED, ex.code());
    }

    @Test
    void writeEnfileiraNoDispatcherComODonoAtual() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();

        handle.write("in_octets", new Sample(1L, 42.0));

        assertEquals(1, dispatcher.enqueued.size());
        assertEquals(OWNER_A.value(), dispatcher.enqueued.get(0).owner());
        assertEquals("in_octets", dispatcher.enqueued.get(0).write().dsName());
    }

    @Test
    void openComTimeoutDeTransporteNaPrimeiraTentativaRetentaEAbreNaSegunda() {
        // B3(ii) (achado do Refuter): um TIMEOUT (ex.: o dono ainda reconectando após restart) não deve
        // subir direto como exceção ao chamador — o handle retenta com backoff.
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado");
        });
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));

        handle.open();

        assertEquals(2, rpc.calls().size());
        assertEquals(Commands.OPEN, rpc.calls().get(0).command());
        assertEquals(Commands.OPEN, rpc.calls().get(1).command());
    }

    @Test
    void checkpointComFalhaDeTransporteNoTransportRetentaEConclui() {
        // B3(ii): REMOTE_ERROR cuja causa é IOException (formato usado por TransportClusterRpc para
        // "No connection available") é o outro formato de falha de transporte reconhecido — mesma
        // retentativa do TIMEOUT.
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();

        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "falha de transporte simulada",
                    new IOException("No connection available for storage-a"));
        });
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));

        handle.checkpoint();

        List<String> commands = rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList();
        assertEquals(List.of(Commands.OPEN, Commands.CHECKPOINT, Commands.CHECKPOINT), commands);
    }

    @Test
    void peerDisconnectDuringOpenIsRetried() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "peer disconnected",
                    new dev.nishisan.utils.ngrid.cluster.transport.PeerDisconnectedException(
                            OWNER_A, java.util.UUID.randomUUID()));
        });
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();
        assertEquals(2, rpc.calls().size());
    }

    @Test
    void erroDeAplicacaoNaoEhRetentadoComoFalhaDeTransporte() {
        // Um REMOTE_ERROR de aplicação (sem IOException como causa — o próprio storage node reportou um
        // erro de verdade) não é falha de transporte: deve subir direto, sem retentativa.
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();

        rpc.respondDefault((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "erro de aplicação simulado");
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class, handle::checkpoint);
        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
        List<String> commands = rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList();
        assertEquals(List.of(Commands.OPEN, Commands.CHECKPOINT), commands, "não deveria ter retentado");
    }

    /** {@link PlacementLookup} fake: sempre devolve o dono atual configurado, sem RPC ao líder. */
    private static final class FakePlacementLookup implements PlacementLookup {
        private volatile String owner;
        private final AtomicInteger resolveCalls = new AtomicInteger();

        FakePlacementLookup(String initialOwner) {
            this.owner = initialOwner;
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            resolveCalls.incrementAndGet();
            return SeriesPlacement.active(owner, 0L);
        }

        @Override
        public void invalidate(String seriesKey) {
            // não usado diretamente pelos cenários cobertos aqui
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
            this.owner = ownerNodeId;
        }
    }

    /** {@link WriteBuffer} fake: só grava o que foi enfileirado, nunca envia nada de fato. */
    private static final class NoOpWriteBuffer implements WriteBuffer {
        record Enqueued(String owner, SeriesWrite write) {
        }

        final List<Enqueued> enqueued = new CopyOnWriteArrayList<>();

        @Override
        public void enqueue(String ownerNodeId, SeriesWrite write) {
            enqueued.add(new Enqueued(ownerNodeId, write));
        }

        @Override
        public void flushNodeSync(String ownerNodeId) {
            // no-op
        }

        @Override
        public void flushNodeSync(String ownerNodeId, Duration maxWait) {
            // no-op
        }
    }
}
