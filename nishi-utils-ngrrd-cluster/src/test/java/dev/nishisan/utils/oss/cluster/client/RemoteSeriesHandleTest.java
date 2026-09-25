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
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.ReadPresetResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link RemoteSeriesHandle} com {@link RecordingClusterRpc} fake e um
 * {@link WriteBuffer}/{@link PlacementLookup} fakes locais — sem
 * {@code WriteDispatcher} nem cluster real.
 */
class RemoteSeriesHandleTest {

    private static final String SERIES_KEY = "device:r1/iface:eth0";
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final NodeId OWNER_A = NodeId.of("storage-a");
    private static final NodeId OWNER_B = NodeId.of("storage-b");

    private RecordingClusterRpc rpc;
    private FakePlacementLookup resolver;
    private NoOpWriteBuffer dispatcher;
    private final List<String> onCloseCalls = new CopyOnWriteArrayList<>();

    private RemoteSeriesHandle newHandle() {
        return newHandle(Duration.ofSeconds(2), Ngrrd.OpenOptions.defaults());
    }

    private RemoteSeriesHandle newHandle(Duration retryTimeout) {
        return newHandle(retryTimeout, Ngrrd.OpenOptions.defaults());
    }

    private RemoteSeriesHandle newHandle(Ngrrd.OpenOptions options) {
        return newHandle(Duration.ofSeconds(2), options);
    }

    private RemoteSeriesHandle newHandle(Duration retryTimeout, Ngrrd.OpenOptions options) {
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        resolver = new FakePlacementLookup(OWNER_A.value());
        dispatcher = new NoOpWriteBuffer();
        RetryPolicy retry = new RetryPolicy(retryTimeout, Duration.ofMillis(5), Duration.ofMillis(50));
        return new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(), options,
                resolver, rpc, dispatcher, retry, Duration.ofSeconds(5), Duration.ofSeconds(5), Clock.systemUTC(),
                (key, handle) -> onCloseCalls.add(key));
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

    @ParameterizedTest
    @ValueSource(strings = {Commands.CHECKPOINT, Commands.FLUSH, Commands.READ, Commands.READ_PRESET})
    void redirectDoesNotConsumeReopenAttempt(String command) {
        RemoteSeriesHandle handle = openedHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.WRONG_OWNER, OWNER_B.value()));
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_B.value()));
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_B.value()));

        invoke(handle, command);

        assertEquals(List.of(Commands.OPEN, command, command, Commands.OPEN, command),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList());
        assertEquals(List.of(OWNER_A, OWNER_A, OWNER_B, OWNER_B, OWNER_B),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::target).toList());
    }

    @ParameterizedTest
    @ValueSource(strings = {Commands.CHECKPOINT, Commands.FLUSH, Commands.READ, Commands.READ_PRESET})
    void migrationAfterReopenAllowsAnotherRedirectAndReopen(String command) {
        RemoteSeriesHandle handle = openedHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value()));
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.WRONG_OWNER, OWNER_B.value()));
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_B.value()));
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_B.value()));

        invoke(handle, command);

        assertEquals(List.of(Commands.OPEN, command, Commands.OPEN, command, command, Commands.OPEN, command),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList());
        assertEquals(OWNER_B, rpc.calls().getLast().target());
    }

    @ParameterizedTest
    @ValueSource(strings = {Commands.CHECKPOINT, Commands.FLUSH, Commands.READ, Commands.READ_PRESET})
    void ownerUpdatedByWriterStillAllowsSecondNotOpen(String command) {
        RemoteSeriesHandle handle = openedHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value()));
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));
        rpc.respondNext((cmd, body) -> {
            // A write response may update the handle while this RPC is in flight.
            resolver.noteOwner(SERIES_KEY, OWNER_B.value());
            handle.ownerChanged(OWNER_B.value());
            return response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value());
        });
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_B.value()));

        invoke(handle, command);

        assertEquals(List.of(Commands.OPEN, command, Commands.OPEN, command, Commands.OPEN, command),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList());
        assertEquals(OWNER_B, rpc.calls().getLast().target());
    }

    private RemoteSeriesHandle openedHandle() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));
        handle.open();
        return handle;
    }

    private static Object response(String command, SeriesStatus status, String owner) {
        var result = new SeriesResult("in_bps", "rra", ConsolidationFunction.AVERAGE, 300, List.of());
        return switch (command) {
            case Commands.READ -> new ReadResponse(status, owner, result, null);
            case Commands.READ_PRESET -> new ReadPresetResponse(status, owner, Map.of("in_bps", result), null);
            default -> new SeriesStatusResponse(status, owner, null);
        };
    }

    private static void invoke(RemoteSeriesHandle handle, String command) {
        switch (command) {
            case Commands.CHECKPOINT -> handle.checkpoint();
            case Commands.FLUSH -> handle.flush();
            case Commands.READ -> assertEquals("in_bps", handle.read("in_bps",
                    new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100)).dsName());
            case Commands.READ_PRESET -> assertEquals(1, handle.read("preset").size());
            default -> throw new AssertionError(command);
        }
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

    @Test
    void openSemCriarNaoFazPlaceEEnviaFlag() {
        RemoteSeriesHandle handle = newHandle(Ngrrd.OpenOptions.defaults().withCreateIfMissing(false));
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));

        handle.open();

        assertEquals(0, resolver.resolveCalls.get(), "open sem criar nunca posiciona (ngrrd.place)");
        assertEquals(1, resolver.resolveExistingCalls.get());
        OpenRequest request = (OpenRequest) rpc.calls().get(0).body();
        assertEquals(Boolean.FALSE, request.createIfMissing());
    }

    @Test
    void openPadraoEnviaFlagNula() {
        RemoteSeriesHandle handle = newHandle();
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));

        handle.open();

        assertEquals(1, resolver.resolveCalls.get());
        assertEquals(0, resolver.resolveExistingCalls.get());
        OpenRequest request = (OpenRequest) rpc.calls().get(0).body();
        assertNull(request.createIfMissing(), "createIfMissing=true não deve viajar no request (compat)");
    }

    @Test
    void openSemCriarComNotFoundDoStorageLancaSeriesNotFound() {
        RemoteSeriesHandle handle = newHandle(Ngrrd.OpenOptions.defaults().withCreateIfMissing(false));
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null));

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class, () -> handle.open());
        assertEquals(SERIES_KEY, ex.seriesKey());
    }

    @Test
    void openSemCriarSemPlacementLancaSeriesNotFound() {
        RemoteSeriesHandle handle = newHandle(Ngrrd.OpenOptions.defaults().withCreateIfMissing(false));
        resolver.resolveExistingFailure = new SeriesNotFoundException(SERIES_KEY);

        assertThrows(SeriesNotFoundException.class, () -> handle.open());
        assertTrue(rpc.calls().isEmpty(), "sem placement, o handle nunca chega a chamar OPEN no dono");
    }

    @Test
    void escritaEmHandleSomenteLeituraLancaIllegalStateSemTocarODispatcher() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();

        IllegalStateException onWrite = assertThrows(IllegalStateException.class,
                () -> handle.write("in_octets", new Sample(1L, 1.0)));
        assertTrue(onWrite.getMessage().contains(SERIES_KEY), onWrite.getMessage());
        assertTrue(onWrite.getMessage().contains("somente leitura"), onWrite.getMessage());
        assertThrows(IllegalStateException.class, handle::flush);
        assertThrows(IllegalStateException.class, handle::checkpoint);

        assertEquals(0, dispatcher.calls.get(), "handle somente leitura nunca chama o dispatcher");
        assertEquals(List.of(Commands.OPEN), commands(), "nenhum RPC além do OPEN inicial");
        assertTrue(handle.isOpen(), "a recusa não fecha o handle");
    }

    @Test
    void readDeHandleSomenteLeituraFunciona() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));

        SeriesResult result = handle.read("in_bps", new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100));
        Map<String, SeriesResult> preset = handle.read("daily");

        assertEquals("in_bps", result.dsName());
        assertTrue(preset.containsKey("in_bps"), preset.keySet().toString());
        assertEquals(List.of(Commands.OPEN, Commands.READ, Commands.READ_PRESET), commands());
        assertEquals(0, dispatcher.calls.get());
    }

    @Test
    void readComNotOpenReabreSemCriar() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value()));
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));

        handle.read("in_bps", new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100));

        assertEquals(List.of(Commands.OPEN, Commands.READ, Commands.OPEN, Commands.READ), commands());
        assertEquals(0, resolver.resolveCalls.get(), "reabertura de handle somente leitura nunca posiciona");
        assertEquals(2, resolver.resolveExistingCalls.get());
        OpenRequest reopen = (OpenRequest) rpc.calls().get(2).body();
        assertEquals(Boolean.FALSE, reopen.createIfMissing());
    }

    @Test
    void readComNotOpenQueDescobreNotFoundFechaHandleSomenteLeitura() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value()));
        rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null));

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                () -> handle.read("in_bps", new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100)));
        assertEquals(SERIES_KEY, ex.seriesKey());
        assertEquals(List.of(SERIES_KEY), onCloseCalls, "handle deve se remover do mapa do cliente");
        assertFalse(handle.isOpen());

        assertThrows(SeriesNotFoundException.class, () -> handle.read("daily"));
        assertEquals(0, dispatcher.calls.get());
    }

    @Test
    void wrongOwnerSemDonoInformadoQueDescobreSerieAusenteFechaHandleSomenteLeitura() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.WRONG_OWNER, null));
        resolver.resolveExistingFailure = new SeriesNotFoundException(SERIES_KEY);

        assertThrows(SeriesNotFoundException.class, () -> handle.read("daily"));
        assertEquals(List.of(SERIES_KEY), onCloseCalls);
        assertFalse(handle.isOpen());
        assertEquals(0, resolver.resolveCalls.get(), "reposicionamento de handle somente leitura nunca posiciona");
    }

    @Test
    void closeDeHandleSomenteLeituraELocalSemRpcNemDispatcher() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();

        handle.close();
        handle.close();

        assertFalse(handle.isOpen());
        assertEquals(List.of(Commands.OPEN), commands(), "close de handle somente leitura não envia CLOSE");
        assertEquals(0, dispatcher.calls.get(), "close de handle somente leitura não drena o dispatcher");
        assertEquals(List.of(SERIES_KEY), onCloseCalls, "sai do mapa do cliente uma única vez");
        assertThrows(NgrrdClusterException.class, () -> handle.read("daily"));
    }

    @Test
    void reopenDeHandleSomenteLeituraNaoFazRpc() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();

        assertFalse(handle.reopen());

        assertEquals(List.of(Commands.OPEN), commands());
        assertTrue(handle.isOpen());
    }

    @Test
    void promocaoTornaHandleSomenteLeituraGravavelComCloseRemoto() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));

        assertTrue(handle.tryPromoteToWritable());
        assertTrue(handle.tryPromoteToWritable(), "promover de novo é idempotente");
        handle.write("in_octets", new Sample(1L, 1.0));
        handle.close();

        assertEquals(1, dispatcher.enqueued.size());
        assertEquals(List.of(Commands.OPEN, Commands.CLOSE), commands(), "promovido, o close envia CLOSE");
        assertTrue(dispatcher.calls.get() > 1, "promovido, o close drena o dispatcher");
    }

    @Test
    void reaberturaDeHandlePromovidoPosicionaComCriacao() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        assertTrue(handle.tryPromoteToWritable());
        rpc.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value()));
        rpc.respondDefault((cmd, body) -> response(cmd, SeriesStatus.OK, OWNER_A.value()));

        handle.checkpoint();

        assertEquals(List.of(Commands.OPEN, Commands.CHECKPOINT, Commands.OPEN, Commands.CHECKPOINT), commands());
        assertEquals(1, resolver.resolveCalls.get(), "promovido, a reabertura posiciona (cria se preciso)");
        assertNull(((OpenRequest) rpc.calls().get(2).body()).createIfMissing());
    }

    @Test
    void promocaoDeHandleFechadoFalha() {
        RemoteSeriesHandle handle = readOnlyOpenedHandle();
        handle.close();

        assertFalse(handle.tryPromoteToWritable());
        assertFalse(handle.isOpen());
    }

    @Test
    void handleComCriacaoJaEhGravavel() {
        RemoteSeriesHandle handle = openedHandle();

        assertTrue(handle.tryPromoteToWritable());
        handle.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size());
    }

    @Test
    void corridaNaRemocaoNuncaApagaUmHandleNovoDaMesmaChave() throws InterruptedException {
        // Reproduz o wiring real do cliente: onClose remove do mapa condicionalmente por instância
        // (Map#remove(key, value)), nunca por chave sozinha. O handle A fica bloqueado no meio da
        // reabertura que vai descobrir NOT_FOUND (e por isso vai chamar onClose bem mais tarde); enquanto
        // ele está preso, um handle B "abre" na mesma chave (simulando um open() concorrente do cliente).
        // Quando A finalmente terminar e chamar onClose, B precisa sobreviver.
        ConcurrentMap<String, RemoteSeriesHandle> handles = new ConcurrentHashMap<>();
        CountDownLatch openGate = new CountDownLatch(1);
        RecordingClusterRpc rpcA = new RecordingClusterRpc(NodeId.of("client-under-test"));
        FakePlacementLookup resolverA = new FakePlacementLookup(OWNER_A.value());
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(50));

        RemoteSeriesHandle handleA = new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(),
                Ngrrd.OpenOptions.defaults().withCreateIfMissing(false), resolverA, rpcA, new NoOpWriteBuffer(),
                retry, Duration.ofSeconds(5), Duration.ofSeconds(5), Clock.systemUTC(),
                (key, handle) -> handles.remove(key, handle));
        handles.put(SERIES_KEY, handleA);
        rpcA.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handleA.open();

        rpcA.respondNext((cmd, body) -> response(cmd, SeriesStatus.NOT_OPEN, OWNER_A.value()));
        rpcA.respondDefault((cmd, body) -> {
            awaitLatch(openGate);
            return new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null);
        });

        Thread reader = new Thread(() -> {
            // A SeriesNotFoundException é esperada; o que importa aqui é a remoção condicional.
            assertThrows(SeriesNotFoundException.class, () -> handleA.read("daily"));
        }, "test-read-A");
        reader.start();
        Await.untilTrue("A bloqueado dentro do RPC de reabertura", AWAIT_TIMEOUT,
                () -> reader.getState() == Thread.State.WAITING || reader.getState() == Thread.State.TIMED_WAITING);

        RemoteSeriesHandle handleB = new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(),
                Ngrrd.OpenOptions.defaults(), new FakePlacementLookup(OWNER_A.value()),
                new RecordingClusterRpc(NodeId.of("client-under-test")), new NoOpWriteBuffer(), retry,
                Duration.ofSeconds(5), Duration.ofSeconds(5), Clock.systemUTC(),
                (key, handle) -> handles.remove(key, handle));
        handles.put(SERIES_KEY, handleB);

        openGate.countDown();
        reader.join(AWAIT_TIMEOUT.toMillis());

        assertFalse(reader.isAlive(), "thread de leitura de A deveria ter terminado");
        assertFalse(handleA.isOpen());
        assertSame(handleB, handles.get(SERIES_KEY), "a remoção condicional de A não pode apagar o handle B");
    }

    private RemoteSeriesHandle readOnlyOpenedHandle() {
        RemoteSeriesHandle handle = newHandle(Ngrrd.OpenOptions.defaults().withCreateIfMissing(false));
        rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
        handle.open();
        onCloseCalls.clear();
        return handle;
    }

    private List<String> commands() {
        return rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /** {@link PlacementLookup} fake: sempre devolve o dono atual configurado, sem RPC ao líder. */
    private static final class FakePlacementLookup implements PlacementLookup {
        private volatile String owner;
        private final AtomicInteger resolveCalls = new AtomicInteger();
        private final AtomicInteger resolveExistingCalls = new AtomicInteger();
        private volatile RuntimeException resolveExistingFailure;

        FakePlacementLookup(String initialOwner) {
            this.owner = initialOwner;
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            resolveCalls.incrementAndGet();
            return SeriesPlacement.active(owner, 0L);
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            resolveExistingCalls.incrementAndGet();
            if (resolveExistingFailure != null) {
                throw resolveExistingFailure;
            }
            return SeriesPlacement.active(owner, 0L);
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            return Optional.of(SeriesPlacement.active(owner, 0L));
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

    /**
     * {@link WriteBuffer} fake: grava o que foi enfileirado e conta TODA chamada recebida, nunca envia
     * nada de fato.
     */
    private static final class NoOpWriteBuffer implements WriteBuffer {
        record Enqueued(String owner, SeriesWrite write) {
        }

        final List<Enqueued> enqueued = new CopyOnWriteArrayList<>();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void enqueue(String ownerNodeId, SeriesWrite write) {
            calls.incrementAndGet();
            enqueued.add(new Enqueued(ownerNodeId, write));
        }

        @Override
        public void flushNodeSync(String ownerNodeId) {
            calls.incrementAndGet();
        }

        @Override
        public void flushNodeSync(String ownerNodeId, Duration maxWait) {
            calls.incrementAndGet();
        }

        @Override
        public void flushSeriesSync(String seriesKey, String ownerNodeId) {
            calls.incrementAndGet();
        }

        @Override
        public void flushSeriesSync(String seriesKey, String ownerNodeId, Duration maxWait) {
            calls.incrementAndGet();
        }
    }
}
