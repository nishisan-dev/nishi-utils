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

package dev.nishisan.utils.oss.cluster.rpc;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link TransportClusterRpc} com um {@link Transport} fake (interface,
 * sem precisar de socket real): sucesso, erro remoto explícito, tipo de corpo
 * inesperado, timeout — direto e embrulhado em {@link ExecutionException}
 * (B3) —, despacho local e comando local sem handler (m3).
 */
class TransportClusterRpcTest {

    private static final NodeId LOCAL = NodeId.of("node-local");
    private static final NodeId TARGET = NodeId.of("node-target");

    private FakeTransport transport;
    private ClusterCoordinator coordinator;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        transport = new FakeTransport(LOCAL);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private TransportClusterRpc rpc(Duration requestTimeout) {
        return new TransportClusterRpc(transport, coordinator, requestTimeout);
    }

    @Test
    void chamadaRemotaComSucessoDevolveOCorpoDaResposta() {
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        transport.onSendAndAwait(request -> {
            ClientRequestPayload payload = request.payload(ClientRequestPayload.class);
            ClientResponsePayload response = new ClientResponsePayload(payload.requestId(), true, "resposta-ok", null);
            return CompletableFuture.completedFuture(ClusterMessage.response(request, response));
        });

        String result = rpc.call(TARGET, "algum.comando", "corpo", String.class);

        assertEquals("resposta-ok", result);
    }

    @Test
    void chamadaRemotaComSuccessFalseVieraRemoteErrorComAMensagemDoServidor() {
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        transport.onSendAndAwait(request -> {
            ClientRequestPayload payload = request.payload(ClientRequestPayload.class);
            ClientResponsePayload response = new ClientResponsePayload(payload.requestId(), false, null, "deu ruim no servidor");
            return CompletableFuture.completedFuture(ClusterMessage.response(request, response));
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> rpc.call(TARGET, "algum.comando", "corpo", String.class));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
        assertEquals("deu ruim no servidor", ex.getMessage());
    }

    @Test
    void chamadaRemotaComCorpoDeTipoInesperadoVieraRemoteError() {
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        transport.onSendAndAwait(request -> {
            ClientRequestPayload payload = request.payload(ClientRequestPayload.class);
            ClientResponsePayload response = new ClientResponsePayload(payload.requestId(), true, 42, null);
            return CompletableFuture.completedFuture(ClusterMessage.response(request, response));
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> rpc.call(TARGET, "algum.comando", "corpo", String.class));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
    }

    @Test
    void futureQueCompletaComTimeoutExceptionEmbrulhadaVieraTimeout() {
        // B3: o future falha com ExecutionException cuja causa é um TimeoutException, em vez de o
        // próprio get(timeout) estourar diretamente — TransportClusterRpc precisa desembrulhar.
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        transport.onSendAndAwait(request -> {
            CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
            future.completeExceptionally(new TimeoutException("timeout simulado do transporte"));
            return future;
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> rpc.call(TARGET, "algum.comando", "corpo", String.class));

        assertEquals(ErrorCode.TIMEOUT, ex.code());
    }

    @Test
    void futureQueCompletaComTimeoutExceptionAninhadaMaisFundoAindaVieraTimeout() {
        // Variante de B3: TimeoutException não é a causa direta, mas aparece mais fundo na cadeia.
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        transport.onSendAndAwait(request -> {
            CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
            RuntimeException wrapper = new RuntimeException("falha de I/O",
                    new TimeoutException("timeout de verdade, mais fundo na cadeia"));
            future.completeExceptionally(wrapper);
            return future;
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> rpc.call(TARGET, "algum.comando", "corpo", String.class));

        assertEquals(ErrorCode.TIMEOUT, ex.code());
    }

    @Test
    void futureQueNuncaCompletaVieraTimeoutPeloGetComPrazo() {
        TransportClusterRpc rpc = rpc(Duration.ofMillis(200));
        transport.onSendAndAwait(request -> new CompletableFuture<>());

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> rpc.call(TARGET, "algum.comando", "corpo", String.class));

        assertEquals(ErrorCode.TIMEOUT, ex.code());
    }

    @Test
    void alvoLocalDespachaSemPassarPeloTransporte() {
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        transport.onSendAndAwait(request -> {
            throw new AssertionError("não deveria usar o transporte para o alvo local");
        });
        rpc.registerLocalHandler(new FakeLocalHandler("meu.comando", body -> "local-ok"));

        String result = rpc.call(LOCAL, "meu.comando", "corpo", String.class);

        assertEquals("local-ok", result);
    }

    @Test
    void comandoLocalSemHandlerLancaExcecaoClara() {
        TransportClusterRpc rpc = rpc(Duration.ofSeconds(5));
        rpc.registerLocalHandler(new FakeLocalHandler("outro.comando", body -> "irrelevante"));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> rpc.call(LOCAL, "meu.comando", "corpo", String.class));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
        assertTrue(ex.getMessage().contains("meu.comando"), "mensagem deveria citar o comando: " + ex.getMessage());
    }

    /** Handler local fake: {@code handles} responde só ao comando configurado. */
    private static final class FakeLocalHandler implements LocalRequestHandler {
        private final String command;
        private final Function<Object, Object> fn;

        FakeLocalHandler(String command, Function<Object, Object> fn) {
            this.command = command;
            this.fn = fn;
        }

        @Override
        public boolean handles(String candidate) {
            return command.equals(candidate);
        }

        @Override
        public Object handleLocal(String candidate, Object body) {
            return fn.apply(body);
        }
    }

    /** {@link Transport} fake: {@code sendAndAwait} é configurável por teste; o resto é no-op. */
    private static final class FakeTransport implements Transport {
        private final NodeInfo local;
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();
        private volatile Function<ClusterMessage, CompletableFuture<ClusterMessage>> sendAndAwaitHandler;

        FakeTransport(NodeId id) {
            this.local = new NodeInfo(id, "127.0.0.1", 0);
        }

        void onSendAndAwait(Function<ClusterMessage, CompletableFuture<ClusterMessage>> handler) {
            this.sendAndAwaitHandler = handler;
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            return List.of();
        }

        @Override
        public void addListener(TransportListener listener) {
        }

        @Override
        public void removeListener(TransportListener listener) {
        }

        @Override
        public void broadcast(ClusterMessage message) {
        }

        @Override
        public void send(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            if (sendAndAwaitHandler == null) {
                throw new IllegalStateException("sendAndAwaitHandler não configurado neste teste");
            }
            return sendAndAwaitHandler.apply(message);
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return false;
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return false;
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() {
        }
    }
}
