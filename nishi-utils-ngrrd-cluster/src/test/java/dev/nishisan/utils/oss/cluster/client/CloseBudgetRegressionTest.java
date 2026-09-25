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
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B1 (achado do Refuter): regressão do orçamento TOTAL de fechamento — reproduz, num nível de
 * unidade (sem cluster real), a sequência {@code DefaultNgrrdClusterClient.close()} de N handles
 * seguidos do {@code WriteDispatcher.close(Duration)}, com um {@link ClusterRpc} fake que nunca
 * responde a tempo, para provar que o tempo TOTAL é limitado por {@code closeTimeout} — não por
 * {@code N × requestTimeout} (o bug original: cada handle contra um dono morto pagava o
 * {@code requestTimeout} inteiro antes de desistir, e 20 handles chegavam a somar minutos).
 */
class CloseBudgetRegressionTest {

    private static final int HANDLE_COUNT = 20;
    private static final String DEAD_OWNER = "storage-dead";

    private List<RemoteSeriesHandle> handles;
    private WriteDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        // Os handles/dispatcher dos testes já são fechados dentro do próprio teste (é o que se está
        // verificando); um close() adicional aqui seria no-op (idempotente) — só uma rede de segurança.
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void fecharVinteHandlesContraDonoMortoNaoUltrapassaCloseTimeoutMaisUmSegundo() throws Exception {
        Duration closeTimeout = Duration.ofSeconds(2);
        Duration requestTimeout = Duration.ofSeconds(5); // deliberadamente MAIOR que closeTimeout
        SleepingClusterRpc rpc = new SleepingClusterRpc(false, requestTimeout);
        handles = newHandles(rpc, requestTimeout, closeTimeout);
        dispatcher = newDispatcher(rpc, closeTimeout);

        long startedAtMs = System.currentTimeMillis();
        closeAllWithSharedBudget(handles, dispatcher, closeTimeout);
        long elapsedMs = System.currentTimeMillis() - startedAtMs;

        assertTrue(elapsedMs <= closeTimeout.toMillis() + 1_000L,
                "close() de " + HANDLE_COUNT + " handles contra dono INALCANÇÁVEL (isConnected=false) levou "
                        + elapsedMs + " ms, esperava <= " + (closeTimeout.toMillis() + 1_000L) + " ms");
        // isConnected=false: nenhum CLOSE remoto deveria sequer ter sido tentado (pulado com WARN) —
        // achado do Refuter, item 3. Confirma que o tempo baixo acima não é coincidência de sorte com
        // o backoff, e sim o curto-circuito de isConnected.
        assertTrue(rpc.callCount() == 0, "com isConnected=false, nenhuma chamada CLOSE deveria ter sido feita: "
                + rpc.callCount());
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void fecharVinteHandlesContraDonoVivoMasLentoRespeitaOOrcamentoTotal() throws Exception {
        Duration closeTimeout = Duration.ofSeconds(2);
        Duration requestTimeout = Duration.ofSeconds(5); // deliberadamente MAIOR que closeTimeout
        SleepingClusterRpc rpc = new SleepingClusterRpc(true, requestTimeout);
        handles = newHandles(rpc, requestTimeout, closeTimeout);
        dispatcher = newDispatcher(rpc, closeTimeout);

        long startedAtMs = System.currentTimeMillis();
        closeAllWithSharedBudget(handles, dispatcher, closeTimeout);
        long elapsedMs = System.currentTimeMillis() - startedAtMs;

        assertTrue(elapsedMs <= closeTimeout.toMillis() + 1_000L,
                "close() de " + HANDLE_COUNT + " handles contra dono vivo mas LENTO (cada tentativa demora "
                        + requestTimeout + ") levou " + elapsedMs + " ms, esperava <= "
                        + (closeTimeout.toMillis() + 1_000L) + " ms — o orçamento TOTAL (closeTimeout) deveria "
                        + "vencer, não requestTimeout × " + HANDLE_COUNT);
        // Prova que pelo menos UMA tentativa de CLOSE remoto de fato aconteceu (dono conectado) — sem
        // isso o teste acima passaria mesmo com uma regressão que pulasse close() incondicionalmente.
        assertTrue(rpc.callCount() >= 1, "esperava ao menos uma tentativa de CLOSE remoto (dono conectado)");
    }

    private static List<RemoteSeriesHandle> newHandles(ClusterRpc rpc, Duration requestTimeout, Duration closeTimeout)
            throws Exception {
        RetryPolicy retryPolicy = new RetryPolicy(Duration.ofSeconds(60), Duration.ofMillis(10), Duration.ofMillis(100));
        UnusedPlacementLookup resolver = new UnusedPlacementLookup();
        List<RemoteSeriesHandle> result = new ArrayList<>(HANDLE_COUNT);
        for (int i = 0; i < HANDLE_COUNT; i++) {
            String seriesKey = "series-" + i;
            RemoteSeriesHandle handle = new RemoteSeriesHandle(seriesKey, "yaml: fake", "hash-" + i, Map.of(),
                    Ngrrd.OpenOptions.defaults(), resolver, rpc, new NoOpWriteBuffer(), retryPolicy, requestTimeout,
                    closeTimeout, Clock.systemUTC(), (key, h) -> { });
            // open() não é usado de propósito: o fake de RPC nunca responde OK a nada, então open()
            // ficaria preso na própria retentativa dele. O dono é o que interessa testar aqui (close);
            // setado direto via reflexão, mesmo padrão já usado em StorageRequestHandlerTest.
            setOwner(handle, DEAD_OWNER);
            result.add(handle);
        }
        return result;
    }

    private static void setOwner(RemoteSeriesHandle handle, String owner) throws Exception {
        Field field = RemoteSeriesHandle.class.getDeclaredField("owner");
        field.setAccessible(true);
        field.set(handle, owner);
    }

    private static WriteDispatcher newDispatcher(ClusterRpc rpc, Duration closeTimeout) {
        RetryPolicy retryPolicy = new RetryPolicy(Duration.ofSeconds(60), Duration.ofMillis(10), Duration.ofMillis(100));
        return new WriteDispatcher(rpc, new UnusedPlacementLookup(), retryPolicy, 500, Duration.ofMillis(50),
                100_000L, NgrrdClusterConfig.BufferFullPolicy.BLOCK, closeTimeout, key -> false, Clock.systemUTC());
    }

    /**
     * Reproduz {@code DefaultNgrrdClusterClient.close()}: um único deadline alimenta TODOS os handles
     * e, na sequência, o dispatcher — nunca um {@code closeTimeout} inteiro "renovado" por fase.
     */
    private static void closeAllWithSharedBudget(List<RemoteSeriesHandle> handles, WriteDispatcher dispatcher,
            Duration closeTimeout) {
        long deadline = System.currentTimeMillis() + closeTimeout.toMillis();
        for (RemoteSeriesHandle handle : handles) {
            long remainingMs = deadline - System.currentTimeMillis();
            Duration budget = remainingMs > 0 ? Duration.ofMillis(remainingMs) : Duration.ZERO;
            handle.close(budget);
        }
        long dispatcherRemainingMs = deadline - System.currentTimeMillis();
        Duration dispatcherBudget = dispatcherRemainingMs > 0 ? Duration.ofMillis(dispatcherRemainingMs) : Duration.ZERO;
        dispatcher.close(dispatcherBudget);
    }

    /**
     * {@link ClusterRpc} fake: a sobrecarga com teto explícito DORME pelo {@code timeout} recebido e
     * então lança {@link ErrorCode#TIMEOUT} — simula um dono que nunca responde a tempo. A sobrecarga
     * simples delega para a com teto usando {@code requestTimeout}, o mesmo padrão de
     * {@code TransportClusterRpc} de produção.
     */
    private static final class SleepingClusterRpc implements ClusterRpc {
        private final boolean connected;
        private final Duration requestTimeout;
        private final AtomicInteger callCount = new AtomicInteger();

        SleepingClusterRpc(boolean connected, Duration requestTimeout) {
            this.connected = connected;
            this.requestTimeout = requestTimeout;
        }

        int callCount() {
            return callCount.get();
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            return call(target, command, body, responseType, requestTimeout);
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType, Duration timeout) {
            callCount.incrementAndGet();
            sleepQuietly(timeout);
            throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                    "simulado: " + command + " para " + target + " nunca respondeu em " + timeout);
        }

        @Override
        public NodeId localId() {
            return NodeId.of("client-under-test");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
        }

        @Override
        public boolean isConnected(NodeId target) {
            return connected;
        }

        private static void sleepQuietly(Duration duration) {
            try {
                Thread.sleep(Math.max(0L, duration.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@link PlacementLookup} fake: não deveria ser chamado por nenhum caminho exercitado aqui (só close()). */
    private static final class UnusedPlacementLookup implements PlacementLookup {
        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            throw new UnsupportedOperationException("não usado neste teste (só close())");
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado neste teste (só close())");
        }

        @Override
        public SeriesPlacement resolveExistingAtLeader(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado neste teste (só close())");
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            throw new UnsupportedOperationException("não usado neste teste (só close())");
        }

        @Override
        public void invalidate(String seriesKey) {
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
        }
    }

    /** {@link WriteBuffer} fake: sem pendências — o foco é o CLOSE remoto do handle, não o flush. */
    private static final class NoOpWriteBuffer implements WriteBuffer {
        @Override
        public void enqueue(String ownerNodeId, SeriesWrite write) {
            throw new UnsupportedOperationException("não usado neste teste");
        }

        @Override
        public void flushNodeSync(String ownerNodeId) {
        }

        @Override
        public void flushNodeSync(String ownerNodeId, Duration maxWait) {
        }
    }
}
