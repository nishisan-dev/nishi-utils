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
import dev.nishisan.utils.oss.cluster.api.ClientMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.WriteMark;
import dev.nishisan.utils.oss.cluster.api.WriteMarkResult;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.metrics.NgrrdClusterMetricsListener;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Enfileira escritas por nó de destino e as envia em lotes {@code WRITE_BATCH}
 * — por tamanho ({@code batchMaxSamples}) ou por tempo ({@code batchMaxDelay},
 * via a thread única {@code ngrrd-write-dispatcher}) — com no máximo um flush
 * em voo por nó e retentativa transparente para {@code WRONG_OWNER},
 * {@code NOT_OPEN} e {@code MIGRATING}.
 *
 * <p>Cada nó de destino tem seu próprio buffer limitado, com FIFO por série
 * ({@code maxBufferedSamplesPerNode}); ao encher, {@link #enqueue} bloqueia
 * ({@link NgrrdClusterConfig.BufferFullPolicy#BLOCK}) ou lança
 * {@link ErrorCode#BUFFER_FULL} ({@link NgrrdClusterConfig.BufferFullPolicy#FAIL}).</p>
 *
 * <p><strong>Dicas de dono contraditórias (#177).</strong> Um {@code WRONG_OWNER} com dono informado é
 * seguido enquanto a cadeia de saltos for plausível, com backoff exponencial por série que atravessa os
 * nós. Quando a dica contradiz o episódio de redirecionamento — aponta o próprio nó, um nó que já
 * redirecionou a série, diverge do dono confirmado pelo líder ou excede {@value #MAX_REDIRECT_HOPS}
 * saltos —, só aquela série é pausada e o dono é confirmado no líder por uma consulta em lote
 * ({@link PlacementLookup#resolveExistingAtLeader(java.util.Collection, Duration)}), coalescida entre as
 * séries. O primeiro {@code OK} da série encerra o episódio.</p>
 *
 * <p><strong>Marcas de escrita ({@link NgrrdClusterConfig.WriteFailureReporting#MARKS}).</strong> {@link #mark()}
 * fotografa {@code submitted} das rotas pendentes e registra em cada uma um waiter (marca, fronteira), sem esperar.
 * {@code completeWrites} libera os waiters cuja fronteira foi alcançada e atribui a cada marca as falhas ainda não
 * reportadas até a fronteira — cada falha vai para exatamente uma marca. Uma rota com falha não reportada fica no
 * índice de pendentes até ser colhida; as barreiras, nesse modo, não lançam por falha de escrita. As marcas concluem
 * em ordem, num executor próprio e fora de qualquer lock interno. Ordem de locks: {@code markLock → route.lock →}
 * lock da marca; nunca {@code route.lock → markLock}.</p>
 *
 * <p><strong>Release de séries.</strong> {@link #beginRelease} aposenta a rota e fotografa a fronteira,
 * {@link #drainForRelease} espera cada fronteira sem lançar e colhe as falhas até ela (cada falha vai para uma marca
 * ou para um release, nunca para os dois), e {@link #finishRelease} solta o holder e, com a falha entregue, dispensa
 * {@code isIdle} na remoção da rota.</p>
 */
public final class WriteDispatcher implements WriteBuffer, Closeable {

    private static final Logger LOGGER = Logger.getLogger(WriteDispatcher.class.getName());
    private static final long ENQUEUE_WAIT_POLL_MS = 200L;
    private static final long DRAIN_POLL_MS = 20L;
    /** Default dos construtores que não recebem {@code ownerChanged} explicitamente (testes antigos). */
    private static final BiConsumer<String, String> NO_OP_OWNER_CHANGED = (seriesKey, newOwner) -> { };
    /**
     * Saltos de {@code WRONG_OWNER} aceitos num mesmo episódio antes de desconfiar da cadeia e confirmar o
     * dono no líder — folga para a cadeia legítima origem → intermediário → destino.
     */
    static final int MAX_REDIRECT_HOPS = 4;
    /** Teto de cada consulta ao líder para desempatar dicas contraditórias (limitado também por {@code retryPolicy.timeout()}). */
    static final Duration OWNER_LOOKUP_MAX_WAIT = Duration.ofSeconds(5);
    /** Sem release em curso numa rota ({@code SeriesRoute.releaseBoundary}). */
    private static final long NO_RELEASE = -1L;
    /** Holder das rotas abertas sem holder explícito ({@link #beginGeneration(String, String, String)}): nunca sai. */
    private static final Object PINNED_HOLDER = new Object();

    private final ClusterRpc rpc;
    private final PlacementLookup placementLookup;
    private final RetryPolicy retryPolicy;
    private final int batchMaxSamples;
    private final long batchMaxDelayMillis;
    private final long maxBufferedSamplesPerNode;
    private final NgrrdClusterConfig.BufferFullPolicy bufferFullPolicy;
    private final long closeTimeoutMillis;
    private final Function<String, Boolean> reopener;
    /**
     * M3 (achado do Refuter do M1c): notifica o {@code RemoteSeriesHandle} da série quando um
     * {@code WRONG_OWNER} traz um dono novo conhecido — sem isso, {@code RemoteSeriesHandle.owner}
     * nunca muda por este caminho e cada lote seguinte da MESMA série seria reroteado de novo,
     * reintroduzindo a inversão de ordem que {@link #extractSeriesFrom} existe para evitar. Recebe
     * {@code (seriesKey, newOwnerNodeId)}; nunca chamado com {@code newOwnerNodeId == null}.
     */
    private final BiConsumer<String, String> ownerChanged;
    private final Clock clock;
    /** {@code null} = nenhuma integração de métricas configurada (ver {@link NgrrdClusterConfig#metricsListener()}). */
    private final NgrrdClusterMetricsListener metricsListener;
    /** {@code null} junto com {@link #metricsListener}; monta o {@code ClientMetricsSnapshot} completo sob demanda. */
    private final Supplier<ClientMetricsSnapshot> metricsSupplier;

    /** A cada {@value #METRICS_TICK_INTERVAL} ticks (~{@code batchMaxDelay × 50}, ≈10 s por padrão). */
    private static final int METRICS_TICK_INTERVAL = 50;

    private final ConcurrentMap<String, NodeBuffer> buffers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SeriesRoute> routes = new ConcurrentHashMap<>();
    // Only outstanding or permanently failed routes participate in global ACK barriers.
    // A linked set keeps snapshots proportional to pending work, even after a large burst.
    // Mutations hold route.lock first; snapshots release pendingLock BEFORE locking routes.
    private final Object pendingLock = new Object();
    private final Set<SeriesRoute> pendingRoutes = new LinkedHashSet<>();
    private final ExecutorService flushPool;
    // Bound OPEN traffic independently of WRITE_BATCH; mass cold starts must not flood the leader.
    private final ExecutorService recoveryPool = Executors.newFixedThreadPool(4,
            Thread.ofPlatform().daemon(true).name("ngrrd-write-reopen-", 0).factory());
    /**
     * Consultas ao líder para dicas de dono contraditórias: uma tarefa por vez ({@link #ownerLookupRunning}),
     * que drena {@link #ownerLookupQueue} em lotes. Executor próprio, não o {@link #recoveryPool}: cada
     * reabertura ali pode esperar até {@code retryTimeout} (OPEN com retentativas), e uma migração em massa
     * ocupa as 4 threads com reaberturas — a consulta que desfaz o pingue-pongue ficaria na fila atrás
     * delas. Virtual thread: a tarefa só bloqueia em RPC (mesmo perfil das threads de flush).
     */
    private final ExecutorService ownerLookupPool = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("ngrrd-owner-lookup-", 0).factory());
    private final ConcurrentLinkedQueue<String> ownerLookupQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean ownerLookupRunning = new AtomicBoolean(false);
    /** Último WARNING de falha da consulta ao líder — rate limit global (uma consulta cobre várias séries). */
    private final AtomicLong lastOwnerLookupFailureLogMs = new AtomicLong(Long.MIN_VALUE);
    private final Thread tickThread;
    record OwnerHint(String seriesKey, String generation, String owner) { }
    private volatile java.util.function.Consumer<OwnerHint> generationOwnerListener = hint -> { };
    void generationOwnerListener(java.util.function.Consumer<OwnerHint> listener) { generationOwnerListener = listener; }
    private void notifyOwner(String key, String owner, SeriesRoute route) {
        ownerChanged.accept(key, owner);
        generationOwnerListener.accept(new OwnerHint(key, route.generation, owner));
    }

    private volatile boolean closed;

    /** {@code true} em {@link NgrrdClusterConfig.WriteFailureReporting#MARKS}. */
    private final boolean marksMode;
    /** Serializa {@link #mark()} e protege {@link #openMarks}, {@link #nextMarkId} e {@link #lastMarkAdmitted}. */
    private final ReentrantLock markLock = new ReentrantLock();
    /** Marcas ainda não concluídas, em ordem de criação. */
    private final ArrayDeque<Mark> openMarks = new ArrayDeque<>();
    private long nextMarkId = 1L;
    private long lastMarkAdmitted;
    /** Uma drenagem de marcas prontas por vez — garante a conclusão em ordem. */
    private final AtomicBoolean markCompletionRunning = new AtomicBoolean(false);
    /**
     * Conclui as marcas fora de qualquer lock interno: {@code completeWrites} roda sob {@code route.lock} (que o
     * {@code applyStatus} segura em volta dele) e callbacks não assíncronos do chamador rodam na thread que conclui.
     */
    private final ExecutorService markCompletionPool = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("ngrrd-write-mark-", 0).factory());

    private volatile java.util.function.BiConsumer<SeriesWrite, NgrrdClusterException> terminalListener = (key, failure) -> { };
    public void terminalListener(java.util.function.BiConsumer<SeriesWrite, NgrrdClusterException> listener) {
        terminalListener = listener;
    }

    /**
     * Sem holder explícito: a rota fica presa a um holder permanente ({@link #PINNED_HOLDER}) e nunca sai do
     * mapa, como antes da 8.15.0. Os handles usam {@link #beginGeneration(String, String, String, Object)}.
     */
    @Override public void beginGeneration(String key, String generation, String owner) {
        beginGeneration(key, generation, owner, PINNED_HOLDER);
    }

    /**
     * Abre (ou reaproveita) a rota da geração {@code generation} de {@code key} para {@code holder}.
     *
     * <ul>
     *   <li>Mesma geração: adiciona o holder (por identidade; a reabertura do mesmo handle é idempotente) e zera
     *       {@code retired}. Sem holder ({@code null}, handle somente leitura), mantém a rota como está.</li>
     *   <li>Geração diferente: aposenta a rota anterior, com as escritas em buffer e em voo, e cria a nova com
     *       {@code {holder}}. Sem holder, não cria rota: a geração antiga sai do mapa e nenhuma é publicada.</li>
     *   <li>Sem rota: cria com {@code {holder}}; sem holder, não cria.</li>
     * </ul>
     *
     * <p>Roda dentro de {@code routes.compute} (bin do mapa → {@code route.lock}); nunca remove outra rota daqui.</p>
     */
    @Override public void beginGeneration(String key, String generation, String owner, Object holder) {
        boolean[] marksReady = {false};
        boolean[] dropped = {false};
        routes.compute(key, (ignored, previous) -> {
            if (previous == null) return holder == null ? null : new SeriesRoute(key, owner, generation, holder);
            previous.lock.lock();
            try {
                if (Objects.equals(previous.generation, generation)) {
                    if (holder != null) {
                        previous.holders.add(holder);
                        previous.retired = false;
                        // Religada (por exemplo, open() concorrente a um release): volta a ser uma rota comum, e
                        // uma falha posterior não pode sair do mapa sem ser reportada.
                        previous.failuresDelivered = false;
                    }
                    return previous;
                }
                // Explicit OPEN retires a removed generation, including buffered and in-flight writes.
                // Destacada já aqui, sob previous.lock: o compute só publica a rota nova depois que este lambda
                // solta o lock, e quem pegar o lock nessa janela ainda veria previous no mapa.
                previous.detached = true;
                for (NodeBuffer buffer : buffers.values()) extractSeriesFrom(buffer, key);
                long remaining = previous.submitted - previous.completed;
                samplesFailedCount.add(remaining);
                if (marksMode && remaining > 0) {
                    previous.recordFailure(previous.completed + 1, previous.submitted, SeriesStatus.SERIES_DELETED);
                }
                previous.completed = previous.submitted;
                previous.firstFailedSequence = 0;
                previous.failureCode = ErrorCode.SERIES_DELETED;
                previous.failureMessage = "generation replaced: " + key;
                marksReady[0] = releaseWaiters(previous);
                previous.progress.signalAll();
                // MARKS: the retired route stays indexed until a mark collects its unreported failures.
                if (!marksMode || isIdle(previous)) {
                    synchronized (pendingLock) { pendingRoutes.remove(previous); }
                }
                if (holder == null) {
                    dropped[0] = true;
                    return null;
                }
                return new SeriesRoute(key, owner, generation, holder);
            } finally { previous.lock.unlock(); }
        });
        if (dropped[0]) {
            // Fora do compute: a geração aposentada saiu do mapa sem rota nova (abertura somente leitura).
            routesRemovedCount.increment();
            lastRetryLogMs.remove(key);
        }
        if (marksReady[0]) scheduleMarkCompletion();
    }

    /**
     * {@code holder} deixou de usar a rota da geração {@code generation} de {@code key}. Sem holder, a rota fica
     * {@code retired}: novas admissões recebem {@link ErrorCode#CLOSED}, e as escritas já admitidas seguem até o
     * desfecho; ociosa, sai do mapa aqui, senão quando a última resposta chegar ({@link #retirable}). Um holder que
     * não está na rota (ou outra geração no mapa) é no-op. Nunca chamar sob {@code route.lock}.
     */
    @Override public void endGeneration(String key, String generation, Object holder) {
        if (holder == null) {
            return;
        }
        SeriesRoute route = routes.get(key);
        if (route == null) {
            return;
        }
        boolean candidate;
        route.lock.lock();
        try {
            if (!Objects.equals(route.generation, generation) || !route.holders.remove(holder)) {
                return;
            }
            if (route.holders.isEmpty()) {
                route.retired = true;
            }
            candidate = retirable(route);
        } finally {
            route.lock.unlock();
        }
        if (candidate) {
            tryRemove(route);
        }
    }

    /**
     * Fase 1 da remoção (marcar): a rota não tem holder, nenhuma escrita sem desfecho (inclusive em disco) e
     * nenhuma falha a reportar — ou a falha já foi entregue ao chamador ({@code failuresDelivered}) —, nem marca
     * esperando, redirecionamento ou consulta de dono pendentes. Só avalia, nunca remove. Chamado sob
     * {@code route.lock}; o chamador roda {@link #tryRemove} depois de soltá-lo.
     */
    private boolean retirable(SeriesRoute route) {
        return route.retired
                && route.holders.isEmpty()
                && (isIdle(route) || (route.failuresDelivered && route.submitted == route.completed))
                && route.waiters.isEmpty()
                && !route.redirectPending
                && !route.ownerLookupPending;
    }

    /**
     * Fase 2 da remoção: tira {@code route} do mapa se ela ainda for a rota da chave e continuar
     * {@link #retirable}. Ordem de locks bin do mapa → {@code route.lock}, a mesma de {@link #beginGeneration};
     * por isso nunca roda sob um {@code route.lock} nem dentro de outro {@code compute} de {@link #routes}. Depois
     * da remoção, já sem lock de rota, limpa o índice de pendentes, o rate limit de log e as tentativas da série nos
     * buffers. Devolve {@code true} se removeu.
     */
    private boolean tryRemove(SeriesRoute route) {
        boolean[] removed = {false};
        routes.computeIfPresent(route.key, (key, current) -> {
            if (current != route) {
                return current;
            }
            current.lock.lock();
            try {
                if (!retirable(current)) {
                    return current;
                }
                // Mesma janela do beginGeneration: o mapa só remove depois que o lambda solta o lock.
                current.detached = true;
                removed[0] = true;
                return null;
            } finally {
                current.lock.unlock();
            }
        });
        if (!removed[0]) {
            return false;
        }
        routesRemovedCount.increment();
        synchronized (pendingLock) {
            pendingRoutes.remove(route);
        }
        lastRetryLogMs.remove(route.key);
        for (NodeBuffer buf : buffers.values()) {
            buf.lock.lock();
            try {
                buf.queue.forgetSeries(route.key);
            } finally {
                buf.lock.unlock();
            }
        }
        return true;
    }

    /**
     * Confere a geração de um handle gravável antes da admissão. Sem rota, a geração do handle não tem mais onde
     * escrever (substituída e depois removida): lança {@link RouteClosedException}, em vez de seguir em silêncio.
     */
    @Override public void checkGeneration(String key, String generation) {
        SeriesRoute route = routes.get(key);
        if (route == null) throw closedRoute(key);
        route.lock.lock();
        try {
            if (!Objects.equals(route.generation, generation))
                throw new NgrrdClusterException(ErrorCode.SERIES_DELETED, "generation replaced: " + key);
            if (route.failureCode == ErrorCode.SERIES_DELETED || route.failureCode == ErrorCode.QUARANTINED)
                throw new NgrrdClusterException(route.failureCode, route.failureMessage);
        } finally { route.lock.unlock(); }
    }

    /** Barreira da geração de um handle gravável; sem rota, como {@link #checkGeneration}, lança {@link RouteClosedException}. */
    @Override public void flushGenerationSync(String key, String generation, String owner, Duration wait) {
        SeriesRoute route = routes.get(key);
        if (route == null) throw closedRoute(key);
        long boundary;
        route.lock.lock();
        try {
            if (!Objects.equals(route.generation, generation))
                throw new NgrrdClusterException(ErrorCode.SERIES_DELETED, "generation replaced: " + key);
            boundary = route.submitted;
        } finally { route.lock.unlock(); }
        awaitBarriers(Map.of(route, boundary), wait, true);
    }

    /**
     * {@code true} a partir do descarte final do {@code close()}: uma consulta ao líder que termine depois
     * disso não move mais nada entre buffers (criaria um buffer fora da contabilidade de
     * {@code samplesFailed}).
     */
    private volatile boolean drained;

    private final LongAdder samplesEnqueuedCount = new LongAdder();
    private final LongAdder samplesSentCount = new LongAdder();
    private final LongAdder samplesFailedCount = new LongAdder();
    private final LongAdder batchesSentCount = new LongAdder();
    private final ConcurrentMap<SeriesStatus, LongAdder> retryCounts = new ConcurrentHashMap<>();
    private final LongAdder ownerLookupsCount = new LongAdder();
    private final LongAdder redirectCyclesCount = new LongAdder();
    private final LongAdder routesRemovedCount = new LongAdder();
    /** Marca de tempo do último log de retry por série, para o rate limit de {@link #logRetryRateLimited}. */
    private final ConcurrentMap<String, Long> lastRetryLogMs = new ConcurrentHashMap<>();
    private static final long RETRY_LOG_INTERVAL_MS = 10_000L;

    public WriteDispatcher(ClusterRpc rpc, PlacementLookup placementLookup, RetryPolicy retryPolicy,
            int batchMaxSamples, Duration batchMaxDelay, long maxBufferedSamplesPerNode,
            NgrrdClusterConfig.BufferFullPolicy bufferFullPolicy, Duration closeTimeout,
            Function<String, Boolean> reopener, Clock clock) {
        this(rpc, placementLookup, retryPolicy, batchMaxSamples, batchMaxDelay, maxBufferedSamplesPerNode,
                bufferFullPolicy, closeTimeout, reopener, NO_OP_OWNER_CHANGED, clock, null, null);
    }

    /**
     * Variante completa, com integração de métricas: {@code metricsListener}/{@code metricsSupplier}
     * ou ambos {@code null} (nenhuma integração) — nunca só um dos dois.
     * {@link NgrrdClusterMetricsListener#onClientMetrics} é chamado a cada
     * {@value #METRICS_TICK_INTERVAL} execuções do {@code tickLoop} (a mesma thread
     * {@code ngrrd-write-dispatcher} do flush por tempo), ≈ {@code batchMaxDelay × 50} de distância.
     */
    public WriteDispatcher(ClusterRpc rpc, PlacementLookup placementLookup, RetryPolicy retryPolicy,
            int batchMaxSamples, Duration batchMaxDelay, long maxBufferedSamplesPerNode,
            NgrrdClusterConfig.BufferFullPolicy bufferFullPolicy, Duration closeTimeout,
            Function<String, Boolean> reopener, Clock clock, NgrrdClusterMetricsListener metricsListener,
            Supplier<ClientMetricsSnapshot> metricsSupplier) {
        this(rpc, placementLookup, retryPolicy, batchMaxSamples, batchMaxDelay, maxBufferedSamplesPerNode,
                bufferFullPolicy, closeTimeout, reopener, NO_OP_OWNER_CHANGED, clock, metricsListener,
                metricsSupplier);
    }

    /**
     * Variante completa (M3), com o callback {@link #ownerChanged} de reroteamento — usada por
     * {@code DefaultNgrrdClusterClient} para manter {@code RemoteSeriesHandle.owner} em dia após um
     * {@code WRONG_OWNER}. {@code metricsListener}/{@code metricsSupplier} seguem a mesma regra: ambos
     * {@code null}, ou nenhum dos dois.
     */
    public WriteDispatcher(ClusterRpc rpc, PlacementLookup placementLookup, RetryPolicy retryPolicy,
            int batchMaxSamples, Duration batchMaxDelay, long maxBufferedSamplesPerNode,
            NgrrdClusterConfig.BufferFullPolicy bufferFullPolicy, Duration closeTimeout,
            Function<String, Boolean> reopener, BiConsumer<String, String> ownerChanged, Clock clock,
            NgrrdClusterMetricsListener metricsListener, Supplier<ClientMetricsSnapshot> metricsSupplier) {
        this(rpc, placementLookup, retryPolicy, batchMaxSamples, batchMaxDelay, maxBufferedSamplesPerNode,
                bufferFullPolicy, closeTimeout, reopener, ownerChanged, clock, metricsListener, metricsSupplier,
                NgrrdClusterConfig.WriteFailureReporting.BARRIER);
    }

    /**
     * Variante completa com o modo de reporte de falhas de escrita ({@code failureReporting}) — usada por
     * {@code DefaultNgrrdClusterClient} a partir de {@link NgrrdClusterConfig#writeFailureReporting()}.
     */
    public WriteDispatcher(ClusterRpc rpc, PlacementLookup placementLookup, RetryPolicy retryPolicy,
            int batchMaxSamples, Duration batchMaxDelay, long maxBufferedSamplesPerNode,
            NgrrdClusterConfig.BufferFullPolicy bufferFullPolicy, Duration closeTimeout,
            Function<String, Boolean> reopener, BiConsumer<String, String> ownerChanged, Clock clock,
            NgrrdClusterMetricsListener metricsListener, Supplier<ClientMetricsSnapshot> metricsSupplier,
            NgrrdClusterConfig.WriteFailureReporting failureReporting) {
        this.marksMode = Objects.requireNonNull(failureReporting, "failureReporting")
                == NgrrdClusterConfig.WriteFailureReporting.MARKS;
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.placementLookup = Objects.requireNonNull(placementLookup, "placementLookup");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        if (batchMaxSamples <= 0) {
            throw new IllegalArgumentException("batchMaxSamples deve ser > 0: " + batchMaxSamples);
        }
        this.batchMaxSamples = batchMaxSamples;
        Objects.requireNonNull(batchMaxDelay, "batchMaxDelay");
        if (batchMaxDelay.isNegative() || batchMaxDelay.isZero()) {
            throw new IllegalArgumentException("batchMaxDelay deve ser > 0");
        }
        this.batchMaxDelayMillis = batchMaxDelay.toMillis();
        if (maxBufferedSamplesPerNode <= 0) {
            throw new IllegalArgumentException("maxBufferedSamplesPerNode deve ser > 0: " + maxBufferedSamplesPerNode);
        }
        this.maxBufferedSamplesPerNode = maxBufferedSamplesPerNode;
        this.bufferFullPolicy = Objects.requireNonNull(bufferFullPolicy, "bufferFullPolicy");
        Objects.requireNonNull(closeTimeout, "closeTimeout");
        if (closeTimeout.isNegative() || closeTimeout.isZero()) {
            throw new IllegalArgumentException("closeTimeout deve ser > 0");
        }
        this.closeTimeoutMillis = closeTimeout.toMillis();
        this.reopener = Objects.requireNonNull(reopener, "reopener");
        this.ownerChanged = Objects.requireNonNull(ownerChanged, "ownerChanged");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.metricsListener = metricsListener;
        this.metricsSupplier = metricsSupplier;

        this.flushPool = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("ngrrd-write-flush-", 0).factory());
        this.tickThread = new Thread(this::tickLoop, "ngrrd-write-dispatcher");
        this.tickThread.setDaemon(true);
        this.tickThread.start();
    }

    @Override
    public void enqueue(String ownerNodeId, SeriesWrite write) {
        Objects.requireNonNull(ownerNodeId, "ownerNodeId");
        Objects.requireNonNull(write, "write");
        // Só beginGeneration cria rota: uma escrita que passou do ensureWritable antes de um close/release não
        // recria a rota depois da remoção (seria admitida sem dreno).
        SeriesRoute route = routes.get(write.seriesKey());
        if (route == null) {
            if (closed) {
                throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
            }
            throw closedRoute(write.seriesKey());
        }
        for (;;) {
            NodeBuffer buf;
            String owner;
            boolean accepted = false;
            boolean triggerFlush = false;
            route.lock.lock();
            try {
                while (route.redirectPending) {
                    try { route.progress.await(); }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new NgrrdClusterException(ErrorCode.CLOSED, "admission interrupted", e);
                    }
                }
                if (closed) {
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
                }
                // Fora do mapa (ou destacada, ainda a caminho de sair) ou aposentada: por fechamento/release
                // (CLOSED) ou por geração nova (SERIES_DELETED).
                if (route.detached || routes.get(write.seriesKey()) != route || route.retired) {
                    throw route.retired ? closedRoute(write.seriesKey())
                            : new NgrrdClusterException(ErrorCode.SERIES_DELETED, "generation replaced: " + write.seriesKey());
                }
                if (!Objects.equals(route.generation, write.generationId()))
                    throw new NgrrdClusterException(ErrorCode.SERIES_DELETED, "generation replaced: " + write.seriesKey());
                if (route.failureCode == ErrorCode.SERIES_DELETED || route.failureCode == ErrorCode.QUARANTINED)
                    throw new NgrrdClusterException(route.failureCode, route.failureMessage);
                // The caller may have read RemoteSeriesHandle.owner before a concurrent redirect.
                // Every admission uses the same route as the backlog, under the series lock.
                owner = route.owner;
                buf = buffers.computeIfAbsent(owner, id -> new NodeBuffer());
                buf.lock.lock();
                try {
                    if (buf.queue.size() < maxBufferedSamplesPerNode) {
                        buf.queue.addLast(write);
                        if (isIdle(route)) {
                            synchronized (pendingLock) {
                                pendingRoutes.add(route);
                            }
                        }
                        route.submitted++;
                        samplesEnqueuedCount.increment();
                        accepted = true;
                        triggerFlush = buf.queue.size() >= batchMaxSamples;
                    } else if (bufferFullPolicy == NgrrdClusterConfig.BufferFullPolicy.FAIL) {
                        throw new NgrrdClusterException(ErrorCode.BUFFER_FULL,
                                "buffer de escrita cheio para o nó " + owner);
                    }
                } finally {
                    buf.lock.unlock();
                }
            } finally {
                route.lock.unlock();
            }
            if (accepted) {
                if (triggerFlush) {
                    scheduleFlush(owner, buf);
                }
                return;
            }
            // Backpressure never holds the series lock: the flush may need it to redirect
            // this very series and free capacity. Re-read the route after every wake-up.
            buf.lock.lock();
            try {
                if (!closed && buf.queue.size() >= maxBufferedSamplesPerNode) {
                    buf.notFull.await(ENQUEUE_WAIT_POLL_MS, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando espaço no buffer", e);
            } finally {
                buf.lock.unlock();
            }
        }
    }

    @Override
    public void flushNodeSync(String ownerNodeId) {
        flushNodeSync(ownerNodeId, Duration.ofMillis(closeTimeoutMillis));
    }

    @Override
    public void flushNodeSync(String ownerNodeId, Duration maxWait) {
        awaitBarriers(snapshotBarriers(ownerNodeId), maxWait, false);
    }

    @Override
    public void flushSeriesSync(String seriesKey, String ownerNodeId) {
        flushSeriesSync(seriesKey, ownerNodeId, Duration.ofMillis(closeTimeoutMillis));
    }

    @Override
    public void flushSeriesSync(String seriesKey, String ownerNodeId, Duration maxWait) {
        SeriesRoute route = routes.get(seriesKey);
        if (route == null) {
            return;
        }
        long boundary;
        route.lock.lock();
        try {
            boundary = route.submitted;
        } finally {
            route.lock.unlock();
        }
        awaitBarriers(Map.of(route, boundary), maxWait, true);
    }

    /** Waits for all writes admitted before this call, even if they change destination. */
    public void flushAllSync() {
        awaitBarriers(snapshotBarriers(null), Duration.ofMillis(closeTimeoutMillis), false);
    }

    /**
     * Captura a fronteira de tudo o que foi admitido até agora, sem esperar ACK (ver {@link WriteMark}).
     * Custo O(rotas pendentes); serializado com outras chamadas de {@code mark()}.
     *
     * @throws IllegalStateException  fora de {@link NgrrdClusterConfig.WriteFailureReporting#MARKS}
     * @throws NgrrdClusterException {@link ErrorCode#CLOSED} se o dispatcher já foi fechado
     */
    public WriteMark mark() {
        if (!marksMode) {
            throw new IllegalStateException("mark() exige writeFailureReporting=MARKS na configuração do cliente");
        }
        // Rotas sem holder que ficaram ociosas ao colher as falhas: removidas depois de soltar o markLock.
        List<SeriesRoute> retirable = new ArrayList<>();
        Mark mark;
        markLock.lock();
        try {
            if (closed) {
                throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
            }
            long admitted = samplesEnqueuedCount.sum();
            mark = new Mark(nextMarkId++, Math.max(0L, admitted - lastMarkAdmitted));
            lastMarkAdmitted = admitted;
            openMarks.addLast(mark);
            for (SeriesRoute route : snapshotPendingRoutes()) {
                route.lock.lock();
                try {
                    if (route.completed >= route.submitted) {
                        // Nothing in flight: collect what is still unreported right away.
                        route.collectFailures(mark, route.submitted);
                        if (isIdle(route)) {
                            synchronized (pendingLock) {
                                pendingRoutes.remove(route);
                            }
                        }
                        if (retirable(route)) {
                            retirable.add(route);
                        }
                    } else if (route.waiters.isEmpty() || route.waiters.peekLast().boundary() < route.submitted) {
                        mark.acquire();
                        route.waiters.addLast(new MarkWaiter(mark, route.submitted));
                    }
                    // Same boundary as the previous mark's waiter: that mark collects every failure up to
                    // it and completes first (in-order completion), so this one needs no waiter here. A
                    // stuck owner (node down, no write deadline) keeps one waiter per route, not one per mark.
                } finally {
                    route.lock.unlock();
                }
            }
            // Guard released under markLock: mark N is released before mark N+1 exists, so
            // releases (and completions) are monotonic in mark order.
            mark.release();
            scheduleMarkCompletion();
        } finally {
            markLock.unlock();
        }
        for (SeriesRoute route : retirable) {
            tryRemove(route);
        }
        return mark;
    }

    /**
     * Etapa 1 do release de {@code key} (admissão encerrada): sob {@code route.lock}, aposenta a rota ({@code retired})
     * e fotografa a fronteira ({@code submitted}). Daí em diante, uma admissão na rota recebe {@link ErrorCode#CLOSED};
     * um {@link #beginGeneration} posterior com a mesma geração religa a rota (zera {@code retired}), e o dreno continua
     * esperando só a fronteira fotografada.
     *
     * <p>A rota só é aposentada se {@code holder} for o ÚNICO holder dela na geração {@code generation}, ou se ela já
     * não tiver holder; com outro holder (um handle aberto por fora do release), a rota segue viva e é só drenada. O
     * holder continua na rota até {@link #finishRelease}. O chamador segura o lock de abertura da chave no cache de
     * handles, de modo que um {@code open()} concorrente da mesma chave só religa a rota depois desta fotografia.
     * Devolve {@code null} se não houver rota para a chave.</p>
     *
     * <p>Em {@code MARKS}, a fronteira também fica guardada na rota, junto com o id da primeira marca criada depois
     * daqui (sob {@code markLock}, então nenhuma marca fica pela metade): marcas criadas durante o release não colhem as
     * falhas até a fronteira, que são do release. Uma marca aberta antes continua colhendo-as (spec 8.15.0, 3.7). Com
     * releases sobrepostos da mesma rota, vale a união: o menor id de marca (o do primeiro release ativo) e a maior
     * fronteira; a proteção só sai quando o último release ativo termina ({@link #endReleaseProtection}).</p>
     */
    ReleaseTicket beginRelease(String key, String generation, Object holder) {
        SeriesRoute route = routes.get(key);
        if (route == null) {
            return null;
        }
        // Ordem de locks markLock → route.lock, a mesma de mark().
        if (marksMode) {
            markLock.lock();
        }
        try {
            route.lock.lock();
            try {
                boolean owned = holder != null && Objects.equals(route.generation, generation)
                        && route.holders.contains(holder);
                if ((owned && route.holders.size() == 1) || route.holders.isEmpty()) {
                    route.retired = true;
                }
                if (marksMode) {
                    if (route.activeReleases == 0) {
                        route.releaseFirstMarkId = nextMarkId;
                        route.releaseBoundary = route.submitted;
                    } else {
                        route.releaseBoundary = Math.max(route.releaseBoundary, route.submitted);
                    }
                    route.activeReleases++;
                }
                return new ReleaseTicket(key, route, route.submitted, owned ? holder : null, marksMode);
            } finally {
                route.lock.unlock();
            }
        } finally {
            if (marksMode) {
                markLock.unlock();
            }
        }
    }

    /**
     * Etapa 2 do release: espera cada rota alcançar a fronteira de {@link #beginRelease}, com {@code budget} como prazo
     * TOTAL, como {@link #awaitBarriers}, mas sem lançar por falha de escrita: devolve um desfecho por chave, na ordem
     * dos tickets.
     *
     * <p>Ao alcançar a fronteira, sob {@code route.lock}, conclui os waiters de marca já cobertos e colhe as falhas:
     * em {@code MARKS}, as faixas ainda não reportadas até a fronteira, contadas por status — elas deixam de ir para
     * uma marca, então cada falha é reportada uma única vez entre marcas e releases; com releases sobrepostos da mesma
     * rota, cada um deles reporta as falhas que a sua fronteira cobre, inclusive as que outro já colheu; em
     * {@code BARRIER}, a primeira
     * falha da rota, se estiver até a fronteira. Série removida ou em quarentena ({@code failureCode}) vira
     * {@link ReleaseDrain.Outcome#SERIES_DELETED}. Uma rota que não alcança a fronteira no prazo vira
     * {@link ReleaseDrain.Outcome#TIMED_OUT}, sem colheita: as escritas seguem em segundo plano e as falhas que
     * surgirem vão para a próxima marca ({@code MARKS}) ou ficam na rota ({@code BARRIER}).</p>
     *
     * @throws NgrrdClusterException {@link ErrorCode#CLOSED} se o dispatcher fechar ou a thread for interrompida
     */
    Map<String, ReleaseDrain> drainForRelease(Collection<ReleaseTicket> tickets, Duration budget) {
        long startedAt = System.nanoTime();
        long budgetNanos = Math.max(0L, budget.toNanos());
        Map<String, ReleaseDrain> outcomes = new LinkedHashMap<>();
        boolean marksReady = false;
        try {
            for (ReleaseTicket ticket : tickets) {
                SeriesRoute route = ticket.route;
                for (;;) {
                    route.lock.lock();
                    try {
                        if (closed) {
                            throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
                        }
                        if (route.completed >= ticket.boundary) {
                            marksReady |= releaseWaiters(route);
                            outcomes.put(ticket.key, harvestForRelease(route, ticket));
                            break;
                        }
                        long remaining = budgetNanos - (System.nanoTime() - startedAt);
                        if (remaining <= 0) {
                            outcomes.put(ticket.key, ReleaseDrain.timedOut(budget));
                            break;
                        }
                        scheduleFlush(route.owner, buffers.get(route.owner));
                        // Mesmo esquema do awaitBarriers: o ACK acorda na hora; o poll cobre backoff e reroteamento.
                        route.progress.awaitNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(DRAIN_POLL_MS)));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new NgrrdClusterException(ErrorCode.CLOSED, "release interrompido", e);
                    } finally {
                        route.lock.unlock();
                    }
                }
            }
        } finally {
            if (marksReady) {
                scheduleMarkCompletion();
            }
        }
        return outcomes;
    }

    /** Colhe o desfecho de uma rota que alcançou {@code boundary} (ver {@link #drainForRelease}). Chamado sob {@code route.lock}. */
    private ReleaseDrain harvestForRelease(SeriesRoute route, ReleaseTicket ticket) {
        long boundary = ticket.boundary;
        Map<SeriesStatus, Long> failed = new EnumMap<>(SeriesStatus.class);
        boolean writeFailures;
        String message;
        if (marksMode) {
            List<FailedRange> taken = new ArrayList<>();
            route.takeFailures(boundary, taken);
            for (FailedRange range : taken) {
                failed.merge(range.status(), range.to() - range.from() + 1, Long::sum);
            }
            // Falhas que outro release sobreposto da mesma rota já colheu continuam valendo para este, até a fronteira
            // dele: a marca nunca as vê, mas cada release ativo que as cobre as reporta.
            if (route.releaseHarvested != null) {
                for (FailedRange range : route.releaseHarvested) {
                    if (range.from() <= boundary) {
                        failed.merge(range.status(), Math.min(range.to(), boundary) - range.from() + 1, Long::sum);
                    }
                }
            }
            endReleaseProtection(route, ticket);
            if (route.activeReleases > 0 && !taken.isEmpty()) {
                if (route.releaseHarvested == null) {
                    route.releaseHarvested = new ArrayList<>();
                }
                route.releaseHarvested.addAll(taken);
            }
            if (isIdle(route)) {
                synchronized (pendingLock) {
                    pendingRoutes.remove(route);
                }
            }
            writeFailures = !failed.isEmpty();
            message = writeFailures ? "escritas com falha final antes do release de " + route.key + ": " + failed : null;
        } else {
            writeFailures = route.firstFailedSequence <= boundary;
            message = writeFailures ? route.failureMessage : null;
        }
        if (isLifecycleTerminal(route.failureCode)) {
            return new ReleaseDrain(ReleaseDrain.Outcome.SERIES_DELETED, failed, route.failureCode, route.failureMessage);
        }
        if (writeFailures) {
            return new ReleaseDrain(ReleaseDrain.Outcome.WRITE_FAILURES, failed, route.failureCode, message);
        }
        return ReleaseDrain.DRAINED;
    }

    /**
     * Etapa 5 do release (limpeza local): o holder do ticket deixa a rota, como em {@link #endGeneration}; sem holder,
     * a rota fica aposentada. Com {@code delivered} (o desfecho do dreno foi entregue ao chamador) e a rota sem holder,
     * liga {@code failuresDelivered}, que dispensa {@code isIdle} na remoção — exceto se houver, em {@code MARKS}, falha
     * depois da fronteira ainda não reportada (escrita de um {@code open()} que religou a rota e já a soltou). A remoção
     * segue a regra de duas fases: sem escrita em voo, sem waiter, sem redirecionamento nem consulta de dono pendentes.
     * Em {@code BARRIER}, só a falha até a fronteira foi entregue: uma primeira falha depois dela mantém a rota. Uma
     * rota religada por outro holder não é tocada além disso. Desfaz a proteção das falhas até a fronteira contra marcas
     * novas (a partir daqui, elas vão para a próxima marca, como depois de um {@code TIMED_OUT}). Nunca chamar sob
     * {@code route.lock}.
     */
    void finishRelease(ReleaseTicket ticket, boolean delivered) {
        SeriesRoute route = ticket.route;
        boolean candidate;
        route.lock.lock();
        try {
            if (ticket.holder != null && route.holders.remove(ticket.holder) && route.holders.isEmpty()) {
                route.retired = true;
            }
            boolean onlyDeliveredFailures = marksMode ? route.unreportedFailures.isEmpty()
                    : route.firstFailedSequence <= ticket.boundary || route.firstFailedSequence == Long.MAX_VALUE;
            if (delivered && route.holders.isEmpty() && onlyDeliveredFailures) {
                route.failuresDelivered = true;
            }
            endReleaseProtection(route, ticket);
            candidate = retirable(route);
        } finally {
            route.lock.unlock();
        }
        if (candidate) {
            tryRemove(route);
        }
    }

    /**
     * O release do ticket deixa de proteger as falhas da rota contra marcas novas, uma única vez por ticket (na
     * colheita ou no {@link #finishRelease}). A proteção da rota — e a lista das falhas já colhidas por releases
     * sobrepostos — só sai quando o último release ativo termina. Chamado sob {@code route.lock}.
     */
    private static void endReleaseProtection(SeriesRoute route, ReleaseTicket ticket) {
        if (!ticket.protecting) {
            return;
        }
        ticket.protecting = false;
        if (--route.activeReleases == 0) {
            route.releaseBoundary = NO_RELEASE;
            route.releaseHarvested = null;
        }
    }

    private Map<SeriesRoute, Long> snapshotBarriers(String owner) {
        Map<SeriesRoute, Long> barriers = new LinkedHashMap<>();
        for (SeriesRoute route : snapshotPendingRoutes()) {
            route.lock.lock();
            try {
                if (owner == null || route.destinations.contains(owner)) {
                    barriers.put(route, route.submitted);
                }
            } finally {
                route.lock.unlock();
            }
        }
        return barriers;
    }

    private List<SeriesRoute> snapshotPendingRoutes() {
        synchronized (pendingLock) {
            return List.copyOf(pendingRoutes);
        }
    }

    /**
     * Espera cada rota alcançar a fronteira. {@code BARRIER}: lança na primeira falha de escrita coberta.
     * {@code MARKS}: falha de escrita não lança (vai para as marcas); só uma barreira por série
     * ({@code perSeries}) lança se a série estiver terminal ({@code SERIES_DELETED}/{@code QUARANTINED}).
     */
    private void awaitBarriers(Map<SeriesRoute, Long> barriers, Duration maxWait, boolean perSeries) {
        long startedAt = System.nanoTime();
        long budgetNanos = Math.max(0L, maxWait.toNanos());
        for (Map.Entry<SeriesRoute, Long> entry : barriers.entrySet()) {
            SeriesRoute route = entry.getKey();
            for (;;) {
                route.lock.lock();
                try {
                    if (marksMode) {
                        if (perSeries && isLifecycleTerminal(route.failureCode)) {
                            throw new NgrrdClusterException(route.failureCode, route.failureMessage);
                        }
                    } else if (route.firstFailedSequence <= entry.getValue()) {
                        throw new NgrrdClusterException(route.failureCode, route.failureMessage);
                    }
                    if (route.completed >= entry.getValue()) {
                        break;
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new NgrrdClusterException(ErrorCode.CLOSED, "flush interrompido");
                    }
                    long remaining = budgetNanos - (System.nanoTime() - startedAt);
                    if (remaining <= 0) {
                        throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                                "escritas anteriores ao flush não confirmadas dentro de " + maxWait);
                    }
                    scheduleFlush(route.owner, buffers.get(route.owner));
                    // ACK/error signals wake waiters immediately. Keep a bounded retry
                    // fallback for transport backoff/rerouting when no final ACK exists yet.
                    // await releases route.lock atomically, so an ACK cannot be missed.
                    route.progress.awaitNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(DRAIN_POLL_MS)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "flush interrompido", e);
                } finally {
                    route.lock.unlock();
                }
            }
        }
    }

    @Override
    public void close() {
        close(Duration.ofMillis(closeTimeoutMillis));
    }

    /**
     * Como {@link #close()}, mas com um orçamento TOTAL explícito em vez do {@code closeTimeout} do
     * próprio dispatcher — usado por {@code DefaultNgrrdClusterClient.close()} para que handles e
     * dispatcher compartilhem um único deadline (B1, achado do Refuter), em vez de cada fase do
     * fechamento renovar o {@code closeTimeout} inteiro por conta própria.
     */
    public void close(Duration budget) {
        if (closed) {
            return;
        }
        closed = true;
        tickThread.interrupt();
        for (NodeBuffer buf : buffers.values()) {
            buf.lock.lock();
            try {
                buf.notFull.signalAll();
            } finally {
                buf.lock.unlock();
            }
        }

        long budgetMillis = Math.max(0L, budget.toMillis());
        long deadline = clock.millis() + budgetMillis;
        while (clock.millis() < deadline && hasOutstandingWrites()) {
            for (Map.Entry<String, NodeBuffer> entry : buffers.entrySet()) {
                scheduleFlush(entry.getKey(), entry.getValue());
            }
            sleepQuietly(DRAIN_POLL_MS);
        }

        settleMarksOnClose();
        recoveryPool.shutdownNow();
        ownerLookupPool.shutdownNow();
        flushPool.shutdown();
        try {
            if (!flushPool.awaitTermination(2, TimeUnit.SECONDS)) {
                flushPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flushPool.shutdownNow();
        }
        try {
            // shutdownNow já interrompeu a consulta em andamento; espera ela soltar antes do descarte final.
            ownerLookupPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        markCompletionPool.shutdown();
        try {
            tickThread.join(1_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        drained = true;

        for (Map.Entry<String, NodeBuffer> entry : buffers.entrySet()) {
            NodeBuffer buf = entry.getValue();
            int remaining;
            buf.lock.lock();
            try {
                remaining = buf.queue.size();
                buf.queue.clear();
            } finally {
                buf.lock.unlock();
            }
            if (remaining > 0) {
                samplesFailedCount.add(remaining);
                LOGGER.log(Level.SEVERE, remaining + " amostra(s) pendente(s) para " + entry.getKey()
                        + " descartada(s) ao fechar o dispatcher (orçamento de " + budgetMillis + " ms esgotado)");
            }
        }
    }

    private static NgrrdClusterException closedRoute(String seriesKey) {
        return new RouteClosedException(seriesKey);
    }

    /** Se há rota de escrita para {@code seriesKey} — visível para testes. */
    boolean hasRoute(String seriesKey) {
        return routes.containsKey(seriesKey);
    }

    /** Se sobrou estado de retentativa de {@code seriesKey} (rate limit de log ou tentativas num buffer) — visível para testes. */
    boolean hasRetryState(String seriesKey) {
        if (lastRetryLogMs.containsKey(seriesKey)) {
            return true;
        }
        for (NodeBuffer buf : buffers.values()) {
            buf.lock.lock();
            try {
                if (buf.queue.hasAttempts(seriesKey)) {
                    return true;
                }
            } finally {
                buf.lock.unlock();
            }
        }
        return false;
    }

    /** Holders da rota de {@code seriesKey} ({@code 0} sem rota) — visível para testes. */
    int routeHolders(String seriesKey) {
        SeriesRoute route = routes.get(seriesKey);
        if (route == null) {
            return 0;
        }
        route.lock.lock();
        try {
            return route.holders.size();
        } finally {
            route.lock.unlock();
        }
    }

    /** Waiters de marca registrados na rota de {@code seriesKey} — visível para testes. */
    int markWaiters(String seriesKey) {
        SeriesRoute route = routes.get(seriesKey);
        if (route == null) {
            return 0;
        }
        route.lock.lock();
        try {
            return route.waiters.size();
        } finally {
            route.lock.unlock();
        }
    }

    /** Total de amostras enfileiradas desde a criação deste dispatcher. */
    public long samplesEnqueued() {
        return samplesEnqueuedCount.sum();
    }

    /** Total de amostras confirmadas ({@code OK}) por um dono. */
    public long samplesSent() {
        return samplesSentCount.sum();
    }

    /** Total de amostras descartadas ({@code ERROR} do dono, ou pendências não drenadas a tempo no {@code close()}). */
    public long samplesFailed() {
        return samplesFailedCount.sum();
    }

    /** Total de lotes {@code WRITE_BATCH} efetivamente enviados (sucesso de transporte). */
    public long batchesSent() {
        return batchesSentCount.sum();
    }

    /** Consultas ao líder feitas para desempatar dicas de dono contraditórias (#177). */
    public long ownerLookups() {
        return ownerLookupsCount.sum();
    }

    /** Dicas de {@code WRONG_OWNER} classificadas como contraditórias (ciclo, auto-redirecionamento, excesso de saltos). */
    public long redirectCycles() {
        return redirectCyclesCount.sum();
    }

    /** Rotas de escrita no mapa agora (uma por série com handle gravável ou escrita sem desfecho). */
    public int routes() {
        return routes.size();
    }

    /** Rotas de escrita removidas do mapa desde a criação deste dispatcher (sem holder e ociosas). */
    public long routesRemoved() {
        return routesRemovedCount.sum();
    }

    /** Retentativas observadas, agrupadas pelo {@link SeriesStatus} que as motivou. */
    public Map<SeriesStatus, Long> retriesByStatus() {
        return retryCounts.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sum()));
    }

    /** Amostras atualmente no buffer de cada nó de destino conhecido. */
    public Map<String, Long> bufferedSamples() {
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<String, NodeBuffer> entry : buffers.entrySet()) {
            NodeBuffer buf = entry.getValue();
            buf.lock.lock();
            try {
                result.put(entry.getKey(), (long) buf.queue.size());
            } finally {
                buf.lock.unlock();
            }
        }
        return result;
    }

    private void tickLoop() {
        long tickCount = 0L;
        while (!closed) {
            try {
                Thread.sleep(batchMaxDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            for (Map.Entry<String, NodeBuffer> entry : buffers.entrySet()) {
                NodeBuffer buf = entry.getValue();
                if (hasQueued(buf)) {
                    scheduleFlush(entry.getKey(), buf);
                }
            }
            tickCount++;
            if (metricsListener != null && metricsSupplier != null && tickCount % METRICS_TICK_INTERVAL == 0) {
                publishMetricsQuietly();
            }
        }
    }

    /**
     * Notifica {@link #metricsListener}; uma falha do listener do chamador não derruba o tickLoop.
     *
     * <p>M3 (achado do Refuter): captura {@link Throwable}, não só {@link RuntimeException} — mesmo
     * raciocínio de {@code NodeStatusReporter.tick()}. {@code tickLoop} chama este método direto, sem
     * outro try/catch em volta; um {@link Error} do listener (ex.: um bug num exporter de métricas de
     * terceiros) escaparia daqui, terminaria a thread {@code ngrrd-write-dispatcher} sem aviso, e o
     * dispatcher pararia de drenar batches por tempo (tick) para sempre — silenciosamente, até o
     * próximo {@code close()}.</p>
     */
    private void publishMetricsQuietly() {
        try {
            metricsListener.onClientMetrics(metricsSupplier.get());
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "Falha ao publicar métricas do cliente via NgrrdClusterMetricsListener", e);
        }
    }

    private void scheduleFlush(String owner, NodeBuffer buf) {
        // Sem checagem de `closed` aqui de propósito: close() marca `closed=true` antes de drenar
        // as pendências (dentro do próprio prazo de closeTimeout) — um guard aqui impediria
        // exatamente o flush final que close() precisa disparar. O pool só é encerrado depois
        // desse dreno (ver close()), então agendar continua seguro até lá.
        if (buf == null || flushPool.isShutdown() || !buf.inFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            flushPool.execute(() -> {
                try { drainLoop(owner, buf); }
                finally { buf.inFlight.set(false); }
            });
        } catch (RejectedExecutionException closing) {
            buf.inFlight.set(false);
        }
    }

    private void drainLoop(String owner, NodeBuffer buf) {
        for (;;) {
            if (clock.millis() < buf.backoffUntilMs) {
                return;
            }
            List<SeriesWrite> batch = takeBatch(buf);
            if (batch.isEmpty()) {
                return;
            }
            sendBatch(owner, buf, batch);
        }
    }

    private List<SeriesWrite> takeBatch(NodeBuffer buf) {
        buf.lock.lock();
        try {
            int n = Math.min(batchMaxSamples, buf.queue.size());
            if (n == 0) {
                return List.of();
            }
            List<SeriesWrite> batch = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                SeriesWrite write = buf.queue.pollFirst(clock.millis());
                if (write == null) { break; }
                SeriesRoute current = routes.get(write.seriesKey());
                if (current != null && Objects.equals(current.generation, write.generationId())) batch.add(write);
            }
            buf.notFull.signalAll();
            return batch;
        } finally {
            buf.lock.unlock();
        }
    }

    private void sendBatch(String owner, NodeBuffer buf, List<SeriesWrite> batch) {
        WriteBatchResponse response;
        try {
            response = rpc.call(NodeId.of(owner), Commands.WRITE_BATCH, new WriteBatchRequest(batch),
                    WriteBatchResponse.class);
        } catch (NgrrdClusterException e) {
            requeueFront(buf, batch);
            buf.backoffUntilMs = clock.millis() + retryPolicy.backoffFor(buf.nextBackoffAttempt()).toMillis();
            LOGGER.log(Level.WARNING, "Falha de transporte ao enviar WRITE_BATCH para " + owner, e);
            return;
        }
        batchesSentCount.increment();
        buf.resetBackoffAttempts();

        Map<String, List<SeriesWrite>> bySeries = batch.stream()
                .collect(Collectors.groupingBy(SeriesWrite::seriesKey, LinkedHashMap::new, Collectors.toList()));
        for (Map.Entry<String, List<SeriesWrite>> entry : bySeries.entrySet()) {
            applyStatus(owner, buf, entry.getKey(), entry.getValue(), response);
        }
    }

    private void applyStatus(String owner, NodeBuffer buf, String seriesKey, List<SeriesWrite> writes,
            WriteBatchResponse response) {
        SeriesRoute currentRoute = routes.get(seriesKey);
        if (currentRoute == null) return;
        // Fase 2 da remoção e aviso de série terminal: só depois de soltar o route.lock (ordem bin → route.lock;
        // o aviso pode fechar o handle, que libera a rota).
        boolean retire = false;
        NgrrdClusterException terminal = null;
        currentRoute.lock.lock();
        try {
        if (currentRoute.detached || routes.get(seriesKey) != currentRoute
                || !Objects.equals(currentRoute.generation, writes.get(0).generationId())) return;
        SeriesStatus status = response.statusBySeries().getOrDefault(seriesKey, SeriesStatus.ERROR);
        switch (status) {
            case OK -> {
                buf.lock.lock();
                try { buf.queue.succeeded(seriesKey); }
                finally { buf.lock.unlock(); }
                samplesSentCount.add(writes.size());
                retire = completeWrites(currentRoute, writes.size(), null, SeriesStatus.OK);
            }
            case WRONG_OWNER -> {
                recordRetry(SeriesStatus.WRONG_OWNER);
                String newOwner = response.ownerBySeries().get(seriesKey);
                if (newOwner != null) {
                    currentRoute.lock.unlock();
                    try { followOwnerHint(owner, buf, seriesKey, writes, newOwner, currentRoute); }
                    finally { currentRoute.lock.lock(); }
                } else {
                    // "Não sei de quem é" (resposta sem ownerBySeries) NÃO é o mesmo que "sei que não é
                    // meu": pode ser exatamente este nó, só que com a réplica local do catálogo ainda
                    // vazia (ex.: logo após um restart). Invalida o placement conhecido e força uma
                    // reabertura pelo MESMO caminho do NOT_OPEN — só recorre a backoff se a reabertura
                    // falhar; se funcionar, reenvia imediato (o dono provável já está pronto).
                    logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER,
                            "dono desconhecido — invalidando placement e reabrindo");
                    placementLookup.invalidate(seriesKey);
                    reopenAsync(owner, buf, currentRoute, writes);
                }
            }
            case NOT_OPEN -> {
                recordRetry(SeriesStatus.NOT_OPEN);
                logRetryRateLimited(seriesKey, SeriesStatus.NOT_OPEN, "reabrindo via reopener");
                reopenAsync(owner, buf, currentRoute, writes);
            }
            case MIGRATING -> {
                recordRetry(SeriesStatus.MIGRATING);
                logRetryRateLimited(seriesKey, SeriesStatus.MIGRATING, "aguardando fim da migração");
                requeueFrontAt(owner, writes);
                deferSeries(buf, seriesKey, -1);
            }
            case SERIES_DELETED, QUARANTINED -> {
                ErrorCode code = status == SeriesStatus.SERIES_DELETED ? ErrorCode.SERIES_DELETED : ErrorCode.QUARANTINED;
                String message = response.errorBySeries().getOrDefault(seriesKey, status + ": " + seriesKey);
                currentRoute.failureCode = code;
                if (marksMode) currentRoute.failureMessage = message;
                samplesFailedCount.add(writes.size());
                retire = completeWrites(currentRoute, writes.size(), message, status);
                terminal = new NgrrdClusterException(code, message);
            }
            case ERROR -> {
                recordRetry(SeriesStatus.ERROR);
                samplesFailedCount.add(writes.size());
                retire = completeWrites(currentRoute, writes.size(), "WRITE_BATCH falhou para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey), SeriesStatus.ERROR);
                LOGGER.warning("WRITE_BATCH respondeu ERROR para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey));
            }
            default -> {
                recordRetry(status);
                samplesFailedCount.add(writes.size());
                retire = completeWrites(currentRoute, writes.size(), "WRITE_BATCH respondeu " + status + " para " + seriesKey,
                        status);
                LOGGER.warning("WRITE_BATCH respondeu status inesperado " + status + " para " + seriesKey);
            }
        }
        } finally { currentRoute.lock.unlock(); }
        if (terminal != null) {
            terminalListener.accept(writes.get(0), terminal);
        }
        if (retire) {
            tryRemove(currentRoute);
        }
    }

    /**
     * {@code WRONG_OWNER} com dono informado ({@code newOwner}) para escritas enviadas a {@code owner}.
     *
     * <p>Dica plausível: reroteia a série (com o backlog dela) para {@code newOwner}, com backoff exponencial
     * por série ({@code redirectAttempts}, que atravessa os nós; o 1º salto espera {@code backoffMin}, como
     * antes). Dica contraditória (#177 — ver {@link #isContradictory}): mantém as escritas na frente do
     * buffer atual, pausa só esta série e agenda a confirmação do dono no líder ({@link #scheduleOwnerLookup}).
     * Isso também encerra o laço quente de um nó que aponta a si mesmo.</p>
     */
    private void followOwnerHint(String owner, NodeBuffer buf, String seriesKey, List<SeriesWrite> writes,
            String newOwner, SeriesRoute route) {
        boolean contradictory;
        boolean lookup = false;
        long delayMs;
        String visited;
        route.lock.lock();
        try {
            if (route.detached || routes.get(seriesKey) != route) return;
            contradictory = isContradictory(route, owner, newOwner);
            route.redirectAttempts++;
            route.redirectedBy.add(owner);
            route.lastHint = newOwner;
            visited = String.valueOf(route.redirectedBy);
            if (contradictory) {
                buf.lock.lock();
                try {
                    for (int i = writes.size() - 1; i >= 0; i--) { buf.queue.addFirst(writes.get(i)); }
                    // Pausada até a resposta do líder, como numa reabertura (reopenAsync).
                    buf.queue.defer(seriesKey, Long.MAX_VALUE);
                } finally { buf.lock.unlock(); }
                if (!route.ownerLookupPending) {
                    route.ownerLookupPending = true;
                    lookup = true;
                }
                delayMs = 0L;
            } else {
                // Publish the new route only after its older writes are queued. New
                // admissions (including callers with a stale owner) cannot overtake them.
                delayMs = rerouteLocked(route, buf, seriesKey, writes, newOwner,
                        retryPolicy.backoffFor(route.redirectAttempts).toMillis());
                placementLookup.noteOwner(seriesKey, newOwner);
                route.redirectPending = true;

            }
        } finally {
            route.lock.unlock();
        }
        if (contradictory) {
            redirectCyclesCount.increment();
            logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER, "dicas de dono contraditórias ("
                    + visited + " → " + newOwner + "); consultando o líder");
            if (lookup) {
                scheduleOwnerLookup(seriesKey);
            }
            return;
        }
        try {
            if (routes.get(seriesKey) == route) notifyOwner(seriesKey, newOwner, route);
        } finally {
            route.lock.lock();
            try { route.redirectPending = false; route.progress.signalAll(); }
            finally { route.lock.unlock(); }
        }
        logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER, "novo dono informado: " + newOwner);
        // O dono novo tem seu próprio NodeBuffer, fora do drainLoop atual (que só itera o buffer de
        // `owner`) — sem acionar o flush dele aqui, as amostras reenfileiradas só seriam enviadas no
        // próximo tick (até batchMaxDelay depois). O atraso nunca é zero: dois nós que discordam sobre o
        // dono reenviariam um para o outro sem pausa; e cresce a cada salto do episódio.
        triggerRetryAfter(newOwner, Duration.ofMillis(delayMs));
    }

    /**
     * Uma dica de dono é contraditória quando aponta o próprio nó que respondeu, um nó que já redirecionou
     * a série neste episódio, diverge do dono que o líder confirmou neste episódio, ou chega depois de
     * {@value #MAX_REDIRECT_HOPS} saltos. Chamado sob {@code route.lock}.
     */
    private static boolean isContradictory(SeriesRoute route, String owner, String newOwner) {
        return newOwner.equals(owner)
                || route.redirectedBy.contains(newOwner)
                || (route.confirmedOwner != null && !newOwner.equals(route.confirmedOwner))
                || route.redirectAttempts >= MAX_REDIRECT_HOPS;
    }

    /**
     * Move a série de {@code fromBuf} para o buffer de {@code newOwner} (que deve ser outro nó): primeiro
     * {@code frontWrites}, depois todo o backlog da série que ainda estava em {@code fromBuf}, na ordem. O
     * buffer de destino herda as tentativas da série (o backoff de {@code MIGRATING}/reabertura não zera a
     * cada salto) e a pausa por {@code delayMs} ({@code < 0}: backoff por série de {@code MIGRATING}).
     * Só então publica a rota nova. Chamado sob {@code route.lock}; devolve o atraso aplicado.
     */
    private long rerouteLocked(SeriesRoute route, NodeBuffer fromBuf, String seriesKey, List<SeriesWrite> frontWrites,
            String newOwner, long delayMs) {
        Extracted extracted = extractSeriesFrom(fromBuf, seriesKey);
        List<SeriesWrite> reordered = new ArrayList<>(frontWrites.size() + extracted.writes().size());
        reordered.addAll(frontWrites);
        reordered.addAll(extracted.writes());
        NodeBuffer target = buffers.computeIfAbsent(newOwner, id -> new NodeBuffer());
        long applied;
        target.lock.lock();
        try {
            requeueFront(target, reordered);
            target.queue.carryAttempts(seriesKey, extracted.attempts());
            applied = deferSeries(target, seriesKey, delayMs);
        } finally { target.lock.unlock(); }
        route.owner = newOwner;
        route.destinations.add(newOwner);
        return applied;
    }

    /** Enfileira {@code seriesKey} para a consulta coalescida ao líder e garante uma tarefa de consulta ativa. */
    private void scheduleOwnerLookup(String seriesKey) {
        ownerLookupQueue.add(seriesKey);
        startOwnerLookupTask();
    }

    private void startOwnerLookupTask() {
        if (!ownerLookupRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            ownerLookupPool.execute(this::runOwnerLookups);
        } catch (RejectedExecutionException closing) {
            ownerLookupRunning.set(false);
            for (String seriesKey; (seriesKey = ownerLookupQueue.poll()) != null; ) {
                releaseWithoutLookup(seriesKey);
            }
        }
    }

    private void runOwnerLookups() {
        try {
            for (;;) {
                Set<String> keys = new LinkedHashSet<>();
                for (String seriesKey; (seriesKey = ownerLookupQueue.poll()) != null; ) {
                    keys.add(seriesKey);
                }
                if (keys.isEmpty()) {
                    return;
                }
                resolveOwnersAtLeader(keys);
            }
        } finally {
            ownerLookupRunning.set(false);
            // Uma chave enfileirada entre o último poll e o set(false) não pode ficar órfã.
            if (!ownerLookupQueue.isEmpty()) {
                startOwnerLookupTask();
            }
        }
    }

    /**
     * Uma consulta ao líder para o lote; cada série recebe o desfecho, inclusive quando a consulta falha —
     * também por um {@link Error}: as séries drenadas da fila são liberadas antes de o {@code Error} ser
     * relançado, senão ficariam pausadas ({@code Long.MAX_VALUE}) para sempre.
     */
    private void resolveOwnersAtLeader(Set<String> keys) {
        Duration maxWait = OWNER_LOOKUP_MAX_WAIT.compareTo(retryPolicy.timeout()) < 0
                ? OWNER_LOOKUP_MAX_WAIT : retryPolicy.timeout();
        Map<String, SeriesPlacement> found = null;
        Throwable failure = null;
        Map<String, SeriesRoute> expectedRoutes = new HashMap<>();
        for (String key : keys) expectedRoutes.put(key, routes.get(key));
        ownerLookupsCount.increment();
        try {
            found = placementLookup.resolveExistingAtLeader(keys, maxWait);
            if (found == null) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "consulta ao líder sem resposta");
            }
        } catch (Throwable e) {
            failure = e;
            logOwnerLookupFailure(keys.size(), e);
        }
        Error error = failure instanceof Error lookupError ? lookupError : null;
        for (String seriesKey : keys) {
            try {
                applyOwnerLookup(seriesKey, found, failure, expectedRoutes.get(seriesKey));
            } catch (Throwable e) {
                LOGGER.log(Level.WARNING, "Falha ao aplicar o dono confirmado pelo líder para " + seriesKey, e);
                if (error == null && e instanceof Error applyError) {
                    error = applyError;
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }

    /**
     * WARNING no máximo uma vez a cada {@value #RETRY_LOG_INTERVAL_MS} ms para todo o dispatcher (um líder
     * sem {@code catalog.lookup} faria cada contradição falhar); a pilha só em FINE.
     */
    private void logOwnerLookupFailure(int seriesCount, Throwable failure) {
        long now = clock.millis();
        long last = lastOwnerLookupFailureLogMs.get();
        if ((last == Long.MIN_VALUE || now - last >= RETRY_LOG_INTERVAL_MS)
                && lastOwnerLookupFailureLogMs.compareAndSet(last, now)) {
            LOGGER.warning("Falha ao confirmar no líder o dono de " + seriesCount + " série(s) com dicas de"
                    + " WRONG_OWNER contraditórias; seguindo a última dica com backoff: " + failure);
        }
        LOGGER.log(Level.FINE, "Falha na consulta de dono ao líder", failure);
    }

    /**
     * Aplica a resposta do líder a uma série pausada por dicas contraditórias: {@code ACTIVE(X)} reroteia
     * para X e fixa X como dono confirmado do episódio; {@code MIGRATING} vai para a origem com o backoff
     * de {@code MIGRATING}; ausente invalida o placement e reabre. Falha da consulta segue a última dica
     * recebida, com o backoff por série: sem autoridade, ficar no nó que deu a dica contraditória prenderia a
     * série ali para sempre se ele não for o dono (ex.: o dono real com réplica atrasada, que depois se
     * atualiza). Cada nova contradição volta a consultar o líder, que desempata assim que responder.
     * Depois do descarte final do {@code close()}, não faz nada além de liberar a flag de consulta.
     */
    private void applyOwnerLookup(String seriesKey, Map<String, SeriesPlacement> found, Throwable failure, SeriesRoute route) {
        if (route == null) {
            return;
        }
        String flushOwner;
        NodeBuffer reopenBuf = null;
        long delayMs = 0L;
        String changedOwner = null;
        boolean retire;
        route.lock.lock();
        try {
            if (route.detached || routes.get(seriesKey) != route || !route.ownerLookupPending) return;
            if (drained) {
                route.ownerLookupPending = false;
                return;
            }
            try {
                String current = route.owner;
                NodeBuffer currentBuf = buffers.computeIfAbsent(current, id -> new NodeBuffer());
                long backoffMs = retryPolicy.backoffFor(route.redirectAttempts).toMillis();
                SeriesPlacement placement = failure == null ? found.get(seriesKey) : null;
                flushOwner = current;
                if (failure != null) {
                    String hint = route.lastHint;
                    if (hint == null || hint.equals(current)) {
                        delayMs = deferSeries(currentBuf, seriesKey, backoffMs);
                    } else {
                        delayMs = rerouteLocked(route, currentBuf, seriesKey, List.of(), hint, backoffMs);
                        placementLookup.noteOwner(seriesKey, hint);
                        flushOwner = hint;
                        changedOwner = hint;
                    }
                } else if (placement == null) {
                    placementLookup.invalidate(seriesKey);
                    reopenBuf = currentBuf;
                } else if (placement.state() == PlacementState.ACTIVE) {
                    String confirmed = placement.ownerNodeId();
                    route.confirmedOwner = confirmed;
                    route.redirectedBy.clear();
                    delayMs = confirmed.equals(current) ? deferSeries(currentBuf, seriesKey, backoffMs)
                            : rerouteLocked(route, currentBuf, seriesKey, List.of(), confirmed, backoffMs);
                    flushOwner = confirmed;
                    changedOwner = confirmed;
                } else {
                    // MIGRATING: a origem responde MIGRATING até o fim da cópia (backoff de MIGRATING por série).
                    String source = placement.ownerNodeId();
                    delayMs = source.equals(current) ? deferSeries(currentBuf, seriesKey, -1)
                            : rerouteLocked(route, currentBuf, seriesKey, List.of(), source, -1);
                    flushOwner = source;
                    changedOwner = source;
                }
            } catch (Throwable e) {
                // Nunca deixa a série pausada para sempre (Long.MAX_VALUE) por uma falha inesperada aqui.
                NodeBuffer buf = buffers.get(route.owner);
                if (buf != null) {
                    deferSeries(buf, seriesKey, -1);
                }
                throw e;
            } finally {
                route.ownerLookupPending = false;
            }
            retire = retirable(route);
            if (changedOwner != null) {
                notifyOwner(seriesKey, changedOwner, route);
            }
        } finally {
            route.lock.unlock();
        }
        if (retire && tryRemove(route)) {
            // Sem escrita a reenviar: nada a reabrir nem a acordar para uma rota que saiu do mapa.
            return;
        }
        if (reopenBuf != null) {
            logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER, "líder não conhece a série — reabrindo");
            reopenAsync(flushOwner, reopenBuf, route, List.of());
        } else {
            triggerRetryAfter(flushOwner, Duration.ofMillis(delayMs));
        }
    }

    /** Fechamento em curso: libera a série pausada sem consulta, com o backoff de {@code MIGRATING}. */
    private void releaseWithoutLookup(String seriesKey) {
        SeriesRoute route = routes.get(seriesKey);
        if (route == null) {
            return;
        }
        boolean retire;
        route.lock.lock();
        try {
            route.ownerLookupPending = false;
            NodeBuffer buf = buffers.get(route.owner);
            if (buf != null) {
                deferSeries(buf, seriesKey, -1);
            }
            retire = retirable(route);
        } finally {
            route.lock.unlock();
        }
        if (retire) {
            tryRemove(route);
        }
    }

    // FIFO admission/rerouting ensures completions form a prefix of each series. A failed
    // prefix remains observable: a later checkpoint cannot certify those lost samples.
    // BARRIER: until restart (or a release that delivers it). MARKS: until a mark reports it.
    // Returns whether the route became retirable (phase 1); the caller removes it after releasing route.lock.
    private boolean completeWrites(SeriesRoute route, int count, String failure, SeriesStatus status) {
        boolean marksReady;
        boolean retire;
        route.lock.lock();
        try {
            if (failure != null) {
                if (marksMode) {
                    route.recordFailure(route.completed + 1, route.completed + count, status);
                } else if (route.firstFailedSequence == Long.MAX_VALUE) {
                    route.firstFailedSequence = route.completed + 1;
                    route.failureMessage = failure;
                }
            }
            route.completed += count;
            if (failure == null) {
                // OK do dono: o episódio de redirecionamento (#177) terminou.
                route.resetRedirectEpisode();
            }
            marksReady = releaseWaiters(route);
            if (isIdle(route)) {
                synchronized (pendingLock) {
                    pendingRoutes.remove(route);
                }
            }
            // Failed routes stay indexed: subsequent successful writes cannot erase
            // an earlier failure and allow a Kafka commit to certify a lost prefix.
            route.progress.signalAll();
            retire = retirable(route);
        } finally {
            route.lock.unlock();
        }
        // Still possibly under the caller's route.lock (applyStatus): completion is asynchronous.
        if (marksReady) {
            scheduleMarkCompletion();
        }
        return retire;
    }

    /**
     * Rota sem escrita em voo e sem falha a reportar: fora do índice de pendentes. {@code BARRIER}: uma falha
     * mantém a rota indexada para sempre; {@code MARKS}: até uma marca colhê-la. Chamado sob {@code route.lock}.
     */
    private boolean isIdle(SeriesRoute route) {
        return route.submitted == route.completed
                && (marksMode ? route.unreportedFailures.isEmpty() : route.firstFailedSequence == Long.MAX_VALUE);
    }

    private static boolean isLifecycleTerminal(ErrorCode code) {
        return code == ErrorCode.SERIES_DELETED || code == ErrorCode.QUARANTINED;
    }

    /**
     * Libera os waiters cuja fronteira já foi alcançada, colhendo as falhas até cada fronteira. Chamado sob
     * {@code route.lock}; devolve {@code true} se alguma marca ficou pronta.
     */
    private static boolean releaseWaiters(SeriesRoute route) {
        boolean ready = false;
        while (!route.waiters.isEmpty() && route.waiters.peekFirst().boundary() <= route.completed) {
            MarkWaiter waiter = route.waiters.pollFirst();
            route.collectFailures(waiter.mark(), waiter.boundary());
            ready |= waiter.mark().release();
        }
        return ready;
    }

    /** Garante uma drenagem das marcas prontas, sem segurar lock (ver {@link #markCompletionPool}). */
    private void scheduleMarkCompletion() {
        if (!markCompletionRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            markCompletionPool.execute(this::completeReadyMarks);
        } catch (RejectedExecutionException closing) {
            // close() conclui ou falha as marcas restantes em settleMarksOnClose().
            markCompletionRunning.set(false);
        }
    }

    private void completeReadyMarks() {
        try {
            for (Mark mark; (mark = pollReadyMark()) != null; ) {
                mark.complete();
            }
        } finally {
            markCompletionRunning.set(false);
            // A mark released between the last poll and set(false) cannot be left behind.
            if (peekReadyMark()) {
                scheduleMarkCompletion();
            }
        }
    }

    private Mark pollReadyMark() {
        markLock.lock();
        try {
            Mark head = openMarks.peekFirst();
            return head != null && head.released() ? openMarks.pollFirst() : null;
        } finally {
            markLock.unlock();
        }
    }

    private boolean peekReadyMark() {
        markLock.lock();
        try {
            Mark head = openMarks.peekFirst();
            return head != null && head.released();
        } finally {
            markLock.unlock();
        }
    }

    /**
     * Fim do dreno do {@code close()}: conclui, em ordem, as marcas já liberadas e falha, a partir da primeira
     * não liberada, todas as seguintes com {@link ErrorCode#CLOSED} — uma marca posterior pode estar liberada
     * só porque deixou à anterior a espera de uma rota com a mesma fronteira. Fora de qualquer lock interno.
     */
    private void settleMarksOnClose() {
        // Takes the drain flag for good (scheduleMarkCompletion becomes a no-op): an in-flight drain must
        // finish its mark before later marks are settled here, or results would leave out of order.
        long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!markCompletionRunning.compareAndSet(false, true) && System.nanoTime() < waitUntil
                && !Thread.currentThread().isInterrupted()) {
            sleepQuietly(1L);
        }
        List<Mark> pending;
        markLock.lock();
        try {
            pending = new ArrayList<>(openMarks);
            openMarks.clear();
        } finally {
            markLock.unlock();
        }
        boolean failing = false;
        for (Mark mark : pending) {
            failing |= !mark.released();
            if (!failing) {
                mark.complete();
            } else {
                mark.fail(new NgrrdClusterException(ErrorCode.CLOSED,
                        "cliente fechado antes da conclusão da marca " + mark.id()));
            }
        }
    }

    private boolean hasOutstandingWrites() {
        // Shutdown also synchronizes with an admission that passed the closed check
        // but has not entered the pending index yet. Keep the history scan off the
        // hot path, here only, so close cannot overlook that in-progress admission.
        for (SeriesRoute route : routes.values()) {
            route.lock.lock();
            try {
                if (route.submitted > route.completed) {
                    return true;
                }
            } finally {
                route.lock.unlock();
            }
        }
        return false;
    }

    private void requeueFront(NodeBuffer buf, List<SeriesWrite> writes) {
        if (writes.isEmpty()) {
            return;
        }
        buf.lock.lock();
        try {
            for (int i = writes.size() - 1; i >= 0; i--) {
                buf.queue.addFirst(writes.get(i));
            }
        } finally {
            buf.lock.unlock();
        }
    }

    /**
     * B4 (achado do Refuter): remove de {@code buf}, na ordem em que estão, TODAS as escritas de
     * {@code seriesKey} ainda enfileiradas — usado ao rerotear uma série por {@code WRONG_OWNER} para
     * levar junto qualquer backlog da MESMA série que ainda estivesse atrás na fila do dono antigo, em
     * vez de deixá-lo ser enviado depois ao dono errado.
     */
    private static Extracted extractSeriesFrom(NodeBuffer buf, String seriesKey) {
        buf.lock.lock();
        try {
            Extracted extracted = buf.queue.extract(seriesKey);
            if (!extracted.writes().isEmpty()) {
                buf.notFull.signalAll();
            }
            return extracted;
        } finally {
            buf.lock.unlock();
        }
    }

    private void requeueFrontAt(String ownerNodeId, List<SeriesWrite> writes) {
        if (writes.isEmpty()) {
            return;
        }
        NodeBuffer target = buffers.computeIfAbsent(ownerNodeId, id -> new NodeBuffer());
        requeueFront(target, writes);
    }

    // Only this series waits. Node-wide backoff is reserved for transport failures.
    // Returns the applied delay (delayMillis < 0: per-series exponential backoff).
    private long deferSeries(NodeBuffer buf, String key, long delayMillis) {
        buf.lock.lock();
        try {
            long delay = delayMillis >= 0 ? delayMillis
                    : retryPolicy.backoffFor(buf.queue.nextAttempt(key)).toMillis();
            buf.queue.defer(key, clock.millis() + delay);
            return delay;
        } finally { buf.lock.unlock(); }
    }

    /**
     * Pausa a série de {@code expected} em {@code buf} (com {@code writes} na frente) até a reabertura terminar. A
     * pausa sem prazo só é aplicada a uma rota ainda no mapa, sob o lock dela: uma rota destacada (substituída ou
     * removida) não pausa a chave, que já pode ser de outra geração. Ao terminar, se a rota tiver saído do mapa
     * nesse meio tempo, desfaz só a própria pausa (a da geração nova, se houver, fica).
     */
    private void reopenAsync(String owner, NodeBuffer buf, SeriesRoute expected, List<SeriesWrite> writes) {
        String key = expected.key;
        PendingWrites.Delay pause;
        expected.lock.lock();
        try {
            if (!isCurrent(expected)) {
                // Desfecho já contado na troca/remoção; nada a reenviar nem a pausar.
                return;
            }
            buf.lock.lock();
            try {
                for (int i = writes.size() - 1; i >= 0; i--) { buf.queue.addFirst(writes.get(i)); }
                pause = buf.queue.pause(key, Long.MAX_VALUE);
            } finally { buf.lock.unlock(); }
        } finally { expected.lock.unlock(); }
        try {
            recoveryPool.execute(() -> {
                boolean opened = false;
                try { if (isCurrentLocked(expected)) opened = Boolean.TRUE.equals(reopener.apply(key)); }
                catch (RuntimeException e) { LOGGER.log(Level.FINE, "Reabertura pendente de " + key, e); }
                finally { finishReopen(owner, buf, expected, pause, opened); }
            });
        } catch (RejectedExecutionException closing) {
            deferSeries(buf, key, -1);
        }
    }

    /** Fim da reabertura: libera a série com o atraso devido, ou desfaz a própria pausa de uma rota que saiu do mapa. */
    private void finishReopen(String owner, NodeBuffer buf, SeriesRoute expected, PendingWrites.Delay pause,
            boolean opened) {
        boolean current;
        expected.lock.lock();
        try {
            current = isCurrent(expected);
            if (current) {
                deferSeries(buf, expected.key, opened ? 0 : -1);
            } else {
                buf.lock.lock();
                try {
                    buf.queue.resume(expected.key, pause);
                } finally { buf.lock.unlock(); }
            }
        } finally { expected.lock.unlock(); }
        scheduleFlush(owner, buf);
    }

    /** {@code route} ainda é a rota da chave no mapa. Chamado sob {@code route.lock}. */
    private boolean isCurrent(SeriesRoute route) {
        return !route.detached && routes.get(route.key) == route;
    }

    private boolean isCurrentLocked(SeriesRoute route) {
        route.lock.lock();
        try {
            return isCurrent(route);
        } finally {
            route.lock.unlock();
        }
    }

    /**
     * Agenda um flush de {@code ownerNodeId} depois de {@code delay}, sem depender do próximo tick
     * (que pode estar a até {@code batchMaxDelay} de distância). Usado para o backoff mínimo de O2 —
     * um {@code delay} pequeno não deve, na prática, virar uma espera de {@code batchMaxDelay}.
     */
    private void triggerRetryAfter(String ownerNodeId, Duration delay) {
        NodeBuffer buf = buffers.get(ownerNodeId);
        if (buf == null) {
            return;
        }
        CompletableFuture.delayedExecutor(Math.max(1L, delay.toMillis()), TimeUnit.MILLISECONDS, flushPool)
                .execute(() -> scheduleFlush(ownerNodeId, buf));
    }

    private void recordRetry(SeriesStatus status) {
        retryCounts.computeIfAbsent(status, ignored -> new LongAdder()).increment();
    }

    /**
     * Loga em WARN que {@code seriesKey} entrou em retry por {@code status}, no máximo uma vez a cada
     * {@value #RETRY_LOG_INTERVAL_MS} ms por série — visibilidade sem inundar o log numa rajada de
     * lotes da mesma série em backoff curto. Nunca mais um retry totalmente silencioso (achado do
     * Debugger: o loop de WRONG_OWNER sem dono não deixava rastro algum).
     */
    private void logRetryRateLimited(String seriesKey, SeriesStatus status, String detail) {
        long now = clock.millis();
        Long last = lastRetryLogMs.get(seriesKey);
        if (last != null && now - last < RETRY_LOG_INTERVAL_MS) {
            return;
        }
        lastRetryLogMs.put(seriesKey, now);
        LOGGER.warning("Série " + seriesKey + " em retry (" + status + "): " + detail);
    }

    private static boolean hasQueued(NodeBuffer buf) {
        buf.lock.lock();
        try {
            return !buf.queue.isEmpty();
        } finally {
            buf.lock.unlock();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Routing and admission order for one series, independent of the caller's owner hint. */
    private static final class SeriesRoute {
        private final String key;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition progress = lock.newCondition();
        private String owner;
        private final Set<String> destinations = new HashSet<>();
        private long submitted;
        private long completed;
        private long firstFailedSequence = Long.MAX_VALUE;
        private String failureMessage;
        private ErrorCode failureCode = ErrorCode.REMOTE_ERROR;
        private final String generation;
        // Redirect episode (#177), from the first WRONG_OWNER to the next OK of the series.
        /** Nós que responderam WRONG_OWNER no episódio atual, na ordem. */
        private final Set<String> redirectedBy = new LinkedHashSet<>();
        /** Saltos do episódio: expoente do backoff por série, que (ao contrário do buffer) atravessa os nós. */
        private int redirectAttempts;
        /** Dono confirmado pelo líder neste episódio; uma dica divergente dele é contraditória. */
        private String confirmedOwner;
        /** Série pausada aguardando a consulta ao líder ({@code scheduleOwnerLookup}). */
        private boolean ownerLookupPending;
        private boolean redirectPending;
        /** Última dica de dono recebida no episódio; seguida quando a consulta ao líder falha. */
        private String lastHint;
        /** MARKS: faixas de sequência com falha final ainda não reportadas por uma marca, em ordem. */
        private final ArrayDeque<FailedRange> unreportedFailures = new ArrayDeque<>();
        /** MARKS: marcas esperando esta rota, em ordem de marca (fronteiras não decrescentes). */
        private final ArrayDeque<MarkWaiter> waiters = new ArrayDeque<>();
        /** Handles graváveis que usam a rota, por identidade (normalmente um). Só muda sob {@link #lock}. */
        private final Set<Object> holders = Collections.newSetFromMap(new IdentityHashMap<>());
        /** Sem holder (fechamento ou release): novas admissões recebem {@code CLOSED}. Só muda sob {@link #lock}. */
        private boolean retired;
        /**
         * A rota saiu (ou está saindo) do mapa: substituída por outra geração ou removida. Ligado sob {@link #lock}
         * dentro do {@code compute} que a tira do mapa, antes da publicação; quem pega o lock depois trata a rota
         * como fora do mapa mesmo que {@code routes.get} ainda a devolva. Nunca volta a {@code false}.
         */
        private boolean detached;
        /**
         * A falha da rota já foi entregue ao chamador (por um {@code release}): dispensa {@code isIdle} na remoção,
         * mas não {@code submitted == completed}. Só muda sob {@link #lock}.
         */
        private boolean failuresDelivered;
        /**
         * {@code MARKS}: fronteira de um release em curso ({@link #NO_RELEASE} sem release). Marcas com id
         * {@code >= releaseFirstMarkId} (criadas depois do {@code beginRelease}) não colhem falhas até ela. Só muda sob
         * {@link #lock}.
         */
        private long releaseBoundary = NO_RELEASE;
        /** Id da primeira marca criada depois do {@code beginRelease} em curso. Só muda sob {@link #lock}. */
        private long releaseFirstMarkId;
        /**
         * {@code MARKS}: releases desta rota com proteção ativa (tickets ainda sem colheita nem fim). Só muda sob
         * {@link #lock}.
         */
        private int activeReleases;
        /**
         * {@code MARKS}: faixas colhidas por um release enquanto outro da mesma rota seguia ativo ({@code null} sem
         * nenhuma). Cada release ativo soma as que a fronteira dele cobre; sai quando {@code activeReleases} chega a 0.
         * Só muda sob {@link #lock}.
         */
        private List<FailedRange> releaseHarvested;

        SeriesRoute(String key, String owner, String generation, Object holder) {
            this.key = key;
            this.generation = generation;
            this.owner = owner;
            destinations.add(owner);
            holders.add(holder);
        }

        void resetRedirectEpisode() {
            redirectedBy.clear();
            redirectAttempts = 0;
            confirmedOwner = null;
            lastHint = null;
        }

        /** Registra {@code [from, to]} como falha final; junta com a faixa anterior contígua de mesmo status. */
        void recordFailure(long from, long to, SeriesStatus status) {
            FailedRange last = unreportedFailures.peekLast();
            if (last != null && last.to() + 1 == from && last.status() == status) {
                unreportedFailures.pollLast();
                unreportedFailures.addLast(new FailedRange(last.from(), to, status));
            } else {
                unreportedFailures.addLast(new FailedRange(from, to, status));
            }
        }

        /**
         * Atribui a {@code mark} as falhas com sequência {@code <= boundary}, partindo a faixa que cruza a fronteira. Uma
         * marca criada durante um release desta rota não leva as falhas até a fronteira do release, que ficam para ele.
         */
        void collectFailures(Mark mark, long boundary) {
            long reserved = releaseBoundary != NO_RELEASE && mark.id() >= releaseFirstMarkId ? releaseBoundary : 0L;
            FailureSink sink = (status, count) -> mark.addFailures(key, status, count);
            if (reserved <= 0L) {
                collectFailures(sink, boundary);
                return;
            }
            // Separa o prefixo até a fronteira do release, colhe o resto e devolve o prefixo à frente, na ordem.
            ArrayDeque<FailedRange> kept = new ArrayDeque<>();
            while (!unreportedFailures.isEmpty() && unreportedFailures.peekFirst().from() <= reserved) {
                FailedRange range = unreportedFailures.pollFirst();
                if (range.to() > reserved) {
                    kept.addLast(new FailedRange(range.from(), reserved, range.status()));
                    unreportedFailures.addFirst(new FailedRange(reserved + 1, range.to(), range.status()));
                    break;
                }
                kept.addLast(range);
            }
            collectFailures(sink, boundary);
            for (var it = kept.descendingIterator(); it.hasNext(); ) {
                unreportedFailures.addFirst(it.next());
            }
        }

        /**
         * Entrega a {@code sink} as falhas com sequência {@code <= boundary}, partindo a faixa que cruza a fronteira;
         * o que foi entregue sai de {@link #unreportedFailures} e não vai mais para nenhuma marca.
         */
        /**
         * Tira de {@link #unreportedFailures} as faixas com sequência {@code <= boundary}, partindo a que cruza a
         * fronteira, e as acrescenta a {@code into}, na ordem.
         */
        void takeFailures(long boundary, List<FailedRange> into) {
            while (!unreportedFailures.isEmpty() && unreportedFailures.peekFirst().from() <= boundary) {
                FailedRange range = unreportedFailures.pollFirst();
                if (range.to() > boundary) {
                    unreportedFailures.addFirst(new FailedRange(boundary + 1, range.to(), range.status()));
                    into.add(new FailedRange(range.from(), boundary, range.status()));
                } else {
                    into.add(range);
                }
            }
        }

        void collectFailures(FailureSink sink, long boundary) {
            while (!unreportedFailures.isEmpty() && unreportedFailures.peekFirst().from() <= boundary) {
                FailedRange range = unreportedFailures.pollFirst();
                if (range.to() > boundary) {
                    unreportedFailures.addFirst(new FailedRange(boundary + 1, range.to(), range.status()));
                    sink.accept(range.status(), boundary - range.from() + 1);
                } else {
                    sink.accept(range.status(), range.to() - range.from() + 1);
                }
            }
        }
    }

    /**
     * Rota de uma série em release, com a fronteira fotografada por {@link #beginRelease} e o holder que a aposentou
     * ({@code null} se o release só drena a rota). Opaco fora do dispatcher.
     */
    static final class ReleaseTicket {
        private final String key;
        private final SeriesRoute route;
        private final long boundary;
        private final Object holder;
        /** Este ticket ainda conta em {@code route.activeReleases}. Só muda sob o {@code route.lock} da rota. */
        private boolean protecting;

        private ReleaseTicket(String key, SeriesRoute route, long boundary, Object holder, boolean protecting) {
            this.key = key;
            this.route = route;
            this.boundary = boundary;
            this.holder = holder;
            this.protecting = protecting;
        }

        /** Chave da série. */
        String key() {
            return key;
        }

        /** Fronteira do dreno: escritas admitidas na rota até {@link #beginRelease}. */
        long boundary() {
            return boundary;
        }
    }

    /**
     * Desfecho do dreno de uma série em release ({@link #drainForRelease}).
     *
     * @param outcome               resultado do dreno
     * @param failedSamplesByStatus falhas colhidas até a fronteira, por status ({@code MARKS}); vazio em {@code BARRIER},
     *                              que guarda só a primeira falha da série
     * @param errorCode             código da falha; {@code null} em {@link Outcome#DRAINED}
     * @param message               descrição da falha; {@code null} em {@link Outcome#DRAINED}
     */
    record ReleaseDrain(Outcome outcome, Map<SeriesStatus, Long> failedSamplesByStatus, ErrorCode errorCode,
            String message) {

        /** Dreno sem falha. */
        static final ReleaseDrain DRAINED = new ReleaseDrain(Outcome.DRAINED, Map.of(), null, null);

        ReleaseDrain {
            failedSamplesByStatus = Map.copyOf(failedSamplesByStatus);
        }

        static ReleaseDrain timedOut(Duration budget) {
            return new ReleaseDrain(Outcome.TIMED_OUT, Map.of(), ErrorCode.TIMEOUT,
                    "escritas anteriores ao release não confirmadas dentro de " + budget);
        }

        /** Resultado do dreno de uma série. */
        enum Outcome {
            /** Toda escrita até a fronteira teve OK final. */
            DRAINED,
            /** Drenou, com falha de escrita até a fronteira. */
            WRITE_FAILURES,
            /** A geração foi removida ou posta em quarentena. */
            SERIES_DELETED,
            /** A rota não alcançou a fronteira no prazo. */
            TIMED_OUT
        }
    }

    /** Faixa {@code [from, to]} de sequências de uma série que terminou com {@code status} final. */
    private record FailedRange(long from, long to, SeriesStatus status) { }

    /** Destino das falhas colhidas de uma rota: uma marca ou o resultado de um release. */
    @FunctionalInterface
    private interface FailureSink {
        void accept(SeriesStatus status, long count);
    }

    /** Marca esperando a rota alcançar {@code boundary}. */
    private record MarkWaiter(Mark mark, long boundary) { }

    /**
     * Implementação de {@link WriteMark}. {@link #remaining} começa em 1 (guarda liberada no fim do registro em
     * {@link WriteDispatcher#mark()}) e soma um por waiter registrado.
     */
    private static final class Mark implements WriteMark {
        private final long id;
        private final long samplesAdmitted;
        private final AtomicLong remaining = new AtomicLong(1L);
        private final CompletableFuture<WriteMarkResult> future = new CompletableFuture<>();
        private final Lock stateLock = new ReentrantLock();
        /**
         * Futures entregues por {@link #completion()} antes da conclusão, sob {@link #stateLock}. Só quem conclui a
         * marca as completa, em ordem e na própria thread: um {@code future.copy()} deixaria uma thread que chama
         * {@code completion()} durante a conclusão executar os callbacks já registrados nesta marca (o
         * {@code postComplete} do {@code CompletableFuture} é cooperativo), enquanto a thread de conclusão já
         * conclui a marca seguinte — callbacks de marcas diferentes sairiam fora de ordem.
         */
        private final List<CompletableFuture<WriteMarkResult>> dependents = new ArrayList<>();
        private long samplesFailed;
        private final Map<SeriesStatus, Long> failuresByStatus = new LinkedHashMap<>();
        private final Set<String> failedSeries = new LinkedHashSet<>();
        private boolean failedSeriesTruncated;
        private volatile WriteMarkResult result;

        Mark(long id, long samplesAdmitted) {
            this.id = id;
            this.samplesAdmitted = samplesAdmitted;
        }

        void acquire() {
            remaining.incrementAndGet();
        }

        /** {@code true} se esta liberação deixou a marca pronta. */
        boolean release() {
            return remaining.decrementAndGet() == 0L;
        }

        boolean released() {
            return remaining.get() == 0L;
        }

        void addFailures(String seriesKey, SeriesStatus status, long count) {
            stateLock.lock();
            try {
                samplesFailed += count;
                failuresByStatus.merge(status, count, Long::sum);
                if (!failedSeries.contains(seriesKey)) {
                    if (failedSeries.size() < WriteMarkResult.MAX_FAILED_SERIES_SAMPLE) {
                        failedSeries.add(seriesKey);
                    } else {
                        failedSeriesTruncated = true;
                    }
                }
            } finally {
                stateLock.unlock();
            }
        }

        void complete() {
            WriteMarkResult built;
            List<CompletableFuture<WriteMarkResult>> pending;
            stateLock.lock();
            try {
                built = new WriteMarkResult(id, samplesAdmitted, samplesFailed, failuresByStatus,
                        List.copyOf(failedSeries), failedSeriesTruncated);
                result = built;
                future.complete(built);
                pending = takeDependents();
            } finally {
                stateLock.unlock();
            }
            for (CompletableFuture<WriteMarkResult> dependent : pending) {
                dependent.complete(built);
            }
        }

        void fail(NgrrdClusterException failure) {
            List<CompletableFuture<WriteMarkResult>> pending;
            stateLock.lock();
            try {
                future.completeExceptionally(failure);
                pending = takeDependents();
            } finally {
                stateLock.unlock();
            }
            for (CompletableFuture<WriteMarkResult> dependent : pending) {
                dependent.completeExceptionally(failure);
            }
        }

        /** Chamado sob {@link #stateLock}. */
        private List<CompletableFuture<WriteMarkResult>> takeDependents() {
            List<CompletableFuture<WriteMarkResult>> pending = List.copyOf(dependents);
            dependents.clear();
            return pending;
        }

        @Override public long id() { return id; }
        @Override public boolean isDone() { return future.isDone(); }
        @Override public Optional<WriteMarkResult> result() { return Optional.ofNullable(result); }
        @Override public CompletableFuture<WriteMarkResult> completion() {
            stateLock.lock();
            try {
                if (!future.isDone()) {
                    CompletableFuture<WriteMarkResult> dependent = new CompletableFuture<>();
                    dependents.add(dependent);
                    return dependent;
                }
            } finally {
                stateLock.unlock();
            }
            // Já concluída: a cópia nasce concluída e não toca os dependentes de outras threads.
            return future.copy();
        }
        @Override public String toString() { return "WriteMark[" + id + "]"; }
    }

    /** Escritas de uma série retiradas de um buffer, com as tentativas que ela acumulava ali. */
    private record Extracted(List<SeriesWrite> writes, int attempts) { }

    /** All access is under NodeBuffer.lock. FIFO within a series; round-robin between ready series. */
    private static final class PendingWrites {
        private final Map<String, ArrayDeque<SeriesWrite>> series = new HashMap<>();
        private final LinkedHashSet<String> ready = new LinkedHashSet<>();
        private final Map<String, Delay> paused = new HashMap<>();
        private final PriorityQueue<Delay> wakeups = new PriorityQueue<>(Comparator.comparingLong(Delay::until));
        private final Map<String, Integer> attempts = new HashMap<>();
        private int size;
        /**
         * Pausa de uma série. Igualdade por identidade, de propósito: toda pausa sem prazo de uma chave vale
         * {@code (key, Long.MAX_VALUE)}, e {@link #resume} só pode desfazer a pausa que o chamador aplicou — nunca a
         * de outra geração da mesma chave aplicada depois ({@code paused.remove(key, delay)} e
         * {@code wakeups.remove(delay)} comparam por {@code equals}).
         */
        private static final class Delay {
            private final String key;
            private final long until;

            private Delay(String key, long until) {
                this.key = key;
                this.until = until;
            }

            String key() { return key; }
            long until() { return until; }
        }

        int size() { return size; }
        boolean isEmpty() { return size == 0; }
        void addLast(SeriesWrite write) { add(write, false); }
        void addFirst(SeriesWrite write) { add(write, true); }
        private void add(SeriesWrite write, boolean first) {
            var queue = series.computeIfAbsent(write.seriesKey(), ignored -> new ArrayDeque<>());
            if (first) { queue.addFirst(write); } else { queue.addLast(write); }
            size++;
            if (!paused.containsKey(write.seriesKey())) { ready.add(write.seriesKey()); }
        }
        SeriesWrite pollFirst(long now) {
            while (!wakeups.isEmpty() && wakeups.peek().until() <= now) {
                Delay delay = wakeups.remove();
                if (paused.remove(delay.key(), delay) && series.containsKey(delay.key())) {
                    ready.add(delay.key());
                }
            }
            if (ready.isEmpty()) { return null; }
            String key = ready.removeFirst();
            var queue = series.get(key);
            SeriesWrite write = queue.removeFirst();
            size--;
            if (queue.isEmpty()) { series.remove(key); } else { ready.add(key); }
            return write;
        }
        /** Como {@link #defer}, devolvendo a pausa aplicada, para desfazê-la depois só se ainda for ela. */
        Delay pause(String key, long until) {
            defer(key, until);
            return paused.get(key);
        }
        /** Desfaz {@code delay} se ela ainda for a pausa de {@code key}; a série volta a ficar pronta se tiver escrita. */
        void resume(String key, Delay delay) {
            if (delay != null && paused.remove(key, delay)) {
                wakeups.remove(delay);
                if (series.containsKey(key)) { ready.add(key); }
            }
        }
        void defer(String key, long until) {
            Delay old = paused.put(key, new Delay(key, until));
            if (old != null) { wakeups.remove(old); }
            wakeups.add(paused.get(key));
            ready.remove(key);
        }
        int nextAttempt(String key) { return attempts.merge(key, 1, Integer::sum); }
        void succeeded(String key) { attempts.remove(key); }
        boolean hasAttempts(String key) { return attempts.containsKey(key); }
        /**
         * A rota da série saiu do mapa: esquece as tentativas dela e, se não houver escrita da série neste buffer,
         * também uma pausa que ficou para trás (uma pausa sem prazo prenderia a próxima rota da mesma chave).
         */
        void forgetSeries(String key) {
            attempts.remove(key);
            if (!series.containsKey(key)) {
                Delay delay = paused.remove(key);
                if (delay != null) { wakeups.remove(delay); }
            }
        }
        /** Keeps the per-series retry count across buffers when a series is rerouted. */
        void carryAttempts(String key, int count) {
            if (count > 0) { attempts.merge(key, count, Math::max); }
        }
        Extracted extract(String key) {
            var queue = series.remove(key);
            ready.remove(key);
            Delay delay = paused.remove(key);
            if (delay != null) { wakeups.remove(delay); }
            Integer carried = attempts.remove(key);
            int count = carried == null ? 0 : carried;
            if (queue == null) { return new Extracted(List.of(), count); }
            size -= queue.size();
            return new Extracted(new ArrayList<>(queue), count);
        }
        void clear() { series.clear(); ready.clear(); paused.clear(); wakeups.clear(); attempts.clear(); size = 0; }
    }

    /** Buffer FIFO de um nó de destino, com controle de backoff e de flush em voo. */
    private static final class NodeBuffer {
        private final PendingWrites queue = new PendingWrites();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notFull = lock.newCondition();
        private final AtomicBoolean inFlight = new AtomicBoolean(false);
        private final AtomicInteger backoffAttempts = new AtomicInteger(0);
        private volatile long backoffUntilMs = 0L;

        int nextBackoffAttempt() {
            return backoffAttempts.incrementAndGet();
        }

        void resetBackoffAttempts() {
            backoffAttempts.set(0);
        }
    }
}
