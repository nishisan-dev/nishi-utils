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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Objects;
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
    private volatile boolean closed;

    private final LongAdder samplesEnqueuedCount = new LongAdder();
    private final LongAdder samplesSentCount = new LongAdder();
    private final LongAdder samplesFailedCount = new LongAdder();
    private final LongAdder batchesSentCount = new LongAdder();
    private final ConcurrentMap<SeriesStatus, LongAdder> retryCounts = new ConcurrentHashMap<>();
    private final LongAdder ownerLookupsCount = new LongAdder();
    private final LongAdder redirectCyclesCount = new LongAdder();
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
        SeriesRoute route = routes.computeIfAbsent(write.seriesKey(), key -> new SeriesRoute(ownerNodeId));
        for (;;) {
            NodeBuffer buf;
            String owner;
            boolean accepted = false;
            boolean triggerFlush = false;
            route.lock.lock();
            try {
                if (closed) {
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
                }
                // The caller may have read RemoteSeriesHandle.owner before a concurrent redirect.
                // Every admission uses the same route as the backlog, under the series lock.
                owner = route.owner;
                buf = buffers.computeIfAbsent(owner, id -> new NodeBuffer());
                buf.lock.lock();
                try {
                    if (buf.queue.size() < maxBufferedSamplesPerNode) {
                        buf.queue.addLast(write);
                        if (route.submitted == route.completed && route.firstFailedSequence == Long.MAX_VALUE) {
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
        awaitBarriers(snapshotBarriers(ownerNodeId), maxWait);
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
        awaitBarriers(Map.of(route, boundary), maxWait);
    }

    /** Waits for all writes admitted before this call, even if they change destination. */
    public void flushAllSync() {
        awaitBarriers(snapshotBarriers(null), Duration.ofMillis(closeTimeoutMillis));
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

    private void awaitBarriers(Map<SeriesRoute, Long> barriers, Duration maxWait) {
        long startedAt = System.nanoTime();
        long budgetNanos = Math.max(0L, maxWait.toNanos());
        for (Map.Entry<SeriesRoute, Long> entry : barriers.entrySet()) {
            SeriesRoute route = entry.getKey();
            for (;;) {
                route.lock.lock();
                try {
                    if (route.firstFailedSequence <= entry.getValue()) {
                        throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, route.failureMessage);
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
            tickThread.join(1_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

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
                batch.add(write);
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
        SeriesStatus status = response.statusBySeries().getOrDefault(seriesKey, SeriesStatus.ERROR);
        switch (status) {
            case OK -> {
                buf.lock.lock();
                try { buf.queue.succeeded(seriesKey); }
                finally { buf.lock.unlock(); }
                samplesSentCount.add(writes.size());
                completeWrites(seriesKey, writes.size(), null);
            }
            case WRONG_OWNER -> {
                recordRetry(SeriesStatus.WRONG_OWNER);
                String newOwner = response.ownerBySeries().get(seriesKey);
                if (newOwner != null) {
                    followOwnerHint(owner, buf, seriesKey, writes, newOwner);
                } else {
                    // "Não sei de quem é" (resposta sem ownerBySeries) NÃO é o mesmo que "sei que não é
                    // meu": pode ser exatamente este nó, só que com a réplica local do catálogo ainda
                    // vazia (ex.: logo após um restart). Invalida o placement conhecido e força uma
                    // reabertura pelo MESMO caminho do NOT_OPEN — só recorre a backoff se a reabertura
                    // falhar; se funcionar, reenvia imediato (o dono provável já está pronto).
                    logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER,
                            "dono desconhecido — invalidando placement e reabrindo");
                    placementLookup.invalidate(seriesKey);
                    reopenAsync(owner, buf, seriesKey, writes);
                }
            }
            case NOT_OPEN -> {
                recordRetry(SeriesStatus.NOT_OPEN);
                logRetryRateLimited(seriesKey, SeriesStatus.NOT_OPEN, "reabrindo via reopener");
                reopenAsync(owner, buf, seriesKey, writes);
            }
            case MIGRATING -> {
                recordRetry(SeriesStatus.MIGRATING);
                logRetryRateLimited(seriesKey, SeriesStatus.MIGRATING, "aguardando fim da migração");
                requeueFrontAt(owner, writes);
                deferSeries(buf, seriesKey, -1);
            }
            case ERROR -> {
                recordRetry(SeriesStatus.ERROR);
                samplesFailedCount.add(writes.size());
                completeWrites(seriesKey, writes.size(), "WRITE_BATCH falhou para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey));
                LOGGER.warning("WRITE_BATCH respondeu ERROR para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey));
            }
            default -> {
                recordRetry(status);
                samplesFailedCount.add(writes.size());
                completeWrites(seriesKey, writes.size(), "WRITE_BATCH respondeu " + status + " para " + seriesKey);
                LOGGER.warning("WRITE_BATCH respondeu status inesperado " + status + " para " + seriesKey);
            }
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
            String newOwner) {
        SeriesRoute route = routes.get(seriesKey);
        boolean contradictory;
        boolean lookup = false;
        long delayMs;
        String visited;
        route.lock.lock();
        try {
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
                ownerChanged.accept(seriesKey, newOwner);
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
                applyOwnerLookup(seriesKey, found, failure);
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
     */
    private void applyOwnerLookup(String seriesKey, Map<String, SeriesPlacement> found, Throwable failure) {
        SeriesRoute route = routes.get(seriesKey);
        if (route == null) {
            return;
        }
        String flushOwner;
        NodeBuffer reopenBuf = null;
        long delayMs = 0L;
        String changedOwner = null;
        route.lock.lock();
        try {
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
            if (changedOwner != null) {
                ownerChanged.accept(seriesKey, changedOwner);
            }
        } finally {
            route.lock.unlock();
        }
        if (reopenBuf != null) {
            logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER, "líder não conhece a série — reabrindo");
            reopenAsync(flushOwner, reopenBuf, seriesKey, List.of());
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
        route.lock.lock();
        try {
            route.ownerLookupPending = false;
            NodeBuffer buf = buffers.get(route.owner);
            if (buf != null) {
                deferSeries(buf, seriesKey, -1);
            }
        } finally {
            route.lock.unlock();
        }
    }

    // FIFO admission/rerouting ensures completions form a prefix of each series. A failed
    // prefix remains observable: a later checkpoint cannot certify those lost samples.
    private void completeWrites(String seriesKey, int count, String failure) {
        SeriesRoute route = routes.get(seriesKey);
        route.lock.lock();
        try {
            if (failure != null && route.firstFailedSequence == Long.MAX_VALUE) {
                route.firstFailedSequence = route.completed + 1;
                route.failureMessage = failure;
            }
            route.completed += count;
            if (failure == null) {
                // OK do dono: o episódio de redirecionamento (#177) terminou.
                route.resetRedirectEpisode();
            }
            if (route.completed == route.submitted && route.firstFailedSequence == Long.MAX_VALUE) {
                synchronized (pendingLock) {
                    pendingRoutes.remove(route);
                }
            }
            // Failed routes stay indexed: subsequent successful writes cannot erase
            // an earlier failure and allow a Kafka commit to certify a lost prefix.
            route.progress.signalAll();
        } finally {
            route.lock.unlock();
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

    private void reopenAsync(String owner, NodeBuffer buf, String key, List<SeriesWrite> writes) {
        buf.lock.lock();
        try {
            for (int i = writes.size() - 1; i >= 0; i--) { buf.queue.addFirst(writes.get(i)); }
            buf.queue.defer(key, Long.MAX_VALUE);
        } finally { buf.lock.unlock(); }
        try {
            recoveryPool.execute(() -> {
                boolean opened = false;
                try { opened = Boolean.TRUE.equals(reopener.apply(key)); }
                catch (RuntimeException e) { LOGGER.log(Level.FINE, "Reabertura pendente de " + key, e); }
                finally {
                    deferSeries(buf, key, opened ? 0 : -1);
                    scheduleFlush(owner, buf);
                }
            });
        } catch (RejectedExecutionException closing) {
            deferSeries(buf, key, -1);
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
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition progress = lock.newCondition();
        private String owner;
        private final Set<String> destinations = new HashSet<>();
        private long submitted;
        private long completed;
        private long firstFailedSequence = Long.MAX_VALUE;
        private String failureMessage;
        // Redirect episode (#177), from the first WRONG_OWNER to the next OK of the series.
        /** Nós que responderam WRONG_OWNER no episódio atual, na ordem. */
        private final Set<String> redirectedBy = new LinkedHashSet<>();
        /** Saltos do episódio: expoente do backoff por série, que (ao contrário do buffer) atravessa os nós. */
        private int redirectAttempts;
        /** Dono confirmado pelo líder neste episódio; uma dica divergente dele é contraditória. */
        private String confirmedOwner;
        /** Série pausada aguardando a consulta ao líder ({@code scheduleOwnerLookup}). */
        private boolean ownerLookupPending;
        /** Última dica de dono recebida no episódio; seguida quando a consulta ao líder falha. */
        private String lastHint;

        SeriesRoute(String owner) {
            this.owner = owner;
            destinations.add(owner);
        }

        void resetRedirectEpisode() {
            redirectedBy.clear();
            redirectAttempts = 0;
            confirmedOwner = null;
            lastHint = null;
        }
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
        private record Delay(String key, long until) { }

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
        void defer(String key, long until) {
            Delay old = paused.put(key, new Delay(key, until));
            if (old != null) { wakeups.remove(old); }
            wakeups.add(paused.get(key));
            ready.remove(key);
        }
        int nextAttempt(String key) { return attempts.merge(key, 1, Integer::sum); }
        void succeeded(String key) { attempts.remove(key); }
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
