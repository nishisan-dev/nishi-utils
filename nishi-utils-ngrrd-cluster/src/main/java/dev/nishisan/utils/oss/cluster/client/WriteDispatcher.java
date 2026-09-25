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
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.cluster.api.ClientMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>Série confirmada inexistente ({@link #failSeries}, ou o reopener lançando
 * {@link SeriesNotFoundException}): a rota da chave fica marcada — admissões recusadas com
 * {@link SeriesNotFoundException}, pendências falhadas sem reabrir nem retentar — até um handle novo da
 * mesma chave abrir ({@link #resetSeries}, que também inaugura rota nova se a anterior ainda tem escritas
 * em voo). Cada escrita enfileirada carrega a rota que a admitiu e é
 * concluída nela, de modo que a marca de uma geração da série nunca alcança as escritas da seguinte.</p>
 */
public final class WriteDispatcher implements WriteBuffer, Closeable {

    private static final Logger LOGGER = Logger.getLogger(WriteDispatcher.class.getName());
    private static final long ENQUEUE_WAIT_POLL_MS = 200L;
    private static final long DRAIN_POLL_MS = 20L;
    /** Default dos construtores que não recebem {@code ownerChanged} explicitamente (testes antigos). */
    private static final BiConsumer<String, String> NO_OP_OWNER_CHANGED = (seriesKey, newOwner) -> { };

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
    private final Thread tickThread;
    private volatile boolean closed;

    private final LongAdder samplesEnqueuedCount = new LongAdder();
    private final LongAdder samplesSentCount = new LongAdder();
    private final LongAdder samplesFailedCount = new LongAdder();
    private final LongAdder batchesSentCount = new LongAdder();
    private final ConcurrentMap<SeriesStatus, LongAdder> retryCounts = new ConcurrentHashMap<>();
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
        for (;;) {
            SeriesRoute route = routes.computeIfAbsent(write.seriesKey(), key -> new SeriesRoute(ownerNodeId));
            NodeBuffer buf;
            String owner;
            boolean accepted = false;
            boolean triggerFlush = false;
            route.lock.lock();
            try {
                // resetSeries troca a entrada de routes por uma instância nova enquanto detém o MESMO
                // lock que acabamos de conseguir — se perdemos essa corrida, a rota que travamos já foi
                // aposentada (geração de um handle anterior, marcada ou não): solta e tenta de novo, para
                // cair na rota atual em vez de admitir na geração antiga ou ser recusada pela marca dela.
                if (routes.get(write.seriesKey()) != route) {
                    continue;
                }
                if (closed) {
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
                }
                // Série confirmada inexistente: recusa na hora, sem admitir. Uma escrita admitida aqui
                // receberia NOT_OPEN sem handle algum para reabrir e ficaria em retentativa para sempre.
                if (route.notFound) {
                    throw new SeriesNotFoundException(write.seriesKey());
                }
                // The caller may have read RemoteSeriesHandle.owner before a concurrent redirect.
                // Every admission uses the same route as the backlog, under the series lock.
                owner = route.owner;
                buf = buffers.computeIfAbsent(owner, id -> new NodeBuffer());
                buf.lock.lock();
                try {
                    if (buf.queue.size() < maxBufferedSamplesPerNode) {
                        buf.queue.addLast(new PendingWrite(route, write));
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
                    if (route.owner != null) {
                        scheduleFlush(route.owner, buffers.get(route.owner));
                    }
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
            List<PendingWrite> batch = takeBatch(buf);
            if (batch.isEmpty()) {
                return;
            }
            sendBatch(owner, buf, batch);
        }
    }

    private List<PendingWrite> takeBatch(NodeBuffer buf) {
        buf.lock.lock();
        try {
            int n = Math.min(batchMaxSamples, buf.queue.size());
            if (n == 0) {
                return List.of();
            }
            List<PendingWrite> batch = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                PendingWrite pending = buf.queue.pollFirst(clock.millis());
                if (pending == null) { break; }
                batch.add(pending);
            }
            buf.notFull.signalAll();
            return batch;
        } finally {
            buf.lock.unlock();
        }
    }

    private void sendBatch(String owner, NodeBuffer buf, List<PendingWrite> batch) {
        List<SeriesWrite> writes = new ArrayList<>(batch.size());
        for (PendingWrite pending : batch) {
            writes.add(pending.write());
        }
        WriteBatchResponse response;
        try {
            response = rpc.call(NodeId.of(owner), Commands.WRITE_BATCH, new WriteBatchRequest(writes),
                    WriteBatchResponse.class);
        } catch (NgrrdClusterException e) {
            requeueFront(buf, batch);
            failMarkedRoutesIn(buf, batch);
            buf.backoffUntilMs = clock.millis() + retryPolicy.backoffFor(buf.nextBackoffAttempt()).toMillis();
            LOGGER.log(Level.WARNING, "Falha de transporte ao enviar WRITE_BATCH para " + owner, e);
            return;
        }
        batchesSentCount.increment();
        buf.resetBackoffAttempts();

        // Agrupa por rota, não só por chave: escritas da mesma série podem pertencer a gerações
        // diferentes (a rota marcada inexistente de um handle antigo e a rota nova de um handle
        // reaberto) e cada uma é concluída na rota que a admitiu.
        Map<SeriesRoute, List<PendingWrite>> byRoute = new LinkedHashMap<>();
        for (PendingWrite pending : batch) {
            byRoute.computeIfAbsent(pending.route(), ignored -> new ArrayList<>()).add(pending);
        }
        for (Map.Entry<SeriesRoute, List<PendingWrite>> entry : byRoute.entrySet()) {
            applyStatus(owner, buf, entry.getKey(), entry.getValue(), response);
        }
    }

    private void applyStatus(String owner, NodeBuffer buf, SeriesRoute route, List<PendingWrite> pendings,
            WriteBatchResponse response) {
        String seriesKey = pendings.getFirst().write().seriesKey();
        SeriesStatus status = response.statusBySeries().getOrDefault(seriesKey, SeriesStatus.ERROR);
        if (status != SeriesStatus.OK && route.notFound) {
            // Série confirmada inexistente enquanto este lote estava em voo: qualquer resposta que não
            // seja OK encerra as escritas como falha — reabrir, reenfileirar ou mover só levaria a uma
            // retentativa sem fim (não há handle para reabrir uma série que não existe).
            LOGGER.log(Level.FINE, () -> "WRITE_BATCH respondeu " + status + " para " + seriesKey
                    + " (marcada inexistente) — " + pendings.size() + " escrita(s) falhada(s)");
            failWrites(route, seriesKey, pendings.size());
            return;
        }
        switch (status) {
            case OK -> {
                buf.lock.lock();
                try { buf.queue.succeeded(seriesKey); }
                finally { buf.lock.unlock(); }
                samplesSentCount.add(pendings.size());
                completeWrites(route, pendings.size(), null);
            }
            case WRONG_OWNER -> {
                recordRetry(SeriesStatus.WRONG_OWNER);
                String newOwner = response.ownerBySeries().get(seriesKey);
                if (newOwner != null) {
                    logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER, "novo dono informado: " + newOwner);
                    boolean moved = false;
                    route.lock.lock();
                    try {
                        // A marca é conferida sob o MESMO lock que failRoute usa para marcar e para
                        // fotografar destinations: ou a marca vem antes e nada é movido, ou o movimento
                        // vem antes e o dono novo já está em destinations quando failRoute extrai.
                        if (!route.notFound) {
                            List<PendingWrite> reordered = new ArrayList<>(pendings);
                            if (!newOwner.equals(owner)) {
                                reordered.addAll(extractSeriesFrom(buf, seriesKey, route));
                            }
                            // Publish the new route only after its older writes are queued. New
                            // admissions (including callers with a stale owner) cannot overtake them.
                            NodeBuffer target = buffers.computeIfAbsent(newOwner, id -> new NodeBuffer());
                            target.lock.lock();
                            try {
                                requeueFront(target, reordered);
                                if (!newOwner.equals(owner)) {
                                    target.queue.defer(seriesKey, clock.millis() + retryPolicy.backoffMin().toMillis());
                                }
                            } finally { target.lock.unlock(); }
                            route.owner = newOwner;
                            route.destinations.add(newOwner);
                            placementLookup.noteOwner(seriesKey, newOwner);
                            ownerChanged.accept(seriesKey, newOwner);
                            moved = true;
                        }
                    } finally {
                        route.lock.unlock();
                    }
                    if (!moved) {
                        failWrites(route, seriesKey, pendings.size());
                        return;
                    }
                    if (!newOwner.equals(owner)) {
                        // O dono novo tem seu próprio NodeBuffer, fora do drainLoop atual (que só itera
                        // o buffer de `owner`) — sem acionar o flush dele aqui, as amostras
                        // reenfileiradas só seriam enviadas no próximo tick (até batchMaxDelay depois).
                        // backoffMin (não zero) antes do reenvio: dois nós que discordam sobre quem é o
                        // dono (ex.: durante uma reconvergência) reenviariam um para o outro em
                        // ping-pong sem NENHUMA pausa se o retry fosse imediato. Agendado explicitamente
                        // (não só via backoffUntilMs) para não depender do próximo tick — que pode estar
                        // a até batchMaxDelay de distância, bem mais que o backoffMin desejado aqui.
                        triggerRetryAfter(newOwner, retryPolicy.backoffMin());
                    }
                } else {
                    // "Não sei de quem é" (resposta sem ownerBySeries) NÃO é o mesmo que "sei que não é
                    // meu": pode ser exatamente este nó, só que com a réplica local do catálogo ainda
                    // vazia (ex.: logo após um restart). Invalida o placement conhecido e força uma
                    // reabertura pelo MESMO caminho do NOT_OPEN — só recorre a backoff se a reabertura
                    // falhar; se funcionar, reenvia imediato (o dono provável já está pronto).
                    logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER,
                            "dono desconhecido — invalidando placement e reabrindo");
                    placementLookup.invalidate(seriesKey);
                    reopenAsync(owner, buf, route, seriesKey, pendings);
                }
            }
            case NOT_OPEN -> {
                recordRetry(SeriesStatus.NOT_OPEN);
                logRetryRateLimited(seriesKey, SeriesStatus.NOT_OPEN, "reabrindo via reopener");
                reopenAsync(owner, buf, route, seriesKey, pendings);
            }
            case MIGRATING -> {
                recordRetry(SeriesStatus.MIGRATING);
                logRetryRateLimited(seriesKey, SeriesStatus.MIGRATING, "aguardando fim da migração");
                requeueFrontAt(owner, pendings);
                deferSeries(buf, seriesKey, -1);
                // Marcada entre a checagem acima e o reenfileiramento: failRoute pode ter extraído o
                // buffer antes destas escritas voltarem — recolhe-as aqui.
                if (route.notFound) {
                    failPendingIn(buf, route, seriesKey);
                }
            }
            case ERROR -> {
                recordRetry(SeriesStatus.ERROR);
                samplesFailedCount.add(pendings.size());
                completeWrites(route, pendings.size(), "WRITE_BATCH falhou para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey));
                LOGGER.warning("WRITE_BATCH respondeu ERROR para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey));
            }
            default -> {
                recordRetry(status);
                samplesFailedCount.add(pendings.size());
                completeWrites(route, pendings.size(), "WRITE_BATCH respondeu " + status + " para " + seriesKey);
                LOGGER.warning("WRITE_BATCH respondeu status inesperado " + status + " para " + seriesKey);
            }
        }
    }

    // FIFO admission/rerouting ensures completions form a prefix of each series. A failed
    // prefix remains observable: a later checkpoint cannot certify those lost samples.
    //
    // A rota é sempre a que admitiu as escritas (carregada em PendingWrite), nunca routes.get(chave):
    // depois de um resetSeries, a chave aponta para a rota de outra geração.
    private void completeWrites(SeriesRoute route, int count, String failure) {
        route.lock.lock();
        try {
            if (failure != null && route.firstFailedSequence == Long.MAX_VALUE) {
                route.firstFailedSequence = route.completed + 1;
                route.failureMessage = failure;
            }
            route.completed += count;
            // Failed routes stay indexed: subsequent successful writes cannot erase
            // an earlier failure and allow a Kafka commit to certify a lost prefix.
            // Exceção: rota marcada inexistente sai do índice quando nada mais está em voo — ninguém
            // mais admite nela, quem já esperava recebe a falha pelo signalAll abaixo, e mantê-la
            // envenenaria flushAll para sempre.
            if (route.completed == route.submitted
                    && (route.firstFailedSequence == Long.MAX_VALUE || route.notFound)) {
                synchronized (pendingLock) {
                    pendingRoutes.remove(route);
                }
            }
            route.progress.signalAll();
        } finally {
            route.lock.unlock();
        }
    }

    private boolean hasOutstandingWrites() {
        // Shutdown also synchronizes with an admission that passed the closed check
        // but has not entered the pending index yet. Keep the history scan off the
        // hot path, here only, so close cannot overlook that in-progress admission.
        // O índice de pendências cobre as rotas de gerações anteriores (trocadas por resetSeries com
        // escritas ainda em voo), que já não estão em routes.
        List<SeriesRoute> candidates = new ArrayList<>(routes.values());
        candidates.addAll(snapshotPendingRoutes());
        for (SeriesRoute route : candidates) {
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

    private void requeueFront(NodeBuffer buf, List<PendingWrite> pendings) {
        if (pendings.isEmpty()) {
            return;
        }
        buf.lock.lock();
        try {
            for (int i = pendings.size() - 1; i >= 0; i--) {
                buf.queue.addFirst(pendings.get(i));
            }
        } finally {
            buf.lock.unlock();
        }
    }

    /**
     * Remove de {@code buf}, na ordem em que estão, as escritas de
     * {@code seriesKey} admitidas por {@code route} ainda enfileiradas — usado ao rerotear uma série
     * por {@code WRONG_OWNER} para levar junto qualquer backlog da MESMA série que ainda estivesse atrás
     * na fila do dono antigo, em vez de deixá-lo ser enviado depois ao dono errado, e ao falhar as
     * pendências de uma rota marcada inexistente. Escritas da mesma chave admitidas por outra rota
     * (outra geração da série) ficam onde estão.
     */
    private static List<PendingWrite> extractSeriesFrom(NodeBuffer buf, String seriesKey, SeriesRoute route) {
        buf.lock.lock();
        try {
            List<PendingWrite> extracted = buf.queue.extract(seriesKey, route);
            if (!extracted.isEmpty()) {
                buf.notFull.signalAll();
            }
            return extracted;
        } finally {
            buf.lock.unlock();
        }
    }

    private void requeueFrontAt(String ownerNodeId, List<PendingWrite> pendings) {
        if (pendings.isEmpty()) {
            return;
        }
        NodeBuffer target = buffers.computeIfAbsent(ownerNodeId, id -> new NodeBuffer());
        requeueFront(target, pendings);
    }

    // Only this series waits. Node-wide backoff is reserved for transport failures.
    private void deferSeries(NodeBuffer buf, String key, long delayMillis) {
        buf.lock.lock();
        try {
            long delay = delayMillis >= 0 ? delayMillis
                    : retryPolicy.backoffFor(buf.queue.nextAttempt(key)).toMillis();
            buf.queue.defer(key, clock.millis() + delay);
        } finally { buf.lock.unlock(); }
    }

    private void reopenAsync(String owner, NodeBuffer buf, SeriesRoute route, String key,
            List<PendingWrite> pendings) {
        buf.lock.lock();
        try {
            for (int i = pendings.size() - 1; i >= 0; i--) { buf.queue.addFirst(pendings.get(i)); }
            buf.queue.defer(key, Long.MAX_VALUE);
        } finally { buf.lock.unlock(); }
        try {
            recoveryPool.execute(() -> {
                // Marcada depois da checagem de applyStatus (ex.: por um read/checkpoint síncrono do
                // handle): o reopener não acharia handle algum e devolveria false para sempre.
                if (route.notFound) {
                    settleMarkedRoute(owner, buf, route, key);
                    return;
                }
                boolean opened;
                try {
                    opened = Boolean.TRUE.equals(reopener.apply(key));
                } catch (SeriesNotFoundException e) {
                    // A série sumiu (createIfMissing=false) e nunca vai reabrir sozinha — diferente das
                    // demais falhas de reabertura, deferSeries(-1) aqui adiaria para sempre. Marca a
                    // rota destas escritas e falha tudo que está pendente nela em vez de retentar.
                    failRoute(key, route, e);
                    settleMarkedRoute(owner, buf, route, key);
                    return;
                } catch (RuntimeException e) {
                    LOGGER.log(Level.FINE, "Reabertura pendente de " + key, e);
                    opened = false;
                }
                if (route.notFound) {
                    settleMarkedRoute(owner, buf, route, key);
                    return;
                }
                deferSeries(buf, key, opened ? 0 : -1);
                scheduleFlush(owner, buf);
            });
        } catch (RejectedExecutionException closing) {
            deferSeries(buf, key, -1);
        }
    }

    /**
     * Encerra uma reabertura cuja rota está marcada inexistente: falha o que ainda estiver em
     * {@code buf} para ela e desfaz a pausa da chave — escritas da mesma série admitidas pela rota de
     * um handle novo podem estar atrás na fila e não podem ficar presas na pausa da reabertura antiga.
     */
    private void settleMarkedRoute(String owner, NodeBuffer buf, SeriesRoute route, String key) {
        failPendingIn(buf, route, key);
        deferSeries(buf, key, 0);
        scheduleFlush(owner, buf);
    }

    /**
     * Marca a série como confirmadamente inexistente ({@link SeriesNotFoundException} descoberta pelo
     * {@link RemoteSeriesHandle} num caminho síncrono, depois de já ter parado de aceitar operações) e
     * falha as escritas pendentes dela. A marca vale para a rota atual da chave — criada já marcada, se
     * a série nunca teve escrita — e tem três efeitos:
     * <ul>
     *   <li>{@link #enqueue} passa a recusar na hora, com {@link SeriesNotFoundException}, qualquer
     *       escrita da chave (inclusive a de um {@code write()} que passou pelo {@code ensureOpen()} do
     *       handle antes da marcação);</li>
     *   <li>as escritas ainda em buffer, em QUALQUER nó já associado à rota
     *       ({@link SeriesRoute#destinations}), falham agora;</li>
     *   <li>as escritas em voo (lote já enviado, RPC pendente) falham na resposta, seja ela qual for
     *       exceto {@code OK} — sem reabrir, reenfileirar ou mover — e, concluída a última delas, a rota
     *       sai de {@link #pendingRoutes}, liberando {@code flushAll()}.</li>
     * </ul>
     * A marca só é desfeita por {@link #resetSeries}, quando um handle novo da mesma chave abre com
     * sucesso. Idempotente.
     */
    @Override
    public void failSeries(String seriesKey, Throwable cause) {
        failRoute(seriesKey, routes.computeIfAbsent(seriesKey, key -> new SeriesRoute(null)), cause);
    }

    /**
     * Inaugura, se preciso, uma geração nova da rota de {@code seriesKey}, com dono {@code ownerNodeId} —
     * {@code RemoteSeriesHandle.open()} chama isto quando um handle novo da mesma chave abre com
     * sucesso, ANTES de o handle ficar visível a quem escreve. Semântica:
     * <ul>
     *   <li>chave sem rota, ou com rota não marcada e sem escrita em voo: nada muda (o caminho de
     *       sempre);</li>
     *   <li>rota marcada inexistente OU com escritas de um handle anterior ainda em voo
     *       ({@code submitted > completed}): troca por uma rota nova e limpa. A troca acontece sob o lock
     *       da rota antiga, o mesmo que {@link #enqueue} confere antes de admitir — toda escrita posterior
     *       cai na rota nova e nunca herda a marca antiga, nem uma marca tardia da rota antiga (ex.: a
     *       reabertura pendente do handle anterior descobrindo {@code NOT_FOUND} depois desta
     *       abertura);</li>
     *   <li>as escritas da geração anterior carregam a própria rota ({@link PendingWrite}) e são
     *       concluídas nela. A rota antiga continua em {@link #pendingRoutes} enquanto tiver escrita em
     *       voo ou falha registrada — {@code flushAll()} segue vendo-a — e sai quando marcada e drenada,
     *       como qualquer rota marcada. Barreiras por série ({@link #flushSeriesSync}) de quem resolve a
     *       chave depois da troca só veem a rota nova.</li>
     * </ul>
     */
    @Override
    public void resetSeries(String seriesKey, String ownerNodeId) {
        Objects.requireNonNull(ownerNodeId, "ownerNodeId");
        SeriesRoute route = routes.get(seriesKey);
        if (route == null) {
            return;
        }
        boolean replaced = false;
        route.lock.lock();
        try {
            if (route.notFound || route.submitted > route.completed) {
                replaced = routes.replace(seriesKey, route, new SeriesRoute(ownerNodeId));
            }
        } finally {
            route.lock.unlock();
        }
        if (replaced && route.notFound) {
            retireIfDrained(route);
        }
    }

    /**
     * Marca {@code route} (a geração de {@code key} cujas escritas estão sendo falhadas) e falha tudo que
     * ela tem em buffer. A marca e a foto de {@code destinations} acontecem sob {@code route.lock}, o
     * mesmo lock que o reposicionamento por {@code WRONG_OWNER} usa para mover escritas: nenhuma escrita
     * escapa para um nó fora da foto. A extração de cada buffer acontece depois, sem {@code route.lock}.
     */
    private void failRoute(String key, SeriesRoute route, Throwable cause) {
        boolean firstMark;
        Set<String> destinations;
        route.lock.lock();
        try {
            firstMark = !route.notFound;
            route.notFound = true;
            destinations = Set.copyOf(route.destinations);
        } finally {
            route.lock.unlock();
        }
        long lost = 0;
        for (String ownerNodeId : destinations) {
            NodeBuffer buf = buffers.get(ownerNodeId);
            if (buf != null) {
                lost += failPendingIn(buf, route, key);
            }
        }
        retireIfDrained(route);
        if (firstMark) {
            LOGGER.log(Level.WARNING, "Série " + key + " confirmada inexistente — " + lost + " escrita(s) "
                    + "pendente(s) descartada(s) do buffer; escritas em voo falham na resposta, sem novas "
                    + "tentativas", cause);
        }
    }

    /** Falha, via {@link #completeWrites}, toda escrita de {@code route} ainda em {@code buf}; devolve quantas. */
    private long failPendingIn(NodeBuffer buf, SeriesRoute route, String key) {
        List<PendingWrite> lost = extractSeriesFrom(buf, key, route);
        if (lost.isEmpty()) {
            return 0;
        }
        failWrites(route, key, lost.size());
        return lost.size();
    }

    /**
     * Conclui {@code count} escritas de {@code route} como falha de série inexistente. O contador
     * {@link #samplesFailed()} só é atualizado depois de {@link #completeWrites} — quem observa o
     * contador já encontra a rota fora de {@link #pendingRoutes} se nada mais estiver em voo.
     */
    private void failWrites(SeriesRoute route, String key, int count) {
        completeWrites(route, count, "série inexistente: " + key);
        samplesFailedCount.add(count);
    }

    /** Após falha de transporte, falha (em vez de reenviar) o que acabou de voltar para {@code buf} de rotas marcadas. */
    private void failMarkedRoutesIn(NodeBuffer buf, List<PendingWrite> batch) {
        Map<SeriesRoute, String> marked = new LinkedHashMap<>();
        for (PendingWrite pending : batch) {
            if (pending.route().notFound) {
                marked.putIfAbsent(pending.route(), pending.write().seriesKey());
            }
        }
        for (Map.Entry<SeriesRoute, String> entry : marked.entrySet()) {
            failPendingIn(buf, entry.getKey(), entry.getValue());
        }
    }

    /** Tira de {@link #pendingRoutes} uma rota marcada sem nada em voo (nenhuma admissão nova é possível nela). */
    private void retireIfDrained(SeriesRoute route) {
        boolean drained;
        route.lock.lock();
        try {
            drained = route.submitted == route.completed;
        } finally {
            route.lock.unlock();
        }
        if (drained) {
            synchronized (pendingLock) {
                pendingRoutes.remove(route);
            }
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
        /** {@code null} só numa rota criada já marcada por {@link #failSeries}, que nunca admite escrita. */
        private String owner;
        private final Set<String> destinations = new HashSet<>();
        private long submitted;
        private long completed;
        private long firstFailedSequence = Long.MAX_VALUE;
        private String failureMessage;
        /**
         * Série confirmada inexistente (ver {@link #failSeries}). Escrito sob {@link #lock}; lido também
         * fora dele nas decisões do caminho de resposta, que reconferem depois de devolver escritas a um
         * buffer. Nunca volta a {@code false}: {@link #resetSeries} troca a rota inteira.
         */
        private volatile boolean notFound;

        SeriesRoute(String owner) {
            this.owner = owner;
            if (owner != null) {
                destinations.add(owner);
            }
        }
    }

    /** Escrita enfileirada junto com a rota que a admitiu — é nela que a escrita é concluída. */
    private record PendingWrite(SeriesRoute route, SeriesWrite write) {
    }

    /** All access is under NodeBuffer.lock. FIFO within a series; round-robin between ready series. */
    private static final class PendingWrites {
        private final Map<String, ArrayDeque<PendingWrite>> series = new HashMap<>();
        private final LinkedHashSet<String> ready = new LinkedHashSet<>();
        private final Map<String, Delay> paused = new HashMap<>();
        private final PriorityQueue<Delay> wakeups = new PriorityQueue<>(Comparator.comparingLong(Delay::until));
        private final Map<String, Integer> attempts = new HashMap<>();
        private int size;
        private record Delay(String key, long until) { }

        int size() { return size; }
        boolean isEmpty() { return size == 0; }
        void addLast(PendingWrite pending) { add(pending, false); }
        void addFirst(PendingWrite pending) { add(pending, true); }
        private void add(PendingWrite pending, boolean first) {
            String key = pending.write().seriesKey();
            var queue = series.computeIfAbsent(key, ignored -> new ArrayDeque<>());
            if (first) { queue.addFirst(pending); } else { queue.addLast(pending); }
            size++;
            if (!paused.containsKey(key)) { ready.add(key); }
        }
        PendingWrite pollFirst(long now) {
            while (!wakeups.isEmpty() && wakeups.peek().until() <= now) {
                Delay delay = wakeups.remove();
                if (paused.remove(delay.key(), delay) && series.containsKey(delay.key())) {
                    ready.add(delay.key());
                }
            }
            if (ready.isEmpty()) { return null; }
            String key = ready.removeFirst();
            var queue = series.get(key);
            PendingWrite pending = queue.removeFirst();
            size--;
            if (queue.isEmpty()) { series.remove(key); } else { ready.add(key); }
            return pending;
        }
        void defer(String key, long until) {
            Delay old = paused.put(key, new Delay(key, until));
            if (old != null) { wakeups.remove(old); }
            wakeups.add(paused.get(key));
            ready.remove(key);
        }
        int nextAttempt(String key) { return attempts.merge(key, 1, Integer::sum); }
        void succeeded(String key) { attempts.remove(key); }
        /**
         * Remove, na ordem, as escritas de {@code key} admitidas por {@code route}; as de outra rota da
         * mesma chave continuam na fila, com a pausa/estado de prontidão da chave intactos. Sem mais nada
         * da chave na fila, todo o estado dela (pausa, prontidão, tentativas) é descartado.
         */
        List<PendingWrite> extract(String key, SeriesRoute route) {
            var queue = series.get(key);
            List<PendingWrite> extracted = new ArrayList<>();
            if (queue != null) {
                for (var it = queue.iterator(); it.hasNext(); ) {
                    PendingWrite pending = it.next();
                    if (pending.route() == route) {
                        extracted.add(pending);
                        it.remove();
                    }
                }
                size -= extracted.size();
            }
            if (queue == null || queue.isEmpty()) {
                series.remove(key);
                ready.remove(key);
                Delay delay = paused.remove(key);
                if (delay != null) { wakeups.remove(delay); }
                attempts.remove(key);
            }
            return extracted;
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
