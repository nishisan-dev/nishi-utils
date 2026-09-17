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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * <p>Cada nó de destino tem seu próprio buffer FIFO limitado
 * ({@code maxBufferedSamplesPerNode}); ao encher, {@link #enqueue} bloqueia
 * ({@link NgrrdClusterConfig.BufferFullPolicy#BLOCK}) ou lança
 * {@link ErrorCode#BUFFER_FULL} ({@link NgrrdClusterConfig.BufferFullPolicy#FAIL}).</p>
 */
public final class WriteDispatcher implements WriteBuffer, Closeable {

    private static final Logger LOGGER = Logger.getLogger(WriteDispatcher.class.getName());
    private static final long ENQUEUE_WAIT_POLL_MS = 200L;
    private static final long DRAIN_POLL_MS = 20L;
    private static final int FLUSH_POOL_SIZE = 4;
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
    private final ExecutorService flushPool;
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

        this.flushPool = Executors.newFixedThreadPool(FLUSH_POOL_SIZE, WriteDispatcher::newDaemonFlushThread);
        this.tickThread = new Thread(this::tickLoop, "ngrrd-write-dispatcher");
        this.tickThread.setDaemon(true);
        this.tickThread.start();
    }

    private static Thread newDaemonFlushThread(Runnable task) {
        Thread thread = new Thread(task, "ngrrd-write-flush");
        thread.setDaemon(true);
        return thread;
    }

    @Override
    public void enqueue(String ownerNodeId, SeriesWrite write) {
        Objects.requireNonNull(ownerNodeId, "ownerNodeId");
        Objects.requireNonNull(write, "write");
        if (closed) {
            throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado");
        }
        NodeBuffer buf = buffers.computeIfAbsent(ownerNodeId, id -> new NodeBuffer());
        boolean triggerFlush;
        buf.lock.lock();
        try {
            while (buf.queue.size() >= maxBufferedSamplesPerNode) {
                if (bufferFullPolicy == NgrrdClusterConfig.BufferFullPolicy.FAIL) {
                    throw new NgrrdClusterException(ErrorCode.BUFFER_FULL,
                            "buffer de escrita cheio para o nó " + ownerNodeId);
                }
                if (closed) {
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado durante espera por espaço");
                }
                try {
                    buf.notFull.await(ENQUEUE_WAIT_POLL_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando espaço no buffer", e);
                }
                if (closed) {
                    throw new NgrrdClusterException(ErrorCode.CLOSED, "dispatcher fechado durante espera por espaço");
                }
            }
            buf.queue.addLast(write);
            samplesEnqueuedCount.increment();
            triggerFlush = buf.queue.size() >= batchMaxSamples;
        } finally {
            buf.lock.unlock();
        }
        if (triggerFlush) {
            scheduleFlush(ownerNodeId, buf);
        }
    }

    @Override
    public void flushNodeSync(String ownerNodeId) {
        flushNodeSync(ownerNodeId, Duration.ofMillis(closeTimeoutMillis));
    }

    @Override
    public void flushNodeSync(String ownerNodeId, Duration maxWait) {
        NodeBuffer buf = buffers.get(ownerNodeId);
        if (buf == null) {
            return;
        }
        // Limitado por maxWait: sem um prazo aqui, uma condição persistente que nunca progride (ex.:
        // catálogo do dono ainda não convergiu após um restart, ou a conexão cliente→dono ainda não
        // foi reestabelecida) faria este laço esperar para sempre — o backoff em applyStatus evita o
        // busy-loop de CPU, mas não por si só um prazo total. De propósito NÃO usa
        // retryPolicy.timeout() (o retryTimeout do cliente, minutos, pensado para a espera de
        // MIGRATING): usar esse prazo aqui já causou, na prática, uma chamada síncrona travar por
        // vários minutos. O prazo padrão (via flushNodeSync(String)) é closeTimeoutMillis; O1 permite
        // um teto explícito, menor, para respeitar um orçamento TOTAL compartilhado entre vários
        // handles em DefaultNgrrdClusterClient.close().
        long startedAt = clock.millis();
        long maxWaitMillis = Math.max(0L, maxWait.toMillis());
        while (hasPending(buf)) {
            if (clock.millis() - startedAt >= maxWaitMillis) {
                throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                        "flush do nó " + ownerNodeId + " não completou dentro de " + maxWaitMillis + " ms");
            }
            scheduleFlush(ownerNodeId, buf);
            sleepQuietly(DRAIN_POLL_MS);
        }
    }

    /**
     * Força o flush síncrono de todos os nós de destino conhecidos.
     *
     * <p>item 11 (achado do Refuter): orçamento TOTAL (não por nó) — antes, cada nó recebia seu
     * próprio {@code closeTimeoutMillis} inteiro, então {@code N} nós lentos podiam multiplicar o
     * tempo total de {@code flushAllSync} por {@code N} vezes o prazo de um único nó. Mesmo raciocínio
     * de {@code DefaultNgrrdClusterClient.close()} (O1): o que sobrar do orçamento vai para o próximo
     * nó; um nó que não coube no orçamento restante simplesmente não é esperado (suas pendências
     * continuam no buffer, sujeitas ao próximo flush/close).</p>
     */
    public void flushAllSync() {
        long deadline = clock.millis() + closeTimeoutMillis;
        for (String owner : List.copyOf(buffers.keySet())) {
            long remainingMs = deadline - clock.millis();
            if (remainingMs <= 0) {
                return;
            }
            flushNodeSync(owner, Duration.ofMillis(remainingMs));
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
        for (Map.Entry<String, NodeBuffer> entry : buffers.entrySet()) {
            NodeBuffer buf = entry.getValue();
            while (clock.millis() < deadline && hasPending(buf)) {
                scheduleFlush(entry.getKey(), buf);
                sleepQuietly(DRAIN_POLL_MS);
            }
        }

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
        if (!buf.inFlight.compareAndSet(false, true)) {
            return;
        }
        flushPool.execute(() -> {
            try {
                drainLoop(owner, buf);
            } finally {
                buf.inFlight.set(false);
            }
        });
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
                batch.add(buf.queue.pollFirst());
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
            case OK -> samplesSentCount.add(writes.size());
            case WRONG_OWNER -> {
                recordRetry(SeriesStatus.WRONG_OWNER);
                String newOwner = response.ownerBySeries().get(seriesKey);
                if (newOwner != null) {
                    logRetryRateLimited(seriesKey, SeriesStatus.WRONG_OWNER, "novo dono informado: " + newOwner);
                    placementLookup.noteOwner(seriesKey, newOwner);
                    // M3 (nota do Refuter do M1c): sem isto, RemoteSeriesHandle.owner nunca muda por
                    // este caminho e cada lote SEGUINTE da mesma série seria reroteado de novo — o
                    // handle continuaria enfileirando no dono antigo até a próxima resposta WRONG_OWNER.
                    ownerChanged.accept(seriesKey, newOwner);
                    // B4 (achado do Refuter): antes de reenfileirar só o lote que acabou de falhar no
                    // novo dono, extrai TAMBÉM todas as escritas desta MESMA série que ainda estejam na
                    // fila de `owner` (lotes seguintes, ainda não enviados) — sem isso, elas seriam
                    // enviadas depois ao dono ERRADO, chegando ao dono novo fora de ordem crescente de
                    // timestamp (o lote seguinte, mais recente, chegaria antes do backlog reroteado).
                    List<SeriesWrite> stillQueuedAtOldOwner = newOwner.equals(owner)
                            ? List.of()
                            : extractSeriesFrom(buf, seriesKey);
                    List<SeriesWrite> reordered = writes;
                    if (!stillQueuedAtOldOwner.isEmpty()) {
                        reordered = new ArrayList<>(writes.size() + stillQueuedAtOldOwner.size());
                        reordered.addAll(writes);
                        reordered.addAll(stillQueuedAtOldOwner);
                    }
                    requeueFrontAt(newOwner, reordered);
                    if (!newOwner.equals(owner)) {
                        // O dono novo tem seu próprio NodeBuffer, fora do drainLoop atual (que só itera
                        // o buffer de `owner`) — sem acionar o flush dele aqui, as amostras
                        // reenfileiradas só seriam enviadas no próximo tick (até batchMaxDelay depois).
                        // backoffMin (não zero) antes do reenvio: dois nós que discordam sobre quem é o
                        // dono (ex.: durante uma reconvergência) reenviariam um para o outro em
                        // ping-pong sem NENHUMA pausa se o retry fosse imediato. Agendado explicitamente
                        // (não só via backoffUntilMs) para não depender do próximo tick — que pode estar
                        // a até batchMaxDelay de distância, bem mais que o backoffMin desejado aqui.
                        markBackoff(newOwner, retryPolicy.backoffMin());
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
                    Boolean reopened = reopener.apply(seriesKey);
                    requeueFrontAt(owner, writes);
                    if (reopened == null || !reopened) {
                        buf.backoffUntilMs = clock.millis() + retryPolicy.backoffFor(buf.nextBackoffAttempt()).toMillis();
                    }
                }
            }
            case NOT_OPEN -> {
                recordRetry(SeriesStatus.NOT_OPEN);
                logRetryRateLimited(seriesKey, SeriesStatus.NOT_OPEN, "reabrindo via reopener");
                Boolean reopened = reopener.apply(seriesKey);
                requeueFrontAt(owner, writes);
                if (reopened == null || !reopened) {
                    // Mesmo raciocínio do WRONG_OWNER acima: sem backoff, uma reabertura que continua
                    // falhando (ex.: dono temporariamente inalcançável logo após um restart) vira
                    // busy-loop em vez de esperar e tentar de novo.
                    buf.backoffUntilMs = clock.millis() + retryPolicy.backoffFor(buf.nextBackoffAttempt()).toMillis();
                }
            }
            case MIGRATING -> {
                recordRetry(SeriesStatus.MIGRATING);
                logRetryRateLimited(seriesKey, SeriesStatus.MIGRATING, "aguardando fim da migração");
                requeueFrontAt(owner, writes);
                buf.backoffUntilMs = clock.millis() + retryPolicy.backoffFor(buf.nextBackoffAttempt()).toMillis();
            }
            case ERROR -> {
                recordRetry(SeriesStatus.ERROR);
                samplesFailedCount.add(writes.size());
                LOGGER.warning("WRITE_BATCH respondeu ERROR para " + seriesKey + ": "
                        + response.errorBySeries().get(seriesKey));
            }
            default -> {
                recordRetry(status);
                samplesFailedCount.add(writes.size());
                LOGGER.warning("WRITE_BATCH respondeu status inesperado " + status + " para " + seriesKey);
            }
        }
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
    private static List<SeriesWrite> extractSeriesFrom(NodeBuffer buf, String seriesKey) {
        buf.lock.lock();
        try {
            List<SeriesWrite> extracted = new ArrayList<>();
            Iterator<SeriesWrite> it = buf.queue.iterator();
            while (it.hasNext()) {
                SeriesWrite write = it.next();
                if (write.seriesKey().equals(seriesKey)) {
                    extracted.add(write);
                    it.remove();
                }
            }
            if (!extracted.isEmpty()) {
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

    /** Marca o buffer de {@code ownerNodeId} (criando-o se preciso) com um backoff mínimo de {@code duration}. */
    private void markBackoff(String ownerNodeId, Duration duration) {
        NodeBuffer buf = buffers.computeIfAbsent(ownerNodeId, id -> new NodeBuffer());
        buf.backoffUntilMs = clock.millis() + duration.toMillis();
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

    private static boolean hasPending(NodeBuffer buf) {
        return hasQueued(buf) || buf.inFlight.get();
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Buffer FIFO de um nó de destino, com controle de backoff e de flush em voo. */
    private static final class NodeBuffer {
        private final ArrayDeque<SeriesWrite> queue = new ArrayDeque<>();
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
