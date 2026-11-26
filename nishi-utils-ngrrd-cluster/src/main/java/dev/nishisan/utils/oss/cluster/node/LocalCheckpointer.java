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

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.NgrrdHandle.CheckpointTiming;
import dev.nishisan.utils.oss.cluster.metrics.LatencyHistogram;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Checkpoint local periódico das séries sujas do storage node ({@code ngrrd.checkpoint}, 8.14.0, opt-in).
 * Torna a durabilidade das escritas independente do {@code FLUSH}/{@code CHECKPOINT} remoto do cliente.
 *
 * <h2>Ciclo</h2>
 *
 * <p>Um executor de thread única ({@code ngrrd-local-checkpoint}) roda um ciclo com
 * {@code scheduleWithFixedDelay(interval)}: o próximo só começa {@code interval} depois do fim do
 * anterior, então ciclos nunca se sobrepõem. Cada ciclo:</p>
 * <ol>
 *   <li>tira um instantâneo das séries candidatas ({@link SeriesHandleRegistry#dirtySeries()}). Com o
 *       checkpoint local ligado, o registro mantém um índice incremental de sujas (8.15.0) e o instantâneo é
 *       uma cópia dele, sem varrer o registro: um superconjunto das sujas, que inclui séries checkpointadas
 *       desde a última poda. Elas saem rápido ({@code CLEAN}, que poda a chave do índice), mas ocupam uma
 *       vaga da cadência, então a fração útil do espalhamento fica um pouco menor;</li>
 *   <li>espalha os disparos por {@code spreadFraction} do intervalo (padrão 0,8) com cadência por prazo: o
 *       disparo {@code i} de {@code n} sai no instante {@code início + i × spreadFraction × interval / n}. A thread só dorme quando está
 *       adiantada em pelo menos 1 ms, então a cadência se mantém mesmo com centenas de milhares de séries
 *       (pausas de microssegundos viram rajadas curtas de poucos disparos, sem acumular atraso);</li>
 *   <li>para cada série, adquire uma vaga do semáforo de {@code maxInFlight} (fora de qualquer lock) e
 *       chama {@link SeriesHandleRegistry#tryCheckpointAsync}: {@code tryLock} da série, sem renovar o
 *       TTL de ociosidade; ocupada → conta {@code skippedBusy} e fica para o próximo ciclo; limpa,
 *       fechada, ausente ou congelada por migração → pula. Enfileirado o checkpoint, o lock é solto na
 *       hora; a vaga do semáforo só volta quando a future conclui.</li>
 * </ol>
 *
 * <p>Falha de checkpoint conta em {@code failures}, loga WARNING com limite de frequência e deixa a série
 * suja (o próximo ciclo tenta de novo). Um ciclo mais longo que {@code interval} loga WARNING com a
 * duração e o número de séries e conta em {@code overruns}. Toda exceção da tarefa é capturada, inclusive
 * {@link Error}, para nunca derrubar o agendamento.</p>
 *
 * <h2>Encerramento</h2>
 *
 * <p>{@link #close()} acorda na hora a cadência de um ciclo em curso (o ciclo abandona o restante),
 * cancela o agendamento, interrompe o executor e espera até 5 s os checkpoints locais ainda em voo. Passado
 * esse prazo, eles são abandonados sem risco: o {@code registry.close()} seguinte faz checkpoint+close
 * de cada handle, e a fila FIFO do writer conclui antes o checkpoint já enfileirado. O
 * {@link NgrrdStorageNode} fecha este componente antes dos handlers e do registry.</p>
 */
public final class LocalCheckpointer implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(LocalCheckpointer.class.getName());

    /** Folga mínima para a thread dormir em vez de seguir disparando. */
    private static final long MIN_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    /** Prazo de cada etapa do encerramento. */
    private static final long CLOSE_TIMEOUT_MS = 5_000L;
    /** Intervalo mínimo entre dois WARNINGs de falha de checkpoint. */
    private static final long FAILURE_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(60);

    private final SeriesHandleRegistry registry;
    private final LocalCheckpointSettings settings;
    private final Semaphore inFlight;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> task;
    private volatile boolean closed;
    /** Sinalizado pelo {@link #close()}: acorda na hora a cadência de um ciclo em curso. */
    private final CountDownLatch closeSignal = new CountDownLatch(1);

    private final AtomicLong cycles = new AtomicLong();
    private final AtomicLong lastCycleMs = new AtomicLong();
    /** Candidatas do último ciclo (sujas e limpas ainda no índice de sujas). */
    private final AtomicLong lastCycleSeries = new AtomicLong();
    /** Checkpoints de fato enfileirados ({@code STARTED}) no último ciclo. */
    private final AtomicLong lastCycleStarted = new AtomicLong();
    private final LongAdder checkpointed = new LongAdder();
    private final LongAdder skippedBusy = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final AtomicLong overruns = new AtomicLong();
    private final AtomicLong lastFailureLogNanos = new AtomicLong(System.nanoTime() - FAILURE_LOG_INTERVAL_NANOS);
    private final LongAdder suppressedFailureLogs = new LongAdder();
    /** Espera em fila até o início do force, só dos checkpoints locais que forçaram (não coalescidos). */
    private final LatencyHistogram queueWait = new LatencyHistogram();
    /** Duração do force, só dos checkpoints locais que forçaram (não coalescidos). */
    private final LatencyHistogram force = new LatencyHistogram();
    /** Checkpoints locais coalescidos: sem escrita nova desde o último force, não forçaram nada. */
    private final LongAdder coalesced = new LongAdder();

    /**
     * @throws IllegalArgumentException se {@code settings} estiver desligado: com o checkpoint local
     *                                  desligado, o nó não cria este componente
     */
    public LocalCheckpointer(SeriesHandleRegistry registry, LocalCheckpointSettings settings) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.settings = Objects.requireNonNull(settings, "settings");
        if (!settings.enabled()) {
            throw new IllegalArgumentException("LocalCheckpointer só é criado com ngrrd.checkpoint.enabled=true");
        }
        this.inFlight = new Semaphore(settings.maxInFlight());
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ngrrd-local-checkpoint");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Agenda o primeiro ciclo para daqui a {@code interval} e os seguintes {@code interval} após o fim de cada um. */
    public void start() {
        long intervalMs = settings.interval().toMillis();
        task = scheduler.scheduleWithFixedDelay(this::safeCycle, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOGGER.info("NGRRD_LOCAL_CHECKPOINT started interval=" + settings.interval()
                + " maxInFlight=" + settings.maxInFlight());
    }

    private void safeCycle() {
        try {
            runCycle();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, "Falha no ciclo de checkpoint local", e);
        }
    }

    /**
     * Executa um ciclo completo na thread chamadora (o agendador chama a partir da sua thread; os testes,
     * direto). Volta depois de disparar todos os checkpoints do instantâneo, sem esperar que concluam.
     *
     * @throws InterruptedException se a thread for interrompida na cadência ou à espera de vaga
     */
    void runCycle() throws InterruptedException {
        long startedNanos = System.nanoTime();
        List<String> dirty = new ArrayList<>(registry.dirtySeries());
        int count = dirty.size();
        long spreadNanos = (long) (settings.interval().toNanos() * settings.spreadFraction());
        int started = 0;
        for (int i = 0; i < count && !closed; i++) {
            long dueNanos = startedNanos + (count > 1 ? spreadNanos / count * i : 0L);
            long aheadNanos = dueNanos - System.nanoTime();
            if (aheadNanos >= MIN_SLEEP_NANOS && closeSignal.await(aheadNanos, TimeUnit.NANOSECONDS)) {
                break; // close() durante a cadência: abandona o restante do ciclo.
            }
            if (checkpointOne(dirty.get(i))) {
                started++;
            }
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
        cycles.incrementAndGet();
        lastCycleMs.set(elapsedMs);
        lastCycleSeries.set(count);
        lastCycleStarted.set(started);
        if (elapsedMs > settings.interval().toMillis()) {
            overruns.incrementAndGet();
            LOGGER.warning("NGRRD_LOCAL_CHECKPOINT_OVERRUN ciclo de checkpoint local levou " + elapsedMs
                    + " ms para " + count + " séries sujas, acima do intervalo de " + settings.interval()
                    + " — considere aumentar ngrrd.checkpoint.interval ou maxInFlight");
        } else {
            LOGGER.fine(() -> "NGRRD_LOCAL_CHECKPOINT cycle series=" + count + " elapsedMs=" + elapsedMs);
        }
    }

    /**
     * Um disparo do ciclo: vaga do semáforo, {@code tryCheckpointAsync} e contagem. Visível para testes.
     *
     * @return {@code true} se o checkpoint foi de fato enfileirado ({@code STARTED})
     */
    boolean checkpointOne(String seriesKey) throws InterruptedException {
        inFlight.acquire();
        boolean started = false;
        try {
            SeriesHandleRegistry.CheckpointAttempt attempt = registry.tryCheckpointAsync(seriesKey);
            switch (attempt.outcome()) {
                case STARTED -> {
                    started = true;
                    attempt.future().whenComplete((timing, failure) -> onCompleted(seriesKey, timing, failure));
                }
                case BUSY -> skippedBusy.increment();
                case CLEAN, UNAVAILABLE -> { }
            }
        } catch (RuntimeException e) {
            recordFailure(seriesKey, e);
        } finally {
            if (!started) {
                inFlight.release();
            }
        }
        return started;
    }

    /**
     * Conclusão de um checkpoint local. Pode rodar na thread do pool de force (ou na do writer): só conta e
     * grava em estruturas sem bloqueio, nunca espera nada.
     */
    private void onCompleted(String seriesKey, CheckpointTiming timing, Throwable failure) {
        try {
            if (failure == null) {
                checkpointed.increment();
                recordTiming(timing);
            } else {
                recordFailure(seriesKey, failure instanceof CompletionException && failure.getCause() != null
                        ? failure.getCause() : failure);
            }
        } finally {
            inFlight.release();
        }
    }

    private void recordTiming(CheckpointTiming timing) {
        // NONE significa "sem medida" (default da interface, writer fechado, Sync concluído pelo
        // fechamento), apesar de coalesced=true: comparado por identidade, não entra em nenhuma métrica.
        if (timing == null || timing == CheckpointTiming.NONE) {
            return;
        }
        if (timing.coalesced()) {
            coalesced.increment();
        } else {
            queueWait.record(Math.max(0L, timing.queueWaitNanos()));
            force.record(Math.max(0L, timing.forceNanos()));
        }
    }

    /** Conta a falha e loga WARNING no máximo uma vez por minuto (as demais vão a FINE e são somadas). */
    private void recordFailure(String seriesKey, Throwable failure) {
        failures.increment();
        long now = System.nanoTime();
        long last = lastFailureLogNanos.get();
        if (now - last >= FAILURE_LOG_INTERVAL_NANOS && lastFailureLogNanos.compareAndSet(last, now)) {
            long suppressed = suppressedFailureLogs.sumThenReset();
            LOGGER.log(Level.WARNING, "Falha no checkpoint local da série " + seriesKey
                    + " (a série segue suja e entra no próximo ciclo; " + suppressed
                    + " falhas omitidas desde o último aviso)", failure);
        } else {
            suppressedFailureLogs.increment();
            LOGGER.log(Level.FINE, "Falha no checkpoint local da série " + seriesKey, failure);
        }
    }

    /**
     * Métricas para {@code NodeMetricsSnapshot.lifecycleMetrics}, chaves {@code localCheckpoint.*}. Além dos
     * contadores do ciclo (8.14.0), a 8.15.0 mede só os checkpoints disparados por este componente (via
     * {@code checkpointAsyncTimed()}), em milissegundos: {@code queueWaitMsP99}/{@code queueWaitMsMax} (do
     * enfileiramento do Sync até o início do force: fila FIFO da série, pool de writers e pool de force),
     * {@code forceMsP99}/{@code forceMsMax} (duração do force) e {@code coalesced} (checkpoints sem escrita
     * nova, que não forçaram; resultados sem medida, {@code CheckpointTiming.NONE}, não contam). Os percentis e o
     * máximo cobrem só os que forçaram; o máximo é o recente (janela de 60 a 120 s, {@code recentMaxMicros}). O
     * checkpoint remoto tem a própria métrica ({@code checkpointLatency}).
     *
     * <p>Com o índice incremental de sujas (8.15.0), {@code dirtySeries} é o tamanho do índice: um limite
     * superior atrasado em um ciclo. {@code lastCycleSeries} conta as candidatas do último ciclo (sujas e
     * limpas ainda no índice) e {@code lastCycleStarted}, os checkpoints de fato enfileirados nele.</p>
     */
    public Map<String, Long> metrics() {
        return metricsMap(1L, registry.dirtyCount(), cycles.get(), lastCycleMs.get(), lastCycleSeries.get(),
                checkpointed.sum(), skippedBusy.sum(), failures.sum(), overruns.get(), queueWait.snapshot(),
                force.snapshot(), coalesced.sum(), lastCycleStarted.get());
    }

    /**
     * Métricas com o checkpoint local desligado: {@code enabled=0}, contadores zerados e
     * {@code dirtySeries} ainda calculado (mostra quanto depende do checkpoint remoto ou do close).
     */
    public static Map<String, Long> disabledMetrics(SeriesHandleRegistry registry) {
        return metricsMap(0L, registry.dirtyCount(), 0L, 0L, 0L, 0L, 0L, 0L, 0L, LatencySnapshot.EMPTY,
                LatencySnapshot.EMPTY, 0L, 0L);
    }

    private static Map<String, Long> metricsMap(long enabled, long dirtySeries, long cycles, long lastCycleMs,
            long lastCycleSeries, long checkpointed, long skippedBusy, long failures, long overruns,
            LatencySnapshot queueWait, LatencySnapshot force, long coalesced, long lastCycleStarted) {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("localCheckpoint.enabled", enabled);
        metrics.put("localCheckpoint.dirtySeries", dirtySeries);
        metrics.put("localCheckpoint.cycles", cycles);
        metrics.put("localCheckpoint.lastCycleMs", lastCycleMs);
        metrics.put("localCheckpoint.lastCycleSeries", lastCycleSeries);
        metrics.put("localCheckpoint.checkpointed", checkpointed);
        metrics.put("localCheckpoint.skippedBusy", skippedBusy);
        metrics.put("localCheckpoint.failures", failures);
        metrics.put("localCheckpoint.overruns", overruns);
        metrics.put("localCheckpoint.queueWaitMsP99", queueWait.p99Micros() / 1_000L);
        metrics.put("localCheckpoint.queueWaitMsMax", queueWait.recentMaxMicros() / 1_000L);
        metrics.put("localCheckpoint.forceMsP99", force.p99Micros() / 1_000L);
        metrics.put("localCheckpoint.forceMsMax", force.recentMaxMicros() / 1_000L);
        metrics.put("localCheckpoint.coalesced", coalesced);
        metrics.put("localCheckpoint.lastCycleStarted", lastCycleStarted);
        return metrics;
    }

    /** Checkpoints locais enfileirados e ainda não concluídos agora. */
    int inFlightCount() {
        return settings.maxInFlight() - inFlight.availablePermits();
    }

    /**
     * Encerra sem esperar a cadência: sinaliza o {@link #closeSignal} (acorda o ciclo que dorme entre
     * disparos), cancela o agendamento e interrompe o executor na hora ({@code shutdownNow}). A
     * interrupção só pode encontrar o ciclo dormindo ou à espera de vaga no semáforo: ele nunca segura
     * lock de série nem vaga nesses pontos, e o enfileiramento em si não é interrompível. Depois espera
     * até 5 s o executor terminar e até 5 s os checkpoints em voo. Idempotente. Ver o Javadoc da classe.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeSignal.countDown();
        ScheduledFuture<?> current = task;
        if (current != null) {
            current.cancel(false);
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOGGER.warning("NGRRD_LOCAL_CHECKPOINT executor não terminou em " + CLOSE_TIMEOUT_MS + " ms");
            }
            if (!inFlight.tryAcquire(settings.maxInFlight(), CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOGGER.warning("NGRRD_LOCAL_CHECKPOINT " + inFlightCount() + " checkpoints locais ainda em voo após "
                        + CLOSE_TIMEOUT_MS + " ms; o fechamento do registry os conclui antes de fechar os handles");
            } else {
                inFlight.release(settings.maxInFlight());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }
}
