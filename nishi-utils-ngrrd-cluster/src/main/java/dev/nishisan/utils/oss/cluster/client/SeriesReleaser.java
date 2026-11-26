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

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.ReleaseOptions;
import dev.nishisan.utils.oss.cluster.api.ReleaseResult;
import dev.nishisan.utils.oss.cluster.api.ReleaseStatus;
import dev.nishisan.utils.oss.cluster.api.SeriesRelease;
import dev.nishisan.utils.oss.cluster.client.WriteDispatcher.ReleaseDrain;
import dev.nishisan.utils.oss.cluster.client.WriteDispatcher.ReleaseTicket;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementação de {@code NgrrdClusterClient.release}: entrega as séries pedidas, de forma assíncrona, num executor
 * de virtual threads ({@code ngrrd-release-}). Por série:
 * <ol>
 *   <li><b>admissão encerrada</b>, sob o lock de abertura da chave ({@link SeriesHandleCache#withOpenLock}): o handle
 *       principal sai do mapa e é fechado localmente, e o dispatcher aposenta a rota e fotografa a fronteira
 *       ({@link WriteDispatcher#beginRelease});</li>
 *   <li><b>dreno</b> de todas as fronteiras ({@link WriteDispatcher#drainForRelease}), com colheita das falhas;</li>
 *   <li><b>checkpoint</b> opcional, só nas séries sem falha de escrita;</li>
 *   <li><b>{@code CLOSE}</b> opcional no nó;</li>
 *   <li><b>limpeza local</b>: {@link WriteDispatcher#finishRelease} (com a falha entregue, a rota sai mesmo com falha)
 *       e {@code resolver.invalidate}.</li>
 * </ol>
 * As RPCs de {@code CHECKPOINT}/{@code CLOSE} correm em até {@code maxConcurrentPerNode} workers por nó dono. O prazo
 * TOTAL ({@code timeout}, padrão {@code closeTimeout}) vale para dreno, checkpoint e close. A future só falha com
 * {@link ErrorCode#CLOSED} (cliente fechado), {@link NullPointerException} (chaves nulas) ou com um erro
 * inesperado, repassado sem conversão; nunca por uma série.
 */
final class SeriesReleaser implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(SeriesReleaser.class.getName());
    private static final SeriesRelease RELEASED = new SeriesRelease(ReleaseStatus.RELEASED, Map.of(), null, null);
    private static final SeriesRelease NOT_HELD = new SeriesRelease(ReleaseStatus.NOT_HELD, Map.of(), null, null);

    private final SeriesHandleCache handles;
    private final WriteDispatcher dispatcher;
    private final PlacementLookup resolver;
    private final Duration defaultTimeout;
    private final BooleanSupplier clientClosed;
    private final Consumer<String> admissionProbe;
    private final ExecutorService executor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("ngrrd-release-", 0).factory());

    SeriesReleaser(SeriesHandleCache handles, WriteDispatcher dispatcher, PlacementLookup resolver,
            Duration defaultTimeout, BooleanSupplier clientClosed) {
        this(handles, dispatcher, resolver, defaultTimeout, clientClosed, key -> { });
    }

    /** Como o construtor acima, com um gancho chamado antes da admissão de cada série (para testes). */
    SeriesReleaser(SeriesHandleCache handles, WriteDispatcher dispatcher, PlacementLookup resolver,
            Duration defaultTimeout, BooleanSupplier clientClosed, Consumer<String> admissionProbe) {
        this.admissionProbe = Objects.requireNonNull(admissionProbe, "admissionProbe");
        this.handles = Objects.requireNonNull(handles, "handles");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "defaultTimeout");
        this.clientClosed = Objects.requireNonNull(clientClosed, "clientClosed");
    }

    /**
     * Agenda o release e devolve a future na hora, sem bloquear: a cópia das chaves (sem repetição, na ordem pedida) é o
     * único trabalho feito na thread do chamador. {@code options} nulo vale {@link ReleaseOptions#defaults()}.
     */
    CompletableFuture<ReleaseResult> release(Collection<String> seriesKeys, ReleaseOptions options) {
        if (seriesKeys == null) {
            return CompletableFuture.failedFuture(new NullPointerException("seriesKeys"));
        }
        List<String> keys;
        try {
            keys = List.copyOf(new LinkedHashSet<>(seriesKeys));
        } catch (NullPointerException e) {
            return CompletableFuture.failedFuture(new NullPointerException("seriesKeys contém chave nula"));
        }
        if (clientClosed.getAsBoolean()) {
            return CompletableFuture.failedFuture(closed());
        }
        ReleaseOptions effective = options != null ? options : ReleaseOptions.defaults();
        CompletableFuture<ReleaseResult> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    future.complete(run(keys, effective));
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(closed());
        }
        return future;
    }

    /** Fecha o executor: releases em curso terminam (o dreno falha com {@code CLOSED} quando o dispatcher fecha). */
    @Override
    public void close() {
        executor.shutdown();
    }

    private ReleaseResult run(List<String> keys, ReleaseOptions options) {
        Duration timeout = options.timeout() != null ? options.timeout() : defaultTimeout;
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Map<String, SeriesRelease> results = new LinkedHashMap<>();
        Map<String, Held> held = new LinkedHashMap<>();
        Set<String> finished = new HashSet<>();
        try {
            for (String key : keys) {
                admissionProbe.accept(key);
                Held entry = handles.withOpenLock(key, () -> closeAdmission(key));
                if (entry == null) {
                    results.put(key, NOT_HELD);
                } else {
                    results.put(key, RELEASED);
                    held.put(key, entry);
                }
            }

            List<ReleaseTicket> tickets = new ArrayList<>(held.size());
            for (Held entry : held.values()) {
                if (entry.ticket() != null) {
                    tickets.add(entry.ticket());
                }
            }
            Map<String, ReleaseDrain> drained = dispatcher.drainForRelease(tickets, remaining(deadlineNanos));
            drained.forEach((key, drain) -> results.put(key, toRelease(drain)));

            if (options.checkpoint() || options.closeOnNode()) {
                runRemoteCommands(held, results, options, deadlineNanos);
            }

            for (Map.Entry<String, Held> entry : held.entrySet()) {
                String key = entry.getKey();
                ReleaseStatus status = results.get(key).status();
                finish(key, entry.getValue(), status != ReleaseStatus.TIMED_OUT);
                finished.add(key);
                resolver.invalidate(key);
            }
        } finally {
            if (finished.size() < held.size()) {
                abandon(held, finished);
            }
        }
        if (clientClosed.getAsBoolean()) {
            throw closed();
        }
        ReleaseResult result = new ReleaseResult(results);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("release de " + keys.size() + " série(s): " + held.size() + " em uso, não liberadas="
                    + result.notReleased().size() + ", amostras com falha=" + result.failedSamples());
        }
        return result;
    }

    /** Etapa 5 de uma série: solta o ticket (e a proteção dele) e o holder do handle fechado pelo release. */
    private void finish(String key, Held value, boolean delivered) {
        if (value.ticket() != null) {
            dispatcher.finishRelease(value.ticket(), delivered);
        }
        if (value.writableClosed()) {
            // Idempotente: cobre o handle cuja rota não era a do mapa (ticket sem holder).
            dispatcher.endGeneration(key, value.handle().generationId(), value.handle());
        }
    }

    /**
     * Saída do release por uma falha fora do desfecho por série (dispatcher fechado, erro inesperado): finaliza, sem
     * dar falha alguma como entregue, os tickets que ainda não foram finalizados — senão a rota ficaria com holder e
     * com a proteção contra marcas para sempre. Cada série uma vez só; uma falha aqui vai para o log e não esconde a
     * original.
     */
    private void abandon(Map<String, Held> held, Set<String> finished) {
        for (Map.Entry<String, Held> entry : held.entrySet()) {
            if (finished.contains(entry.getKey())) {
                continue;
            }
            try {
                finish(entry.getKey(), entry.getValue(), false);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Falha ao finalizar o release interrompido de " + entry.getKey(), e);
            }
        }
    }

    /**
     * Etapa 1, sob o lock de abertura da chave: tira o handle principal do mapa, fecha-o localmente e aposenta a rota.
     * Devolve {@code null} se o cliente não tinha handle nem rota da série.
     */
    private Held closeAdmission(String key) {
        RemoteSeriesHandle handle = handles.get(key);
        boolean writableClosed = false;
        if (handle != null) {
            handles.remove(key, handle);
            writableClosed = handle.closeForRelease();
        }
        ReleaseTicket ticket = dispatcher.beginRelease(key, handle != null ? handle.generationId() : null,
                writableClosed ? handle : null);
        if (handle == null && ticket == null) {
            return null;
        }
        return new Held(handle, writableClosed, ticket);
    }

    private static SeriesRelease toRelease(ReleaseDrain drain) {
        return switch (drain.outcome()) {
            case DRAINED -> RELEASED;
            case WRITE_FAILURES -> new SeriesRelease(ReleaseStatus.WRITE_FAILURES, drain.failedSamplesByStatus(),
                    drain.errorCode(), drain.message());
            case SERIES_DELETED -> new SeriesRelease(ReleaseStatus.SERIES_DELETED, drain.failedSamplesByStatus(),
                    drain.errorCode(), drain.message());
            case TIMED_OUT -> new SeriesRelease(ReleaseStatus.TIMED_OUT, Map.of(), drain.errorCode(), drain.message());
        };
    }

    /**
     * Etapas 3 e 4: {@code CHECKPOINT} das séries drenadas sem falha e {@code CLOSE} das séries drenadas, pelos handles
     * graváveis fechados por este release, em até {@code maxConcurrentPerNode} workers por nó dono. Uma série cujas
     * escritas foram admitidas por outro handle (já fechado) não tem como receber o checkpoint e vira
     * {@link ReleaseStatus#CHECKPOINT_FAILED}.
     */
    private void runRemoteCommands(Map<String, Held> held, Map<String, SeriesRelease> results, ReleaseOptions options,
            long deadlineNanos) {
        Map<String, ConcurrentLinkedQueue<RemoteTask>> byOwner = new LinkedHashMap<>();
        for (Map.Entry<String, Held> entry : held.entrySet()) {
            String key = entry.getKey();
            Held value = entry.getValue();
            ReleaseStatus status = results.get(key).status();
            boolean checkpoint = options.checkpoint() && status == ReleaseStatus.RELEASED;
            if (checkpoint && !value.writableClosed()) {
                if (value.ticket() != null && value.ticket().boundary() > 0) {
                    results.put(key, new SeriesRelease(ReleaseStatus.CHECKPOINT_FAILED, Map.of(), ErrorCode.CLOSED,
                            "checkpoint não executado: as escritas de " + key + " vieram de um handle já fechado"));
                }
                continue;
            }
            boolean close = options.closeOnNode() && value.writableClosed()
                    && (status == ReleaseStatus.RELEASED || status == ReleaseStatus.WRITE_FAILURES);
            if (checkpoint || close) {
                byOwner.computeIfAbsent(value.handle().owner(), owner -> new ConcurrentLinkedQueue<>())
                        .add(new RemoteTask(key, value.handle(), checkpoint, options.closeOnNode()));
            }
        }
        if (byOwner.isEmpty()) {
            return;
        }
        Map<String, SeriesRelease> remoteResults = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> workers = new ArrayList<>();
        for (ConcurrentLinkedQueue<RemoteTask> queue : byOwner.values()) {
            int count = Math.min(options.maxConcurrentPerNode(), queue.size());
            for (int i = 0; i < count; i++) {
                try {
                    workers.add(CompletableFuture.runAsync(() -> {
                        for (RemoteTask task; (task = queue.poll()) != null; ) {
                            SeriesRelease outcome = runRemote(task, deadlineNanos);
                            if (outcome != null) {
                                remoteResults.put(task.key(), outcome);
                            }
                        }
                    }, executor));
                } catch (RejectedExecutionException e) {
                    // Executor encerrado pelo close() do cliente durante o release.
                    throw closed();
                }
            }
        }
        // Cada RPC é limitada pelo prazo do release (tentativa e retentativas): os workers terminam com ele.
        CompletableFuture.allOf(workers.toArray(CompletableFuture[]::new)).join();
        results.putAll(remoteResults);
    }

    /** Checkpoint e/ou close de uma série; devolve o novo desfecho se o checkpoint falhou, senão {@code null}. */
    private SeriesRelease runRemote(RemoteTask task, long deadlineNanos) {
        SeriesRelease outcome = null;
        if (task.checkpoint()) {
            try {
                task.handle().checkpointForRelease(remaining(deadlineNanos));
            } catch (NgrrdClusterException e) {
                boolean deleted = e.code() == ErrorCode.SERIES_DELETED || e.code() == ErrorCode.QUARANTINED;
                outcome = new SeriesRelease(deleted ? ReleaseStatus.SERIES_DELETED : ReleaseStatus.CHECKPOINT_FAILED,
                        Map.of(), e.code(), e.getMessage());
            } catch (RuntimeException e) {
                LOGGER.log(Level.FINE, "Falha no checkpoint do release de " + task.key(), e);
                outcome = new SeriesRelease(ReleaseStatus.CHECKPOINT_FAILED, Map.of(), ErrorCode.REMOTE_ERROR,
                        String.valueOf(e));
            }
        }
        if (task.close() && (outcome == null || outcome.status() == ReleaseStatus.CHECKPOINT_FAILED)) {
            task.handle().closeOnNodeForRelease(remaining(deadlineNanos));
        }
        return outcome;
    }

    private static Duration remaining(long deadlineNanos) {
        return Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime()));
    }

    private static NgrrdClusterException closed() {
        return new NgrrdClusterException(ErrorCode.CLOSED, "cliente do cluster ngrrd já foi fechado");
    }

    /** Série em uso no início do release: o handle principal (se havia), se ele era gravável e o ticket da rota. */
    private record Held(RemoteSeriesHandle handle, boolean writableClosed, ReleaseTicket ticket) { }

    /** RPCs de uma série: {@code CHECKPOINT} e/ou {@code CLOSE}. */
    private record RemoteTask(String key, RemoteSeriesHandle handle, boolean checkpoint, boolean close) { }
}
