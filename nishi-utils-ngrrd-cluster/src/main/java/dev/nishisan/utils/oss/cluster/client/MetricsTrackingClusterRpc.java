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
import dev.nishisan.utils.oss.cluster.metrics.LatencyHistogram;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Decorador de {@link ClusterRpc} que mede, do lado do cliente, a latência de
 * toda chamada síncrona ({@link #call}), agregada e por comando, e conta quantas foram
 * {@link Commands#PLACE} — alimenta {@code rpcLatencyByCommand}, {@code rpcLatency} (obsoleto desde a
 * 8.15.0) e {@code placeCount} de {@code ClientMetricsSnapshot}.
 *
 * <p>A latência é medida mesmo quando {@link #call} lança exceção (timeout,
 * erro remoto): o tempo gasto esperando uma falha também é sinal útil sobre o
 * comportamento do cluster do ponto de vista do cliente.</p>
 */
final class MetricsTrackingClusterRpc implements ClusterRpc {

    private final ClusterRpc delegate;
    private final LatencyHistogram latency = new LatencyHistogram();
    /** Latência por comando, criada sob demanda (conjunto fechado em {@link Commands}, com teto de proteção). */
    private final ConcurrentMap<String, LatencyHistogram> byCommand = new ConcurrentHashMap<>();
    private final LongAdder placeCount = new LongAdder();

    /** Teto de histogramas por comando; nomes além dele vão para {@link #OTHER}. */
    static final int MAX_COMMANDS = 64;
    /** Chave dos comandos inesperados, quando o teto de {@link #MAX_COMMANDS} já foi atingido. */
    static final String OTHER = "other";

    MetricsTrackingClusterRpc(ClusterRpc delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
        return timed(command, () -> delegate.call(target, command, body, responseType));
    }

    @Override
    public <R> R call(NodeId target, String command, Object body, Class<R> responseType, Duration timeout) {
        // B1 (achado do Refuter): repassa o teto explícito ao delegate — o mesmo raciocínio de
        // instrumentação da sobrecarga simples, sem alterar a semântica de prazo.
        return timed(command, () -> delegate.call(target, command, body, responseType, timeout));
    }

    private <R> R timed(String command, Supplier<R> call) {
        long startNanos = System.nanoTime();
        try {
            return call.get();
        } finally {
            long elapsedNanos = System.nanoTime() - startNanos;
            latency.record(elapsedNanos);
            histogramFor(command).record(elapsedNanos);
            if (Commands.PLACE.equals(command)) {
                placeCount.increment();
            }
        }
    }

    /** Histograma do comando; ao atingir o teto, nomes novos (ou nulos) caem em {@link #OTHER}. */
    private LatencyHistogram histogramFor(String command) {
        if (command == null) {
            return byCommand.computeIfAbsent(OTHER, k -> new LatencyHistogram());
        }
        LatencyHistogram existing = byCommand.get(command);
        if (existing != null) {
            return existing;
        }
        // Reserva uma vaga para "other": o total nunca passa de MAX_COMMANDS.
        String key = byCommand.size() >= MAX_COMMANDS - 1 ? OTHER : command;
        return byCommand.computeIfAbsent(key, k -> new LatencyHistogram());
    }

    @Override
    public NodeId localId() {
        return delegate.localId();
    }

    @Override
    public Optional<NodeId> leaderId() {
        return delegate.leaderId();
    }

    @Override
    public boolean isConnected(NodeId target) {
        return delegate.isConnected(target);
    }

    /** Snapshot atual da latência agregada de todas as chamadas feitas por este cliente. */
    LatencySnapshot latencySnapshot() {
        return latency.snapshot();
    }

    /**
     * Snapshot da latência de cada comando já chamado, sem ordem definida: o {@code ClientMetricsSnapshot}
     * guarda uma cópia imutável ({@code Map.copyOf}), que não preserva ordem.
     */
    Map<String, LatencySnapshot> latencySnapshotByCommand() {
        Map<String, LatencySnapshot> result = new HashMap<>();
        byCommand.forEach((command, histogram) -> result.put(command, histogram.snapshot()));
        return result;
    }

    /** Total de chamadas {@link Commands#PLACE} feitas por este cliente. */
    long placeCount() {
        return placeCount.sum();
    }
}
