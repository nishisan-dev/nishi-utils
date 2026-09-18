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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Decorador de {@link ClusterRpc} que mede, do lado do cliente, a latência de
 * toda chamada síncrona ({@link #call}) e conta quantas foram {@link Commands#PLACE}
 * — alimenta {@code rpcLatency}/{@code placeCount} de {@code ClientMetricsSnapshot}.
 *
 * <p>A latência é medida mesmo quando {@link #call} lança exceção (timeout,
 * erro remoto): o tempo gasto esperando uma falha também é sinal útil sobre o
 * comportamento do cluster do ponto de vista do cliente.</p>
 */
final class MetricsTrackingClusterRpc implements ClusterRpc {

    private final ClusterRpc delegate;
    private final LatencyHistogram latency = new LatencyHistogram();
    private final LongAdder placeCount = new LongAdder();

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
            latency.record(System.nanoTime() - startNanos);
            if (Commands.PLACE.equals(command)) {
                placeCount.increment();
            }
        }
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

    /** Total de chamadas {@link Commands#PLACE} feitas por este cliente. */
    long placeCount() {
        return placeCount.sum();
    }
}
