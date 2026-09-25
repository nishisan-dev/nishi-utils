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
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.io.IOException;
import java.time.Duration;

/**
 * B3 (achado do Refuter): reconhece uma falha de TRANSPORTE (retentável) e espera a conexão de
 * transporte se restabelecer antes de uma nova tentativa — em vez de tentar de novo às cegas
 * enquanto o TCP ainda está no meio de uma reconexão. Compartilhada por
 * {@link PlacementResolver#placeAtLeader} (PLACE) e {@link RemoteSeriesHandle} (OPEN/CHECKPOINT/
 * FLUSH/READ/CLOSE) — o cenário raiz observado (achado do Refuter) foi o segundo cliente de
 * {@code DistributedWriteReadClusterTest} recebendo "No connection available for storage-0" por até
 * 60s: o transporte TCP ainda não tinha terminado de conectar, e a chamada falhava direto sem
 * retentativa nenhuma.
 */
final class TransportRetry {

    /** Intervalo de polling à espera de {@code rpc.isConnected(target)} ficar verdadeiro. */
    private static final Duration CONNECTION_POLL_INTERVAL = Duration.ofMillis(50);

    private TransportRetry() {
    }

    /**
     * Indica se {@code e} representa uma falha de transporte (não de aplicação): {@code TIMEOUT}, ou
     * {@code REMOTE_ERROR} cuja causa é uma {@link IOException} — o formato usado por
     * {@code TransportClusterRpc} para "No connection available" e falhas equivalentes do
     * {@code Transport} do NGrid. Um {@code REMOTE_ERROR} de aplicação (erro reportado pelo próprio
     * storage node) não tem {@link IOException} como causa e não deve ser retentado aqui.
     */
    static boolean isTransportFailure(NgrrdClusterException e) {
        if (e.code() == ErrorCode.TIMEOUT) {
            return true;
        }
        if (e.code() != ErrorCode.REMOTE_ERROR) return false;
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (Throwable cause = e.getCause(); cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof IOException
                    || cause instanceof dev.nishisan.utils.ngrid.cluster.transport.PeerDisconnectedException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Espera até {@code backoff} por {@code rpc.isConnected(target)} ficar verdadeiro — se já estiver
     * conectado, apenas dorme {@code backoff} por inteiro (o problema pode não ser a conexão em si,
     * ex.: um TIMEOUT de aplicação lento do outro lado).
     */
    static void awaitConnectionOrBackoff(ClusterRpc rpc, NodeId target, Duration backoff) {
        if (rpc.isConnected(target)) {
            sleepQuietly(backoff);
            return;
        }
        long deadline = System.currentTimeMillis() + backoff.toMillis();
        while (!rpc.isConnected(target)) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return;
            }
            sleepQuietly(Duration.ofMillis(Math.min(CONNECTION_POLL_INTERVAL.toMillis(), remaining)));
        }
    }

    static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando retentativa de transporte", e);
        }
    }
}
