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

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

/**
 * Lógica de prazo/backoff/espera por líder compartilhada entre {@link PlacementResolver#placeAtLeader}
 * (PLACE) e {@link CatalogLookupClient} (CATALOG_LOOKUP): as duas operam sob um {@code deadline} único
 * (epoch millis, de {@link Clock#millis()}), fazem polling de {@link ClusterRpc#leaderId()} até um
 * líder aparecer e limitam qualquer backoff ao tempo restante até o prazo — extraída para evitar a
 * duplicação que existia entre elas.
 */
final class LeaderCalls {

    /** Intervalo de polling à espera de um líder eleito — curto de propósito, nunca o único fator de prazo. */
    static final Duration LEADER_POLL_INTERVAL = Duration.ofMillis(50);

    private LeaderCalls() {
    }

    /**
     * Aguarda um líder eleito até {@code deadline}, fazendo polling de {@link ClusterRpc#leaderId()}.
     *
     * @throws NgrrdClusterException com {@link ErrorCode#NO_LEADER} se o prazo se esgotar sem líder eleito
     */
    static NodeId awaitLeaderOrThrow(ClusterRpc rpc, Clock clock, long deadline, String operationDescription) {
        Optional<NodeId> leader = rpc.leaderId();
        while (leader.isEmpty() && clock.millis() < deadline) {
            sleepQuietly(cappedBackoff(clock, LEADER_POLL_INTERVAL, deadline, operationDescription));
            leader = rpc.leaderId();
        }
        return leader.orElseThrow(() -> new NgrrdClusterException(ErrorCode.NO_LEADER,
                "nenhum líder eleito para " + operationDescription));
    }

    /**
     * Tempo restante até {@code deadline}.
     *
     * @throws NgrrdClusterException com {@link ErrorCode#TIMEOUT} se o prazo já tiver se esgotado
     */
    static Duration remainingUntil(Clock clock, long deadline, String operationDescription) {
        long remaining = deadline - clock.millis();
        if (remaining <= 0) {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "prazo esgotado para " + operationDescription);
        }
        return Duration.ofMillis(remaining);
    }

    /** {@code backoff} limitado ao tempo restante até {@code deadline} — nunca ultrapassa o prazo. */
    static Duration cappedBackoff(Clock clock, Duration backoff, long deadline, String operationDescription) {
        Duration remaining = remainingUntil(clock, deadline, operationDescription);
        return backoff.compareTo(remaining) > 0 ? remaining : backoff;
    }

    /** Dorme {@code duration}; interrupção vira {@link NgrrdClusterException} com {@link ErrorCode#CLOSED}. */
    static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando retentativa", e);
        }
    }
}
