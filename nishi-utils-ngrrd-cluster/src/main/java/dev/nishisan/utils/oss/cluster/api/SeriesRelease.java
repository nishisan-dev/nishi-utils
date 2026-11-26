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

package dev.nishisan.utils.oss.cluster.api;

import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;

import java.util.Map;
import java.util.Objects;

/**
 * Desfecho do release de uma série ({@link ReleaseResult#series()}).
 *
 * @param status                o desfecho
 * @param failedSamplesByStatus amostras admitidas antes do release com falha final, por status, entregues por este
 *                              release (cada falha vai a uma única marca, ou a todo release ainda ativo da mesma
 *                              série cuja fronteira a cubra; um release que começa depois de outro já ter colhido
 *                              a série não a vê de novo, e o release que admitiu a escrita sempre a recebe). Só
 *                              preenchido em {@link NgrrdClusterConfig.WriteFailureReporting#MARKS}; em
 *                              {@code BARRIER}, que guarda só a primeira falha da série, fica vazio mesmo com
 *                              {@link ReleaseStatus#WRITE_FAILURES}. Uma geração substituída conta como
 *                              {@link SeriesStatus#SERIES_DELETED}
 * @param errorCode             código da falha ({@link ErrorCode#TIMEOUT} em {@link ReleaseStatus#TIMED_OUT},
 *                              {@link ErrorCode#SERIES_DELETED}/{@link ErrorCode#QUARANTINED} em
 *                              {@link ReleaseStatus#SERIES_DELETED}); {@code null} em {@link ReleaseStatus#RELEASED}
 *                              e {@link ReleaseStatus#NOT_HELD}
 * @param message               descrição da falha; {@code null} quando não há falha
 * @since 8.15.0
 */
public record SeriesRelease(ReleaseStatus status, Map<SeriesStatus, Long> failedSamplesByStatus,
        ErrorCode errorCode, String message) {

    public SeriesRelease {
        Objects.requireNonNull(status, "status");
        failedSamplesByStatus = Map.copyOf(Objects.requireNonNullElse(failedSamplesByStatus, Map.of()));
    }

    /** Total de {@link #failedSamplesByStatus()}. */
    public long failedSamples() {
        long total = 0L;
        for (long count : failedSamplesByStatus.values()) {
            total += count;
        }
        return total;
    }
}
