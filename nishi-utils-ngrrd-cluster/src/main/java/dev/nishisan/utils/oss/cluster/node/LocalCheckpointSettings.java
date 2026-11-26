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

import java.time.Duration;
import java.util.Objects;

/**
 * Configuração do checkpoint local do storage node ({@code ngrrd.checkpoint}, 8.14.0): o
 * {@link LocalCheckpointer} faz, no próprio nó, o checkpoint periódico das séries sujas, sem depender
 * do {@code FLUSH}/{@code CHECKPOINT} remoto do cliente.
 *
 * @param enabled     liga o checkpoint local; desligado por padrão (nenhum executor é criado)
 * @param interval    intervalo entre o fim de um ciclo e o início do próximo; o ciclo espalha os
 *                    checkpoints por ~80% dele. Positivo; padrão {@link #DEFAULT_INTERVAL}
 * @param maxInFlight teto de checkpoints locais em voo (enfileirados e ainda não concluídos) ao mesmo
 *                    tempo; {@code >= 1}. Padrão {@link #defaultMaxInFlight()}
 * @param threads     tamanho do pool dedicado de force ({@code ngrrd.checkpoint.threads}, 8.15.0): o
 *                    msync/fdatasync de todo checkpoint do nó (local, {@code CHECKPOINT}/{@code FLUSH}
 *                    remoto e migração) sai da thread do writer da série e roda nesse pool. {@code >= 0};
 *                    {@code 0} (padrão) = force inline no writer, o comportamento anterior. Vale
 *                    também com {@code enabled=false}
 */
public record LocalCheckpointSettings(boolean enabled, Duration interval, int maxInFlight, int threads) {

    /** Intervalo padrão entre ciclos: 300 s. */
    public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(300);

    /** Pool de force desligado por padrão: o force roda inline no writer, como antes da 8.15.0. */
    public static final int DEFAULT_THREADS = 0;

    public LocalCheckpointSettings {
        Objects.requireNonNull(interval, "ngrrd.checkpoint.interval é obrigatório");
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("ngrrd.checkpoint.interval deve ser > 0: " + interval);
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("ngrrd.checkpoint.maxInFlight deve ser >= 1: " + maxInFlight);
        }
        if (threads < 0) {
            throw new IllegalArgumentException("ngrrd.checkpoint.threads deve ser >= 0: " + threads);
        }
    }

    /** Forma da 8.14.0, sem pool de force ({@code threads=0}). */
    public LocalCheckpointSettings(boolean enabled, Duration interval, int maxInFlight) {
        this(enabled, interval, maxInFlight, DEFAULT_THREADS);
    }

    /** {@code min(8, max(2, processadores disponíveis))}: o tamanho do pool de writers, limitado a 8. */
    public static int defaultMaxInFlight() {
        return Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors()));
    }

    /** Desligado, com {@link #DEFAULT_INTERVAL}, {@link #defaultMaxInFlight()} e sem pool de force: o padrão do nó. */
    public static LocalCheckpointSettings disabled() {
        return new LocalCheckpointSettings(false, DEFAULT_INTERVAL, defaultMaxInFlight(), DEFAULT_THREADS);
    }
}
