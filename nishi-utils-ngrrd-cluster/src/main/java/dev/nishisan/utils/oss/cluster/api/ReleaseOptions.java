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

import java.time.Duration;

/**
 * Opções de {@link NgrrdClusterClient#release(java.util.Collection, ReleaseOptions)}.
 *
 * @param checkpoint           {@code true}: depois do dreno, pede {@code CHECKPOINT} ao dono de cada série sem falha
 *                             de escrita — a garantia passa a ser fsync confirmado no dono, ao custo de uma RPC por
 *                             série. {@code false} (padrão): a garantia é a de uma marca de escrita, ACK do dono sem
 *                             fsync; o checkpoint local periódico do nó limita a janela de perda numa queda abrupta
 * @param closeOnNode          {@code true}: manda {@code CLOSE} ao dono de cada série drenada, para desativação
 *                             planejada. {@code false} (padrão) na entrega entre clientes: o próximo dono reabre a
 *                             mesma série no mesmo nó, e o {@code CLOSE} faria o nó fazer checkpoint, fechar e
 *                             reabrir à toa — e descarta a definição no nó, de modo que o cliente que já abriu a
 *                             série paga um {@code NOT_OPEN} e um {@code OPEN} na escrita seguinte. Uma série
 *                             abandonada de vez fecha pela ociosidade do nó
 * @param timeout              prazo TOTAL do release (dreno, checkpoint e close); {@code null} (padrão) usa
 *                             {@link NgrrdClusterConfig#closeTimeout()}. Positivo quando informado
 * @param maxConcurrentPerNode teto de RPCs de {@code CHECKPOINT}/{@code CLOSE} simultâneas por nó dono, {@code >= 1}
 *                             (padrão 32)
 * @since 8.15.0
 */
public record ReleaseOptions(boolean checkpoint, boolean closeOnNode, Duration timeout, int maxConcurrentPerNode) {

    private static final int DEFAULT_MAX_CONCURRENT_PER_NODE = 32;
    private static final ReleaseOptions DEFAULTS =
            new ReleaseOptions(false, false, null, DEFAULT_MAX_CONCURRENT_PER_NODE);

    public ReleaseOptions {
        if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
            throw new IllegalArgumentException("timeout deve ser > 0: " + timeout);
        }
        if (maxConcurrentPerNode < 1) {
            throw new IllegalArgumentException("maxConcurrentPerNode deve ser >= 1: " + maxConcurrentPerNode);
        }
    }

    /**
     * {@code checkpoint=false}, {@code closeOnNode=false}, {@code timeout=null} (o {@code closeTimeout} do cliente) e
     * {@code maxConcurrentPerNode=32}.
     */
    public static ReleaseOptions defaults() {
        return DEFAULTS;
    }

    /** Cópia com {@link #checkpoint()} = {@code value}. */
    public ReleaseOptions withCheckpoint(boolean value) {
        return new ReleaseOptions(value, closeOnNode, timeout, maxConcurrentPerNode);
    }

    /** Cópia com {@link #closeOnNode()} = {@code value}. */
    public ReleaseOptions withCloseOnNode(boolean value) {
        return new ReleaseOptions(checkpoint, value, timeout, maxConcurrentPerNode);
    }

    /** Cópia com {@link #timeout()} = {@code value} ({@code null} volta ao {@code closeTimeout} do cliente). */
    public ReleaseOptions withTimeout(Duration value) {
        return new ReleaseOptions(checkpoint, closeOnNode, value, maxConcurrentPerNode);
    }

    /** Cópia com {@link #maxConcurrentPerNode()} = {@code value}, que deve ser {@code >= 1}. */
    public ReleaseOptions withMaxConcurrentPerNode(int value) {
        return new ReleaseOptions(checkpoint, closeOnNode, timeout, value);
    }
}
