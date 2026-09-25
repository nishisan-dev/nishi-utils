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

import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;

import java.time.Duration;

/**
 * Buffer de escrita consumido por {@link RemoteSeriesHandle} — isola a
 * dependência de {@link WriteDispatcher} (threads e pool próprios) para
 * permitir um fake nos testes unitários do handle.
 */
public interface WriteBuffer {

    /** Enfileira {@code write} no buffer do nó {@code ownerNodeId}. */
    void enqueue(String ownerNodeId, SeriesWrite write);

    /** Força o flush do buffer de {@code ownerNodeId} e espera sua conclusão, com o prazo padrão do dispatcher. */
    void flushNodeSync(String ownerNodeId);

    /**
     * Como {@link #flushNodeSync(String)}, mas com um teto explícito em vez do prazo padrão do
     * dispatcher — usado por {@code DefaultNgrrdClusterClient.close()} para respeitar um orçamento
     * TOTAL compartilhado entre vários handles (O1).
     */
    void flushNodeSync(String ownerNodeId, Duration maxWait);
    /**
     * Waits for writes already admitted for this series, regardless of redirects. A permanent
     * write failure must be reported instead of allowing a checkpoint to claim durability.
     * Implementations without per-series tracking can conservatively drain the whole node.
     */
    default void flushSeriesSync(String seriesKey, String ownerNodeId) {
        flushNodeSync(ownerNodeId);
    }

    /** Per-series barrier sharing the caller's total timeout budget. */
    default void flushSeriesSync(String seriesKey, String ownerNodeId, Duration maxWait) {
        flushNodeSync(ownerNodeId, maxWait);
    }

    /**
     * Marca {@code seriesKey} como confirmadamente inexistente — chamado por {@link RemoteSeriesHandle}
     * ao descobrir {@code SeriesNotFoundException} fora da reabertura assíncrona do dispatcher, ANTES de
     * sair do mapa do cliente. Falha as escritas pendentes da série (em buffer agora; em voo, na
     * resposta), recusa com {@code SeriesNotFoundException} qualquer {@link #enqueue} posterior da chave
     * e nunca reabre nem retenta essas escritas: sem isto, uma escrita receberia {@code NOT_OPEN} mais
     * tarde e o reopener, sem handle para a chave, devolveria {@code false} para sempre. A marca vale
     * até {@link #resetSeries}. Idempotente. Implementações sem rastreamento por série (fakes de teste)
     * podem ignorar.
     */
    default void failSeries(String seriesKey, Throwable cause) {
    }

    /**
     * Desfaz a marca de {@link #failSeries} para {@code seriesKey} — chamado quando um handle NOVO da
     * mesma chave abre com sucesso (a série existe de novo, ou foi recriada). As escritas desse handle
     * passam a usar uma rota nova, com dono {@code ownerNodeId}; escritas da geração marcada ainda em
     * voo continuam sendo concluídas (como falha, salvo {@code OK}) na rota antiga, sem afetar a nova.
     * No-op se a chave não estiver marcada. Implementações sem rastreamento por série podem ignorar.
     */
    default void resetSeries(String seriesKey, String ownerNodeId) {
    }
}
