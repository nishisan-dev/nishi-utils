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

package dev.nishisan.utils.oss.cluster.catalog;

import dev.nishisan.utils.ngrid.replication.ReplicationManager;

import java.io.Serializable;

/**
 * Estado da réplica local do mapa {@value CatalogService#CATALOG_MAP} num storage node, publicado no
 * {@link StorageNodeStatus} (issue #177). É o lag <strong>por tópico</strong> do NGrid — o
 * {@code HIGH_REPLICATION_LAG} global do snapshot operacional não serve para decidir se a réplica do
 * catálogo deste nó é confiável.
 *
 * <p>{@code lag = 0} só significa "em dia" quando o high-watermark do líder já é conhecido:
 * {@code ReplicationManager.getReplicationLag} devolve {@code 0} também quando o HWM ainda é
 * desconhecido ({@code leaderHighWatermark <= 0}). Por isso {@link #lagKnown()} e
 * {@link #caughtUp(long)} nunca leem {@code lag} sozinho.</p>
 *
 * @param leader               se o nó era o líder no instante da coleta (a réplica dele é a fonte)
 * @param lag                  high-watermark do líder menos a fronteira aplicada localmente
 * @param leaderHighWatermark  maior sequência do tópico conhecida no líder; {@code <= 0} = desconhecida
 * @param nextExpectedSequence próxima sequência que a réplica local espera aplicar
 * @param syncing              se há sincronização por snapshot em curso para o tópico
 * @param pendingBootstrap     se a reaplicação do relay aguarda o bootstrap do tópico
 * @param streaming            se o nó está puxando o tópico do líder por streaming agora
 */
public record CatalogReplicaStatus(
        boolean leader,
        long lag,
        long leaderHighWatermark,
        long nextExpectedSequence,
        boolean syncing,
        boolean pendingBootstrap,
        boolean streaming) implements Serializable {

    /** Ver Javadoc de {@code SeriesPlacement#serialVersionUID}. */
    private static final long serialVersionUID = 1L;

    private static final CatalogReplicaStatus LEADER = new CatalogReplicaStatus(true, 0L, 0L, 0L, false, false, false);
    private static final CatalogReplicaStatus UNKNOWN = new CatalogReplicaStatus(false, 0L, 0L, 0L, false, false, false);

    /** Réplica do líder: é a fonte do catálogo, por definição em dia. */
    public static CatalogReplicaStatus ofLeader() {
        return LEADER;
    }

    /**
     * Monta o status a partir do estado do tópico no {@link ReplicationManager}.
     *
     * @param leader se este nó é o líder agora ({@link #ofLeader()}, ignorando {@code status})
     * @param status estado do tópico do catálogo; {@code null} (tópico ainda não registrado) devolve um
     *               status com lag desconhecido ({@code leaderHighWatermark = 0})
     */
    public static CatalogReplicaStatus from(boolean leader, ReplicationManager.TopicReplicationStatus status) {
        if (leader) {
            return LEADER;
        }
        if (status == null) {
            return UNKNOWN;
        }
        return new CatalogReplicaStatus(false, status.lag(), status.leaderHighWatermark(),
                status.nextExpectedSequence(), status.syncing(), status.relayPendingBootstrap(), status.streaming());
    }

    /** Se {@link #lag()} é significativo: líder, ou seguidor que já conhece o HWM do líder. */
    public boolean lagKnown() {
        return leader || leaderHighWatermark > 0;
    }

    /**
     * Se a réplica está em dia dentro de {@code maxLag}: líder, ou lag conhecido, sem sincronização nem
     * bootstrap pendente, e {@code lag <= maxLag}.
     */
    public boolean caughtUp(long maxLag) {
        return leader || (lagKnown() && !syncing && !pendingBootstrap && lag <= maxLag);
    }

    /**
     * Forma curta do lag para operadores — a coluna {@code CAT_LAG} do CLI e o {@code catalogLag=} do
     * {@code NGRRD_NODE_STATUS}: {@code lider}, o lag numérico, {@code sync} (sincronizando),
     * {@code boot} (bootstrap pendente), {@code ?} (lag desconhecido) ou {@code -} ({@code replica} nula:
     * nó de versão anterior ou estado não coletado).
     */
    public static String describeLag(CatalogReplicaStatus replica) {
        if (replica == null) {
            return "-";
        }
        if (replica.leader) {
            return "lider";
        }
        if (replica.syncing) {
            return "sync";
        }
        if (replica.pendingBootstrap) {
            return "boot";
        }
        if (!replica.lagKnown()) {
            return "?";
        }
        return Long.toString(replica.lag);
    }
}
