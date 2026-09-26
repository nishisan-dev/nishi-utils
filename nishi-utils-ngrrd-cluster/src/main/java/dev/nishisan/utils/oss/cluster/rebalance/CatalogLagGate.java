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

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.util.Objects;
import java.util.Optional;

/**
 * Decide se um storage node pode ser <strong>destino</strong> de migração conforme o estado da réplica
 * local do catálogo que ele publica (issue #177): um destino com a réplica atrasada responde pela réplica
 * durante o corte de dono e alimenta o pingue-pongue de {@code WRONG_OWNER}. A porta vale só para o
 * destino — o nó continua podendo ser origem e continua no cálculo da distribuição alvo.
 */
public final class CatalogLagGate {

    private CatalogLagGate() {
    }

    /**
     * Motivo para excluir {@code node} como destino, ou vazio se ele é elegível.
     *
     * @param node   último status publicado pelo nó
     * @param maxLag lag máximo aceito ({@code ngrrd.rebalance.maxDestinationCatalogLag}); negativo desliga a
     *               porta, {@code 0} exige a réplica em dia
     * @return vazio quando elegível: porta desligada, status sem o campo (nó de versão anterior durante um
     *         rolling upgrade) ou réplica em dia; senão {@code "lag desconhecido"}, {@code "sincronizando"},
     *         {@code "bootstrap pendente"} ou {@code "lag=N>M"}
     */
    public static Optional<String> exclusionReason(StorageNodeStatus node, long maxLag) {
        Objects.requireNonNull(node, "node");
        CatalogReplicaStatus replica = node.catalogReplica();
        if (maxLag < 0 || replica == null || replica.leader()) {
            return Optional.empty();
        }
        if (replica.syncing()) {
            return Optional.of("sincronizando");
        }
        if (replica.pendingBootstrap()) {
            return Optional.of("bootstrap pendente");
        }
        if (!replica.lagKnown()) {
            return Optional.of("lag desconhecido");
        }
        if (replica.lag() > maxLag) {
            return Optional.of("lag=" + replica.lag() + ">" + maxLag);
        }
        return Optional.empty();
    }
}
