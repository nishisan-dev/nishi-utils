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

package dev.nishisan.utils.oss.cluster.admin;

import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;

import java.time.Clock;
import java.util.Objects;

/**
 * Serviço de administração executado só no líder: transições de estado de um storage node
 * ({@link NodeState#DRAINING}/{@link NodeState#ACTIVE}) atendidas por {@code ngrrd.admin.drain}/
 * {@code ngrrd.admin.activate} (ver {@code AdminRequestHandler}).
 *
 * <p>Ambos os métodos são idempotentes (reescrever o mesmo estado não é um erro) e disparam um ciclo
 * imediato do {@link Rebalancer} — {@code drain} para começar a esvaziar o nó, {@code activate} para
 * ele voltar a receber séries. A promoção {@code DRAINING} → {@link NodeState#DRAINED} não acontece
 * aqui: é o {@link Rebalancer} quem a faz, ao final de cada ciclo (inclusive o disparado por este
 * serviço), quando o nó não possui mais séries {@code ACTIVE} nem migração em curso como origem — ver
 * Javadoc de {@code Rebalancer#promoteDrainedNodes}.</p>
 */
public final class AdminService {

    private final CatalogService catalog;
    private final Rebalancer rebalancer;
    private final Clock clock;

    public AdminService(CatalogService catalog, Rebalancer rebalancer, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.rebalancer = Objects.requireNonNull(rebalancer, "rebalancer");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Marca {@code nodeId} como {@link NodeState#DRAINING}: {@code PlacementPolicy} deixa de escolhê-lo
     * para séries novas e o próximo ciclo do {@link Rebalancer} (disparado imediatamente por esta
     * chamada) começa a esvaziá-lo. Idempotente — chamar de novo sobre um nó já {@code DRAINING} (ou
     * {@code DRAINED}) apenas redispara o ciclo, sem erro.
     *
     * @throws IllegalArgumentException se {@code nodeId} não é conhecido pelo catálogo
     */
    public StorageNodeStatus drain(String nodeId) {
        return transitionTo(nodeId, NodeState.DRAINING);
    }

    /**
     * Marca {@code nodeId} como {@link NodeState#ACTIVE} novamente — volta a ser candidato a novos
     * placements e o {@link Rebalancer} pode movê-lo séries de volta. Idempotente.
     *
     * @throws IllegalArgumentException se {@code nodeId} não é conhecido pelo catálogo
     */
    public StorageNodeStatus activate(String nodeId) {
        return transitionTo(nodeId, NodeState.ACTIVE);
    }

    private StorageNodeStatus transitionTo(String nodeId, NodeState newState) {
        Objects.requireNonNull(nodeId, "nodeId");
        StorageNodeStatus current = catalog.nodeStatusLocal(nodeId)
                .orElseThrow(() -> new IllegalArgumentException("nó desconhecido pelo catálogo: " + nodeId));
        StorageNodeStatus updated = current.withState(newState, clock.millis());
        catalog.putNodeStatus(updated);
        // Best-effort: um ciclo de rebalanceamento pode já estar em curso (triggerNow() devolve 0/0
        // nesse caso) — o ciclo em andamento, ou o próximo agendado, ainda vê o novo estado publicado
        // acima e reage a ele.
        rebalancer.triggerNow();
        return updated;
    }
}
