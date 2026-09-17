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

package dev.nishisan.utils.oss.cluster.placement;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Política de placement do estágio 1: escolhe o storage node com menor carga
 * entre os candidatos elegíveis, com uma ordem de desempate <strong>total e
 * determinística</strong> — o resultado não depende da ordem de iteração de
 * {@link PlacementContext#nodes()}.
 *
 * <p>Um nó é candidato quando: está {@link NodeState#ACTIVE}; é membro
 * alcançável do cluster; seu último status reportado não está velho
 * ({@link StorageNodeStatus#isFresh}); e, quando a capacidade é conhecida
 * ({@code capacityBytes > 0}), sua {@link StorageNodeStatus#fillRatio()} é
 * menor que {@value #CAPACITY_GUARD_FILL_RATIO} — guarda de capacidade que
 * impede empurrar novas séries para um nó praticamente cheio. Sem
 * candidatos, retorna {@link Optional#empty()} — cabe ao chamador responder
 * {@code NO_STORAGE_NODE_AVAILABLE}.</p>
 *
 * <p>Entre os candidatos: se {@code preferredOwnerNodeId} sobreviveu aos
 * filtros acima, ele vence direto (ex.: adoção de série existente pelo
 * {@code LocalReconciler} ou retomada de dono após uma migração abortada).
 * Caso contrário, cada candidato recebe uma chave de ordenação pré-calculada
 * (nenhum ramo condicional entra no comparador em si) e o desempate segue,
 * em ordem:</p>
 * <ol>
 *   <li>menor carga efetiva ({@code seriesCount + pendente});</li>
 *   <li>menor {@link StorageNodeStatus#fillRatio()}, valendo {@code 0.0}
 *       quando a capacidade não é conhecida ({@code capacityBytes <= 0});</li>
 *   <li>menor {@code nodeId} (ordem natural de {@link String} — sempre
 *       decide, garantindo uma ordem total).</li>
 * </ol>
 *
 * <p><strong>Nota de correção:</strong> uma versão anterior comparava
 * {@code fillRatio()} apenas "quando ambos os nós têm capacidade conhecida"
 * e devolvia empate (zero) caso contrário. Isso quebra a transitividade do
 * comparador — A pode empatar com B (capacidade desconhecida em algum dos
 * dois) e B empatar com C, mas A perder de C (ambos com capacidade
 * conhecida), fazendo o vencedor de {@link java.util.stream.Stream#min}
 * depender da ordem de iteração da entrada. A chave pré-calculada por
 * candidato elimina o problema: todo par é sempre comparável.</p>
 */
public final class LeastLoadedPlacementPolicy implements PlacementPolicy {

    /**
     * Acima deste {@code fillRatio} (quando a capacidade é conhecida), o nó
     * deixa de ser candidato a receber novas séries.
     */
    private static final double CAPACITY_GUARD_FILL_RATIO = 0.95;

    @Override
    public Optional<String> choose(PlacementContext ctx) {
        List<StorageNodeStatus> candidates = ctx.nodes().stream()
                .filter(node -> node.state() == NodeState.ACTIVE)
                .filter(node -> ctx.reachableNodeIds().contains(node.nodeId()))
                .filter(node -> node.isFresh(ctx.nowEpochMs(), ctx.statusReportInterval()))
                .filter(node -> !(node.capacityBytes() > 0 && node.fillRatio() >= CAPACITY_GUARD_FILL_RATIO))
                .toList();

        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        String preferred = ctx.preferredOwnerNodeId();
        if (preferred != null && candidates.stream().anyMatch(node -> node.nodeId().equals(preferred))) {
            return Optional.of(preferred);
        }

        Comparator<Candidate> byTotalOrder = Comparator.comparingLong(Candidate::effectiveLoad)
                .thenComparingDouble(Candidate::sortableFillRatio)
                .thenComparing(Candidate::nodeId);

        return candidates.stream()
                .map(node -> new Candidate(node.nodeId(), effectiveLoad(node, ctx), sortableFillRatio(node)))
                .min(byTotalOrder)
                .map(Candidate::nodeId);
    }

    private static long effectiveLoad(StorageNodeStatus node, PlacementContext ctx) {
        long pending = ctx.pendingSeriesByNode().getOrDefault(node.nodeId(), 0L);
        return node.seriesCount() + pending;
    }

    /** {@code fillRatio()} do nó, ou {@code 0.0} quando a capacidade não é conhecida. */
    private static double sortableFillRatio(StorageNodeStatus node) {
        return node.capacityBytes() > 0 ? node.fillRatio() : 0.0;
    }

    /**
     * Chave de ordenação pré-calculada por candidato. Existe para que o
     * comparador de {@link #choose} seja uma composição pura de
     * {@code Comparator.comparingLong/thenComparingDouble/thenComparing},
     * sem nenhum {@code if} par-a-par — a única decisão condicional
     * ({@link #sortableFillRatio}) acontece uma vez, ao montar a chave.
     */
    private record Candidate(String nodeId, long effectiveLoad, double sortableFillRatio) {
    }
}
