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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

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
 * impede empurrar novas séries para um nó praticamente cheio. <strong>Exceção:</strong>
 * se o filtro de frescor eliminar TODOS os nós ACTIVE+alcançáveis de uma vez
 * (sintoma de handoff de liderança, não de queda real), ele é ignorado para
 * esse ciclo de decisão — do contrário um handoff concentraria 100% das
 * séries novas no primeiro nó a reportar ao novo líder. Sem candidato algum
 * (nem ACTIVE+alcançável), retorna {@link Optional#empty()} — cabe ao
 * chamador responder {@code NO_STORAGE_NODE_AVAILABLE}.</p>
 *
 * <p>Issue #167 (item 3): depois da guarda de capacidade, um candidato também é descartado quando
 * {@link DestinationEligibility#reason} devolve um motivo — cota dura do nó ({@code quota_series}/
 * {@code quota_bytes}) ou regra de placement ({@code rule_pinned_elsewhere}/{@code rule_excluded}) —
 * salvo se ele é o {@code preferredOwnerNodeId} (adoção/retomada: o dono preferido ignora cota e
 * regras; o rebalance corrige depois). Um {@code pin} nunca transborda: se nenhum nó fixado sobrevive
 * aos filtros, a decisão é vazia mesmo havendo outros nós livres. Quando esse filtro esvazia um
 * conjunto não vazio, loga {@code NGRRD_PLACEMENT_NO_CANDIDATE series=<chave> excluded=<id>(<motivo>),…}.</p>
 *
 * <p>Entre os candidatos: se {@code preferredOwnerNodeId} sobreviveu aos
 * filtros acima, ele vence direto (ex.: adoção de série existente pelo
 * {@code LocalReconciler} ou retomada de dono após uma migração abortada).
 * Caso contrário, cada candidato recebe uma chave de ordenação pré-calculada
 * (nenhum ramo condicional entra no comparador em si) e o desempate segue,
 * em ordem:</p>
 * <ol>
 *   <li>menor carga efetiva ({@code seriesCount + pendente}) dividida pelo peso resolvido;</li>
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

    private static final Logger LOGGER = Logger.getLogger(LeastLoadedPlacementPolicy.class.getName());

    /**
     * Acima deste {@code fillRatio} (quando a capacidade é conhecida), o nó
     * deixa de ser candidato a receber novas séries.
     */
    private static final double CAPACITY_GUARD_FILL_RATIO = dev.nishisan.utils.oss.storage.blob.CapacityBudget.FILL_RATIO;

    @Override
    public Optional<String> choose(PlacementContext ctx) {
        DistributionWeights weights = DistributionWeights.resolve(ctx.nodes(), ctx.reachableNodeIds());
        List<StorageNodeStatus> withCapacity = ctx.nodes().stream()
                .filter(node -> node.state() == NodeState.ACTIVE)
                .filter(node -> ctx.reachableNodeIds().contains(node.nodeId()))
                .filter(node -> dev.nishisan.utils.oss.storage.blob.CapacityBudget.fits(node.capacityBytes(),
                        node.usedBytes(), Math.max(node.reservedBytes(), ctx.pendingBytesByNode().getOrDefault(node.nodeId(), 0L)),
                        ctx.requestedBytes()))
                .toList();

        // Issue #167 (item 3): cota dura e regras de placement gateiam só o DESTINO; o dono preferido
        // (adoção/retomada) passa direto — o rebalance corrige depois, se for o caso.
        List<StorageNodeStatus> activeReachable = new ArrayList<>(withCapacity.size());
        List<String> excluded = new ArrayList<>();
        for (StorageNodeStatus node : withCapacity) {
            if (node.nodeId().equals(ctx.preferredOwnerNodeId())) {
                Optional<String> ignored = eligibilityReason(node, ctx);
                if (ignored.isPresent()) {
                    LOGGER.log(Level.FINE, "Dono preferido {0} da série {1} ignora cota/regras ({2})",
                            new Object[] {node.nodeId(), ctx.seriesKey(), ignored.get()});
                }
                activeReachable.add(node);
                continue;
            }
            Optional<String> reason = eligibilityReason(node, ctx);
            if (reason.isPresent()) {
                excluded.add(node.nodeId() + "(" + reason.get() + ")");
            } else {
                activeReachable.add(node);
            }
        }
        if (activeReachable.isEmpty() && !withCapacity.isEmpty()) {
            LOGGER.info("NGRRD_PLACEMENT_NO_CANDIDATE series=" + ctx.seriesKey() + " excluded="
                    + String.join(",", excluded));
            return Optional.empty();
        }

        List<StorageNodeStatus> fresh = activeReachable.stream()
                .filter(node -> node.isFresh(ctx.nowEpochMs(), ctx.nodeStatusStaleAfter()))
                .toList();

        List<StorageNodeStatus> candidates;
        if (fresh.isEmpty() && !activeReachable.isEmpty()) {
            // Todos os candidatos ACTIVE+alcançáveis estão "velhos" ao mesmo tempo — mais provável um
            // efeito colateral de handoff de liderança (o novo líder só recebe o próximo relatório de
            // cada nó depois de um ciclo inteiro) do que todo o cluster realmente estar sem reportar.
            // Cair para esses nós ignorando o frescor evita concentrar 100% das séries num só nó
            // (achado F2 do Debugger); a ordem total abaixo continua valendo normalmente.
            LOGGER.log(Level.WARNING,
                    "Nenhum storage node ACTIVE+alcançável tem status fresco; ignorando o filtro de "
                            + "frescor para {0} candidato(s) nesta decisão de placement.",
                    activeReachable.size());
            candidates = activeReachable;
        } else {
            candidates = fresh;
        }

        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        String preferred = ctx.preferredOwnerNodeId();
        if (preferred != null && candidates.stream().anyMatch(node -> node.nodeId().equals(preferred))) {
            return Optional.of(preferred);
        }

        Comparator<Candidate> byTotalOrder = Comparator.comparingDouble(Candidate::effectiveLoad)
                .thenComparingDouble(Candidate::sortableFillRatio)
                .thenComparing(Candidate::nodeId);

        return candidates.stream()
                .map(node -> new Candidate(node.nodeId(), effectiveLoad(node, ctx) / weights.weight(node.nodeId()), sortableFillRatio(node)))
                .min(byTotalOrder)
                .map(Candidate::nodeId);
    }

    /** Cota (com pendências ainda não refletidas no status) e depois regras; vazio = elegível. */
    private static Optional<String> eligibilityReason(StorageNodeStatus node, PlacementContext ctx) {
        return DestinationEligibility.reason(node,
                ctx.pendingSeriesByNode().getOrDefault(node.nodeId(), 0L),
                ctx.pendingBytesByNode().getOrDefault(node.nodeId(), 0L),
                ctx.requestedBytes(), ctx.placementRules(), ctx.seriesKey(), ctx.definitionName());
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
    private record Candidate(String nodeId, double effectiveLoad, double sortableFillRatio) {
    }
}
