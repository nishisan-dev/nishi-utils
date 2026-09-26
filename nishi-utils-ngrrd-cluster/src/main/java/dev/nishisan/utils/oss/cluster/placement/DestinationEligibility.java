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

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Elegibilidade de um storage node como <em>destino</em> de uma série (issue #167, item 3): cota dura
 * ({@code ngrrd.quota.*}) e regras de placement ({@code ngrrd.placement.rules}). Só destinos são
 * filtrados — placement de série nova, receptores de rebalance/drain e o próprio
 * {@code MIGRATE_PREPARE} no destino; um nó que já está acima da cota (adoção, dono preferido, cota
 * reduzida) é tratado como <em>fonte</em> pelo rebalance, nunca é esvaziado à força aqui.
 *
 * <p>Os motivos seguem o formato dos demais motivos de exclusão de destino
 * ({@code CatalogLagGate}): {@code quota_series(<efetivo>/<max>)}, {@code quota_bytes(<efetivo>/<max>)},
 * {@code rule_pinned_elsewhere(<regra>)} e {@code rule_excluded(<regra>)}.</p>
 */
public final class DestinationEligibility {

    private static final Logger LOG = Logger.getLogger(DestinationEligibility.class.getName());
    /** Última divergência avisada (mesmo padrão de {@code DistributionWeights}): avisa uma vez por mudança. */
    private static final AtomicReference<String> LAST_WARNING = new AtomicReference<>();

    private DestinationEligibility() {
    }

    /**
     * Motivo de cota pelo qual {@code node} não pode receber mais uma série de {@code requestedBytes}:
     * séries = {@code seriesCount + pendingSeries + 1 > quotaMaxSeries}; bytes =
     * {@code usedBytes + max(reservedBytes, pendingBytes) + max(requestedBytes, 1) > quotaMaxBytes}.
     * Cota {@code 0} nunca exclui.
     *
     * @param pendingSeries  séries já colocadas pelo líder neste nó desde o último status dele
     * @param pendingBytes   bytes em migração/alocação para este nó ainda não refletidos no status
     * @param requestedBytes tamanho da série a receber (0 conta como 1 byte)
     */
    public static Optional<String> quotaReason(StorageNodeStatus node, long pendingSeries, long pendingBytes,
            long requestedBytes) {
        long effectiveSeries = node.seriesCount() + pendingSeries + 1;
        if (node.seriesQuotaReached(effectiveSeries)) {
            return Optional.of("quota_series(" + effectiveSeries + "/" + node.quotaMaxSeries() + ")");
        }
        long effectiveBytes = node.usedBytes() + Math.max(node.reservedBytes(), pendingBytes) + Math.max(requestedBytes, 1L);
        if (node.bytesQuotaReached(effectiveBytes)) {
            return Optional.of("quota_bytes(" + effectiveBytes + "/" + node.quotaMaxBytes() + ")");
        }
        return Optional.empty();
    }

    /** Motivo de regra ({@link PlacementRules#exclusionReason}) pelo qual {@code nodeId} não pode receber a série. */
    public static Optional<String> ruleReason(PlacementRules rules, String seriesKey, String definitionName,
            String nodeId) {
        if (rules == null || rules.isEmpty() || seriesKey == null) {
            return Optional.empty();
        }
        return rules.exclusionReason(seriesKey, definitionName, nodeId);
    }

    /** Cota primeiro, depois regra; vazio quando o nó é elegível como destino. */
    public static Optional<String> reason(StorageNodeStatus node, long pendingSeries, long pendingBytes,
            long requestedBytes, PlacementRules rules, String seriesKey, String definitionName) {
        Optional<String> quota = quotaReason(node, pendingSeries, pendingBytes, requestedBytes);
        return quota.isPresent() ? quota : ruleReason(rules, seriesKey, definitionName, node.nodeId());
    }

    /**
     * Loga {@code NGRRD_PLACEMENT_RULES divergent leader=<hash> nodes=<id>(<hash|->),…} em WARNING quando o
     * fingerprint de um nó {@code ACTIVE} de {@code nodes} difere do das regras do líder — uma vez por
     * mudança do conjunto divergente (e volta a avisar se, depois de convergir, divergir de novo). O chamador
     * passa só os nós alcançáveis; nós fora de {@code ACTIVE} são ignorados. As regras nunca são descartadas
     * por divergência: o líder segue aplicando a cópia dele.
     */
    public static void warnIfRulesDiverge(PlacementRules leaderRules, Collection<StorageNodeStatus> nodes) {
        String leaderHash = leaderRules == null ? null : leaderRules.fingerprint();
        String divergent = nodes.stream()
                .filter(node -> node.state() == NodeState.ACTIVE)
                .filter(node -> !Objects.equals(leaderHash, node.placementRulesHash()))
                .sorted((a, b) -> a.nodeId().compareTo(b.nodeId()))
                .map(node -> node.nodeId() + "(" + orDash(node.placementRulesHash()) + ")")
                .collect(Collectors.joining(","));
        if (divergent.isEmpty()) {
            LAST_WARNING.set(null);
            return;
        }
        String message = "NGRRD_PLACEMENT_RULES divergent leader=" + orDash(leaderHash) + " nodes=" + divergent;
        if (!message.equals(LAST_WARNING.getAndSet(message))) {
            LOG.warning(message);
        }
    }

    /** Zera a supressão do aviso de divergência (isolamento entre testes). */
    static void resetDivergenceWarningForTests() {
        LAST_WARNING.set(null);
    }

    private static String orDash(String hash) {
        return hash == null ? "-" : hash;
    }
}
