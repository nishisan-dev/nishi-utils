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

import dev.nishisan.utils.oss.cluster.placement.DistributionMode;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * Entrada do catálogo {@code ngrrd.nodes}: último status conhecido de um storage
 * node, publicado periodicamente pelo próprio nó ({@code NodeStatusReporter}).
 *
 * @param nodeId           identificador do storage node
 * @param state            estado operacional do nó
 * @param seriesCount      quantidade de séries que o nó possui hoje
 * @param usedBytes        bytes ocupados no volume local
 * @param capacityBytes    capacidade configurada do volume; {@code <= 0} = desconhecida/sem limite
 * @param reportedAtEpochMs instante da coleta da carga deste status
 * @param distributionMode configured distribution strategy
 * @param weight explicit positive relative weight
 * @param reservedBytes incoming allocation bytes reserved locally
 * @param capabilities     capacidades de protocolo que o nó anuncia ({@link StorageCapabilities}); vazio
 *                         num status publicado por uma versão anterior a elas (campo ausente na
 *                         serialização Java ou no JSON replicado) — nunca {@code null}, cópia imutável
 * @param catalogReplica   estado da réplica local do catálogo neste nó (issue #177); {@code null} quando o
 *                         status foi publicado por uma versão anterior (campo ausente na serialização Java
 *                         ou no JSON replicado) ou não pôde ser coletado — "não reportado", nunca "em dia"
 * @param quotaMaxSeries   cota dura de séries deste nó ({@code ngrrd.quota.maxSeries}, issue #167 item 3);
 *                         {@code 0} = sem limite (também num status publicado por uma versão anterior)
 * @param quotaMaxBytes    cota dura de bytes ocupados+reservados ({@code ngrrd.quota.maxBytes}); {@code 0} =
 *                         sem limite
 * @param placementRulesHash fingerprint das regras de placement carregadas por este nó
 *                         ({@code PlacementRules#fingerprint()}); {@code null} sem regras ou num status
 *                         publicado por uma versão anterior
 */
public record StorageNodeStatus(
        String nodeId,
        NodeState state,
        long seriesCount,
        long usedBytes,
        long capacityBytes,
        long reportedAtEpochMs,
        DistributionMode distributionMode, double weight, long reservedBytes,
        Set<String> capabilities,
        CatalogReplicaStatus catalogReplica,
        long quotaMaxSeries,
        long quotaMaxBytes,
        String placementRulesHash) implements Serializable {

    /** B1 (achado do Debugger): ver Javadoc de {@code SeriesPlacement#serialVersionUID}. */
    private static final long serialVersionUID = 1L;

    public StorageNodeStatus(String nodeId, NodeState state, long seriesCount, long usedBytes,
            long capacityBytes, long reportedAtEpochMs) {
        this(nodeId, state, seriesCount, usedBytes, capacityBytes, reportedAtEpochMs, DistributionMode.COUNT, 1, 0);
    }

    /** Status sem capacidades anunciadas — a forma do record antes delas existirem. */
    public StorageNodeStatus(String nodeId, NodeState state, long seriesCount, long usedBytes,
            long capacityBytes, long reportedAtEpochMs, DistributionMode distributionMode, double weight,
            long reservedBytes) {
        this(nodeId, state, seriesCount, usedBytes, capacityBytes, reportedAtEpochMs, distributionMode, weight,
                reservedBytes, Set.of());
    }

    /** Status sem a réplica do catálogo — a forma do record da 8.6.0, antes dela existir. */
    public StorageNodeStatus(String nodeId, NodeState state, long seriesCount, long usedBytes,
            long capacityBytes, long reportedAtEpochMs, DistributionMode distributionMode, double weight,
            long reservedBytes, Set<String> capabilities) {
        this(nodeId, state, seriesCount, usedBytes, capacityBytes, reportedAtEpochMs, distributionMode, weight,
                reservedBytes, capabilities, null);
    }

    /** Status sem cota nem hash de regras — a forma do record da 8.7.0, antes deles existirem. */
    public StorageNodeStatus(String nodeId, NodeState state, long seriesCount, long usedBytes,
            long capacityBytes, long reportedAtEpochMs, DistributionMode distributionMode, double weight,
            long reservedBytes, Set<String> capabilities, CatalogReplicaStatus catalogReplica) {
        this(nodeId, state, seriesCount, usedBytes, capacityBytes, reportedAtEpochMs, distributionMode, weight,
                reservedBytes, capabilities, catalogReplica, 0L, 0L, null);
    }

    public StorageNodeStatus {
        // Missing fields in the old persistent record are null/zero.
        if (distributionMode == null) { distributionMode = DistributionMode.COUNT; weight = 1; }
        if (!Double.isFinite(weight) || weight <= 0 || reservedBytes < 0) {
            throw new IllegalArgumentException("invalid node weight or reservation");
        }
        if (quotaMaxSeries < 0 || quotaMaxBytes < 0) {
            throw new IllegalArgumentException("quota deve ser >= 0 (0 = sem limite): series=" + quotaMaxSeries
                    + " bytes=" + quotaMaxBytes);
        }
        Objects.requireNonNull(nodeId, "nodeId é obrigatório");
        Objects.requireNonNull(state, "state é obrigatório");
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
    }

    /** Cria o status inicial de um nó recém-ingressado no cluster, sem carga ainda reportada. */
    public static StorageNodeStatus active(String nodeId, long now) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, 0L, 0L, 0L, now);
    }

    /**
     * Atualiza a carga reportada, preservando {@code nodeId}, {@code state}, as capacidades, a réplica do
     * catálogo, a cota e o hash de regras.
     */
    public StorageNodeStatus withLoad(long seriesCount, long usedBytes, long capacityBytes, long now) {
        return new StorageNodeStatus(nodeId, state, seriesCount, usedBytes, capacityBytes, now, distributionMode, weight,
                reservedBytes, capabilities, catalogReplica, quotaMaxSeries, quotaMaxBytes, placementRulesHash);
    }

    /**
     * Transiciona o nó para outro {@link NodeState}, preservando a carga reportada, as capacidades, a
     * réplica do catálogo, a cota e o hash de regras.
     */
    public StorageNodeStatus withState(NodeState newState, long now) {
        return new StorageNodeStatus(nodeId, newState, seriesCount, usedBytes, capacityBytes, now, distributionMode,
                weight, reservedBytes, capabilities, catalogReplica, quotaMaxSeries, quotaMaxBytes, placementRulesHash);
    }

    /**
     * Se {@code effective} séries (as que o nó teria após receber mais uma) ultrapassam a cota de séries;
     * sempre {@code false} sem cota ({@code quotaMaxSeries == 0}).
     */
    public boolean seriesQuotaReached(long effective) {
        return quotaMaxSeries > 0 && effective > quotaMaxSeries;
    }

    /**
     * Se {@code effective} bytes (ocupados + reservados/pendentes + os da série a receber) ultrapassam a
     * cota de bytes; sempre {@code false} sem cota ({@code quotaMaxBytes == 0}).
     */
    public boolean bytesQuotaReached(long effective) {
        return quotaMaxBytes > 0 && effective > quotaMaxBytes;
    }

    /** Se o nó anuncia {@code capability} (ver {@link StorageCapabilities}). */
    public boolean advertises(String capability) {
        return capabilities.contains(capability);
    }

    /** Fração ocupada da capacidade; {@code 0} se a capacidade não é conhecida ({@code capacityBytes <= 0}). */
    public double fillRatio() {
        if (capacityBytes <= 0) {
            return 0.0;
        }
        return (double) usedBytes / (double) capacityBytes;
    }

    /**
     * Indica se o status ainda é considerado válido — dentro de {@code staleAfter} do último
     * relatório. {@code staleAfter} já é o prazo completo (ex.:
     * {@code StorageNodeConfig.nodeStatusStaleAfter()}, tipicamente
     * {@code max(5 × statusReportInterval, 15s)}); esta comparação não multiplica nada por conta
     * própria — a antiga versão (`2 × interval`) descartava um nó legítimo cujo último status visível
     * ao líder já estava "velho" logo após um handoff de liderança (o novo líder só recebe o próximo
     * relatório daquele nó depois de um ciclo inteiro).
     */
    public boolean isFresh(long now, Duration staleAfter) {
        return now - reportedAtEpochMs <= staleAfter.toMillis();
    }
}
