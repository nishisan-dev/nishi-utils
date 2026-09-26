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

package dev.nishisan.utils.oss.cluster.protocol;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Resposta de {@code ngrrd.admin.status}: visão geral do cluster segundo o líder —
 * só o líder responde com {@link SeriesStatus#OK}; qualquer outro nó responde
 * {@link SeriesStatus#NOT_LEADER} (com {@code leaderNodeId} preenchido, se
 * conhecido), mesmo padrão de {@code PlaceResponse}.
 *
 * @param status              {@link SeriesStatus#OK} em caso de sucesso; {@link SeriesStatus#NOT_LEADER}
 *                            se quem respondeu não é o líder atual
 * @param leaderNodeId        líder atual do cluster segundo quem respondeu; pode ser {@code null}
 *                            se {@code status == NOT_LEADER} e nem quem respondeu sabe quem é o líder
 * @param nodes               status de cada storage node conhecido, com alcançabilidade; nunca {@code null}
 * @param migrationsInFlight  quantidade de migrações em curso
 * @param seriesCountByNode   quantidade de séries por nó, segundo o catálogo; nunca {@code null}
 * @param geometriesPending placements whose physical geometry still needs owner confirmation
 * @param placementRulesHash  fingerprint das regras de placement do LÍDER ({@code PlacementRules#fingerprint()},
 *                            issue #167 item 3); {@code null} sem regras ou num líder anterior a este campo. A
 *                            CLI compara com o {@code placementRulesHash} de cada nó para marcar divergência
 * @param placementRulesCount quantidade de regras carregadas pelo líder; {@code 0} sem regras
 */
public record AdminStatusResponse(
        SeriesStatus status,
        String leaderNodeId,
        List<NodeStatusView> nodes,
        int migrationsInFlight,
        Map<String, Long> seriesCountByNode, long geometriesPending,
        String placementRulesHash, int placementRulesCount) {

    public AdminStatusResponse(SeriesStatus status, String leaderNodeId, List<NodeStatusView> nodes,
            int migrationsInFlight, Map<String, Long> seriesCountByNode) {
        this(status, leaderNodeId, nodes, migrationsInFlight, seriesCountByNode, 0);
    }

    /** Forma da 8.7.0, sem as regras do líder. */
    public AdminStatusResponse(SeriesStatus status, String leaderNodeId, List<NodeStatusView> nodes,
            int migrationsInFlight, Map<String, Long> seriesCountByNode, long geometriesPending) {
        this(status, leaderNodeId, nodes, migrationsInFlight, seriesCountByNode, geometriesPending, null, 0);
    }

    public AdminStatusResponse {
        Objects.requireNonNull(status, "status é obrigatório");
        nodes = List.copyOf(Objects.requireNonNullElse(nodes, List.of()));
        seriesCountByNode = Map.copyOf(Objects.requireNonNullElse(seriesCountByNode, Map.of()));
    }
}
