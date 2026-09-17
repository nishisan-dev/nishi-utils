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

/**
 * Resposta de {@code ngrrd.admin.rebalance}: dispara um ciclo imediato do
 * {@code Rebalancer} e devolve quantos movimentos foram planejados/submetidos —
 * mesmo padrão de {@link AdminStatusResponse}: só o líder responde
 * {@link SeriesStatus#OK}, qualquer outro nó responde {@link SeriesStatus#NOT_LEADER}
 * (com {@code leaderNodeId}, se conhecido).
 *
 * @param status       {@link SeriesStatus#OK} em caso de sucesso; {@link SeriesStatus#NOT_LEADER}
 *                     se quem respondeu não é o líder atual
 * @param leaderNodeId líder atual do cluster segundo quem respondeu; pode ser {@code null}
 * @param planned      quantidade de movimentos planejados neste ciclo (0 se um ciclo já estava em
 *                     andamento e este pedido não chegou a planejar um novo)
 * @param started      quantidade de movimentos efetivamente submetidos ao {@code MigrationCoordinator}
 */
public record AdminRebalanceResponse(SeriesStatus status, String leaderNodeId, int planned, int started) {
}
