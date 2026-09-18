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

import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

/**
 * Resposta de {@code ngrrd.admin.drain}/{@code ngrrd.admin.activate}: mesmo padrão de
 * {@link AdminStatusResponse}/{@link AdminRebalanceResponse} — só o líder responde
 * {@link SeriesStatus#OK}; qualquer outro nó responde {@link SeriesStatus#NOT_LEADER}
 * (com {@code leaderNodeId}, se conhecido); um {@code nodeId} desconhecido do catálogo
 * responde {@link SeriesStatus#ERROR} com {@code message} explicando o motivo.
 *
 * @param status       {@link SeriesStatus#OK} em caso de sucesso
 * @param leaderNodeId líder atual do cluster segundo quem respondeu; pode ser {@code null}
 * @param nodeStatus   status resultante do nó alvo após a transição; {@code null} se {@code status != OK}
 * @param message      detalhe legível do erro, ou {@code null}
 */
public record AdminNodeStatusResponse(
        SeriesStatus status,
        String leaderNodeId,
        StorageNodeStatus nodeStatus,
        String message) {
}
