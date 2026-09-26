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
import java.util.Objects;

/**
 * Resposta de {@link Commands#ADMIN_FORGET} (revisão #178, B9; desde a 8.8.0).
 *
 * <p>No líder: {@code status} é {@link SeriesStatus#OK} quando o nó foi removido do catálogo e a ordem
 * de esquecimento foi propagada — {@code forgottenOn} lista os storages (o líder incluído) que
 * esqueceram o peer no seu {@code Transport}; {@code failedOn} os que não responderam ou recusaram
 * (inalcançáveis no momento): neles o comando deve ser repetido quando voltarem, senão continuam a
 * contar o nó esquecido na sua maioria de votantes. {@link SeriesStatus#ERROR} com {@code message}
 * quando o nó ainda tem séries no catálogo ou ainda está alcançável; {@link SeriesStatus#NOT_LEADER}
 * com {@code leaderNodeId} fora do líder. Num nó que só recebeu a ordem propagada, {@code forgottenOn}
 * é o próprio nó e {@code leaderNodeId} é nulo.</p>
 *
 * @param status       resultado
 * @param leaderNodeId líder que coordenou (ou o conhecido, em {@code NOT_LEADER})
 * @param nodeId       nó esquecido
 * @param forgottenOn  storages que esqueceram o peer
 * @param failedOn     storages que não confirmaram
 * @param message      detalhe em {@code ERROR}
 */
public record AdminForgetResponse(
        SeriesStatus status,
        String leaderNodeId,
        String nodeId,
        List<String> forgottenOn,
        List<String> failedOn,
        String message) {

    public AdminForgetResponse {
        forgottenOn = List.copyOf(Objects.requireNonNullElse(forgottenOn, List.of()));
        failedOn = List.copyOf(Objects.requireNonNullElse(failedOn, List.of()));
    }

    /** Resposta de erro/recusa sem listas. */
    public static AdminForgetResponse of(SeriesStatus status, String leaderNodeId, String nodeId, String message) {
        return new AdminForgetResponse(status, leaderNodeId, nodeId, List.of(), List.of(), message);
    }
}
