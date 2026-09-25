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
 * Resposta genérica de operações sobre uma série já aberta: {@code open},
 * {@code checkpoint}, {@code flush} e {@code close}.
 *
 * @param status                 resultado da operação
 * @param ownerNodeId            dono atual da série segundo o respondente; útil quando
 *                               {@code status == WRONG_OWNER} para o cliente redirecionar
 * @param message                detalhe legível do erro, ou {@code null}
 * @param createIfMissingHonored {@code true} quando o storage abriu a série num {@code OPEN} com
 *                               {@code createIfMissing=false} honrando o campo (sem criar); {@code null}
 *                               nas demais respostas e em respostas de storages anteriores a ele — o
 *                               cliente trata {@code OK} sem a confirmação a um {@code OPEN} sem criar
 *                               como storage que não suporta o campo
 */
public record SeriesStatusResponse(SeriesStatus status, String ownerNodeId, String message,
        Boolean createIfMissingHonored) {

    /** Resposta sem a confirmação de {@code createIfMissing} — a forma usada por todas as outras operações. */
    public SeriesStatusResponse(SeriesStatus status, String ownerNodeId, String message) {
        this(status, ownerNodeId, message, null);
    }
}
