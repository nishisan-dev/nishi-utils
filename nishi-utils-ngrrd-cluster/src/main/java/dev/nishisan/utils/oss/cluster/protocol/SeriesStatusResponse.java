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
 * @param status       resultado da operação
 * @param ownerNodeId  dono atual da série segundo o respondente; útil quando
 *                     {@code status == WRONG_OWNER} para o cliente redirecionar
 * @param message      detalhe legível do erro, ou {@code null}
 */
public record SeriesStatusResponse(SeriesStatus status, String ownerNodeId, String message) {
}
