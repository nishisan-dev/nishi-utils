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

import dev.nishisan.utils.oss.api.SeriesResult;

/**
 * Resposta de um {@link ReadRequest}.
 *
 * @param status      resultado da operação
 * @param ownerNodeId dono atual da série segundo o respondente
 * @param result      resultado da leitura; {@code null} se {@code status != OK}
 * @param message     detalhe legível do erro, ou {@code null}
 */
public record ReadResponse(SeriesStatus status, String ownerNodeId, SeriesResult result, String message) {
}
