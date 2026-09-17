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

import java.util.Map;
import java.util.Objects;

/**
 * Resposta de um {@link WriteBatchRequest}: status por série do lote, já que um
 * lote pode misturar séries com resultados diferentes (uma migrando, outra ok).
 *
 * @param statusBySeries status de cada série presente no lote; nunca {@code null}
 * @param ownerBySeries  dono atual reportado para séries com {@code WRONG_OWNER}; nunca {@code null}
 * @param errorBySeries  mensagem de erro para séries com {@code status == ERROR}; nunca {@code null}
 */
public record WriteBatchResponse(
        Map<String, SeriesStatus> statusBySeries,
        Map<String, String> ownerBySeries,
        Map<String, String> errorBySeries) {

    public WriteBatchResponse {
        statusBySeries = Map.copyOf(Objects.requireNonNullElse(statusBySeries, Map.of()));
        ownerBySeries = Map.copyOf(Objects.requireNonNullElse(ownerBySeries, Map.of()));
        errorBySeries = Map.copyOf(Objects.requireNonNullElse(errorBySeries, Map.of()));
    }
}
