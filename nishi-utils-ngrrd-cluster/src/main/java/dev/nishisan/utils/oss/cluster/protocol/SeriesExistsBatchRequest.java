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
 * Pedido de {@link Commands#SERIES_EXISTS_BATCH}: pergunta a um nó específico, em lote, quais séries
 * ele possui fisicamente no seu volume local — sem abrir handle nenhum e sem passar pela checagem de
 * ownership (como {@link SeriesExistsRequest}, mas em lote).
 *
 * @param seriesKeys chaves lógicas a consultar; nunca {@code null} (vazio quando ausente); no máximo
 *                   {@link #MAX_KEYS} chaves por chamada — o servidor recusa (status
 *                   {@link SeriesStatus#ERROR}) um pedido maior
 */
public record SeriesExistsBatchRequest(List<String> seriesKeys) {

    /** Número máximo de chaves aceito por chamada; pedidos maiores são recusados pelo servidor. */
    public static final int MAX_KEYS = 10_000;

    public SeriesExistsBatchRequest {
        seriesKeys = List.copyOf(Objects.requireNonNullElse(seriesKeys, List.of()));
    }
}
