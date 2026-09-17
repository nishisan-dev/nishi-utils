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
 * Uma amostra a aplicar a um data source de uma série, dentro de um {@link WriteBatchRequest}.
 *
 * @param seriesKey chave lógica da série
 * @param dsName    nome do data source dentro da série
 * @param tsEpochMs timestamp da amostra em epoch ms
 * @param value     valor da amostra
 */
public record SeriesWrite(String seriesKey, String dsName, long tsEpochMs, double value) {
}
