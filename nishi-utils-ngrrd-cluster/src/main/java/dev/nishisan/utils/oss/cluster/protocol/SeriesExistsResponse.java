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
 * Resposta de {@link Commands#SERIES_EXISTS}.
 *
 * @param exists {@code true} se o nó que respondeu possui fisicamente o objeto da série no seu volume
 * @param bytes  {@code -1} quando {@code exists=true} — {@code BlobStorage}/{@code NgrrdStorage} não
 *               expõe uma API barata de tamanho (só {@code exists} booleano ou {@code get} que carrega
 *               o objeto inteiro), e {@code handleSeriesExists} nunca lê o objeto só para medi-lo (ver
 *               BAIXO-E do Refuter, M4); {@code 0} quando {@code exists=false}
 */
public record SeriesExistsResponse(boolean exists, long bytes) {
}
