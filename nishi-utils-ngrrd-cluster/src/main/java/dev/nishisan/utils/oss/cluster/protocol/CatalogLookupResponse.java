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

import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;

import java.util.Map;
import java.util.Objects;

/**
 * Resposta do líder (ou de quem pensava ser o líder) a um {@link CatalogLookupRequest}.
 *
 * @param status       {@link SeriesStatus#OK} em caso de sucesso; {@link SeriesStatus#NOT_LEADER} se
 *                     quem respondeu não é o líder atual; {@link SeriesStatus#ERROR} para falha de
 *                     aplicação (ex.: lote maior que {@link CatalogLookupRequest#MAX_KEYS})
 * @param leaderNodeId quando {@code status == NOT_LEADER}, o {@code nodeId} do líder atual segundo
 *                     quem respondeu — {@code null} se nem quem respondeu sabe quem é o líder
 * @param found        placement de cada chave encontrada no catálogo; nunca {@code null} (vazio se
 *                     {@code status != OK}); uma chave do pedido ausente deste mapa significa que o
 *                     líder não tem placement para ela
 * @param message      detalhe legível do erro, ou {@code null}
 */
public record CatalogLookupResponse(
        SeriesStatus status,
        String leaderNodeId,
        Map<String, SeriesPlacement> found,
        String message) {

    public CatalogLookupResponse {
        found = Map.copyOf(Objects.requireNonNullElse(found, Map.of()));
    }

    /** Resposta de sucesso do líder, com o placement de cada chave encontrada no catálogo. */
    public static CatalogLookupResponse ok(Map<String, SeriesPlacement> found) {
        return new CatalogLookupResponse(SeriesStatus.OK, null, found, null);
    }

    /** Resposta de quem não é o líder atual, com um hint de quem é (ou {@code null} se desconhecido). */
    public static CatalogLookupResponse notLeader(String leaderNodeId) {
        return new CatalogLookupResponse(SeriesStatus.NOT_LEADER, leaderNodeId, null, null);
    }

    /** Resposta de falha de aplicação (ex.: lote acima de {@link CatalogLookupRequest#MAX_KEYS}). */
    public static CatalogLookupResponse error(String message) {
        return new CatalogLookupResponse(SeriesStatus.ERROR, null, null, message);
    }
}
