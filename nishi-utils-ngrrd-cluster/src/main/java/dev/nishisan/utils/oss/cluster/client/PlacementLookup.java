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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;

import java.time.Duration;
import java.util.Optional;

/**
 * Resolução e correção de placement consumida por {@link RemoteSeriesHandle} e
 * {@link WriteDispatcher} — isola a dependência de {@link PlacementResolver}
 * (que por sua vez depende de {@code CatalogService}, classe concreta sobre um
 * {@code NGridNode} real) para permitir fakes nos testes unitários desses dois
 * colaboradores.
 */
public interface PlacementLookup {

    /**
     * Placement atual da série, resolvendo com o líder ({@code ngrrd.place})
     * quando ainda não conhecido.
     */
    SeriesPlacement resolve(String seriesKey, String definitionHashHex);

    /** Placement with exact requested physical geometry. */
    default SeriesPlacement resolve(String key, String hash,
            dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry) { return resolve(key, hash); }

    /**
     * Resolves within the caller's remaining operation budget. Blocking implementations should
     * override this method to bound leader discovery, RPCs and retries by {@code maxWait}.
     * The default preserves compatibility with existing in-memory lookups.
     */
    default SeriesPlacement resolve(String key, String hash,
            dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry, java.time.Duration maxWait) {
        return resolve(key, hash, geometry);
    }

    /**
     * Placement existente de {@code seriesKey}, resolvendo com o líder ({@code ngrrd.catalog.lookup})
     * quando o cache local não confirma um placement {@code ACTIVE} — nunca cria posicionamento novo
     * (nunca dispara {@code ngrrd.place}). Um miss verdadeiro (a série não existe) é diferente de uma
     * falha ao consultar: só o primeiro caso vira {@code SeriesNotFoundException}.
     *
     * @throws dev.nishisan.utils.oss.api.SeriesNotFoundException se o líder confirmar que não há
     *         placement para {@code seriesKey}
     * @throws dev.nishisan.utils.oss.cluster.api.NgrrdClusterException se não foi possível confirmar
     *         com o líder (sem líder, timeout, falha de transporte, resposta inválida) — nunca
     *         interpretado como ausência da série
     */
    SeriesPlacement resolveExisting(String seriesKey, Duration maxWait);

    /**
     * Placement mais recente conhecido localmente (override em cache ou catálogo replicado), sem
     * nenhum RPC ao líder — {@link Optional#empty()} se nada estiver disponível localmente.
     */
    Optional<SeriesPlacement> placementCached(String seriesKey);

    /** Descarta o override local conhecido para {@code seriesKey}, se houver. */
    void invalidate(String seriesKey);

    /**
     * Atualiza o override local para {@code ACTIVE(ownerNodeId)} sem consultar
     * o líder — usado quando uma resposta {@code WRONG_OWNER} já traz o dono
     * correto.
     */
    void noteOwner(String seriesKey, String ownerNodeId);
}
