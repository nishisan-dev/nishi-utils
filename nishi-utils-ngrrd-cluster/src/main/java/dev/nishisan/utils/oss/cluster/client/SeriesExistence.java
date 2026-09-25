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

import dev.nishisan.utils.oss.cluster.api.SeriesInfo;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Lógica de {@code exists}/{@code find} de {@link DefaultNgrrdClusterClient}, isolada num
 * colaborador testável sem {@code NGridNode} real: hit no cache local
 * ({@link PlacementLookup#placementCached}) responde sem RPC; misses são confirmados em lote no
 * líder via {@link CatalogLookupClient} — que nunca cria placement e nunca devolve resposta
 * parcial (qualquer falha ao consultar propaga {@link dev.nishisan.utils.oss.cluster.api.NgrrdClusterException}).
 *
 * <p>{@code MIGRATING} conta como existente nos dois caminhos: tanto o cache local quanto o líder
 * devolvem a entrada do catálogo independente do estado, e presença já basta.</p>
 */
final class SeriesExistence {

    private final PlacementLookup placementLookup;
    private final CatalogLookupClient catalogLookupClient;

    SeriesExistence(PlacementLookup placementLookup, CatalogLookupClient catalogLookupClient) {
        this.placementLookup = Objects.requireNonNull(placementLookup, "placementLookup");
        this.catalogLookupClient = Objects.requireNonNull(catalogLookupClient, "catalogLookupClient");
    }

    /** @see dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient#exists(String) */
    boolean exists(String seriesKey, Duration maxWait) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        return exists(List.of(seriesKey), maxWait).get(seriesKey);
    }

    /** @see dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient#exists(Collection) */
    Map<String, Boolean> exists(Collection<String> seriesKeys, Duration maxWait) {
        Objects.requireNonNull(seriesKeys, "seriesKeys");
        Objects.requireNonNull(maxWait, "maxWait");
        List<String> distinctKeys = List.copyOf(new LinkedHashSet<>(seriesKeys));
        Map<String, Boolean> result = new LinkedHashMap<>();
        List<String> misses = new ArrayList<>();
        for (String key : distinctKeys) {
            if (placementLookup.placementCached(key).isPresent()) {
                result.put(key, true);
            } else {
                misses.add(key);
            }
        }
        if (!misses.isEmpty()) {
            Map<String, SeriesPlacement> found = catalogLookupClient.lookup(misses, maxWait);
            for (String key : misses) {
                result.put(key, found.containsKey(key));
            }
        }
        return Map.copyOf(result);
    }

    /** @see dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient#find(String) */
    Optional<SeriesInfo> find(String seriesKey, Duration maxWait) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(maxWait, "maxWait");
        Optional<SeriesPlacement> cached = placementLookup.placementCached(seriesKey);
        if (cached.isPresent()) {
            return Optional.of(SeriesInfo.of(seriesKey, cached.get()));
        }
        Map<String, SeriesPlacement> found = catalogLookupClient.lookup(List.of(seriesKey), maxWait);
        SeriesPlacement placement = found.get(seriesKey);
        return placement == null ? Optional.empty() : Optional.of(SeriesInfo.of(seriesKey, placement));
    }
}
