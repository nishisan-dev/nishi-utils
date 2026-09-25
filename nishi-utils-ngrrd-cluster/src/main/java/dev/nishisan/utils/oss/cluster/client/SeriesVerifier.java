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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.SeriesVerification;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Lógica de {@code verify} de {@link NgrrdClusterClient}, isolada num colaborador testável sem
 * {@code NGridNode} real — mesma técnica de {@link SeriesExistence}.
 *
 * <p>Ao contrário de {@link SeriesExistence} (que só confirma presença no catálogo), confirma no
 * próprio DONO se o objeto físico de cada série existe, agrupando as chaves por {@code ownerNodeId} —
 * a origem, mesmo durante {@code MIGRATING} — e consultando cada dono em páginas sequenciais de
 * {@code batchSize} chaves ({@link Commands#SERIES_EXISTS_BATCH}).</p>
 *
 * <p>Duas falhas têm tratamento bem diferente: uma falha ao consultar o LÍDER (catálogo, via
 * {@link CatalogLookupClient}) sempre propaga {@link NgrrdClusterException} — nada é presumido; já uma
 * falha ao consultar um DONO (capacidade ausente, RPC ou status de erro numa página) marca só as chaves
 * daquela página/nó como {@link SeriesVerification#UNVERIFIED}, sem interromper as demais páginas ou
 * nós — usado só num relatório de conciliação sob demanda, nunca pode transformar uma falha pontual em
 * ausência.</p>
 */
final class SeriesVerifier {

    private final PlacementLookup resolver;
    private final CatalogLookupClient lookup;
    private final ClusterRpc rpc;
    private final NodeCapabilities capabilities;
    private final Duration requestTimeout;
    private final Duration retryTimeout;
    private final int batchSize;

    SeriesVerifier(PlacementLookup resolver, CatalogLookupClient lookup, ClusterRpc rpc,
            NodeCapabilities capabilities, Duration requestTimeout, Duration retryTimeout, int batchSize) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.retryTimeout = Objects.requireNonNull(retryTimeout, "retryTimeout");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize deve ser > 0: " + batchSize);
        }
        this.batchSize = batchSize;
    }

    /** @see NgrrdClusterClient#verify(Collection) */
    Map<String, SeriesVerification> verify(Collection<String> seriesKeys) {
        Objects.requireNonNull(seriesKeys, "seriesKeys");
        List<String> distinctKeys = List.copyOf(new LinkedHashSet<>(seriesKeys));
        Map<String, SeriesVerification> result = new LinkedHashMap<>();
        if (distinctKeys.isEmpty()) {
            return Map.of();
        }

        // Passo 1: placement de cada chave — cache local primeiro, líder só para os misses; falha ao
        // consultar o líder propaga (nada é presumido).
        Map<String, String> ownerByKey = new LinkedHashMap<>();
        List<String> misses = new ArrayList<>();
        for (String key : distinctKeys) {
            Optional<SeriesPlacement> cached = resolver.placementCached(key);
            if (cached.isPresent()) {
                ownerByKey.put(key, cached.get().ownerNodeId());
            } else {
                misses.add(key);
            }
        }
        if (!misses.isEmpty()) {
            Map<String, SeriesPlacement> found = lookup.lookup(misses, retryTimeout);
            for (String key : misses) {
                SeriesPlacement placement = found.get(key);
                if (placement == null) {
                    result.put(key, SeriesVerification.NOT_PLACED);
                } else {
                    ownerByKey.put(key, placement.ownerNodeId());
                }
            }
        }

        // Passo 2: agrupa por dono e confirma fisicamente em páginas sequenciais; chaves ausentes no
        // dono ficam pendentes de reconfirmação (passo 3), nunca viram MISSING_ON_OWNER direto — o
        // dono pode ter migrado entre o placement usado aqui e agora.
        List<String> absentees = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : groupByOwner(ownerByKey).entrySet()) {
            Map<String, Boolean> presence = queryOwner(entry.getKey(), entry.getValue());
            for (String key : entry.getValue()) {
                Boolean present = presence.get(key);
                if (present == null) {
                    result.put(key, SeriesVerification.UNVERIFIED);
                } else if (present) {
                    result.put(key, SeriesVerification.PRESENT);
                } else {
                    absentees.add(key);
                }
            }
        }

        // Passo 3: ausentes no dono são reconfirmados no líder (consulta fresca, sem cache) — dono
        // igual ao usado no passo 2 vira MISSING_ON_OWNER; dono diferente é reperguntado UMA VEZ, com a
        // mesma paginação.
        if (!absentees.isEmpty()) {
            Map<String, SeriesPlacement> fresh = lookup.lookup(absentees, retryTimeout);
            Map<String, List<String>> reaskByNewOwner = new LinkedHashMap<>();
            for (String key : absentees) {
                SeriesPlacement placement = fresh.get(key);
                if (placement == null) {
                    result.put(key, SeriesVerification.NOT_PLACED);
                } else if (placement.ownerNodeId().equals(ownerByKey.get(key))) {
                    result.put(key, SeriesVerification.MISSING_ON_OWNER);
                } else {
                    reaskByNewOwner.computeIfAbsent(placement.ownerNodeId(), id -> new ArrayList<>()).add(key);
                }
            }
            for (Map.Entry<String, List<String>> entry : reaskByNewOwner.entrySet()) {
                Map<String, Boolean> presence = queryOwner(entry.getKey(), entry.getValue());
                for (String key : entry.getValue()) {
                    Boolean present = presence.get(key);
                    if (present == null) {
                        result.put(key, SeriesVerification.UNVERIFIED);
                    } else if (present) {
                        result.put(key, SeriesVerification.PRESENT);
                    } else {
                        result.put(key, SeriesVerification.MISSING_ON_OWNER);
                    }
                }
            }
        }

        return Map.copyOf(result);
    }

    private static Map<String, List<String>> groupByOwner(Map<String, String> ownerByKey) {
        Map<String, List<String>> grouped = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : ownerByKey.entrySet()) {
            grouped.computeIfAbsent(entry.getValue(), id -> new ArrayList<>()).add(entry.getKey());
        }
        return grouped;
    }

    /**
     * Confirma fisicamente {@code keys} no dono {@code ownerId}, em páginas sequenciais de
     * {@link #batchSize}. Devolve {@code chave -> existe} só para as chaves cuja página respondeu com
     * sucesso; uma chave ausente do mapa devolvido significa falha (capacidade ausente, RPC ou status
     * de erro) — o chamador trata isso como {@link SeriesVerification#UNVERIFIED}.
     */
    private Map<String, Boolean> queryOwner(String ownerId, List<String> keys) {
        try {
            capabilities.require(ownerId, StorageCapabilities.SERIES_EXISTS_BATCH, requestTimeout);
        } catch (NgrrdClusterException e) {
            // Nó sem a capacidade (confirmado) ou status indisponível até o fim do prazo: nenhuma chave
            // dele é confirmada — sem RPC nenhum.
            return Map.of();
        }
        NodeId owner = NodeId.of(ownerId);
        Map<String, Boolean> presence = new LinkedHashMap<>();
        for (int start = 0; start < keys.size(); start += batchSize) {
            List<String> page = keys.subList(start, Math.min(start + batchSize, keys.size()));
            queryPage(owner, page, presence);
        }
        return presence;
    }

    private void queryPage(NodeId owner, List<String> page, Map<String, Boolean> presence) {
        SeriesExistsBatchResponse response;
        try {
            response = rpc.call(owner, Commands.SERIES_EXISTS_BATCH, new SeriesExistsBatchRequest(page),
                    SeriesExistsBatchResponse.class, requestTimeout);
        } catch (NgrrdClusterException e) {
            // Falha nesta página (transporte, timeout, erro remoto): as chaves ficam de fora do mapa —
            // o chamador as trata como UNVERIFIED. As demais páginas/nós seguem normalmente.
            return;
        }
        if (response == null || response.status() != SeriesStatus.OK) {
            return;
        }
        Set<String> present = response.present();
        for (String key : page) {
            presence.put(key, present.contains(key));
        }
    }
}
