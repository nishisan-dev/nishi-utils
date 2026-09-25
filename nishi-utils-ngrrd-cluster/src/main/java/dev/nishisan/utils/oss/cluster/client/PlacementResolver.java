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
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Resolve o dono de uma série no cliente: cache local de overrides, catálogo
 * replicado (leitura eventual) e, na ausência dos dois, {@code ngrrd.place} no
 * líder.
 *
 * <p>Erros de rede/RPC não são traduzidos aqui — {@link ClusterRpc#call} já
 * lança {@link NgrrdClusterException} com {@link ErrorCode#TIMEOUT}/
 * {@link ErrorCode#REMOTE_ERROR}, que sobem sem modificação.</p>
 */
public final class PlacementResolver implements PlacementLookup {

    private final CatalogService catalog;
    private final ClusterRpc rpc;
    private final RetryPolicy retry;
    private final Clock clock;
    private final CatalogLookupClient catalogLookupClient;

    private final ConcurrentMap<String, SeriesPlacement> overrides = new ConcurrentHashMap<>();

    public PlacementResolver(CatalogService catalog, ClusterRpc rpc, RetryPolicy retry, Clock clock,
            CatalogLookupClient catalogLookupClient) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.retry = Objects.requireNonNull(retry, "retry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.catalogLookupClient = Objects.requireNonNull(catalogLookupClient, "catalogLookupClient");
    }

    @Override
    public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
        return resolve(seriesKey, definitionHashHex, null);
    }

    @Override
    public SeriesPlacement resolve(String seriesKey, String definitionHashHex,
            dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry) {
        return resolve(seriesKey, definitionHashHex, geometry, retry.timeout());
    }

    @Override
    public SeriesPlacement resolve(String seriesKey, String definitionHashHex,
            dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry, Duration maxWait) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(maxWait, "maxWait");
        long deadline = clock.millis() + Math.min(retry.timeout().toMillis(), maxWait.toMillis());
        LeaderCalls.remainingUntil(clock, deadline, "posicionar a série " + seriesKey);
        // Override e catálogo local coexistem — nenhum tem precedência absoluta: um WRONG_OWNER
        // recente pode ter atualizado o override depois da última replicação do catálogo local (ou
        // vice-versa, se o override estiver simplesmente desatualizado). Vence quem tiver o
        // updatedAtEpochMs mais recente.
        SeriesPlacement cached = overrides.get(seriesKey);
        SeriesPlacement local = catalog.placementLocal(seriesKey).orElse(null);
        SeriesPlacement freshest = freshest(cached, local);
        if (freshest != null && freshest.state() == PlacementState.ACTIVE) {
            return freshest;
        }
        return placeAtLeader(seriesKey, definitionHashHex, geometry, deadline);
    }

    @Override
    public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(maxWait, "maxWait");
        SeriesPlacement cached = overrides.get(seriesKey);
        SeriesPlacement local = catalog.placementLocal(seriesKey).orElse(null);
        SeriesPlacement freshest = freshest(cached, local);
        if (freshest != null && freshest.state() == PlacementState.ACTIVE) {
            return freshest;
        }
        // Ausente ou em MIGRATING: o catálogo local pode estar desatualizado ou ainda não ter
        // recebido a entrada por replicação — só o líder confirma com autoridade se a série existe.
        return lookupAtLeader(seriesKey, maxWait);
    }

    @Override
    public SeriesPlacement resolveExistingAtLeader(String seriesKey, Duration maxWait) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(maxWait, "maxWait");
        return lookupAtLeader(seriesKey, maxWait);
    }

    /**
     * Placement de {@code seriesKey} segundo o líder. Presente e {@code ACTIVE}, vira o override local
     * (mais recente que uma réplica atrasada); ausente, descarta o override e lança
     * {@link SeriesNotFoundException} com {@code NOT_PLACED}.
     */
    private SeriesPlacement lookupAtLeader(String seriesKey, Duration maxWait) {
        Map<String, SeriesPlacement> found = catalogLookupClient.lookup(List.of(seriesKey), maxWait);
        SeriesPlacement placement = found.get(seriesKey);
        if (placement == null) {
            overrides.remove(seriesKey);
            throw new SeriesNotFoundException(seriesKey, SeriesNotFoundException.Reason.NOT_PLACED);
        }
        if (placement.state() == PlacementState.ACTIVE) {
            overrides.put(seriesKey, placement);
        }
        return placement;
    }

    @Override
    public Optional<SeriesPlacement> placementCached(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        return Optional.ofNullable(freshest(overrides.get(seriesKey), catalog.placementLocal(seriesKey).orElse(null)));
    }

    private static SeriesPlacement freshest(SeriesPlacement a, SeriesPlacement b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.updatedAtEpochMs() >= b.updatedAtEpochMs() ? a : b;
    }

    @Override
    public void invalidate(String seriesKey) {
        overrides.remove(seriesKey);
    }

    @Override
    public void noteOwner(String seriesKey, String ownerNodeId) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(ownerNodeId, "ownerNodeId");
        overrides.put(seriesKey, SeriesPlacement.active(ownerNodeId, clock.millis()));
    }

    private SeriesPlacement placeAtLeader(String seriesKey, String definitionHashHex,
            dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry, long deadline) {
        int attempt = 0;
        // B2 (achado do Refuter): quando o nó consultado responde NOT_LEADER indicando quem é o líder
        // atual, vamos direto a ele na próxima tentativa — só cai de volta em rpc.leaderId() (que pode
        // estar vazio/desatualizado bem no meio de um handoff) quando a resposta não trouxe essa
        // indicação.
        NodeId leaderHint = null;
        String description = "posicionar a série " + seriesKey;
        for (;;) {
            attempt++;
            NodeId leader = leaderHint != null ? leaderHint
                    : LeaderCalls.awaitLeaderOrThrow(rpc, clock, deadline, description);
            leaderHint = null;
            PlaceResponse response;
            try {
                response = rpc.call(leader, Commands.PLACE,
                        new PlaceRequest(seriesKey, definitionHashHex, null, geometry), PlaceResponse.class,
                        LeaderCalls.remainingUntil(clock, deadline, description));
            } catch (NgrrdClusterException e) {
                // B3 (achado do Refuter): falha de TRANSPORTE (não de aplicação) ao chamar o líder —
                // retenta com backoff até o prazo de retry.timeout(), esperando a conexão voltar em vez
                // de tentar de novo às cegas.
                if (!TransportRetry.isTransportFailure(e) || clock.millis() >= deadline) {
                    throw e;
                }
                TransportRetry.awaitConnectionOrBackoff(rpc, leader,
                        LeaderCalls.cappedBackoff(clock, retry.backoffFor(attempt), deadline, description));
                continue;
            }
            switch (response.status()) {
                case OK -> {
                    overrides.put(seriesKey, response.placement());
                    return response.placement();
                }
                case NOT_LEADER -> {
                    if (response.leaderNodeId() != null) {
                        leaderHint = NodeId.of(response.leaderNodeId());
                    }
                    LeaderCalls.sleepQuietly(LeaderCalls.cappedBackoff(clock, retry.backoffFor(attempt), deadline,
                            description));
                }
                case NO_STORAGE_NODE_AVAILABLE -> throw new NgrrdClusterException(
                        ErrorCode.NO_STORAGE_NODE_AVAILABLE, response.message());
                default -> throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "PLACE respondeu " + response.status() + " para " + seriesKey);
            }
        }
    }
}
