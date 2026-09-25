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

    /** Intervalo de polling à espera de um líder eleito — curto de propósito, nunca o único fator de prazo. */
    private static final Duration LEADER_POLL_INTERVAL = Duration.ofMillis(50);

    private final CatalogService catalog;
    private final ClusterRpc rpc;
    private final RetryPolicy retry;
    private final Clock clock;

    private final ConcurrentMap<String, SeriesPlacement> overrides = new ConcurrentHashMap<>();

    public PlacementResolver(CatalogService catalog, ClusterRpc rpc, RetryPolicy retry, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.retry = Objects.requireNonNull(retry, "retry");
        this.clock = Objects.requireNonNull(clock, "clock");
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
        remainingUntil(deadline, seriesKey);
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
        for (;;) {
            attempt++;
            NodeId leader = leaderHint != null ? leaderHint : awaitLeaderOrThrow(seriesKey, deadline);
            leaderHint = null;
            PlaceResponse response;
            try {
                response = rpc.call(leader, Commands.PLACE,
                        new PlaceRequest(seriesKey, definitionHashHex, null, geometry), PlaceResponse.class,
                        remainingUntil(deadline, seriesKey));
            } catch (NgrrdClusterException e) {
                // B3 (achado do Refuter): falha de TRANSPORTE (não de aplicação) ao chamar o líder —
                // retenta com backoff até o prazo de retry.timeout(), esperando a conexão voltar em vez
                // de tentar de novo às cegas.
                if (!TransportRetry.isTransportFailure(e) || clock.millis() >= deadline) {
                    throw e;
                }
                TransportRetry.awaitConnectionOrBackoff(rpc, leader,
                        cappedBackoff(retry.backoffFor(attempt), deadline, seriesKey));
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
                    sleepQuietly(cappedBackoff(retry.backoffFor(attempt), deadline, seriesKey));
                }
                case NO_STORAGE_NODE_AVAILABLE -> throw new NgrrdClusterException(
                        ErrorCode.NO_STORAGE_NODE_AVAILABLE, response.message());
                default -> throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "PLACE respondeu " + response.status() + " para " + seriesKey);
            }
        }
    }

    private NodeId awaitLeaderOrThrow(String seriesKey, long deadline) {
        Optional<NodeId> leader = rpc.leaderId();
        while (leader.isEmpty() && clock.millis() < deadline) {
            sleepQuietly(cappedBackoff(LEADER_POLL_INTERVAL, deadline, seriesKey));
            leader = rpc.leaderId();
        }
        return leader.orElseThrow(() -> new NgrrdClusterException(ErrorCode.NO_LEADER,
                "nenhum líder eleito para posicionar a série " + seriesKey));
    }

    private Duration remainingUntil(long deadline, String seriesKey) {
        long remaining = deadline - clock.millis();
        if (remaining <= 0) {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "prazo de placement esgotado para " + seriesKey);
        }
        return Duration.ofMillis(remaining);
    }

    private Duration cappedBackoff(Duration backoff, long deadline, String seriesKey) {
        Duration remaining = remainingUntil(deadline, seriesKey);
        return backoff.compareTo(remaining) > 0 ? remaining : backoff;
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando retentativa de placement", e);
        }
    }
}
