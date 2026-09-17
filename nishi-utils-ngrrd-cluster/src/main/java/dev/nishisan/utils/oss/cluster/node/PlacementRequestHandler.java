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

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.PlacementContext;
import dev.nishisan.utils.oss.cluster.placement.PlacementPolicy;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Atende, apenas no líder, o comando {@link Commands#PLACE}: cria (ou confirma,
 * idempotentemente) o placement de uma série nova via {@link PlacementPolicy}.
 *
 * <p>Serializa decisões por {@code seriesKey} com 64 locks em stripe — chaves
 * diferentes decidem em paralelo, a mesma chave nunca decide duas vezes ao
 * mesmo tempo (evita corrida entre dois {@code PLACE} concorrentes para a mesma
 * série nova).</p>
 */
public final class PlacementRequestHandler extends RequestHandlerSupport implements LeadershipListener {

    private static final int LOCK_STRIPES = 64;

    /**
     * Visão do líder e da malha consumida por este handler — isola a
     * dependência de {@code ClusterCoordinator}/{@code Transport} (classes
     * finais/complexas de montar em teste) para permitir um fake.
     */
    public interface LeaderView {
        boolean isLeader();

        Optional<String> leaderId();

        Set<String> reachableNodeIds();
    }

    private final CatalogService catalog;
    private final LeaderView leaderView;
    private final PlacementPolicy policy;
    private final Duration statusReportInterval;
    private final Clock clock;

    private final Object[] stripeLocks = new Object[LOCK_STRIPES];
    private final ConcurrentMap<String, PendingCounter> pendingByNode = new ConcurrentHashMap<>();

    public PlacementRequestHandler(Transport transport, CatalogService catalog, LeaderView leaderView,
            PlacementPolicy policy, Duration statusReportInterval, Clock clock) {
        super(transport, Set.of(Commands.PLACE));
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.leaderView = Objects.requireNonNull(leaderView, "leaderView");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.statusReportInterval = Objects.requireNonNull(statusReportInterval, "statusReportInterval");
        this.clock = Objects.requireNonNull(clock, "clock");
        for (int i = 0; i < LOCK_STRIPES; i++) {
            stripeLocks[i] = new Object();
        }
    }

    /** {@link LeaderView} de produção, sobre o {@code ClusterCoordinator}/{@code Transport} reais do nó. */
    public static LeaderView fromCoordinator(ClusterCoordinator coordinator, Transport transport) {
        Objects.requireNonNull(coordinator, "coordinator");
        Objects.requireNonNull(transport, "transport");
        return new LeaderView() {
            @Override
            public boolean isLeader() {
                return coordinator.isLeader();
            }

            @Override
            public Optional<String> leaderId() {
                return coordinator.leaderInfo().map(info -> info.nodeId().value());
            }

            @Override
            public Set<String> reachableNodeIds() {
                return coordinator.activeMembers().stream()
                        .map(NodeInfo::nodeId)
                        .filter(transport::isReachable)
                        .map(NodeId::value)
                        .collect(Collectors.toUnmodifiableSet());
            }
        };
    }

    @Override
    protected Object handle(String command, Object body, NodeId source) {
        return handlePlace((PlaceRequest) body);
    }

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        // Perdeu (ou nunca teve) a liderança: os placements pendentes desde o último reporte de
        // status não valem mais para as decisões deste nó — zera para não carregar contagem obsoleta
        // caso ele volte a ser líder mais tarde.
        if (!leaderView.isLeader()) {
            pendingByNode.clear();
        }
    }

    private PlaceResponse handlePlace(PlaceRequest request) {
        if (!leaderView.isLeader()) {
            return new PlaceResponse(SeriesStatus.NOT_LEADER, null, leaderView.leaderId().orElse("no leader"));
        }
        Object lock = stripeLocks[Math.floorMod(request.seriesKey().hashCode(), LOCK_STRIPES)];
        synchronized (lock) {
            // m4: a liderança pode ter mudado entre a checagem acima e a aquisição do lock de stripe.
            if (!leaderView.isLeader()) {
                return new PlaceResponse(SeriesStatus.NOT_LEADER, null, leaderView.leaderId().orElse("no leader"));
            }
            Optional<SeriesPlacement> alreadyPlaced = catalog.placementStrong(request.seriesKey());
            if (alreadyPlaced.isPresent()) {
                return new PlaceResponse(SeriesStatus.OK, alreadyPlaced.get(), null);
            }

            Collection<StorageNodeStatus> nodes = catalog.nodesLocal();
            long now = clock.millis();
            PlacementContext ctx = new PlacementContext(nodes, leaderView.reachableNodeIds(),
                    snapshotPending(nodes), now, statusReportInterval, request.preferredOwnerNodeId());

            Optional<String> chosen = policy.choose(ctx);
            if (chosen.isEmpty()) {
                return new PlaceResponse(SeriesStatus.NO_STORAGE_NODE_AVAILABLE, null,
                        "nenhum storage node candidato disponível para a série " + request.seriesKey());
            }

            // m4: re-checa de novo, o mais perto possível da escrita — decidir o placement (leitura do
            // catálogo + policy.choose) pode levar um tempo perceptível; se a liderança já mudou nesse
            // meio tempo, não grava (evita dois nós escreverem placements divergentes para a mesma série).
            if (!leaderView.isLeader()) {
                return new PlaceResponse(SeriesStatus.NOT_LEADER, null, leaderView.leaderId().orElse("no leader"));
            }

            SeriesPlacement placement = SeriesPlacement.active(chosen.get(), now);
            catalog.putPlacement(request.seriesKey(), placement);
            recordPending(chosen.get(), nodes);
            return new PlaceResponse(SeriesStatus.OK, placement, null);
        }
    }

    private Map<String, Long> snapshotPending(Collection<StorageNodeStatus> nodes) {
        Map<String, Long> pending = new HashMap<>();
        for (StorageNodeStatus node : nodes) {
            PendingCounter counter = pendingByNode.get(node.nodeId());
            if (counter == null) {
                continue;
            }
            long count = counter.peek(node.reportedAtEpochMs());
            if (count > 0) {
                pending.put(node.nodeId(), count);
            }
        }
        return pending;
    }

    private void recordPending(String nodeId, Collection<StorageNodeStatus> nodes) {
        long reportedAt = nodes.stream()
                .filter(node -> node.nodeId().equals(nodeId))
                .findFirst()
                .map(StorageNodeStatus::reportedAtEpochMs)
                .orElseGet(clock::millis);
        pendingByNode.computeIfAbsent(nodeId, ignored -> new PendingCounter()).increment(reportedAt);
    }

    /**
     * Contador de placements pendentes de um nó desde o último
     * {@code reportedAtEpochMs} observado. Zera sozinho quando percebe que o
     * relatório de status avançou — sem isso, uma rajada de placements
     * continuaria contando pendências já refletidas no {@code seriesCount}
     * reportado.
     */
    private static final class PendingCounter {
        private long reportedAtSeen = Long.MIN_VALUE;
        private long count;

        synchronized long peek(long reportedAtEpochMs) {
            resetIfAdvanced(reportedAtEpochMs);
            return count;
        }

        synchronized long increment(long reportedAtEpochMs) {
            resetIfAdvanced(reportedAtEpochMs);
            return ++count;
        }

        private void resetIfAdvanced(long reportedAtEpochMs) {
            if (reportedAtEpochMs != reportedAtSeen) {
                reportedAtSeen = reportedAtEpochMs;
                count = 0L;
            }
        }
    }
}
