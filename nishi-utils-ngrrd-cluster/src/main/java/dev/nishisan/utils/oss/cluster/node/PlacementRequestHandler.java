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
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
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
 * <p>Serializa a admissão de novos placements para contabilizar pendências antes
 * da decisão seguinte. O lock de cada série no catálogo também coordena mudanças
 * de geometria e migrações, evitando atualizações concorrentes do mesmo registro.</p>
 */
public final class PlacementRequestHandler extends RequestHandlerSupport implements LeadershipListener {

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

    private final CatalogView catalog;
    private final LeaderView leaderView;
    private final PlacementPolicy policy;
    private final Duration nodeStatusStaleAfter;
    private final Duration placementGraceAfterLeadership;
    private final Clock clock;

    private volatile boolean needsAdmissionRebuild = true;
    private final ConcurrentMap<String, PendingCounter> pendingByNode = new ConcurrentHashMap<>();
    /**
     * Instante em que este nó percebeu ter assumido a liderança pela última vez — {@code 0}
     * (epoch) enquanto não visto nenhuma vez, para tratar como "fora da janela de graça" por
     * default. Deliberadamente NÃO {@link Long#MIN_VALUE}: {@code clock.millis() - Long.MIN_VALUE}
     * estoura o {@code long} (o resultado matematicamente correto excede {@link Long#MAX_VALUE}) e
     * <em>wrap-around</em> vira um número NEGATIVO — menor que qualquer {@code placementGraceAfterLeadership}
     * positivo — fazendo {@code handlePlace} enxergar "dentro da janela de graça" para sempre, mesmo
     * décadas depois de qualquer liderança real (bug pego pelos testes existentes de
     * {@code PlacementRequestHandlerTest}, que nunca chamam {@code onLeaderChanged}). {@code 0}
     * evita o overflow: {@code clock.millis()} de qualquer relógio real (ou fake baseado numa data
     * real) é sempre muitas ordens de grandeza maior que a janela de graça, então o subtraendo nunca
     * é confundido com "recém-eleito". Só {@link #onLeaderChanged}
     * escreve; {@link #handlePlace} só lê — não precisa de lock próprio, um valor um pouco atrasado
     * só alarga/encolhe a janela por uma chamada, nunca quebra a invariante de segurança (seção 0 da
     * spec do M3).
     */
    private volatile long becameLeaderAtMs = 0L;

    public PlacementRequestHandler(Transport transport, CatalogService catalog, LeaderView leaderView,
            PlacementPolicy policy, Duration nodeStatusStaleAfter, Clock clock) {
        this(transport, catalog, leaderView, policy, nodeStatusStaleAfter, Duration.ofSeconds(3), clock);
    }

    public PlacementRequestHandler(Transport transport, CatalogService catalog, LeaderView leaderView,
            PlacementPolicy policy, Duration nodeStatusStaleAfter, Duration placementGraceAfterLeadership,
            Clock clock) {
        this(transport, (CatalogView) catalog, leaderView, policy, nodeStatusStaleAfter,
                placementGraceAfterLeadership, clock);
    }

    /**
     * Construtor de teste: recebe {@link CatalogView} diretamente (fake), sem passar por
     * {@link CatalogService}/{@code DistributedMap} reais.
     */
    PlacementRequestHandler(Transport transport, CatalogView catalog, LeaderView leaderView,
            PlacementPolicy policy, Duration nodeStatusStaleAfter, Duration placementGraceAfterLeadership,
            Clock clock) {
        super(transport, Set.of(Commands.PLACE));
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.leaderView = Objects.requireNonNull(leaderView, "leaderView");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.nodeStatusStaleAfter = Objects.requireNonNull(nodeStatusStaleAfter, "nodeStatusStaleAfter");
        this.placementGraceAfterLeadership =
                Objects.requireNonNull(placementGraceAfterLeadership, "placementGraceAfterLeadership");
        this.clock = Objects.requireNonNull(clock, "clock");
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
        // One admission decision at a time also serializes pending-count updates across keys.
        synchronized (pendingByNode) { return handlePlace((PlaceRequest) body); }
    }

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        if (leaderView.isLeader()) {
            // Assumiu a liderança agora (ou de novo): reconstrói pendingByNode a partir do catálogo
            // local em vez de começar do zero. Sem isso, um handoff logo após uma rajada de PLACEs
            // feitos pelo líder anterior faz o novo líder enxergar seriesCount desatualizado (ainda
            // não refletido no próximo StorageNodeStatus) E pending=0 para todo mundo — a rajada
            // inteira decidiria pelo mesmo "menos carregado" aparente, concentrando tudo num nó só
            // (achado F2 do Debugger).
            recomputePendingFromCatalog();
            // Seção 0 do M3: marca o início da janela de graça — enquanto ela não passa, handlePlace
            // recusa criar placements NOVOS (responde NOT_LEADER, o cliente retenta), dando tempo da
            // réplica local do catálogo convergir. Placements JÁ existentes continuam respondidos
            // normalmente (não passam por esta janela).
            becameLeaderAtMs = clock.millis();
        } else {
            // Perdeu (ou nunca teve) a liderança: os placements pendentes desde o último reporte de
            // status não valem mais para as decisões deste nó — zera para não carregar contagem
            // obsoleta caso ele volte a ser líder mais tarde.
            needsAdmissionRebuild = true;
            pendingByNode.clear();
        }
    }

    /**
     * Reconta, a partir do catálogo local, quantas séries {@code ACTIVE} de cada dono foram colocadas
     * DEPOIS do último {@code reportedAtEpochMs} conhecido daquele dono — exatamente o que
     * {@link PendingCounter} rastreia incrementalmente durante o mandato, mas partindo do estado real
     * em vez de zero.
     */
    private void recomputePendingFromCatalog() {
        Map<String, StorageNodeStatus> statusByNode = catalog.nodesLocal().stream()
                .collect(Collectors.toMap(StorageNodeStatus::nodeId, status -> status, (a, b) -> a));
        Map<String, Long> countByOwner = new HashMap<>();
        for (SeriesPlacement placement : catalog.placementsLocal().values()) {
            if (placement.state() != PlacementState.ACTIVE) {
                continue;
            }
            StorageNodeStatus ownerStatus = statusByNode.get(placement.ownerNodeId());
            long reportedAt = ownerStatus != null ? ownerStatus.reportedAtEpochMs() : Long.MIN_VALUE;
            if (placement.createdAtEpochMs() > reportedAt) {
                countByOwner.merge(placement.ownerNodeId(), 1L, Long::sum);
            }
        }

        needsAdmissionRebuild = true;
        pendingByNode.clear();
        for (Map.Entry<String, Long> entry : countByOwner.entrySet()) {
            StorageNodeStatus ownerStatus = statusByNode.get(entry.getKey());
            long reportedAt = ownerStatus != null ? ownerStatus.reportedAtEpochMs() : clock.millis();
            PendingCounter counter = new PendingCounter();
            for (long i = 0; i < entry.getValue(); i++) {
                counter.increment(reportedAt);
            }
            pendingByNode.put(entry.getKey(), counter);
        }
    }

    private PlaceResponse handlePlace(PlaceRequest request) {
        if (!leaderView.isLeader()) {
            return notLeaderResponse();
        }
        Object lock = catalog.placementLock(request.seriesKey());
        synchronized (lock) {
            // m4: a liderança pode ter mudado entre a checagem acima e a aquisição do lock de stripe.
            if (!leaderView.isLeader()) {
                return notLeaderResponse();
            }
            Optional<SeriesPlacement> alreadyPlaced = catalog.placementStrong(request.seriesKey());
            if (alreadyPlaced.isPresent()) {
                return new PlaceResponse(SeriesStatus.OK, alreadyPlaced.get(), null, null);
            }

            // Seção 0 do M3: a série não está no catálogo (nem na leitura STRONG, que já foi ao
            // líder). Antes de decidir criar uma série NOVA, garante que não estamos ainda na janela
            // de sincronização logo após assumir a liderança — é exatamente essa janela que permitia
            // ao líder "não achar" uma série que na verdade já existe (réplica local do catálogo ainda
            // convergindo) e criar uma cópia vazia noutro nó.
            if (clock.millis() - becameLeaderAtMs < placementGraceAfterLeadership.toMillis()) {
                return notLeaderResponse();
            }

            if (needsAdmissionRebuild) {
                catalog.resetAdmissionTracking();
                needsAdmissionRebuild = false;
            }
            Collection<StorageNodeStatus> nodes = catalog.nodesLocal();
            long now = clock.millis();
            PlacementContext ctx = new PlacementContext(nodes, leaderView.reachableNodeIds(),
                    snapshotPending(nodes), now, nodeStatusStaleAfter, request.preferredOwnerNodeId(),
                    request.geometry() == null ? 0 : request.geometry().regionBytes(), catalog.pendingBytesByNode());

            Optional<String> chosen = policy.choose(ctx);
            if (chosen.isEmpty()) {
                return new PlaceResponse(SeriesStatus.NO_STORAGE_NODE_AVAILABLE, null,
                        "nenhum storage node candidato disponível para a série " + request.seriesKey(), null);
            }

            // m4: re-checa de novo, o mais perto possível da escrita — decidir o placement (leitura do
            // catálogo + policy.choose) pode levar um tempo perceptível; se a liderança já mudou nesse
            // meio tempo, não grava (evita dois nós escreverem placements divergentes para a mesma série).
            if (!leaderView.isLeader()) {
                return notLeaderResponse();
            }

            SeriesPlacement placement = SeriesPlacement.active(chosen.get(), now);
            if (request.geometry() != null) {
                catalog.putGeometry(request.geometry());
                placement = placement.withGeometry(request.geometry().id(), false, now);
            }
            try {
                catalog.putPlacement(request.seriesKey(), placement);
            } catch (RuntimeException e) {
                // Seção 0 do M3: cobre, entre outras, LeaderSyncingException (o ReplicationManager do
                // core recusa a escrita porque este líder ainda está em catch-up de um mandato
                // anterior) — qualquer falha aqui significa que NADA foi gravado, então não há
                // placement órfão a desfazer. NOT_LEADER (não ERROR): o cliente já sabe retentar.
                return notLeaderResponse();
            }
            recordPending(chosen.get(), nodes);
            return new PlaceResponse(SeriesStatus.OK, placement, null, null);
        }
    }

    /**
     * B2 (achado do Refuter): {@code leaderNodeId} carrega quem, na visão de {@link #leaderView}, é
     * o líder atual — {@code null} se nem este nó sabe. O cliente ({@code PlacementResolver}) usa
     * esse valor para ir direto ao líder indicado na próxima tentativa, em vez de reconsultar
     * {@code ClusterRpc#leaderId()}.
     */
    private PlaceResponse notLeaderResponse() {
        String leaderId = leaderView.leaderId().orElse(null);
        return new PlaceResponse(SeriesStatus.NOT_LEADER, null,
                "este nó não é o líder atual", leaderId);
    }

    private Map<String, Long> snapshotPending(Collection<StorageNodeStatus> nodes) {
        Map<String, Long> pending = new HashMap<>(catalog.pendingMigrationSeriesByNode());
        for (StorageNodeStatus node : nodes) {
            PendingCounter counter = pendingByNode.get(node.nodeId());
            if (counter == null) {
                continue;
            }
            long count = counter.peek(node.reportedAtEpochMs());
            if (count > 0) {
                pending.merge(node.nodeId(), count, Long::sum);
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
