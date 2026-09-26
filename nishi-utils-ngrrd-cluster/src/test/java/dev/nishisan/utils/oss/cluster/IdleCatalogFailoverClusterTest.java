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

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.rebalance.CatalogLagGate;
import dev.nishisan.utils.oss.cluster.rebalance.RebalanceSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #177: depois de uma troca de líder com o catálogo ocioso (nenhuma escrita nele desde a eleição), os
 * seguidores continuam elegíveis como destino de migração. Antes da correção no core
 * ({@code ReplicationManager.currentLeaderTopicSequence}), o líder recém-eleito anunciava
 * {@code leaderHighWatermark = 0} para o tópico do catálogo até produzir a primeira escrita nele; os
 * seguidores publicavam "lag desconhecido" e o {@link CatalogLagGate} excluía todos como destino — o
 * rebalance parava até alguém escrever no catálogo.
 */
@Timeout(value = 240, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class IdleCatalogFailoverClusterTest {

    private static final int STORAGE_NODE_COUNT = 3;
    private static final int SERIES_COUNT = 6;
    private static final Duration STATUS_REPORT_INTERVAL = Duration.ofSeconds(2);
    /** Status publicados a partir deste atraso após a eleição já refletem o HWM anunciado pelo novo líder. */
    private static final Duration SETTLE_AFTER_ELECTION = STATUS_REPORT_INTERVAL;
    /** "Poucos intervalos de status" para os seguidores voltarem a ser elegíveis. */
    private static final Duration ELIGIBILITY_DEADLINE = STATUS_REPORT_INTERVAL.multipliedBy(5);
    private static final Duration ELECTION_TIMEOUT = Duration.ofSeconds(60);
    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata: {name: idle-catalog-failover}
            spec:
              time: {baseStepSec: 1}
              identity:
                seriesKeyTemplate: "sensor:{id}"
                tags: [{name: id}]
              dataSources:
                - {name: value, type: GAUGE, heartbeatSec: 10}
              archives:
                rras:
                  - {name: raw, stepSec: 1, rows: 1024, cf: [AVERAGE], xff: 0.5}
              storage:
                backend: blob
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    @Test
    void seguidoresSeguemElegiveisComoDestinoAposFailoverComCatalogoOcioso(@TempDir Path base) throws Exception {
        try (NgrrdClusterTestHarness harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT,
                builder -> builder.rebalanceEnabled(false).statusReportInterval(STATUS_REPORT_INTERVAL))) {
            harness.awaitNodeStatuses(STORAGE_NODE_COUNT);
            try (NgrrdClusterClient client = harness.connectClient(builder -> builder
                    .requestTimeout(Duration.ofSeconds(5))
                    .retryTimeout(Duration.ofSeconds(30)))) {
                for (int i = 0; i < SERIES_COUNT; i++) {
                    NgrrdHandle handle = client.open(YAML, Map.of("id", Integer.toString(i)));
                    handle.write("value", new Sample(1_747_339_200_000L, i));
                    handle.checkpoint();
                }
            }
            harness.awaitPlacements(SERIES_COUNT);
            NgrrdStorageNode oldLeader = harness.leaderNode();
            for (NgrrdStorageNode node : harness.nodes()) {
                if (!node.nodeId().equals(oldLeader.nodeId())) {
                    harness.awaitCatalogReplicaCaughtUp(node.nodeId());
                }
            }

            // Failover sem nenhuma escrita posterior no catálogo: rebalance desligado, cliente fechado, nenhuma
            // migração em curso para o novo líder retomar.
            oldLeader.close();
            List<NgrrdStorageNode> survivors = harness.nodes().stream()
                    .filter(node -> !node.nodeId().equals(oldLeader.nodeId()))
                    .toList();
            NgrrdStorageNode newLeader = awaitNewLeader(survivors, oldLeader.nodeId());
            long electedAtMs = System.currentTimeMillis();
            List<NgrrdStorageNode> followers = survivors.stream()
                    .filter(node -> !node.nodeId().equals(newLeader.nodeId()))
                    .toList();
            assertEquals(STORAGE_NODE_COUNT - 2, followers.size());

            long deadline = electedAtMs + SETTLE_AFTER_ELECTION.plus(ELIGIBILITY_DEADLINE).toMillis();
            Map<String, String> lastSeen = Map.of();
            while (System.currentTimeMillis() < deadline) {
                lastSeen = followers.stream().collect(Collectors.toMap(NgrrdStorageNode::nodeId,
                        follower -> eligibility(newLeader, follower.nodeId(), electedAtMs)));
                if (lastSeen.values().stream().allMatch(String::isEmpty)) {
                    for (NgrrdStorageNode follower : followers) {
                        System.out.printf("IDLE_CATALOG_FAILOVER newLeader=%s follower=%s replica=%s%n",
                                newLeader.nodeId(), follower.nodeId(), newLeader.catalog()
                                        .nodeStatusLocal(follower.nodeId()).map(StorageNodeStatus::catalogReplica)
                                        .orElse(null));
                    }
                    return;
                }
                Thread.sleep(200L);
            }
            fail("seguidores não voltaram a ser elegíveis como destino em " + ELIGIBILITY_DEADLINE
                    + " após a eleição de " + newLeader.nodeId() + " (catálogo ocioso): " + lastSeen);
        }
    }

    /**
     * Vazio se o último status de {@code nodeId} visto pelo novo líder foi publicado depois de
     * {@link #SETTLE_AFTER_ELECTION}, traz a réplica do catálogo com o HWM do líder conhecido e passa no
     * {@link CatalogLagGate}; senão, o motivo.
     */
    private static String eligibility(NgrrdStorageNode leader, String nodeId, long electedAtMs) {
        Optional<StorageNodeStatus> status = leader.catalog().nodeStatusLocal(nodeId);
        if (status.isEmpty()) {
            return "sem status";
        }
        if (status.get().reportedAtEpochMs() < electedAtMs + SETTLE_AFTER_ELECTION.toMillis()) {
            return "status anterior à eleição";
        }
        CatalogReplicaStatus replica = status.get().catalogReplica();
        if (replica == null) {
            return "sem catalogReplica";
        }
        if (!replica.lagKnown()) {
            return "lag desconhecido: " + replica;
        }
        return CatalogLagGate.exclusionReason(status.get(), RebalanceSettings.DEFAULT_MAX_DESTINATION_CATALOG_LAG)
                .map(reason -> "excluído pelo gate (" + reason + "): " + replica)
                .orElse("");
    }

    /** Espera os sobreviventes concordarem num líder que não é {@code oldLeaderId}. */
    private static NgrrdStorageNode awaitNewLeader(List<NgrrdStorageNode> survivors, String oldLeaderId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + ELECTION_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<NodeInfo> first = survivors.getFirst().node().coordinator().leaderInfo();
            boolean agreed = first.isPresent()
                    && !first.get().nodeId().value().equals(oldLeaderId)
                    && survivors.stream().allMatch(node -> first.equals(node.node().coordinator().leaderInfo()));
            if (agreed) {
                Optional<NgrrdStorageNode> leader = survivors.stream().filter(NgrrdStorageNode::isLeader).findFirst();
                if (leader.isPresent()) {
                    return leader.get();
                }
            }
            Thread.sleep(100L);
        }
        fail("sobreviventes não elegeram um novo líder em " + ELECTION_TIMEOUT);
        return null;
    }
}
