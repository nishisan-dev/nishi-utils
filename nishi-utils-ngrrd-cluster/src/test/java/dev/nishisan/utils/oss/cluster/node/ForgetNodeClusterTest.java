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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.cluster.NgrrdClusterTestHarness;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.protocol.AdminForgetResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Revisão #178 (B9): {@code forget-node} tira um storage substituído da maioria de votantes de TODO o
 * cluster. Quatro storages reais (maioria 3 de 4): sem o comando, perder dois deixa o cluster sem líder;
 * depois de esquecer o primeiro, os três restantes exigem 2 de 3 e o cluster sobrevive à segunda queda
 * (a do próprio líder, para forçar uma eleição nova com só dois votantes vivos).
 */
@Timeout(value = 300, unit = java.util.concurrent.TimeUnit.SECONDS)
class ForgetNodeClusterTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(120);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void esquecerStorageParadoTiraODaMaioriaEmTodosOsNosEDoCatalogo(@TempDir Path base) throws Exception {
        harness = NgrrdClusterTestHarness.start(base, 4, builder -> { });
        harness.awaitLeader();
        harness.awaitNodeStatuses(4);
        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(3))
                .retryTimeout(Duration.ofSeconds(20))
                .closeTimeout(Duration.ofSeconds(10)));
        try {
            NgrrdStorageNode leader = harness.leaderNode();
            NgrrdStorageNode gone = harness.nodes().stream().filter(n -> !n.isLeader()).findFirst().orElseThrow();
            String goneId = gone.nodeId();

            // Ainda alcançável: o líder recusa (o operador tem de parar o processo antes).
            NgrrdClusterException refused = org.junit.jupiter.api.Assertions.assertThrows(
                    NgrrdClusterException.class, () -> client.forgetNode(goneId));
            assertTrue(refused.getMessage().contains("alcançável"), refused.getMessage());

            gone.close();
            AdminForgetResponse response = forgetWhenAccepted(client, goneId);
            assertEquals(SeriesStatus.OK, response.status());
            assertEquals(goneId, response.nodeId());
            List<NgrrdStorageNode> survivors = harness.nodes().stream()
                    .filter(n -> !n.nodeId().equals(goneId)).toList();
            Set<String> survivorIds = survivors.stream().map(NgrrdStorageNode::nodeId).collect(Collectors.toSet());
            assertEquals(survivorIds, Set.copyOf(response.forgottenOn()), "todos os sobreviventes esqueceram: "
                    + response);
            assertTrue(response.failedOn().isEmpty(), "nenhum sobrevivente falhou: " + response);

            for (NgrrdStorageNode survivor : survivors) {
                awaitTrue(survivor.nodeId() + " não lista mais " + goneId + " no transporte", () ->
                        survivor.node().transport().peers().stream().map(NodeInfo::nodeId)
                                .noneMatch(id -> id.equals(NodeId.of(goneId))));
                assertTrue(survivor.node().transport().isDeparted(NodeId.of(goneId)), "tombstone em " + survivor.nodeId());
            }
            awaitTrue("status do líder sem " + goneId, () -> client.clusterStatus().nodes().stream()
                    .map(NodeStatusView::status).noneMatch(status -> status.nodeId().equals(goneId)));

            // Segunda queda: o líder. Com 3 votantes restantes (maioria 2) os 2 sobreviventes elegem;
            // com o nó esquecido ainda contando (4 votantes, maioria 3) ficariam sem líder.
            NgrrdStorageNode currentLeader = harness.leaderNode();
            String deadLeaderId = currentLeader.nodeId();
            currentLeader.close();
            List<NgrrdStorageNode> lastTwo = survivors.stream().filter(n -> !n.nodeId().equals(deadLeaderId)).toList();
            assertEquals(2, lastTwo.size());
            awaitTrue("líder eleito entre os 2 storages restantes (maioria 2 de 3)", () ->
                    lastTwo.stream().anyMatch(NgrrdStorageNode::isLeader));
            assertFalse(lastTwo.stream().allMatch(NgrrdStorageNode::isLeader), "um líder só");
        } finally {
            client.close();
        }
    }

    /** O LEAVE do nó parado chega antes ou depois do comando: repete enquanto o líder recusar por alcançabilidade. */
    private static AdminForgetResponse forgetWhenAccepted(NgrrdClusterClient client, String nodeId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        NgrrdClusterException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return client.forgetNode(nodeId);
            } catch (NgrrdClusterException e) {
                last = e;
                Thread.sleep(500);
            }
        }
        throw new AssertionError("forget-node nunca foi aceito: " + (last != null ? last.getMessage() : "?"), last);
    }

    private static void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
    }
}
