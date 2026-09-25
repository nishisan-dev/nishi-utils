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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationOutcome;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #174, fluxo ponta a ponta: a origem de uma migração reinicia logo depois do
 * {@code MIGRATE_FINISH} e um cliente com cache velho (placement pré-migração) manda um {@code OPEN}
 * com criação ligada direto para ela. A origem não pode recriar a série vazia — o dono de verdade,
 * segundo o líder, é o destino da migração.
 *
 * <p>Num cluster real, a réplica local do catálogo da origem normalmente já reconvergiu para
 * {@code ACTIVE(destino)} no momento em que o {@code OPEN} chega (replicação é rápida e
 * {@code restartStorageNode} recarrega o catálogo persistido) — nesse caso a decisão nem chega a
 * precisar da confirmação forte no líder introduzida pela correção da issue #174: o placement local já
 * aponta para o dono certo. O caso determinístico em que a réplica local FICA presa em
 * {@code ACTIVE(self)} logo após o restart (a marca {@code forgotten}, só em memória, se perdeu, e a
 * replicação ainda não alcançou o nó) é reproduzido de forma controlada em
 * {@code StorageRequestHandlerTest#openCriandoComReplicaAtrasadaAposReinicioRespondeWrongOwnerENaoRecria}.
 * Este teste não tenta forçar essa janela de corrida — ele valida o comportamento observável do fluxo
 * inteiro (migração real + restart real + RPC real pela malha), que deve ser o mesmo
 * ({@code WRONG_OWNER} apontando para o destino, sem recriação) em qualquer um dos dois casos.</p>
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MigrationSourceRestartClusterTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(60);

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void origemReiniciadaAposMigracaoNaoRecriaASerie(@TempDir Path base) throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));
        Map<String, String> tags = Map.of("deviceId", "restart-src", "interfaceId", "eth0",
                "region", "br-sp", "vendor", "x", "role", "core");

        harness = NgrrdClusterTestHarness.start(base, 3, builder -> builder.rebalanceEnabled(false));
        harness.awaitNodeStatuses(3);
        NgrrdClusterClient client = harness.connectClient(builder -> builder
                .requestTimeout(Duration.ofSeconds(5))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(20)));

        // 1) Abre, escreve e faz checkpoint numa série nova.
        NgrrdHandle handle = client.open(yaml, tags);
        String seriesKey = handle.seriesKey();
        long t0 = 1_700_000_000_000L;
        handle.write("in_octets", new Sample(t0, 1_000d));
        handle.write("out_octets", new Sample(t0, 500d));
        handle.checkpoint();

        SeriesPlacement placementBeforeMigration = harness.leaderNode().catalog().placementStrong(seriesKey)
                .orElseThrow(() -> new AssertionError("série sem placement antes da migração"));
        String sourceId = placementBeforeMigration.ownerNodeId();
        int sourceIndex = indexOf(sourceId);
        String destinationId = harness.nodes().stream()
                .map(NgrrdStorageNode::nodeId)
                .filter(id -> !id.equals(sourceId))
                .findFirst()
                .orElseThrow();
        int destinationIndex = indexOf(destinationId);
        NgrrdStorageNode destinationNode = harness.nodes().get(destinationIndex);

        // 2) Migra a série da origem para o destino e espera o placement forte convergir e o objeto
        // sumir do volume da origem.
        MigrationResult result0 = harness.leaderNode().migrationCoordinator()
                .migrate(seriesKey, sourceId, destinationId)
                .get(60, TimeUnit.SECONDS);
        assertEquals(MigrationOutcome.COMPLETED, result0.outcome(), result0.reason());

        String objectKey = objectKey(seriesKey);
        awaitTrue("placement forte convergiu para ACTIVE(" + destinationId + ")", () -> {
            var strong = harness.leaderNode().catalog().placementStrong(seriesKey);
            return strong.isPresent() && strong.get().state() == PlacementState.ACTIVE
                    && destinationId.equals(strong.get().ownerNodeId());
        });
        awaitTrue("objeto da série sumiu do volume da origem (" + sourceId + ")",
                () -> !harness.nodes().get(sourceIndex).volume().storage().exists(objectKey));
        assertTrue(destinationNode.volume().storage().exists(objectKey),
                "objeto da série deveria existir no destino após a migração");
        // Espera a RÉPLICA LOCAL da própria origem (não só a leitura forte do líder) convergir para
        // ACTIVE(destino) antes do restart — é essa convergência (normal num cluster real, ver Javadoc
        // da classe) que faz o OPEN pós-restart cair no caminho "placement local já aponta para o dono
        // certo" em vez de deixar a réplica presa numa foto intermediária (ex.: ainda MIGRATING) tirada
        // bem no instante em que o processo morreu.
        awaitTrue("réplica local da origem convergiu para ACTIVE(" + destinationId + ")", () -> {
            var local = harness.nodes().get(sourceIndex).catalog().placementLocal(seriesKey);
            return local.isPresent() && local.get().state() == PlacementState.ACTIVE
                    && destinationId.equals(local.get().ownerNodeId());
        });

        // 3) Reinicia a origem — mesmo nodeId/porta/diretórios (StorageNodeConfig preservada).
        NgrrdStorageNode restartedSource = harness.restartStorageNode(sourceIndex);
        harness.awaitMeshStable();

        // 4) Um cliente com cache velho manda um OPEN com criação ligada direto para a origem
        // reiniciada, usando o placement PRÉ-migração (ACTIVE(origem)) como hint. O RPC sai pela malha
        // real (o nó de destino chama a origem via TCP), não por despacho local em processo.
        OpenRequest staleOpen = new OpenRequest(seriesKey, yaml, tags, null, null, placementBeforeMigration, true);
        SeriesStatusResponse response = destinationNode.rpc().call(NodeId.of(sourceId), Commands.OPEN, staleOpen,
                SeriesStatusResponse.class);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status(),
                "a origem reiniciada não pode aceitar um OPEN com criação para uma série que já migrou");
        assertEquals(destinationId, response.ownerNodeId());
        assertFalse(restartedSource.volume().storage().exists(objectKey),
                "o OPEN com hint velho não pode ter recriado o objeto na origem reiniciada");

        // 5) Via cliente normal: a escrita subsequente no MESMO handle se autocura (redireciona para o
        // destino) e os dados gravados antes da migração continuam legíveis.
        long t1 = t0 + 300_000L;
        handle.write("in_octets", new Sample(t1, 2_000d));
        handle.write("out_octets", new Sample(t1, 1_000d));
        assertTrue(tryUntil(AWAIT_TIMEOUT, () -> {
            try {
                handle.checkpoint();
                return true;
            } catch (RuntimeException e) {
                return false;
            }
        }), "checkpoint pós-restart não conseguiu se autocurar a tempo");

        ViewQuery query = new ViewQuery(Duration.ofDays(1), 300, ConsolidationFunction.AVERAGE, 500);
        SeriesResult result = handle.read("in_bps", query, t1 + 300_000L);
        long nonNullPoints = result.points().stream().filter(p -> Double.isFinite(p.value())).count();
        assertTrue(nonNullPoints > 0, "os dados gravados antes da migração deveriam continuar legíveis no "
                + "destino após o restart da origem — resultado=" + result);
        assertFalse(restartedSource.volume().storage().exists(objectKey),
                "a série não deveria ter sido recriada na origem em nenhum momento deste fluxo");

        client.close();
    }

    private int indexOf(String nodeId) {
        for (int i = 0; i < harness.nodes().size(); i++) {
            if (harness.nodes().get(i).nodeId().equals(nodeId)) {
                return i;
            }
        }
        throw new AssertionError("nó desconhecido: " + nodeId);
    }

    private static String objectKey(String seriesKey) {
        return "series/" + seriesKey + ".ngrr";
    }

    private static boolean tryUntil(Duration timeout, BooleanSupplier action) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        do {
            if (action.getAsBoolean()) {
                return true;
            }
            Thread.sleep(200L);
        } while (System.currentTimeMillis() < deadline);
        return action.getAsBoolean();
    }

    private void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
        }
    }
}
