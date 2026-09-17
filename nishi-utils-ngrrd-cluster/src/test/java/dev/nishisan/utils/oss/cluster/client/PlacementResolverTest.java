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
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cobre {@link PlacementResolver} com {@link RecordingClusterRpc} fake — sem
 * {@code ClusterCoordinator}/{@code Transport} reais — sobre um
 * {@link CatalogService} real de um único nó ({@link NGrid#local(int)}),
 * mesma técnica de {@code PlacementRequestHandlerTest} (M1a):
 * {@code DistributedMap} é classe final, então não há como fingi-la.
 */
class PlacementResolverTest {

    private static final NodeId LEADER = NodeId.of("leader-1");
    private static final NodeId CLIENT = NodeId.of("client-under-test");

    private NGridCluster cluster;
    private CatalogService catalog;
    private RecordingClusterRpc rpc;
    private PlacementResolver resolver;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .start();
        catalog = CatalogService.from(cluster.node(0));
        rpc = new RecordingClusterRpc(CLIENT);
        rpc.leader(LEADER);
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMillis(10), Duration.ofMillis(100));
        resolver = new PlacementResolver(catalog, rpc, retry, java.time.Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        cluster.close();
    }

    @Test
    void devolveOverrideLocalSemChamarOLiderQuandoJaConhecido() {
        resolver.noteOwner("series-1", "storage-b");

        SeriesPlacement placement = resolver.resolve("series-1", "hash-1");

        assertEquals("storage-b", placement.ownerNodeId());
        assertEquals(0, rpc.calls().size(), "não deveria ter chamado o líder com override em cache");
    }

    @Test
    void devolveOCatalogoLocalAtivoSemChamarOLiderQuandoJaPosicionado() {
        SeriesPlacement active = SeriesPlacement.active("storage-a", 1_000L);
        catalog.putPlacement("series-1", active);

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(active, resolved);
        assertEquals(0, rpc.calls().size());
    }

    @Test
    void chamaOLiderQuandoNaoHaOverrideNemCatalogoLocal() {
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.PLACE, cmd);
            assertEquals(new PlaceRequest("series-1", "hash-1", null), body);
            return new PlaceResponse(SeriesStatus.OK, placed, null, null);
        });

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(placed, resolved);
        assertEquals(1, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
    }

    @Test
    void placeComFalhaDeTransporteNaPrimeiraTentativaRetentaEConclui() {
        // B3 (achado do Refuter): TIMEOUT/REMOTE_ERROR(IOException) ao chamar o líder não deve subir
        // direto — o resolver retenta com backoff em vez de propagar a falha de transporte.
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado");
        });
        rpc.respondNext((cmd, body) -> new PlaceResponse(SeriesStatus.OK, placed, null, null));

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(placed, resolved);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(LEADER, rpc.calls().get(1).target());
    }

    @Test
    void reResolveOLiderAposNotLeaderERepeteAChamada() {
        NodeId secondLeader = NodeId.of("leader-2");
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);

        rpc.respondNext((cmd, body) -> new PlaceResponse(SeriesStatus.NOT_LEADER, null,
                "este nó não é o líder atual", secondLeader.value()));
        rpc.respondNext((cmd, body) -> {
            rpc.leader(secondLeader);
            return new PlaceResponse(SeriesStatus.OK, placed, null, null);
        });

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(placed, resolved);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
    }

    @Test
    void usaOLeaderNodeIdDaRespostaNotLeaderSemReconsultarRpcLeaderId() {
        // B2 (achado do Refuter): a 2a tentativa vai direto ao líder indicado na resposta NOT_LEADER —
        // rpc.leader() nunca muda aqui (continua devolvendo LEADER, que já sabemos estar errado), então
        // só passa se o resolver usou response.leaderNodeId() em vez de reconsultar rpc.leaderId().
        NodeId indicatedLeader = NodeId.of("leader-2");
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);

        rpc.respondNext((cmd, body) -> new PlaceResponse(SeriesStatus.NOT_LEADER, null,
                "este nó não é o líder atual", indicatedLeader.value()));
        rpc.respondNext((cmd, body) -> new PlaceResponse(SeriesStatus.OK, placed, null, null));

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(placed, resolved);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(indicatedLeader, rpc.calls().get(1).target());
    }

    @Test
    void semLeaderNodeIdNaRespostaCaiDeVoltaParaRpcLeaderId() {
        // Sem indicação de líder na resposta NOT_LEADER (leaderNodeId == null), o fallback continua
        // sendo rpc.leaderId() — atualizado aqui (como efeito colateral da 1a resposta, antes da 2a
        // tentativa escolher o alvo) simulando a reconvergência do gossip.
        NodeId fallbackLeader = NodeId.of("leader-2");
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);

        rpc.respondNext((cmd, body) -> {
            rpc.leader(fallbackLeader);
            return new PlaceResponse(SeriesStatus.NOT_LEADER, null, "este nó não é o líder atual", null);
        });
        rpc.respondNext((cmd, body) -> new PlaceResponse(SeriesStatus.OK, placed, null, null));

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(placed, resolved);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(fallbackLeader, rpc.calls().get(1).target());
    }

    @Test
    void lancaExcecaoComCodigoNoStorageNodeAvailable() {
        rpc.respondDefault((cmd, body) ->
                new PlaceResponse(SeriesStatus.NO_STORAGE_NODE_AVAILABLE, null, "sem candidatos", null));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> resolver.resolve("series-1", "hash-1"));
        assertEquals(ErrorCode.NO_STORAGE_NODE_AVAILABLE, ex.code());
    }

    @Test
    void noteOwnerAtualizaOOverrideParaAtivoSemRpc() {
        resolver.noteOwner("series-1", "storage-c");

        SeriesPlacement placement = resolver.resolve("series-1", "hash-1");

        assertEquals("storage-c", placement.ownerNodeId());
        assertEquals(PlacementState.ACTIVE, placement.state());
        assertEquals(0, rpc.calls().size());
    }

    @Test
    void invalidateForcaNovaConsultaAoLider() {
        resolver.noteOwner("series-1", "storage-b");
        resolver.invalidate("series-1");

        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondDefault((cmd, body) -> new PlaceResponse(SeriesStatus.OK, placed, null, null));

        SeriesPlacement resolved = resolver.resolve("series-1", "hash-1");

        assertEquals(placed, resolved);
        assertEquals(1, rpc.calls().size());
    }
}
