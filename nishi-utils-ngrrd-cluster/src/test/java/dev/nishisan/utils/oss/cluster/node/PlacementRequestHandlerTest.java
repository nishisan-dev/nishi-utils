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
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.LeastLoadedPlacementPolicy;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupRequest;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link PlacementRequestHandler} sem montar um cluster de vários nós:
 * {@link LeaderViewFake} substitui {@code ClusterCoordinator}/{@code Transport}
 * (classes finais e complexas de montar em teste) para controlar liderança e
 * alcançabilidade de forma determinística. {@link CatalogService} continua
 * real — {@code DistributedMap} é classe final, então não há como fingi-la —
 * mas sobre um único {@link NGridNode} local ({@link NGrid#local(int)}), que
 * elege a si mesmo líder quase instantaneamente e não depende de rede externa.
 */
class PlacementRequestHandlerTest {

    private static final Duration INTERVAL = Duration.ofSeconds(10);
    /** Janela de graça (seção 0 do M3) usada nestes testes — explícita, não o default de produção. */
    private static final Duration GRACE = Duration.ofSeconds(3);

    private NGridCluster cluster;
    private CatalogService catalog;
    private LeaderViewFake leaderView;
    /** Estado de sincronização (catch-up) da réplica do líder, controlado pelo teste. */
    private final AtomicBoolean leaderSyncing = new AtomicBoolean(false);
    private MutableClock clock;
    private PlacementRequestHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .map(CatalogService.GEOMETRIES_MAP)
                .start();
        NGridNode node = cluster.node(0);
        catalog = CatalogService.from(node);
        leaderView = new LeaderViewFake();
        clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        handler = new PlacementRequestHandler(node.transport(), catalog, leaderView, leaderSyncing::get,
                new LeastLoadedPlacementPolicy(), INTERVAL, GRACE, clock);
    }

    @AfterEach
    void tearDown() throws Exception {
        cluster.close();
    }

    private void putNode(String nodeId, long seriesCount, long reportedAtEpochMs) {
        catalog.putNodeStatus(new StorageNodeStatus(nodeId, NodeState.ACTIVE, seriesCount, 0, 0, reportedAtEpochMs));
        leaderView.reachable.add(nodeId);
    }

    @Test
    void naoLiderRespondeNotLeaderComOIdDoLiderConhecido() {
        // B2 (achado do Refuter): o id do líder conhecido vai no campo dedicado leaderNodeId — não
        // mais dentro de message, que agora é só um texto legível para humano.
        leaderView.leader = false;
        leaderView.leaderId = Optional.of("node-b");

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("node-b", response.leaderNodeId());
    }

    @Test
    void naoLiderSemLiderConhecidoRespondeLeaderNodeIdNulo() {
        leaderView.leader = false;
        leaderView.leaderId = Optional.empty();

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertNull(response.leaderNodeId());
    }

    @Test
    void semNoDisponivelRespondeNoStorageNodeAvailable() {
        leaderView.leader = true;
        // Nenhum nó publicado no catálogo -> nenhum candidato.

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.NO_STORAGE_NODE_AVAILABLE, response.status());
        assertNotNull(response.message());
    }

    @Test
    void placementCountsIncomingMigrationsBeforeTheirDestinationReportsThem() {
        leaderView.leader = true;
        putNode("node-a", 0, clock.millis());
        putNode("node-b", 1, clock.millis());
        for (int i = 0; i < 2; i++) {
            catalog.putPlacement("moving-" + i, SeriesPlacement.migrating(
                    SeriesPlacement.active("node-b", 1), "node-a", "migration-" + i, clock.millis()));
        }
        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-new", "hash-new", null), NodeId.of("client"));
        assertEquals("node-b", response.placement().ownerNodeId());
    }

    @Test
    void primeiroPlaceEscolheOMenosCarregadoEGravaNoCatalogo() {
        leaderView.leader = true;
        putNode("node-a", 10, clock.millis());
        putNode("node-b", 2, clock.millis());

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals("node-b", response.placement().ownerNodeId());
        assertEquals(Optional.of(response.placement()), catalog.placementStrong("series-1"));
    }

    @Test
    void segundoPlaceDaMesmaChaveEhIdempotenteEDevolveOMesmoPlacement() {
        leaderView.leader = true;
        putNode("node-a", 10, clock.millis());
        putNode("node-b", 2, clock.millis());

        PlaceResponse first = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));
        PlaceResponse second = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, second.status());
        assertEquals(first.placement(), second.placement());
    }

    @Test
    void pendingZeraQuandoReportedAtAvancaParaAquelaNo() {
        leaderView.leader = true;
        long t0 = clock.millis();
        putNode("node-a", 0, t0);
        putNode("node-b", 0, t0);

        // 1o place: ambos empatados (seriesCount=0, sem pending) -> desempate por nodeId -> node-a.
        PlaceResponse first = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));
        assertEquals("node-a", first.placement().ownerNodeId());

        // 2o place (mesmo reportedAt de node-a) -> pending[node-a]=1 conta -> node-b agora é o mais leve.
        PlaceResponse second = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-2", "hash-2", null), NodeId.of("client"));
        assertEquals("node-b", second.placement().ownerNodeId());

        // node-a publica um novo status (reportedAt avança) refletindo a série recém-colocada:
        // pending[node-a] deveria zerar, então volta a empatar com node-b (que agora tem pending=1).
        clock.advance(Duration.ofSeconds(1));
        putNode("node-a", 1, clock.millis());

        PlaceResponse third = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-3", "hash-3", null), NodeId.of("client"));
        // node-a: seriesCount=1 + pending(zerado)=0 = 1; node-b: seriesCount=0 + pending=1 = 1 -> empate -> nodeId.
        assertEquals("node-a", third.placement().ownerNodeId());
    }

    @Test
    void rajadaAlternaEntreDoisNosComStatusParado() {
        leaderView.leader = true;
        long t0 = clock.millis();
        putNode("node-a", 0, t0);
        putNode("node-b", 0, t0);

        List<String> chosen = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                    new PlaceRequest("series-" + i, "hash-" + i, null), NodeId.of("client"));
            chosen.add(response.placement().ownerNodeId());
        }

        assertEquals(List.of("node-a", "node-b", "node-a", "node-b", "node-a", "node-b"), chosen);
    }

    @Test
    void onLeaderChangedZeraPendingsAoPerderLideranca() {
        leaderView.leader = true;
        long t0 = clock.millis();
        putNode("node-a", 0, t0);
        putNode("node-b", 0, t0);

        handler.handle(Commands.PLACE, new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));
        // node-a foi escolhido (empate por nodeId); pending[node-a] agora é 1, com o mesmo
        // reportedAt (t0) ainda publicado pelos dois nós.

        leaderView.leader = false;
        handler.onLeaderChanged(NodeId.of("node-b"));
        leaderView.leader = true;

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-2", "hash-2", null), NodeId.of("client"));
        // Sem o reset em onLeaderChanged, pendingByNode ainda teria node-a=1 (de series-1), então
        // node-a (efetivo 1) perderia para node-b (efetivo 0). Com o reset, os dois voltam a 0 -> empate -> nodeId.
        assertEquals("node-a", response.placement().ownerNodeId());
    }

    @Test
    void aoAssumirALiderancaRecomputaPendingByNodeDoCatalogoLocal() {
        // F2.4 (Debugger): série já colocada em node-a por um líder anterior, DEPOIS do último
        // status reportado por node-a (createdAt > reportedAt) — o próximo StorageNodeStatus de
        // node-a ainda não reflete essa série no seriesCount. Ao assumir a liderança, o handler deve
        // recontar isso a partir do catálogo, não começar pendingByNode do zero.
        long t0 = clock.millis();
        putNode("node-a", 0, t0);
        putNode("node-b", 0, t0);
        catalog.putPlacement("series-preexistente", SeriesPlacement.active("node-a", t0 + 1));

        leaderView.leader = true;
        handler.onLeaderChanged(NodeId.of("self"));
        // Passa da janela de graça (seção 0 do M3) — este teste cobre o recompute de pending, não a
        // janela em si (coberta por testes dedicados).
        clock.advance(GRACE.plusSeconds(1));

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-nova", "hash-nova", null), NodeId.of("client"));
        // node-a: seriesCount(0) reportado + pending recomputado(1) = 1; node-b: 0 + 0 = 0 -> node-b
        // vence, mesmo os dois tendo o mesmo seriesCount reportado (sem o recompute, empatariam por
        // nodeId e node-a venceria, sobrecarregando o nó que já tinha uma série "invisível").
        assertEquals("node-b", response.placement().ownerNodeId());
    }

    @Test
    void preferredOwnerNaoDisponivelCaiParaCriteriosNormais() {
        leaderView.leader = true;
        putNode("node-a", 0, clock.millis());

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", "node-inexistente"), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals("node-a", response.placement().ownerNodeId());
    }

    @Test
    void preferredOwnerDisponivelVenceMesmoNaoSendoOMenosCarregado() {
        leaderView.leader = true;
        putNode("node-a", 0, clock.millis());
        putNode("node-b", 50, clock.millis());

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", "node-b"), NodeId.of("client"));

        assertEquals("node-b", response.placement().ownerNodeId());
    }

    // ---------------------------------------------------------------- seção 0 do M3

    @Test
    void handlePlaceDentroDaJanelaDeGracaRecusaCriarNovoPlacement() {
        leaderView.leader = true;
        handler.onLeaderChanged(NodeId.of("self"));
        // Ainda dentro de GRACE (o clock não avançou desde onLeaderChanged) — mesmo com um candidato
        // disponível, handlePlace deve recusar CRIAR um placement novo (responde NOT_LEADER, o
        // cliente retenta) em vez de arriscar recriar uma série que o catálogo local ainda não viu.
        putNode("node-a", 0, clock.millis());

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertNull(response.placement());
        assertEquals(Optional.empty(), catalog.placementStrong("series-1"), "nada deveria ter sido gravado");
    }

    @Test
    void handlePlaceDentroDaJanelaDeGracaContinuaRespondendoPlacementsJaExistentes() {
        leaderView.leader = true;
        putNode("node-a", 0, clock.millis());
        PlaceResponse before = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));
        assertEquals(SeriesStatus.OK, before.status());

        // Simula um handoff bem no instante seguinte: a série já existe no catálogo antes da janela
        // começar a contar, então mesmo dentro da janela de graça a resposta deve ser OK, normalmente.
        handler.onLeaderChanged(NodeId.of("self"));

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(before.placement(), response.placement());
    }

    @Test
    void handlePlaceAposAJanelaDeGracaCriaNormalmente() {
        leaderView.leader = true;
        handler.onLeaderChanged(NodeId.of("self"));
        clock.advance(GRACE.plusSeconds(1));
        putNode("node-a", 0, clock.millis());

        PlaceResponse response = (PlaceResponse) handler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals("node-a", response.placement().ownerNodeId());
    }

    @Test
    void putPlacementLancandoRespondeNotLeaderSemGravarNada() {
        ThrowingCatalogFake fakeCatalog = new ThrowingCatalogFake();
        fakeCatalog.putNode(new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, clock.millis()));
        leaderView.leader = true;
        leaderView.reachable.add("node-a");
        PlacementRequestHandler throwingHandler = new PlacementRequestHandler(cluster.node(0).transport(),
                fakeCatalog, leaderView, leaderSyncing::get, new LeastLoadedPlacementPolicy(), INTERVAL,
                Duration.ZERO, clock);

        PlaceResponse response = (PlaceResponse) throwingHandler.handle(Commands.PLACE,
                new PlaceRequest("series-1", "hash-1", null), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertTrue(fakeCatalog.putAttempted, "putPlacement deveria ter sido tentado");
        assertTrue(fakeCatalog.placementsLocal().isEmpty(), "nenhum placement deveria ter sido gravado após a falha");
    }

    // ---------------------------------------------------------------- ngrrd.catalog.lookup

    @Test
    void catalogLookupNoLiderDevolvePresentesEOmiteAusentes() {
        leaderView.leader = true;
        SeriesPlacement placement = SeriesPlacement.active("node-a", clock.millis());
        catalog.putPlacement("series-1", placement);

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-1", "series-ausente")), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(Map.of("series-1", placement), response.found());
        assertFalse(response.found().containsKey("series-ausente"));
    }

    @Test
    void catalogLookupForaDoLiderRespondeNotLeaderComHint() {
        leaderView.leader = false;
        leaderView.leaderId = Optional.of("node-b");

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-1")), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("node-b", response.leaderNodeId());
    }

    @Test
    void catalogLookupNaJanelaDeGracaComMissRespondeNotLeader() {
        leaderView.leader = true;
        leaderView.leaderId = Optional.of("node-self");
        handler.onLeaderChanged(NodeId.of("self"));
        // Ainda dentro de GRACE (o clock não avançou desde onLeaderChanged) e a chave consultada não
        // tem placement -> não pode virar "não existe" enquanto a réplica local pode não ter convergido.

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-inexistente")), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("node-self", response.leaderNodeId());
    }

    @Test
    void catalogLookupNaJanelaDeGracaSemMissRespondeOk() {
        leaderView.leader = true;
        leaderView.leaderId = Optional.of("node-self");
        SeriesPlacement placement = SeriesPlacement.active("node-a", clock.millis());
        catalog.putPlacement("series-1", placement);
        handler.onLeaderChanged(NodeId.of("self"));
        // Ainda dentro de GRACE, mas todas as chaves pedidas foram encontradas -> não há miss para
        // desconfiar, responde OK normalmente.

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-1")), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(Map.of("series-1", placement), response.found());
        assertNull(response.leaderNodeId(), "resposta OK não carrega hint de líder");
    }

    @Test
    void catalogLookupAposAJanelaDeGracaComMissRespondeOkSemAChave() {
        leaderView.leader = true;
        handler.onLeaderChanged(NodeId.of("self"));
        clock.advance(GRACE.plusSeconds(1));
        // A janela de graça já passou -> um miss agora É definitivo, responde OK com a chave ausente
        // do mapa (a réplica local já teve tempo de convergir desde que este nó assumiu a liderança).

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-inexistente")), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertTrue(response.found().isEmpty());
    }

    @Test
    void catalogLookupComLiderSincronizandoEMissRespondeNotLeader() {
        leaderView.leader = true;
        leaderView.leaderId = Optional.of("node-self");
        handler.onLeaderChanged(NodeId.of("self"));
        clock.advance(GRACE.plusSeconds(1));
        leaderSyncing.set(true);
        // Janela de graça já passou, mas a réplica do líder ainda está em catch-up de um mandato
        // anterior: o miss pode ser só atraso da réplica, não pode virar "não existe" definitivo.

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-inexistente")), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("node-self", response.leaderNodeId());
    }

    @Test
    void catalogLookupComLiderSincronizandoSemMissRespondeOk() {
        leaderView.leader = true;
        SeriesPlacement placement = SeriesPlacement.active("node-a", clock.millis());
        catalog.putPlacement("series-1", placement);
        handler.onLeaderChanged(NodeId.of("self"));
        clock.advance(GRACE.plusSeconds(1));
        leaderSyncing.set(true);
        // Todas as chaves encontradas: não há miss a desconfiar, a sincronização não importa.

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-1")), NodeId.of("client"));

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(Map.of("series-1", placement), response.found());
    }

    @Test
    void catalogLookupNaoCriaPlacement() {
        leaderView.leader = true;

        handler.handle(Commands.CATALOG_LOOKUP, new CatalogLookupRequest(List.of("series-ausente")),
                NodeId.of("client"));

        assertEquals(Optional.empty(), catalog.placementStrong("series-ausente"),
                "a consulta de uma chave ausente não deveria criar placement");
    }

    @Test
    void catalogLookupAcimaDoLimiteRespondeErro() {
        leaderView.leader = true;
        List<String> tooManyKeys = IntStream.rangeClosed(1, CatalogLookupRequest.MAX_KEYS + 1)
                .mapToObj(i -> "series-" + i)
                .collect(Collectors.toList());

        CatalogLookupResponse response = (CatalogLookupResponse) handler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(tooManyKeys), NodeId.of("client"));

        assertEquals(SeriesStatus.ERROR, response.status());
    }

    @Test
    void catalogLookupPerdeLiderancaNoMeioDoLoteRespondeNotLeaderSemConsultarAsDemais() {
        leaderView.leader = true;
        LeaderFlippingCatalogFake fakeCatalog = new LeaderFlippingCatalogFake(leaderView);
        PlacementRequestHandler flippingHandler = new PlacementRequestHandler(cluster.node(0).transport(),
                fakeCatalog, leaderView, leaderSyncing::get, new LeastLoadedPlacementPolicy(), INTERVAL, GRACE, clock);

        CatalogLookupResponse response = (CatalogLookupResponse) flippingHandler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-1", "series-2", "series-3")), NodeId.of("client"));

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        // A liderança cai durante a consulta de series-1 -> a checagem a cada chave deve pegar isso
        // antes de sequer tentar series-2/series-3 (cada uma seria um RPC ao novo líder se não parasse
        // cedo, que a rechecagem final descartaria de qualquer jeito).
        assertEquals(1, fakeCatalog.placementStrongCalls,
                "deveria parar assim que perder a liderança, sem consultar as chaves restantes");
    }

    @Test
    void catalogLookupComFalhaAoConsultarPropagaExcecaoNuncaViraOk() {
        leaderView.leader = true;
        PlacementRequestHandler throwingHandler = new PlacementRequestHandler(cluster.node(0).transport(),
                new ThrowingOnLookupCatalogFake(), leaderView, leaderSyncing::get, new LeastLoadedPlacementPolicy(),
                INTERVAL, GRACE, clock);

        // Falha ao consultar o catálogo (ex.: DistributedMap indisponível) nunca pode virar uma
        // resposta OK com a chave simplesmente ausente — o cliente trataria isso como "não existe".
        assertThrows(RuntimeException.class, () -> throwingHandler.handle(Commands.CATALOG_LOOKUP,
                new CatalogLookupRequest(List.of("series-1")), NodeId.of("client")));
    }

    @Test
    void catalogLookupComMissDuranteARecontagemDaPosseRespondeNotLeader() throws Exception {
        // O coordenador troca o líder ANTES de chamar os listeners: nessa janela isLeader() já é true,
        // mas onLeaderChanged ainda está recontando as pendências a partir do catálogo (cópia inteira,
        // lenta com centenas de milhares de séries). Um miss aqui não pode virar "não existe".
        leaderView.leader = true;
        leaderView.leaderId = Optional.of("node-self");
        BlockingRecountCatalogFake fakeCatalog = new BlockingRecountCatalogFake();
        PlacementRequestHandler slowHandler = new PlacementRequestHandler(cluster.node(0).transport(),
                fakeCatalog, leaderView, leaderSyncing::get, new LeastLoadedPlacementPolicy(), INTERVAL, GRACE, clock);
        Thread takeover = new Thread(() -> slowHandler.onLeaderChanged(NodeId.of("self")));
        takeover.start();
        try {
            assertTrue(fakeCatalog.recountStarted.await(5, TimeUnit.SECONDS),
                    "onLeaderChanged deveria ter começado a recontagem");

            CatalogLookupResponse response = (CatalogLookupResponse) slowHandler.handle(Commands.CATALOG_LOOKUP,
                    new CatalogLookupRequest(List.of("series-inexistente")), NodeId.of("client"));

            assertEquals(SeriesStatus.NOT_LEADER, response.status(),
                    "miss com a posse ainda em andamento não pode ser respondido como OK sem a chave");
            assertEquals("node-self", response.leaderNodeId());
        } finally {
            fakeCatalog.releaseRecount.countDown();
            takeover.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    /**
     * {@link CatalogView} fake cujo {@code placementsLocal} (usado pela recontagem de pendências em
     * {@code onLeaderChanged}) sinaliza que começou e bloqueia até ser liberado — simula a cópia lenta
     * do catálogo inteiro logo após assumir a liderança.
     */
    private static final class BlockingRecountCatalogFake implements CatalogView {
        private final CountDownLatch recountStarted = new CountDownLatch(1);
        private final CountDownLatch releaseRecount = new CountDownLatch(1);

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.empty();
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.empty();
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            throw new UnsupportedOperationException("não usado neste teste");
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.of();
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            recountStarted.countDown();
            try {
                if (!releaseRecount.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("recontagem não foi liberada a tempo");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrompido esperando a liberação da recontagem", e);
            }
            return Map.of();
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException("não usado neste teste");
        }
    }

    /**
     * {@link CatalogView} fake cujo {@code placementStrong} derruba {@link LeaderViewFake#leader} para
     * {@code false} assim que é chamado a primeira vez — simula a liderança caindo no meio de um lote
     * de {@code CATALOG_LOOKUP}.
     */
    private static final class LeaderFlippingCatalogFake implements CatalogView {
        private final LeaderViewFake leaderView;
        private int placementStrongCalls;

        LeaderFlippingCatalogFake(LeaderViewFake leaderView) {
            this.leaderView = leaderView;
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            placementStrongCalls++;
            leaderView.leader = false;
            return Optional.empty();
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.empty();
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            throw new UnsupportedOperationException("não usado neste teste");
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.of();
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.of();
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException("não usado neste teste");
        }
    }

    /** {@link CatalogView} fake cujo {@code placementStrong} sempre lança (simula falha de transporte/RPC). */
    private static final class ThrowingOnLookupCatalogFake implements CatalogView {

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            throw new IllegalStateException("simulando falha ao consultar o catálogo");
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.empty();
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            throw new UnsupportedOperationException("não usado neste teste");
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.of();
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.of();
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException("não usado neste teste");
        }
    }

    /** {@link CatalogView} fake cujo {@code putPlacement} sempre lança (simula {@code LeaderSyncingException}). */
    private static final class ThrowingCatalogFake implements CatalogView {
        private final Map<String, StorageNodeStatus> nodes = new LinkedHashMap<>();
        private final Map<String, SeriesPlacement> placements = new LinkedHashMap<>();
        private boolean putAttempted;

        void putNode(StorageNodeStatus status) {
            nodes.put(status.nodeId(), status);
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.ofNullable(placements.get(seriesKey));
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.ofNullable(nodes.get(nodeId));
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            nodes.put(status.nodeId(), status);
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.copyOf(nodes.values());
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.copyOf(placements);
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            putAttempted = true;
            throw new IllegalStateException("simulando LeaderSyncingException do core");
        }
    }

    /** {@link PlacementRequestHandler.LeaderView} fake, sem {@code ClusterCoordinator}/{@code Transport} reais. */
    private static final class LeaderViewFake implements PlacementRequestHandler.LeaderView {
        private boolean leader;
        private Optional<String> leaderId = Optional.empty();
        private final Set<String> reachable = ConcurrentHashMap.newKeySet();

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public Optional<String> leaderId() {
            return leaderId;
        }

        @Override
        public Set<String> reachableNodeIds() {
            return Set.copyOf(reachable);
        }
    }

    /** {@link Clock} determinístico para os testes de {@code pending}. */
    private static final class MutableClock extends Clock {
        private Instant instant;

        MutableClock(Instant start) {
            this.instant = start;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException("não usado nos testes");
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
