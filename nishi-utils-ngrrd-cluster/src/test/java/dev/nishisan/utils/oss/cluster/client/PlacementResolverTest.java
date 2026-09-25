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
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        catalog.putNodeStatus(CapabilityFixtures.status(LEADER.value(), StorageCapabilities.ALL));
        CatalogLookupClient lookupClient = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000,
                NodeCapabilities.from(catalog));
        resolver = new PlacementResolver(catalog, rpc, retry, Clock.systemUTC(), lookupClient);
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

    @Test
    void leaderRedirectAndTransportFailureUseOnlyTheCallersRemainingBudget() {
        long[] now = {0};
        Clock clock = new Clock() {
            public ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(ZoneId zone) { return this; }
            public Instant instant() { return Instant.ofEpochMilli(now[0]); }
            public long millis() { return now[0]; }
        };
        List<Duration> budgets = new ArrayList<>();
        ClusterRpc boundedRpc = new ClusterRpc() {
            public <R> R call(NodeId target, String command, Object body, Class<R> type) {
                throw new AssertionError("placement RPC must be bounded");
            }
            public <R> R call(NodeId target, String command, Object body, Class<R> type, Duration timeout) {
                budgets.add(timeout);
                if (budgets.size() == 1) {
                    now[0] += 70;
                    return type.cast(new PlaceResponse(SeriesStatus.NOT_LEADER, null, null, "leader-2"));
                }
                assertEquals(NodeId.of("leader-2"), target);
                now[0] += timeout.toMillis();
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "slow leader");
            }
            public NodeId localId() { return CLIENT; }
            public Optional<NodeId> leaderId() { return Optional.of(LEADER); }
        };
        RetryPolicy boundedRetry = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMillis(1), Duration.ofMillis(2));
        var bounded = new PlacementResolver(catalog, boundedRpc, boundedRetry, clock,
                new CatalogLookupClient(boundedRpc, boundedRetry, clock, 2000, NodeCapabilities.from(catalog)));

        assertEquals(ErrorCode.TIMEOUT, assertThrows(NgrrdClusterException.class,
                () -> bounded.resolve("uncached", "hash", null, Duration.ofMillis(100))).code());

        assertEquals(List.of(Duration.ofMillis(100), Duration.ofMillis(30)), budgets);
        assertEquals(100, now[0]);
    }

    @Test
    @Timeout(2)
    void waitingForLeaderDoesNotRestartTheFullRetryTimeout() {
        rpc.leader(null);
        long start = System.nanoTime();
        NgrrdClusterException failure = assertThrows(NgrrdClusterException.class,
                () -> resolver.resolve("uncached", "hash", null, Duration.ofMillis(30)));
        assertTrue(failure.code() == ErrorCode.NO_LEADER || failure.code() == ErrorCode.TIMEOUT);
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 500);
        assertTrue(rpc.calls().isEmpty());
    }

    @Test
    void resolveExistingComPlacementLocalAtivoNaoFazRpc() {
        SeriesPlacement active = SeriesPlacement.active("storage-a", 1_000L);
        catalog.putPlacement("series-1", active);

        SeriesPlacement resolved = resolver.resolveExisting("series-1", Duration.ofSeconds(1));

        assertEquals(active, resolved);
        assertEquals(0, rpc.calls().size(), "placement ACTIVE local não deveria consultar o líder");
    }

    @Test
    void resolveExistingComMissConsultaLiderENuncaFazPlace() {
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            return CatalogLookupResponse.ok(Map.of("series-1", placed));
        });

        SeriesPlacement resolved = resolver.resolveExisting("series-1", Duration.ofSeconds(1));

        assertEquals(placed, resolved);
        assertTrue(rpc.calls().stream().noneMatch(c -> c.command().equals(Commands.PLACE)),
                "resolveExisting nunca deveria disparar PLACE");
    }

    @Test
    void resolveExistingAusenteNoLiderLancaSeriesNotFound() {
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(Map.of()));

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                () -> resolver.resolveExisting("series-1", Duration.ofSeconds(1)));

        assertEquals("series-1", ex.seriesKey());
        assertEquals(SeriesNotFoundException.Reason.NOT_PLACED, ex.reason());
    }

    @Test
    void handleSomenteLeituraComWrongOwnerSemDonoTerminaEmNotPlaced() {
        // Ponta a ponta no cliente: o dono responde WRONG_OWNER sem dono (o líder não tem placement), o
        // handle re-resolve por resolveExisting, o líder confirma a ausência e a exceção sai NOT_PLACED.
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            return CatalogLookupResponse.ok(Map.of("series-1", placed));
        });
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.OPEN, cmd);
            return new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, null, null);
        });
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            return CatalogLookupResponse.ok(Map.of());
        });
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMillis(1), Duration.ofMillis(5));
        RemoteSeriesHandle handle = new RemoteSeriesHandle("series-1", "yaml: fake", "hash-1", Map.of(),
                Ngrrd.OpenOptions.defaults().withCreateIfMissing(false), resolver, rpc, new UnusedWriteBuffer(),
                retry, Duration.ofSeconds(1), Duration.ofSeconds(1), Clock.systemUTC(), (key, h) -> { },
                CapabilityFixtures.advertisingAll());

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class, handle::open);

        assertEquals(SeriesNotFoundException.Reason.NOT_PLACED, ex.reason());
        assertTrue(rpc.calls().stream().noneMatch(c -> c.command().equals(Commands.PLACE)),
                "handle somente leitura nunca posiciona");
    }

    @Test
    void handleSomenteLeituraComReplicaLocalAtrasadaEWrongOwnerSemDonoTerminaRapidoEmNotPlaced() {
        // A réplica local ainda diz ACTIVE(storage-a), mas o líder já não tem placement: o dono responde
        // WRONG_OWNER sem dono e o handle precisa confirmar direto com o líder, sem voltar à réplica.
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L));
        rpc.respondDefault((cmd, body) -> switch (cmd) {
            case Commands.OPEN -> new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, null, null);
            case Commands.CATALOG_LOOKUP -> CatalogLookupResponse.ok(Map.of());
            default -> throw new AssertionError("comando inesperado: " + cmd);
        });
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(1), Duration.ofMillis(5));
        RemoteSeriesHandle handle = new RemoteSeriesHandle("series-1", "yaml: fake", "hash-1", Map.of(),
                Ngrrd.OpenOptions.defaults().withCreateIfMissing(false), resolver, rpc, new UnusedWriteBuffer(),
                retry, Duration.ofSeconds(1), Duration.ofSeconds(1), Clock.systemUTC(), (key, h) -> { },
                CapabilityFixtures.advertisingAll());
        long start = System.nanoTime();

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class, handle::open);

        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        assertEquals(SeriesNotFoundException.Reason.NOT_PLACED, ex.reason());
        assertTrue(elapsedMs < 2_000, "deveria terminar sem esperar o retryTimeout; levou " + elapsedMs + " ms");
        assertEquals(List.of(Commands.OPEN, Commands.CATALOG_LOOKUP),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList());
    }

    @Test
    void handleSomenteLeituraComReplicaLocalAtrasadaEWrongOwnerSemDonoSegueComODonoDoLider() {
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L));
        SeriesPlacement atLeader = SeriesPlacement.active("storage-b", 2_000L);
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.OPEN, cmd);
            return new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, null, null);
        });
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            return CatalogLookupResponse.ok(Map.of("series-1", atLeader));
        });
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.OPEN, cmd);
            return new SeriesStatusResponse(SeriesStatus.OK, "storage-b", null, Boolean.TRUE);
        });
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(1), Duration.ofMillis(5));
        RemoteSeriesHandle handle = new RemoteSeriesHandle("series-1", "yaml: fake", "hash-1", Map.of(),
                Ngrrd.OpenOptions.defaults().withCreateIfMissing(false), resolver, rpc, new UnusedWriteBuffer(),
                retry, Duration.ofSeconds(1), Duration.ofSeconds(1), Clock.systemUTC(), (key, h) -> { },
                CapabilityFixtures.advertisingAll());

        handle.open();

        List<RecordingClusterRpc.Recorded> calls = rpc.calls();
        assertEquals(3, calls.size());
        assertEquals(NodeId.of("storage-a"), calls.get(0).target());
        assertEquals(NodeId.of("storage-b"), calls.get(2).target(), "segue com o dono informado pelo líder");
    }

    @Test
    void resolveExistingAtLeaderIgnoraReplicaLocalEOverride() {
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L));
        resolver.noteOwner("series-1", "storage-c");
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(Map.of()));

        SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class,
                () -> resolver.resolveExistingAtLeader("series-1", Duration.ofSeconds(1)));

        assertEquals(SeriesNotFoundException.Reason.NOT_PLACED, ex.reason());
        assertEquals(List.of(Commands.CATALOG_LOOKUP),
                rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList());
    }

    @Test
    void resolveExistingMigrandoConsultaLider() {
        SeriesPlacement before = SeriesPlacement.active("storage-a", 1_000L);
        SeriesPlacement migrating = SeriesPlacement.migrating(before, "storage-b", "mig-1", 2_000L);
        catalog.putPlacement("series-1", migrating);
        SeriesPlacement completed = SeriesPlacement.active("storage-b", 3_000L);
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            return CatalogLookupResponse.ok(Map.of("series-1", completed));
        });

        SeriesPlacement resolved = resolver.resolveExisting("series-1", Duration.ofSeconds(1));
        // Placement ACTIVE devolvido pelo líder deveria ter sido gravado como override — uma segunda
        // chamada não repete a consulta ao líder.
        SeriesPlacement resolvedAgain = resolver.resolveExisting("series-1", Duration.ofSeconds(1));

        assertEquals(completed, resolved);
        assertEquals(completed, resolvedAgain);
        assertEquals(1, rpc.calls().size(), "placement ACTIVE devolvido pelo líder deveria virar override");
    }

    @Test
    void resolveExistingComPlacementMigrandoNoLiderNaoGravaOverride() {
        SeriesPlacement before = SeriesPlacement.active("storage-a", 1_000L);
        SeriesPlacement migratingAtLeader = SeriesPlacement.migrating(before, "storage-b", "mig-1", 2_000L);
        rpc.respondDefault((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            return CatalogLookupResponse.ok(Map.of("series-1", migratingAtLeader));
        });

        SeriesPlacement first = resolver.resolveExisting("series-1", Duration.ofSeconds(1));
        // Sem override gravado (placement não ACTIVE), a segunda chamada consulta o líder de novo.
        SeriesPlacement second = resolver.resolveExisting("series-1", Duration.ofSeconds(1));

        assertEquals(migratingAtLeader, first);
        assertEquals(migratingAtLeader, second);
        assertEquals(2, rpc.calls().size(), "placement MIGRATING não deveria ser cacheado como override");
    }

    @Test
    @Timeout(2)
    void resolveExistingSemLiderLancaNgrrdClusterExceptionENaoSeriesNotFound() {
        rpc.leader(null);

        assertThrows(NgrrdClusterException.class,
                () -> resolver.resolveExisting("series-1", Duration.ofMillis(30)));
        assertTrue(rpc.calls().isEmpty(), "sem líder eleito, nenhuma chamada deveria ter sido feita");
    }

    /** {@link WriteBuffer} que falha se for usado: um handle somente leitura nunca escreve. */
    private static final class UnusedWriteBuffer implements WriteBuffer {
        @Override
        public void enqueue(String ownerNodeId, SeriesWrite write) {
            throw new AssertionError("handle somente leitura não deveria enfileirar escritas");
        }

        @Override
        public void flushNodeSync(String ownerNodeId) {
            throw new AssertionError("handle somente leitura não deveria drenar o buffer");
        }

        @Override
        public void flushNodeSync(String ownerNodeId, Duration maxWait) {
            throw new AssertionError("handle somente leitura não deveria drenar o buffer");
        }
    }
}
