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
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link LocalReconciler#reconcileOnce()} com um {@link BlobVolume} real (sob {@link TempDir}) e
 * fakes de {@link CatalogView}/{@link ClusterRpc} — sem {@code NGridNode} nem rede real.
 *
 * <p>Casos ALTO-1/ALTO-2/MÉDIO-7 do Refuter (M4, rodada 2): handle aberto pula; leitura forte antes de
 * apagar (diverge da local → não apaga; dono não confirma posse via {@code SERIES_EXISTS} → não apaga);
 * primeiro ciclo nunca apaga; self não confirmado {@code ACTIVE} não adota e a chave fica isenta de
 * deleção para sempre nesta instância.</p>
 */
class LocalReconcilerTest {

    private static final String SELF = "storage-self";
    private static final String VOLUME_NAME = "ngrrd-test";
    private static final Duration ORPHAN_GRACE = Duration.ofMinutes(5);

    private BlobVolumeRegistry registry;
    private BlobVolume volume;
    private SeriesHandleRegistry handleRegistry;
    private CatalogViewFake catalog;
    private RpcFake rpc;
    private String yaml;

    @BeforeEach
    void setUp(@TempDir Path base) throws Exception {
        // Sem overrides de shardCount/segmentBytes: um dos testes abre um handle real com a definição
        // completa de iface-traffic-blob.yaml (RRAs grandes o bastante para não caber num segmento
        // pequeno) — mesmos defaults usados por StorageRequestHandlerTest.
        registry = NgrrdBlob.registry()
                .basePath(base)
                .volume(VOLUME_NAME)
                .build();
        volume = registry.require(VOLUME_NAME);
        handleRegistry = new SeriesHandleRegistry(volume, VOLUME_NAME, Duration.ofMinutes(15), 10_000,
                Clock.systemUTC());
        catalog = new CatalogViewFake();
        rpc = new RpcFake();
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        // Default: self ACTIVE segundo a leitura forte — testes de ALTO-2 sobrescrevem quando precisam.
        catalog.nodeStatuses.put(SELF, new StorageNodeStatus(SELF, NodeState.ACTIVE, 0, 0, 0, 1_000L));
    }

    @AfterEach
    void tearDown() {
        handleRegistry.close();
        registry.close();
    }

    private LocalReconciler reconciler(Clock clock) {
        return new LocalReconciler(volume, catalog, rpc, handleRegistry, SELF, "series", ORPHAN_GRACE,
                Duration.ofMinutes(10), () -> true, clock);
    }

    private void putSeriesObject(String seriesKey) {
        volume.storage().put(objectKey(seriesKey), "conteudo-fake".getBytes(StandardCharsets.UTF_8));
    }

    private static String objectKey(String seriesKey) {
        return "series/" + seriesKey + ".ngrr";
    }

    // ---------------------------------------------------------------- adoção

    @Test
    void serieNoVolumeAusenteDoCatalogoEAdotadaViaPlaceQuandoSelfEstaActive() {
        putSeriesObject("s1");
        rpc.placementFor = key -> SeriesPlacement.active(SELF, 1_000L);

        LocalReconciler.ReconcileReport report = reconciler(Clock.systemUTC()).reconcileOnce();

        assertEquals(1, report.adopted());
        assertEquals(0, report.unplaced());
        assertEquals(1, rpc.placeRequests.size());
        assertEquals("s1", rpc.placeRequests.get(0).seriesKey());
        assertEquals(SELF, rpc.placeRequests.get(0).preferredOwnerNodeId());
    }

    @Test
    void adocaoRecusadaPeloLiderNaoApagaOArquivoLocalEEntraNoExemptSet() {
        putSeriesObject("s1");
        rpc.placementFor = key -> SeriesPlacement.active("storage-other", 1_000L);

        LocalReconciler reconciler = reconciler(Clock.systemUTC());
        LocalReconciler.ReconcileReport first = reconciler.reconcileOnce();
        assertEquals(0, first.adopted());
        assertEquals(1, first.unplaced());
        assertTrue(volume.storage().exists(objectKey("s1")));

        // Segundo ciclo: agora o catálogo (visão em lote) mostra a série ACTIVE(other) há muito tempo —
        // um candidato óbvio a órfã — mas a chave está isenta (foi recusada na adoção), nunca apaga.
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null, 1_000L, 1_000L));
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();
        assertEquals(0, second.orphansDeleted());
        assertTrue(volume.storage().exists(objectKey("s1")), "chave isenta nunca deveria ser apagada");
    }

    // ---------------------------------------------------------------- ALTO-2: self fora de ACTIVE

    @Test
    void selfNaoActiveNaoAdotaEMarcaUnplacedSemChamarPlace() {
        putSeriesObject("s1");
        catalog.nodeStatuses.put(SELF, new StorageNodeStatus(SELF, NodeState.DRAINING, 0, 0, 0, 1_000L));

        LocalReconciler.ReconcileReport report = reconciler(Clock.systemUTC()).reconcileOnce();

        assertEquals(0, report.adopted());
        assertEquals(1, report.unplaced());
        assertTrue(rpc.placeRequests.isEmpty(), "self DRAINING não deveria nem tentar PLACE");
    }

    @Test
    void semLiderNaLeituraFortePropriaNaoAdota() {
        putSeriesObject("s1");
        catalog.nodeStatusStrongThrows = true;

        LocalReconciler.ReconcileReport report = reconciler(Clock.systemUTC()).reconcileOnce();

        assertEquals(0, report.adopted());
        assertEquals(1, report.unplaced());
        assertTrue(rpc.placeRequests.isEmpty());
    }

    @Test
    void doisCiclosComSelfDrainingNadaAdotadoENadaApagadoNoSegundoCiclo() {
        putSeriesObject("s1");
        catalog.nodeStatuses.put(SELF, new StorageNodeStatus(SELF, NodeState.DRAINING, 0, 0, 0, 1_000L));
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(Clock.systemUTC());
        LocalReconciler.ReconcileReport first = reconciler.reconcileOnce();
        assertEquals(0, first.adopted());
        assertEquals(1, first.unplaced());

        // Segundo ciclo: o catálogo passa a mostrar a série ACTIVE noutro dono, há muito tempo, e o
        // dono confirmaria (SERIES_EXISTS=true) — sem a isenção, seria apagada. Continua DRAINING.
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null, 1_000L, 1_000L));
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.adopted());
        assertEquals(0, second.orphansDeleted());
        assertTrue(volume.storage().exists(objectKey("s1")));
    }

    @Test
    void noNovoSemStatusAindaContaComoElegivelEAdota() {
        // MÉDIO-A do Refuter: Optional.empty() (nó novo, ainda sem entrada em ngrrd.nodes) conta como
        // ACTIVE — coerente com o default do NodeStatusReporter. Sem isso, um storage node recém-subido
        // nunca conseguiria adotar as séries do próprio volume no primeiro reconcileOnce().
        catalog.nodeStatuses.remove(SELF);
        putSeriesObject("s1");
        rpc.placementFor = key -> SeriesPlacement.active(SELF, 1_000L);

        LocalReconciler.ReconcileReport report = reconciler(Clock.systemUTC()).reconcileOnce();

        assertEquals(1, report.adopted());
        assertEquals(0, report.unplaced());
        assertEquals(1, rpc.placeRequests.size());
    }

    @Test
    void adocaoBemSucedidaRemoveDoExemptEReabilitaOGcDeOrfaParaMigracaoLegitimaFutura() {
        // MÉDIO-A do Refuter: (1) ciclo 1 — self DRAINING, chave ausente do catálogo -> unplaced, entra
        // no exempt; (2) self volta a ACTIVE e a chave é finalmente adotada de verdade -> sai do exempt;
        // (3) uma migração LEGÍTIMA move a série para outro dono depois -- agora o GC de órfã tem de
        // conseguir apagar a cópia local, já que a chave não está mais isenta.
        putSeriesObject("s1");
        catalog.nodeStatuses.put(SELF, new StorageNodeStatus(SELF, NodeState.DRAINING, 0, 0, 0, 1_000L));
        LocalReconciler reconciler = reconciler(Clock.systemUTC());
        LocalReconciler.ReconcileReport first = reconciler.reconcileOnce();
        assertEquals(0, first.adopted());
        assertEquals(1, first.unplaced());

        catalog.nodeStatuses.put(SELF, new StorageNodeStatus(SELF, NodeState.ACTIVE, 0, 0, 0, 2_000L));
        rpc.placementFor = key -> SeriesPlacement.active(SELF, 2_000L);
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();
        assertEquals(1, second.adopted(), "self voltou a ACTIVE — a adoção deveria ter sucesso agora");

        // Simula a migração legítima: o catálogo agora mostra a série ACTIVE noutro dono, há muito
        // tempo, e esse dono confirma via SERIES_EXISTS.
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null, 3_000L, 3_000L));
        rpc.existsFor = key -> true;
        LocalReconciler.ReconcileReport third = reconciler.reconcileOnce();

        assertEquals(1, third.orphansDeleted(), "chave adotada com sucesso deveria voltar a ser elegível ao GC de órfã");
        assertFalse(volume.storage().exists(objectKey("s1")));
    }

    // ---------------------------------------------------------------- ALTO-1: salvaguardas de deleção

    @Test
    void handleAbertoPulaSemAdotarNemApagar() {
        // A própria abertura já cria o objeto de série de verdade no volume (não um placeholder de
        // bytes arbitrários) — abrir com Ngrrd.open é o único jeito de deixar um handle "aberto" de
        // fato (SeriesHandleRegistry#isOpen), então não chama putSeriesObject aqui.
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null, 1_000L, 1_000L));
        // Marca a série como aberta localmente (ex.: reaberta por auto-cura logo antes deste ciclo).
        handleRegistry.open("s1", yaml, Ngrrd.OpenOptions.defaults());
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(Clock.systemUTC());
        reconciler.reconcileOnce(); // primeiro ciclo — não apagaria de qualquer forma (ALTO-1 d)
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.orphansDeleted(), "handle aberto deveria pular o ramo de deleção");
        assertEquals(0, second.adopted());
        assertTrue(volume.storage().exists(objectKey("s1")));
    }

    @Test
    void primeiroCicloNuncaApagaMesmoComTudoConfirmado() {
        putSeriesObject("s1");
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null,
                now.minusSeconds(600).toEpochMilli(), now.minusSeconds(600).toEpochMilli()));
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(clock);
        LocalReconciler.ReconcileReport first = reconciler.reconcileOnce();
        assertEquals(0, first.orphansDeleted(), "primeiro ciclo nunca apaga, mesmo com tudo confirmado");
        assertTrue(volume.storage().exists(objectKey("s1")));

        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();
        assertEquals(1, second.orphansDeleted(), "segundo ciclo, com tudo confirmado, apaga");
        assertFalse(volume.storage().exists(objectKey("s1")));
    }

    @Test
    void strongDivergindoDaVisaoEmLoteNaoApaga() {
        putSeriesObject("s1");
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        // Visão em lote (início do ciclo): ACTIVE noutro dono, há muito tempo -> pareceria uma órfã óbvia.
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null,
                now.minusSeconds(600).toEpochMilli(), now.minusSeconds(600).toEpochMilli()));
        // Mas a releitura FORTE, feita bem antes de apagar, já mostra a série de volta em self.
        catalog.strongOverrides.put("s1", SeriesPlacement.active(SELF, now.toEpochMilli()));
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(clock);
        reconciler.reconcileOnce();
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.orphansDeleted(), "a leitura forte diverge da visão em lote — não deveria apagar");
        assertTrue(volume.storage().exists(objectKey("s1")));
    }

    @Test
    void placementStrongVazioNaoApaga() {
        putSeriesObject("s1");
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null,
                now.minusSeconds(600).toEpochMilli(), now.minusSeconds(600).toEpochMilli()));
        catalog.strongOverrides.put("s1", null); // placementStrong devolve Optional.empty()
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(clock);
        reconciler.reconcileOnce();
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.orphansDeleted());
        assertTrue(volume.storage().exists(objectKey("s1")));
    }

    @Test
    void donoNaoConfirmaPosseViaSeriesExistsNaoApaga() {
        putSeriesObject("s1");
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null,
                now.minusSeconds(600).toEpochMilli(), now.minusSeconds(600).toEpochMilli()));
        rpc.existsFor = key -> false; // dono forte diz que NÃO tem a cópia

        LocalReconciler reconciler = reconciler(clock);
        reconciler.reconcileOnce();
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.orphansDeleted());
        assertTrue(volume.storage().exists(objectKey("s1")));
        assertEquals(1, rpc.existsRequests.size());
    }

    @Test
    void falhaDeTransporteNoSeriesExistsNaoApaga() {
        putSeriesObject("s1");
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null,
                now.minusSeconds(600).toEpochMilli(), now.minusSeconds(600).toEpochMilli()));
        rpc.seriesExistsThrows = true;

        LocalReconciler reconciler = reconciler(clock);
        reconciler.reconcileOnce();
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.orphansDeleted(), "timeout/indisponibilidade no SERIES_EXISTS nunca autoriza apagar");
        assertTrue(volume.storage().exists(objectKey("s1")));
    }

    @Test
    void orfaDentroDoGraceNaoEApagadaMesmoNoSegundoCiclo() {
        putSeriesObject("s1");
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        catalog.placements.put("s1", new SeriesPlacement("storage-other", null,
                PlacementState.ACTIVE, null,
                now.minusSeconds(60).toEpochMilli(), now.minusSeconds(60).toEpochMilli()));
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(clock);
        reconciler.reconcileOnce();
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.orphansDeleted());
        assertTrue(volume.storage().exists(objectKey("s1")));
    }

    // ---------------------------------------------------------------- demais ramos (retidos do M4 r1)

    @Test
    void serieAtivaNoSelfMasAusenteDoVolumeEReportadaComoMissing() {
        catalog.placements.put("s-missing", SeriesPlacement.active(SELF, 1_000L));

        LocalReconciler.ReconcileReport report = reconciler(Clock.systemUTC()).reconcileOnce();

        assertEquals(1, report.missing());
        assertEquals(0, report.adopted());
        assertEquals(0, report.orphansDeleted());
    }

    @Test
    void serieMigratingEIgnorada() {
        putSeriesObject("s1");
        SeriesPlacement active = SeriesPlacement.active(SELF, 1_000L);
        catalog.placements.put("s1", SeriesPlacement.migrating(active, "storage-other", "migration-1", 2_000L));
        rpc.existsFor = key -> true;

        LocalReconciler reconciler = reconciler(Clock.systemUTC());
        reconciler.reconcileOnce();
        LocalReconciler.ReconcileReport second = reconciler.reconcileOnce();

        assertEquals(0, second.adopted());
        assertEquals(0, second.orphansDeleted());
        assertEquals(0, second.unplaced());
        assertEquals(0, second.missing());
        assertTrue(rpc.placeRequests.isEmpty(), "não deveria tentar PLACE numa série já em migração");
        assertTrue(volume.storage().exists(objectKey("s1")), "não deveria apagar uma série em migração");
    }

    @Test
    void serieAtivaEmSelfNaoGeraNenhumaAcao() {
        putSeriesObject("s1");
        catalog.placements.put("s1", SeriesPlacement.active(SELF, 1_000L));

        LocalReconciler.ReconcileReport report = reconciler(Clock.systemUTC()).reconcileOnce();

        assertEquals(0, report.adopted());
        assertEquals(0, report.orphansDeleted());
        assertEquals(0, report.unplaced());
        assertEquals(0, report.missing());
        assertTrue(rpc.placeRequests.isEmpty());
    }

    // ---------------------------------------------------------------- MÉDIO-C: close() interrompe awaitCatalogStable

    @Test
    void closeDuranteAwaitCatalogStableTerminaRapidoSemChamarRpcDeNovo() throws InterruptedException {
        // Sem líder algum: awaitCatalogStable nunca converge (leaderPresent fica sempre false) e fica
        // preso no laço de poll até close() ou até STABLE_AWAIT_TIMEOUT (30s) — o que se quer provar é
        // que close() interrompe isso bem antes de 1s, sem tocar o rpc de novo depois.
        rpc.leaderPresent = false;
        LocalReconciler reconciler = reconciler(Clock.systemUTC());
        reconciler.start();
        Thread.sleep(100L); // garante que já entrou de fato no laço de awaitCatalogStable

        long startedAt = System.currentTimeMillis();
        reconciler.close();
        long elapsedMs = System.currentTimeMillis() - startedAt;

        assertTrue(elapsedMs < 1_000L,
                "close() deveria terminar bem antes de 1s mesmo preso em awaitCatalogStable: " + elapsedMs + "ms");

        int callsAtClose = rpc.leaderIdCalls.get();
        Thread.sleep(300L); // tempo de sobra para uma chamada indevida acontecer, se o laço não tivesse parado de fato
        assertEquals(callsAtClose, rpc.leaderIdCalls.get(), "não deveria chamar rpc.leaderId() de novo após close()");
    }

    // ---------------------------------------------------------------- fakes

    /** {@link CatalogView} fake: catálogo em memória, sem {@code DistributedMap}/{@code NGridNode} reais. */
    private static final class CatalogViewFake implements CatalogView {
        final Map<String, SeriesPlacement> placements = new LinkedHashMap<>();
        /** Quando contém a chave (mesmo com valor {@code null}), {@link #placementStrong} usa isto em vez de {@link #placements}. */
        final Map<String, SeriesPlacement> strongOverrides = new LinkedHashMap<>();
        final Map<String, StorageNodeStatus> nodeStatuses = new LinkedHashMap<>();
        boolean nodeStatusStrongThrows;

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            if (strongOverrides.containsKey(seriesKey)) {
                return Optional.ofNullable(strongOverrides.get(seriesKey));
            }
            return Optional.ofNullable(placements.get(seriesKey));
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            if (nodeStatusStrongThrows) {
                throw new IllegalStateException("sem líder eleito (simulado)");
            }
            return Optional.ofNullable(nodeStatuses.get(nodeId));
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.copyOf(nodeStatuses.values());
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.copyOf(placements);
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException(
                    "LocalReconciler não deveria escrever o catálogo diretamente (só via PLACE ao líder)");
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            throw new UnsupportedOperationException("LocalReconciler não deveria publicar status de nó");
        }
    }

    /** {@link ClusterRpc} fake: entende {@code ngrrd.place} e {@code ngrrd.series.exists}. */
    private static final class RpcFake implements ClusterRpc {
        Function<String, SeriesPlacement> placementFor;
        Function<String, Boolean> existsFor;
        boolean seriesExistsThrows;
        /** {@code false} simula uma malha sem líder algum — {@link #leaderId()} nunca resolve. */
        volatile boolean leaderPresent = true;
        final AtomicInteger leaderIdCalls = new AtomicInteger();
        final List<PlaceRequest> placeRequests = new CopyOnWriteArrayList<>();
        final List<SeriesExistsRequest> existsRequests = new CopyOnWriteArrayList<>();

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            if (Commands.PLACE.equals(command)) {
                PlaceRequest request = (PlaceRequest) body;
                placeRequests.add(request);
                SeriesPlacement placement = placementFor.apply(request.seriesKey());
                return (R) new PlaceResponse(SeriesStatus.OK, placement, null, null);
            }
            if (Commands.SERIES_EXISTS.equals(command)) {
                SeriesExistsRequest request = (SeriesExistsRequest) body;
                existsRequests.add(request);
                if (seriesExistsThrows) {
                    throw new NgrrdClusterException(ErrorCode.TIMEOUT, "SERIES_EXISTS simulado com timeout");
                }
                boolean exists = existsFor != null && Boolean.TRUE.equals(existsFor.apply(request.seriesKey()));
                return (R) new SeriesExistsResponse(exists, exists ? 123L : 0L);
            }
            throw new IllegalStateException("comando inesperado no fake de LocalReconcilerTest: " + command);
        }

        @Override
        public NodeId localId() {
            return NodeId.of("leader-fake");
        }

        @Override
        public Optional<NodeId> leaderId() {
            leaderIdCalls.incrementAndGet();
            return leaderPresent ? Optional.of(NodeId.of("leader-fake")) : Optional.empty();
        }
    }
}
