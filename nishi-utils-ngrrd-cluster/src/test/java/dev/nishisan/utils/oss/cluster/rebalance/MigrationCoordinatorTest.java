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

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.PlacementRequestHandler;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationOutcome;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationResult;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cobre {@link MigrationCoordinator} com {@link FakeCatalog} e {@link FakeClusterRpc} — nenhuma rede
 * nem catálogo real: o que se quer testar é a máquina de estados do coordenador (placement
 * {@code MIGRATING}→{@code ACTIVE}/{@code aborted}, retentativa de {@code MIGRATE_FINISH},
 * {@code resumeInFlight}), não o transporte.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MigrationCoordinatorTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final String SRC = "storage-src";
    private static final String DST = "storage-dst";

    private FakeCatalog catalog;
    private FakeClusterRpc rpc;
    private LeaderViewFake leaderView;
    private MigrationCoordinator coordinator;

    @BeforeEach
    void setUp() {
        catalog = new FakeCatalog();
        rpc = new FakeClusterRpc();
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.PARTIAL, null));
        leaderView = new LeaderViewFake();
        leaderView.leader = true;
    }

    private void newCoordinator(int maxConcurrentMigrations) {
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, maxConcurrentMigrations,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        coordinator.onLeaderChanged(NodeId.of("self"));
    }

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @Test
    void sourceFailureResolvesPartialTargetWithoutWaitingForTheMigrationTimeout() throws Exception {
        newCoordinator(8);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.PARTIAL, null));
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.ERROR, "chunk failed"));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        var result = coordinator.migrate("s1", SRC, DST).get(2, TimeUnit.SECONDS);
        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertTrue(result.reason().contains("chunk failed"));
        assertTrue(catalog.placementStrong("s1").orElseThrow().isOwnedBy(SRC));
        assertEquals(1, rpc.callsTo(SRC, Commands.MIGRATE_ABORT));
        assertEquals(1, rpc.callsTo(DST, Commands.MIGRATE_ABORT));
    }

    @Test
    void committedDestinationWinsEvenWhenSourceLostTheCommitResponse() throws Exception {
        newCoordinator(8);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.ERROR, "commit timeout"));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 10));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        var result = coordinator.migrate("s1", SRC, DST).get(2, TimeUnit.SECONDS);
        assertEquals(MigrationOutcome.COMPLETED, result.outcome());
        assertTrue(catalog.placementStrong("s1").orElseThrow().isOwnedBy(DST));
        assertEquals(0, rpc.callsTo(SRC, Commands.MIGRATE_ABORT));
    }

    @Test
    void caminhoFelizFlipaOCatalogoEEnviaFinish() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 1_234L));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.COMPLETED, result.outcome());
        assertEquals(1_234L, result.bytes());
        SeriesPlacement finalPlacement = catalog.placementStrong("s1").orElseThrow();
        assertEquals(PlacementState.ACTIVE, finalPlacement.state());
        assertEquals(DST, finalPlacement.ownerNodeId());
        assertTrue(rpc.callsTo(SRC, Commands.MIGRATE_FINISH) >= 1);
    }

    @Test
    void placementNaoAtivoNoSrcEsperadoResultaEmSkipped() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active("outro-nó", 1_000L));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.SKIPPED, result.outcome());
        assertTrue(rpc.calls().isEmpty(), "não deveria ter feito nenhuma chamada RPC");
    }

    @Test
    void erroNoStartAbortaEReverteOCatalogo() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START,
                (target, body) -> { throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado"); });
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        SeriesPlacement reverted = catalog.placementStrong("s1").orElseThrow();
        assertEquals(PlacementState.ACTIVE, reverted.state());
        assertEquals(SRC, reverted.ownerNodeId(), "deveria ter revertido para o dono original");
        assertTrue(rpc.callsTo(SRC, Commands.MIGRATE_ABORT) >= 1);
        assertTrue(rpc.callsTo(DST, Commands.MIGRATE_ABORT) >= 1);
    }

    @Test
    void hashMismatchNoPollAbortaEReverteOCatalogo() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> MigrateResponse.of(MigrateStatus.HASH_MISMATCH, "sha divergente"));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertEquals(SRC, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
    }

    /**
     * Achado bloqueante do Refuter (perda de dados): sem revalidar o placement forte antes de agir,
     * {@code abort()} mandaria {@code MIGRATE_ABORT} ao destino mesmo depois de OUTRO líder já ter
     * completado de verdade a mesma migração (dual-leader/partição) — o destino apagaria a única cópia
     * real da série. Aqui o responder de {@code MIGRATE_STATUS} simula esse "outro líder": no meio do
     * poll, flipa o catálogo para {@code ACTIVE(dst)} por fora, e só DEPOIS devolve HASH_MISMATCH (o
     * gatilho de abort deste teste). {@code abort()} tem de revalidar e desistir.
     */
    @Test
    void abortComPlacementJaAtivoNoDestinoNaoReverteNemMandaAbort() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            SeriesPlacement stillMigrating = catalog.placementStrong("s1").orElseThrow();
            catalog.putPlacement("s1", SeriesPlacement.completed(stillMigrating, 2_000L));
            return MigrateResponse.of(MigrateStatus.HASH_MISMATCH, "sha divergente simulado");
        });
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_ABORT), "não deveria mandar ABORT: já ACTIVE(dst)");
        assertEquals(0L, rpc.callsTo(DST, Commands.MIGRATE_ABORT), "não deveria mandar ABORT: já ACTIVE(dst)");
        SeriesPlacement current = catalog.placementStrong("s1").orElseThrow();
        assertEquals(PlacementState.ACTIVE, current.state());
        assertEquals(DST, current.ownerNodeId(), "não deveria ter revertido o placement já concluído");
    }

    /**
     * Mesma revalidação, outra causa: quando {@code abort()} for agir, o catálogo já não confirma
     * MIGRATING com o MESMO {@code migrationId} (uma migração concorrente sobrescreveu a entrada) —
     * também não pode reverter nem mandar ABORT, sob pena de interferir numa migração que não é a sua.
     */
    @Test
    void abortComMigrationIdDiferenteNoCatalogoNaoAge() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            catalog.putPlacement("s1", new SeriesPlacement(SRC, DST, PlacementState.MIGRATING, "outro-id",
                    500L, 3_000L));
            return MigrateResponse.of(MigrateStatus.HASH_MISMATCH, "sha divergente simulado");
        });
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_ABORT), "não deveria mexer numa migração de outro id");
        assertEquals(0L, rpc.callsTo(DST, Commands.MIGRATE_ABORT), "não deveria mexer numa migração de outro id");
        SeriesPlacement current = catalog.placementStrong("s1").orElseThrow();
        assertEquals(PlacementState.MIGRATING, current.state());
        assertEquals("outro-id", current.migrationId(), "não deveria ter tocado no placement de outra migração");
    }

    /**
     * Achado dos ALTOS residuais do Refuter (r2, item 1): {@code abort()} grava a reversão do catálogo
     * ANTES de mandar {@code MIGRATE_ABORT} às duas pontas (mesma ordem de {@code complete()}: fonte da
     * verdade primeiro, aviso aos nós depois). Observa a ordem através de uma lista de eventos
     * compartilhada entre {@link FakeCatalog} e {@link FakeClusterRpc}.
     */
    @Test
    void abortGravaAReversaoDoCatalogoAntesDeMandarMigrateAbortAsDuasPontas() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> MigrateResponse.of(MigrateStatus.HASH_MISMATCH, "sha divergente simulado"));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        // Só grava eventos a partir daqui — a gravação inicial de ACTIVE(SRC) acima não deve entrar.
        List<String> events = new CopyOnWriteArrayList<>();
        catalog.recordEventsInto(events);
        rpc.recordEventsInto(events);

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        int catalogRevertIndex = events.indexOf("CATALOG ACTIVE s1");
        int abortToSrcIndex = events.indexOf("RPC " + Commands.MIGRATE_ABORT + " " + SRC);
        int abortToDstIndex = events.indexOf("RPC " + Commands.MIGRATE_ABORT + " " + DST);
        assertTrue(catalogRevertIndex >= 0 && abortToSrcIndex >= 0 && abortToDstIndex >= 0,
                "eventos esperados ausentes: " + events);
        assertTrue(catalogRevertIndex < abortToSrcIndex,
                "catálogo deveria ser revertido ANTES do ABORT à origem: " + events);
        assertTrue(catalogRevertIndex < abortToDstIndex,
                "catálogo deveria ser revertido ANTES do ABORT ao destino: " + events);
    }

    /**
     * Achado dos ALTOS residuais do Refuter (r2, item 1): {@code complete()} passa a usar a MESMA
     * pré-condição de {@code abort()} — se o placement já foi trocado por outro {@code migrationId}
     * (outra migração concorrente) entre o {@code COMMITTED} confirmado e o flip, não flipa nem manda
     * {@code MIGRATE_FINISH}.
     */
    @Test
    void completeComPlacementJaAlteradoPorOutroIdNaoFlipaNemMandaFinish() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            catalog.putPlacement("s1", new SeriesPlacement(SRC, DST, PlacementState.MIGRATING, "outro-id",
                    500L, 3_000L));
            return new MigrateResponse(MigrateStatus.COMMITTED, null, 999L);
        });
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_FINISH),
                "não deveria mandar FINISH: o catálogo já mudou de migrationId");
        SeriesPlacement current = catalog.placementStrong("s1").orElseThrow();
        assertEquals(PlacementState.MIGRATING, current.state());
        assertEquals("outro-id", current.migrationId(), "não deveria ter tocado no placement de outra migração");
    }

    /**
     * Achado 1 da revisão pós-merge da PR #172: o destino segura o lock da série durante o commit
     * (fsync) e o mesmo lock atende {@code MIGRATE_STATUS} — é comum, não raro, que o poll do destino
     * estoure timeout de transporte bem na iteração em que a origem já responde erro. Antes desta
     * correção, {@code pollUntilResolved} abortava mesmo com o destino já {@code COMMITTED} (só ainda
     * não confirmado pelo poll que falhou). Aqui o 1º poll do destino lança falha de transporte
     * (devolve {@code null}) na mesma iteração em que a origem responde erro; o laço deve continuar em
     * vez de abortar, e o 2º poll do destino confirma {@code COMMITTED}.
     */
    @Test
    void commitNoDestinoVenceMesmoComPollDoDestinoFalhandoEOrigemEmErro() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        AtomicInteger dstCalls = new AtomicInteger();
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            if (dstCalls.incrementAndGet() == 1) {
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "falha de transporte simulada no poll do destino");
            }
            return new MigrateResponse(MigrateStatus.COMMITTED, null, 777L);
        });
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.ERROR, "falha simulada na origem"));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.COMPLETED, result.outcome());
        assertEquals(777L, result.bytes());
        assertEquals(DST, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
        assertEquals(0L, rpc.callsTo(DST, Commands.MIGRATE_ABORT), "COMMITTED no destino não pode virar ABORT");
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_ABORT), "COMMITTED no destino não pode virar ABORT");
    }

    /**
     * Comportamento preservado (achado 1): quando o poll do destino RESPONDE (não falha de transporte)
     * com um status não-{@code COMMITTED} na mesma iteração em que a origem reporta erro, o abort
     * continua imediato — não é preciso esperar o {@code migrationTimeout}.
     */
    @Test
    void erroDaOrigemComDestinoRespondendoNaoCommittedAborta() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.PARTIAL, null));
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.ERROR, "falha simulada na origem"));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertTrue(result.reason().contains("falha simulada na origem"));
        assertEquals(SRC, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
        assertEquals(1, rpc.callsTo(SRC, Commands.MIGRATE_ABORT));
        assertEquals(1, rpc.callsTo(DST, Commands.MIGRATE_ABORT));
    }

    /**
     * Achado 1: ao estourar o {@code migrationTimeout}, o coordenador reconsulta o destino uma última
     * vez ANTES de abortar — se essa reconsulta final confirmar {@code COMMITTED}, completa em vez de
     * abortar. Usa um {@link Clock} manual para controlar deterministicamente quando o prazo estoura,
     * sem depender de sleeps/tolerâncias de tempo real: os 3 primeiros polls do destino (dentro do
     * laço normal) respondem {@code PARTIAL} e avançam o relógio manual 20 ms cada um, superando o
     * prazo de 50 ms na 3ª iteração; só a reconsulta final (4º poll) responde {@code COMMITTED}.
     */
    @Test
    void timeoutReconsultaDestinoAntesDeAbortar() throws Exception {
        ManualClock clock = new ManualClock(0L);
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(5),
                Duration.ofMillis(50), clock);
        coordinator.onLeaderChanged(NodeId.of("self"));
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.PARTIAL, null));
        AtomicInteger dstCalls = new AtomicInteger();
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            int n = dstCalls.incrementAndGet();
            clock.advance(20L);
            if (n <= 3) {
                return MigrateResponse.of(MigrateStatus.PARTIAL, null);
            }
            return new MigrateResponse(MigrateStatus.COMMITTED, null, 4_321L);
        });
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.COMPLETED, result.outcome());
        assertEquals(4_321L, result.bytes());
        assertEquals(DST, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
        assertEquals(0L, rpc.callsTo(DST, Commands.MIGRATE_ABORT));
        assertTrue(dstCalls.get() >= 4, "deveria ter reconsultado o destino após o timeout");
    }

    /**
     * Fix round 1, item 1 (decisão do controlador): sem NENHUM limite, a correção do achado 1 deixava a
     * série presa em {@code MIGRATING} até o {@code migrationTimeout} inteiro sempre que o destino
     * realmente tivesse caído durante o cutover (poll sempre falhando por transporte) com a origem já em
     * erro. Aqui o {@code migrationTimeout} é bem maior que a carência de {@code
     * SOURCE_FAILURE_DESTINATION_GRACE} (10 s) — cada poll da origem (sempre {@code ERROR}) avança o
     * relógio manual 4 s, cruzando a carência bem antes do timeout completo. O destino nunca responde
     * (falha de transporte em todo poll, inclusive na reconsulta final da carência): o coordenador deve
     * abortar perto da carência, não esperar o {@code migrationTimeout}.
     */
    @Test
    void origemEmErroEDestinoNuncaRespondePorMaisQueACarenciaAbortaPertoDela() throws Exception {
        ManualClock clock = new ManualClock(0L);
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(5),
                Duration.ofMinutes(5), clock);
        coordinator.onLeaderChanged(NodeId.of("self"));
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        AtomicInteger srcCalls = new AtomicInteger();
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> {
            srcCalls.incrementAndGet();
            clock.advance(4_000L); // cruza os 10s de carência em poucas chamadas, sem sleep real.
            return MigrateResponse.of(MigrateStatus.ERROR, "falha simulada na origem");
        });
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> { throw new NgrrdClusterException(ErrorCode.TIMEOUT, "destino inalcançável simulado"); });
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertTrue(result.reason().contains("carência"),
                "deveria abortar citando a carência, não o timeout completo: " + result.reason());
        assertEquals(SRC, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
        assertTrue(srcCalls.get() >= 3 && srcCalls.get() <= 6,
                "deveria abortar logo após cruzar a carência de 10s, não esperar o migrationTimeout de 5 min: "
                        + srcCalls.get() + " polls da origem");
    }

    /**
     * Fix round 1, item 1: dentro da mesma carência, se a reconsulta final ao destino (disparada quando
     * a carência se esgota) confirmar {@code COMMITTED}, a migração completa — mesma prioridade do
     * achado 1 original, só que agora alcançada via a reconsulta da carência em vez da reconsulta do
     * {@code migrationTimeout}.
     *
     * <p>O destino só confirma {@code COMMITTED} quando o relógio manual cruza os 14 s (mesma carência
     * de 10 s computada a partir da 1ª falha da origem, observada em 4 s — ver cálculo em {@link
     * #origemEmErroEDestinoNuncaRespondePorMaisQueACarenciaAbortaPertoDela}) — amarrado de propósito ao
     * ÚNICO mecanismo que avança esse relógio: o poll da origem, que só a carência dispara enquanto o
     * destino falha. No código sem a carência (achado 1 isolado), a origem nunca é consultada enquanto o
     * destino falha, o relógio nunca avança, e o destino nunca chega a confirmar {@code COMMITTED} — a
     * migração trava até o {@code migrationTimeout}, o que este teste prova ao falhar por timeout do
     * próprio teste (RED) se a carência for removida.</p>
     */
    @Test
    void origemEmErroEDestinoCommittedDentroDaCarenciaCompleta() throws Exception {
        ManualClock clock = new ManualClock(0L);
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(5),
                Duration.ofMinutes(5), clock);
        coordinator.onLeaderChanged(NodeId.of("self"));
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        AtomicInteger srcCalls = new AtomicInteger();
        rpc.respond(SRC, Commands.MIGRATE_STATUS, (target, body) -> {
            srcCalls.incrementAndGet();
            clock.advance(4_000L); // 1ª chamada arma a carência em 4s + 10s = 14s (ver Javadoc acima).
            return MigrateResponse.of(MigrateStatus.ERROR, "falha simulada na origem");
        });
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            if (clock.millis() >= 14_000L) {
                return new MigrateResponse(MigrateStatus.COMMITTED, null, 999L);
            }
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "destino inalcançável simulado");
        });
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.COMPLETED, result.outcome());
        assertEquals(999L, result.bytes());
        assertEquals(DST, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_ABORT));
        assertEquals(0L, rpc.callsTo(DST, Commands.MIGRATE_ABORT));
        assertTrue(srcCalls.get() >= 3,
                "deveria ter consultado a origem múltiplas vezes até a carência avançar o relógio o "
                        + "bastante: " + srcCalls.get() + " polls");
    }

    @Test
    void timeoutNoPollAbortaAposMigrationTimeout() throws Exception {
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(10),
                Duration.ofMillis(100), Clock.systemUTC());
        coordinator.onLeaderChanged(NodeId.of("self"));
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        // Nunca confirma COMMITTED -> fica em PARTIAL até o timeout do coordenador.
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.PARTIAL, null));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertTrue(result.reason().contains("timeout"));
        assertEquals(SRC, catalog.placementStrong("s1").orElseThrow().ownerNodeId());
    }

    @Test
    void resumeInFlightComCommittedCompletaAMigracao() {
        String migrationId = "migration-resume-committed";
        catalog.putPlacement("s1",
                new SeriesPlacement(SRC, DST, PlacementState.MIGRATING, migrationId, 1_000L, 1_000L));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 999L));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        // newCoordinator() já dispara a transição leader=false->true (e, com ela, resumeInFlight) —
        // o placement MIGRATING precisa estar no catálogo ANTES dessa chamada.
        newCoordinator(2);

        awaitTrue("placement flipado para ACTIVE(dst) após resumeInFlight", () -> {
            Optional<SeriesPlacement> current = catalog.placementStrong("s1");
            return current.isPresent() && current.get().state() == PlacementState.ACTIVE
                    && current.get().ownerNodeId().equals(DST);
        });
        awaitTrue("MIGRATE_FINISH enviado à origem", () -> rpc.callsTo(SRC, Commands.MIGRATE_FINISH) >= 1);
    }

    @Test
    void resumeInFlightRetentaOFlipDoCatalogoRecusadoPeloLiderEmCatchUp() {
        String migrationId = "migration-resume-syncing";
        catalog.putPlacement("s1",
                new SeriesPlacement(SRC, DST, PlacementState.MIGRATING, migrationId, 1_000L, 1_000L));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 999L));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        // As 3 primeiras gravações do desfecho são recusadas, como um líder ainda em catch-up faz.
        catalog.rejectNextPlacementWrites(3);

        newCoordinator(2);

        awaitTrue("placement flipado para ACTIVE(dst) apesar das recusas iniciais", () -> {
            Optional<SeriesPlacement> current = catalog.placementStrong("s1");
            return current.isPresent() && current.get().state() == PlacementState.ACTIVE
                    && current.get().ownerNodeId().equals(DST);
        });
        awaitTrue("MIGRATE_FINISH só depois do flip", () -> rpc.callsTo(SRC, Commands.MIGRATE_FINISH) >= 1);
    }

    @Test
    void resumeInFlightComPartialAbortaEDevolveParaOSrc() {
        String migrationId = "migration-resume-partial";
        catalog.putPlacement("s1",
                new SeriesPlacement(SRC, DST, PlacementState.MIGRATING, migrationId, 1_000L, 1_000L));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> MigrateResponse.of(MigrateStatus.PARTIAL, null));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        newCoordinator(2);

        awaitTrue("placement revertido para ACTIVE(src) após resumeInFlight", () -> {
            Optional<SeriesPlacement> current = catalog.placementStrong("s1");
            return current.isPresent() && current.get().state() == PlacementState.ACTIVE
                    && current.get().ownerNodeId().equals(SRC);
        });
    }

    @Test
    void semaforoRespeitaMaxConcurrentMigrations() throws Exception {
        newCoordinator(1);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        catalog.putPlacement("s2", SeriesPlacement.active(SRC, 1_000L));

        CountDownLatch firstStartEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstStart = new CountDownLatch(1);
        AtomicInteger concurrentStarts = new AtomicInteger();
        AtomicInteger maxObservedConcurrency = new AtomicInteger();
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> {
            int concurrency = concurrentStarts.incrementAndGet();
            maxObservedConcurrency.accumulateAndGet(concurrency, Math::max);
            firstStartEntered.countDown();
            try {
                assertTrue(releaseFirstStart.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            concurrentStarts.decrementAndGet();
            return MigrateResponse.of(MigrateStatus.OK, null);
        });
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 1L));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        CompletableFuture<MigrationResult> first = coordinator.migrate("s1", SRC, DST);
        assertTrue(firstStartEntered.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        CompletableFuture<MigrationResult> second = coordinator.migrate("s2", SRC, DST);

        // Com maxConcurrentMigrations=1, a segunda migração não pode ter entrado em MIGRATE_START
        // enquanto a primeira ainda está presa nele.
        Thread.sleep(200L);
        assertEquals(1, maxObservedConcurrency.get(), "no máximo 1 migração deveria estar em MIGRATE_START por vez");

        releaseFirstStart.countDown();
        MigrationResult firstResult = first.get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        MigrationResult secondResult = second.get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        assertEquals(MigrationOutcome.COMPLETED, firstResult.outcome());
        assertEquals(MigrationOutcome.COMPLETED, secondResult.outcome());
    }

    /**
     * A reivindicação em {@code activeMigrationIds} impede que as varreduras repetidas de
     * {@code resumeInFlight} (uma a cada 500 ms enquanto o nó é líder) despachem DUAS condução da
     * MESMA migração: enquanto o primeiro {@code MIGRATE_STATUS} está bloqueado, mais varreduras
     * acontecem, mas nem o status nem o {@code MIGRATE_FINISH} são enviados em dobro.
     */
    @Test
    void resumeInFlightNaoConduzDuasVezesAMesmaMigracaoEnquantoAPrimeiraEstaEmCurso() {
        String migrationId = "migration-resume-claimed";
        catalog.putPlacement("s1",
                new SeriesPlacement(SRC, DST, PlacementState.MIGRATING, migrationId, 1_000L, 1_000L));
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger statusAttempts = new AtomicInteger();
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            statusAttempts.incrementAndGet();
            try {
                release.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new MigrateResponse(MigrateStatus.COMMITTED, null, 999L);
        });
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        newCoordinator(2);

        // Pelo menos 3 varreduras da cópia local aconteceram com a primeira condução ainda bloqueada
        // no MIGRATE_STATUS — cada uma delas reencontra o placement MIGRATING e tem de ser barrada.
        awaitTrue("primeira condução iniciada", () -> statusAttempts.get() >= 1);
        awaitTrue("varreduras repetidas de resumeInFlight", () -> catalog.localScans() >= 3);
        assertEquals(1, statusAttempts.get(), "a mesma migração não pode ser conduzida em paralelo");

        release.countDown();
        awaitTrue("placement flipado para ACTIVE(dst)", () -> {
            Optional<SeriesPlacement> current = catalog.placementStrong("s1");
            return current.isPresent() && current.get().state() == PlacementState.ACTIVE
                    && current.get().ownerNodeId().equals(DST);
        });
        awaitTrue("MIGRATE_FINISH enviado à origem", () -> rpc.callsTo(SRC, Commands.MIGRATE_FINISH) >= 1);
        assertEquals(1, statusAttempts.get(), "MIGRATE_STATUS deveria ter sido chamado uma única vez");
        assertEquals(1L, rpc.callsTo(SRC, Commands.MIGRATE_FINISH), "MIGRATE_FINISH deveria ser único");
    }

    /**
     * Achado do Refuter (r3): sem {@code try/catch(Throwable)} em volta da varredura do catálogo local,
     * uma exceção de {@code placementsLocal()} (leitura eventual, pode falhar transitoriamente) escapava
     * o {@code Runnable} do pool sem log nenhum e abortava TODAS as tentativas restantes de {@code
     * resumeInFlight()} (não só a rodada atual). Confirma que, mesmo com {@code placementsLocal()}
     * lançando sempre durante a varredura inicial, nenhuma reivindicação fica presa em {@code
     * activeMigrationIds} e o coordenador continua saudável — uma migração NORMAL, disparada depois,
     * ainda completa.
     */
    @Test
    void resumeInFlightNaoQuebraNemTravaActiveMigrationIdsSePlacementsLocalLancarExcecao() throws Exception {
        catalog.throwOnPlacementsLocal(true);

        newCoordinator(2); // dispara resumeInFlight() via onLeaderChanged; placementsLocal() só lança.

        awaitTrue("pelo menos uma varredura tentou (e falhou) contra placementsLocal()",
                () -> catalog.localScans() >= 1);

        catalog.throwOnPlacementsLocal(false);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 1L));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.COMPLETED, result.outcome(),
                "o pool/coordenador deveria continuar saudável depois das exceções de placementsLocal()");
    }

    /**
     * Um coordenador fechado no meio de uma condução (o líder está sendo derrubado) PARA sem efeitos
     * colaterais: nem {@code MIGRATE_START} nem flip/abort do catálogo — o {@code MIGRATING} fica para o
     * próximo líder resolver. Antes, o {@code shutdownNow} do {@code close()} interrompia a thread
     * bloqueada no hook, o hook devolvia e a condução seguia (START, chunks, COMMIT, ACTIVE(dst)) por
     * cima do encerramento do nó.
     */
    @Test
    void closeDuranteAConducaoParaSemEnviarStartNemTocarOCatalogo() throws Exception {
        CountDownLatch reachedHook = new CountDownLatch(1);
        MigrationCoordinator.MigrationHooks blockingHook = new MigrationCoordinator.MigrationHooks() {
            @Override
            public void beforeStart(String seriesKey, String migrationId) {
                reachedHook.countDown();
                try {
                    new CountDownLatch(1).await(); // bloqueia até o close() interromper
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(20),
                Duration.ofSeconds(5), Clock.systemUTC(), blockingHook);
        coordinator.onLeaderChanged(NodeId.of("self"));
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        CompletableFuture<MigrationResult> future = coordinator.migrate("s1", SRC, DST);
        assertTrue(reachedHook.await(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS), "condução deveria chegar ao hook");
        assertEquals(PlacementState.MIGRATING, catalog.placementStrong("s1").orElseThrow().state());

        coordinator.close();

        awaitTrue("condução encerrada", () -> future.isDone() || future.isCancelled());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_START), "nenhum MIGRATE_START após o close()");
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_ABORT) + rpc.callsTo(DST, Commands.MIGRATE_ABORT),
                "nenhum abort após o close(): o próximo líder resolve");
        SeriesPlacement left = catalog.placementStrong("s1").orElseThrow();
        assertEquals(PlacementState.MIGRATING, left.state(), "o MIGRATING fica para o próximo líder");
        if (!future.isCancelled()) {
            assertEquals(MigrationOutcome.FAILED, future.get().outcome());
        }
    }

    /** {@code close()} durante o poll de {@code MIGRATE_STATUS}: nenhum abort e catálogo intocado. */
    @Test
    void closeDuranteOPollNaoAbortaNemTocaOCatalogo() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        CountDownLatch polling = new CountDownLatch(1);
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS, (target, body) -> {
            polling.countDown();
            return MigrateResponse.of(MigrateStatus.PARTIAL, null);
        });
        rpc.respond(SRC, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_ABORT, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));

        CompletableFuture<MigrationResult> future = coordinator.migrate("s1", SRC, DST);
        assertTrue(polling.await(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS), "condução deveria estar no poll");

        coordinator.close();

        awaitTrue("condução encerrada", () -> future.isDone() || future.isCancelled());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_ABORT) + rpc.callsTo(DST, Commands.MIGRATE_ABORT),
                "nenhum MIGRATE_ABORT após o close()");
        assertEquals(PlacementState.MIGRATING, catalog.placementStrong("s1").orElseThrow().state(),
                "catálogo intocado: o MIGRATING fica para o próximo líder");
    }

    /**
     * {@code close()} depois do flip para {@code ACTIVE(dst)} e antes do {@code MIGRATE_FINISH}: o FINISH
     * não é enviado (origem preservada). Nada fica {@code MIGRATING}, então o {@code resumeInFlight} do
     * próximo líder não a vê; a cópia da origem é uma órfã de migração, apagada pelo
     * {@code LocalReconciler} (placement forte {@code ACTIVE} noutro dono há mais de {@code orphanGrace}
     * e {@code SERIES_EXISTS} confirmado no dono).
     */
    @Test
    void closeAposOFlipEAntesDoFinishNaoApagaAOrigem() throws Exception {
        newCoordinator(2);
        catalog.putPlacement("s1", SeriesPlacement.active(SRC, 1_000L));
        rpc.respond(SRC, Commands.MIGRATE_START, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        rpc.respond(DST, Commands.MIGRATE_STATUS,
                (target, body) -> new MigrateResponse(MigrateStatus.COMMITTED, null, 1_234L));
        rpc.respond(SRC, Commands.MIGRATE_FINISH, (target, body) -> MigrateResponse.of(MigrateStatus.OK, null));
        // O close() acontece exatamente na gravação que flipa o catálogo para ACTIVE(dst).
        catalog.afterPut = () -> {
            SeriesPlacement current = catalog.placementStrong("s1").orElseThrow();
            if (current.state() == PlacementState.ACTIVE && current.ownerNodeId().equals(DST)) {
                coordinator.close();
            }
        };

        MigrationResult result = coordinator.migrate("s1", SRC, DST).get(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        assertEquals(MigrationOutcome.FAILED, result.outcome());
        assertEquals(0L, rpc.callsTo(SRC, Commands.MIGRATE_FINISH), "MIGRATE_FINISH não pode ser enviado após o close()");
        SeriesPlacement flipped = catalog.placementStrong("s1").orElseThrow();
        assertEquals(DST, flipped.ownerNodeId(), "o flip já gravado permanece (a origem vira órfã do reconciliador)");
    }

    private static void awaitTrue(String description, java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrompido aguardando: " + description);
            }
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo: " + description);
        }
    }

    /** {@link CatalogView} fake: um mapa em memória, thread-safe o suficiente para os testes. */
    private static final class FakeCatalog implements CatalogView {
        private final Map<String, SeriesPlacement> placements = new ConcurrentHashMap<>();
        /**
         * Número de {@link #putPlacement} que ainda devem ser rejeitados antes de a gravação passar —
         * simula a janela de {@code LeaderSyncingException} de um líder recém-eleito.
         */
        private final AtomicInteger rejectionsRemaining = new AtomicInteger();
        /**
         * Lista de eventos COMPARTILHADA com {@link FakeClusterRpc} (via {@link #recordEventsInto}) —
         * usada só para observar ORDEM entre uma gravação de catálogo e um envio de RPC (achado dos
         * ALTOS residuais do Refuter: a reversão do catálogo em {@code abort()} tem de ser gravada
         * ANTES de mandar {@code MIGRATE_ABORT}). {@code null} por padrão (não grava nada).
         */
        private List<String> events;

        void recordEventsInto(List<String> events) {
            this.events = events;
        }

        /** Faz as próximas {@code count} gravações de placement falharem como um líder em catch-up. */
        void rejectNextPlacementWrites(int count) {
            rejectionsRemaining.set(count);
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.ofNullable(placements.get(seriesKey));
        }

        @Override
        public Optional<dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.empty();
        }

        @Override
        public void putNodeStatus(dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus status) {
            // Não usado por MigrationCoordinator — sem estado de nó a manter neste fake.
        }

        @Override
        public Collection<dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus> nodesLocal() {
            return List.of();
        }

        /** Quantas varreduras da cópia local já aconteceram (uma por {@code placementsLocal()}). */
        private final AtomicInteger localScans = new AtomicInteger();
        /** Refuter r3: simula {@code placementsLocal()} lançando (ex.: réplica ainda inicializando). */
        private volatile boolean throwOnPlacementsLocal;

        int localScans() {
            return localScans.get();
        }

        void throwOnPlacementsLocal(boolean throwOnPlacementsLocal) {
            this.throwOnPlacementsLocal = throwOnPlacementsLocal;
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            localScans.incrementAndGet();
            if (throwOnPlacementsLocal) {
                throw new IllegalStateException("placementsLocal indisponível (simulado)");
            }
            return Map.copyOf(placements);
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            if (rejectionsRemaining.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                throw new IllegalStateException("Leader is syncing (catch-up in progress), write rejected");
            }
            placements.put(seriesKey, placement);
            if (events != null) {
                events.add("CATALOG " + placement.state() + " " + seriesKey);
            }
            afterPut.run();
        }

        /** Chamado logo DEPOIS de cada gravação bem-sucedida (gancho para simular um close() no meio). */
        volatile Runnable afterPut = () -> { };
    }

    /**
     * {@link Clock} com avanço manual — usado só por {@code timeoutReconsultaDestinoAntesDeAbortar}
     * para tornar o estouro do {@code migrationTimeout} determinístico (sem depender de sleeps/tempo
     * real): cada poll programado do teste avança o relógio explicitamente via {@link #advance}.
     */
    private static final class ManualClock extends Clock {
        private final java.util.concurrent.atomic.AtomicLong millis;

        ManualClock(long startMillis) {
            this.millis = new java.util.concurrent.atomic.AtomicLong(startMillis);
        }

        long advance(long deltaMillis) {
            return millis.addAndGet(deltaMillis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }
    }

    /** {@link PlacementRequestHandler.LeaderView} fake. */
    private static final class LeaderViewFake implements PlacementRequestHandler.LeaderView {
        private volatile boolean leader;

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.empty();
        }

        @Override
        public Set<String> reachableNodeIds() {
            return Set.of();
        }
    }

    /**
     * {@link ClusterRpc} fake: respostas programáveis por (nodeId, comando), sempre a mesma para
     * chamadas repetidas — suficiente para o poll de {@code MIGRATE_STATUS} do coordenador.
     */
    private static final class FakeClusterRpc implements ClusterRpc {
        record Call(NodeId target, String command) {
        }

        private final Map<String, BiFunction<NodeId, Object, MigrateResponse>> responders = new ConcurrentHashMap<>();
        private final Queue<Call> calls = new ConcurrentLinkedQueue<>();
        /** Ver Javadoc do campo homônimo em {@link FakeCatalog}. */
        private List<String> events;

        void respond(String nodeId, String command, BiFunction<NodeId, Object, MigrateResponse> responder) {
            responders.put(key(nodeId, command), responder);
        }

        void recordEventsInto(List<String> events) {
            this.events = events;
        }

        List<Call> calls() {
            return List.copyOf(calls);
        }

        long callsTo(String nodeId, String command) {
            return calls.stream().filter(c -> c.target().value().equals(nodeId) && c.command().equals(command))
                    .count();
        }

        private static String key(String nodeId, String command) {
            return nodeId + "|" + command;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            calls.add(new Call(target, command));
            if (events != null) {
                events.add("RPC " + command + " " + target.value());
            }
            BiFunction<NodeId, Object, MigrateResponse> responder = responders.get(key(target.value(), command));
            if (responder == null) {
                throw new IllegalStateException("nenhuma resposta programada para " + command + " em " + target);
            }
            return (R) responder.apply(target, body);
        }

        @Override
        public NodeId localId() {
            return NodeId.of("test-coordinator");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
        }
    }
}
