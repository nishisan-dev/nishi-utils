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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.oss.cluster.api.DeletePrecondition;
import dev.nishisan.utils.oss.cluster.api.DeleteResult;
import dev.nishisan.utils.oss.cluster.api.DeleteStatus;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesDeletion;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.DeleteControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.DeleteRequest;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Laço de recuperação de exclusões do líder ({@link SeriesDeleteHandler#recover()}): pendentes lidas do índice do
 * {@link CatalogService} (sem copiar o catálogo), reconstrução do índice ao assumir a liderança e periodicamente,
 * e backoff exponencial por {@code operationId}. Catálogo real de um nó ({@link NGrid#local(int)}), embrulhado num
 * contador de chamadas; RPC das fases e transporte são fakes; o journal é real e vazio.
 */
class SeriesDeleteRecoveryTest {

    private static final String SELF = "storage-a";
    private static final String OTHER = "storage-b";
    private static final long NOW = Instant.parse("2026-11-25T12:00:00Z").toEpochMilli();

    @TempDir
    Path tempDir;

    private final MutableClock clock = new MutableClock(Instant.ofEpochMilli(NOW));
    private final ScriptedRpc rpc = new ScriptedRpc();
    private volatile boolean leader = true;
    private NGridCluster cluster;
    private CatalogService service;
    private CountingCatalog catalog;
    /** O mesmo mapa replicado, escrito sem passar pelo {@link CatalogService} (replicação, snapshot, outro líder). */
    private DistributedMap<String, SeriesPlacement> rawMap;
    private SeriesLifecycleJournal journal;
    private SeriesLifecycleService lifecycle;
    private SeriesDeleteHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .start();
        service = CatalogService.from(cluster.node(0));
        rawMap = cluster.node(0).getMap(CatalogService.CATALOG_MAP, String.class, SeriesPlacement.class);
        service.putNodeStatus(status(SELF));
        service.putNodeStatus(status(OTHER));
        catalog = new CountingCatalog(service);
        journal = new SeriesLifecycleJournal(tempDir.resolve("lifecycle"));
        lifecycle = new SeriesLifecycleService(journal, null, catalog, null, SELF, "series", clock, 3_600_000L);
        handler = new SeriesDeleteHandler(fakeTransport(), catalog, rpc, lifecycle, () -> leader, () -> true, clock,
                () -> java.util.Set.of(SELF, OTHER), Duration.ofSeconds(1));
    }

    @AfterEach
    void tearDown() throws Exception {
        handler.close();
        journal.close();
        cluster.close();
    }

    private static StorageNodeStatus status(String nodeId) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, 0, 0, 0, NOW, DistributionMode.COUNT, 1, 0,
                StorageCapabilities.ALL);
    }

    private static SeriesPlacement pending(String operationId, boolean committed) {
        return SeriesPlacement.active(SELF, NOW)
                .withDeletion(new SeriesDeletion(operationId, 0, List.of(SELF, OTHER), committed), NOW);
    }

    private long metric(String name) {
        Long value = lifecycle.deleteMetrics().get(name);
        return value == null ? 0L : value;
    }

    private void failPhases() {
        rpc.script((node, command) -> {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "participante caiu");
        });
    }

    /** Roda um ciclo por segundo durante {@code seconds} segundos e devolve os instantes (s) com tentativa. */
    private List<Long> cyclesEverySecond(int seconds) {
        List<Long> attempts = new ArrayList<>();
        for (int second = 0; second < seconds; second++) {
            long before = metric("seriesDelete.recoveryAttempts");
            handler.recover();
            if (metric("seriesDelete.recoveryAttempts") > before) attempts.add((long) second);
            clock.advance(Duration.ofSeconds(1));
        }
        return attempts;
    }

    @Test
    void intervaloDobraACadaFalhaAteSessentaSegundos() {
        List<Long> delays = new ArrayList<>();
        for (int failures = 1; failures <= 9; failures++) {
            delays.add(SeriesDeleteHandler.recoveryBackoffDelay(failures).toSeconds());
        }
        assertEquals(List.of(1L, 2L, 4L, 8L, 16L, 32L, 60L, 60L, 60L), delays);
        assertEquals(Duration.ofSeconds(60), SeriesDeleteHandler.recoveryBackoffDelay(Integer.MAX_VALUE));
    }

    @Test
    void recuperacaoQueFalhaRespeitaOBackoffDaOperacao() {
        service.putPlacement("s", pending("op-s", true));
        failPhases();

        List<Long> attempts = cyclesEverySecond(250);

        // Falhas em 0, 1, 3, 7, … : intervalos de 1, 2, 4, 8, 16, 32 e depois 60 s.
        assertEquals(List.of(0L, 1L, 3L, 7L, 15L, 31L, 63L, 123L, 183L, 243L), attempts);
        assertEquals(10, metric("seriesDelete.recoveryAttempts"));
        assertEquals(1, metric("seriesDelete.pending"), "a operação em espera continua no gauge");
        assertEquals(1, handler.recoveryBackoffSize());
    }

    @Test
    void backoffNaoEscondeAExclusaoQuandoAFalhaPassa() {
        service.putPlacement("s", pending("op-s", true));
        failPhases();
        cyclesEverySecond(200); // backoff já no teto de 60 s
        rpc.script((node, command) -> null);

        List<Long> attempts = cyclesEverySecond(61);

        assertEquals(1, attempts.size(), "no máximo 60 s até a próxima tentativa");
        assertEquals(1, metric("seriesDelete.completed"));
        assertTrue(service.placementLocal("s").isEmpty());
        handler.recover();
        assertEquals(0, metric("seriesDelete.pending"));
        assertEquals(0, handler.recoveryBackoffSize(), "operação concluída sai do backoff");
    }

    @Test
    void backoffELimpoQuandoAReservaSomeOuALiderancaCai() {
        service.putPlacement("s", pending("op-s", true));
        service.putPlacement("t", pending("op-t", true));
        failPhases();
        handler.recover();
        assertEquals(2, handler.recoveryBackoffSize());

        // A reserva de "s" some fora deste nó (outro líder concluiu): a entrada sai no ciclo seguinte.
        rawMap.remove("s");
        handler.recover();
        assertEquals(1, handler.recoveryBackoffSize());

        leader = false;
        handler.recover();
        assertEquals(0, handler.recoveryBackoffSize());
        assertEquals(0, metric("seriesDelete.pending"));
    }

    @Test
    void novaOperacaoNaMesmaSerieComecaSemBackoff() {
        service.putPlacement("s", pending("op-1", true));
        failPhases();
        cyclesEverySecond(40);
        long before = metric("seriesDelete.recoveryAttempts");

        // A reserva foi cancelada e refeita com outra operação: nada herda o backoff da anterior.
        service.putPlacement("s", pending("op-2", true));
        handler.recover();

        assertEquals(before + 1, metric("seriesDelete.recoveryAttempts"));
        assertEquals(1, handler.recoveryBackoffSize(), "só a operação nova tem entrada");
    }

    @Test
    void mesmaOperacaoEmSeriesDiferentesTemBackoffsSeparados() {
        service.putPlacement("a", pending("op-lote", true));
        service.putPlacement("b", pending("op-lote", true));
        failPhases();
        handler.recover();
        assertEquals(2, handler.recoveryBackoffSize());
        assertEquals(2, metric("seriesDelete.recoveryAttempts"));

        // A reserva de "a" some: o backoff de "b", com o mesmo operationId, continua.
        rawMap.remove("a");
        clock.advance(Duration.ofSeconds(1));
        handler.recover();

        assertEquals(1, handler.recoveryBackoffSize());
        assertEquals(3, metric("seriesDelete.recoveryAttempts"));
    }

    @Test
    void pedidoDeClienteAnotaReservaForaDoIndice() {
        handler.recover();
        // Reserva de outro líder, recebida depois da reconstrução: fora do índice até alguém vê-la.
        rawMap.put("s", pending("op-s", true));
        failPhases();

        assertEquals(DeleteStatus.ERROR,
                handler.delete(new DeleteRequest("s", new DeletePrecondition(0), "op-client")).status());
        clock.advance(Duration.ofSeconds(1));
        handler.recover();

        assertEquals(1, catalog.rebuilds.get(), "sem reconstrução: a anotação do delete basta");
        assertEquals(1, metric("seriesDelete.recoveryAttempts"));
        assertEquals(1, metric("seriesDelete.pending"));
    }

    @Test
    void novoLiderRecuperaReservaQueJaEstavaNoCatalogo() {
        // Reserva gravada pelo líder anterior e recebida por replicação: o índice deste nó não a viu.
        rawMap.put("s", pending("op-s", true));

        handler.recover();

        assertEquals(1, catalog.rebuilds.get(), "o primeiro ciclo como líder reconstrói o índice");
        assertEquals(1, metric("seriesDelete.completed"));
        assertTrue(service.placementLocal("s").isEmpty());
    }

    @Test
    void retomadaDaLiderancaReconstroiOIndice() {
        handler.recover();
        assertEquals(1, catalog.rebuilds.get());

        rawMap.put("s", pending("op-s", true));
        handler.recover();
        assertEquals(1, catalog.rebuilds.get(), "enquanto segue líder, não varre o catálogo a cada ciclo");

        leader = false;
        handler.recover();
        leader = true;
        handler.recover();

        assertEquals(2, catalog.rebuilds.get());
        assertEquals(1, metric("seriesDelete.completed"));
        assertTrue(service.placementLocal("s").isEmpty());
    }

    @Test
    void reconstrucaoPeriodicaCobreEscritaQueNaoPassouPeloIndice() {
        handler.recover();
        // Escrita de um líder antigo, encaminhada a este líder depois da reconstrução feita ao assumir.
        rawMap.put("s", pending("op-s", true));
        clock.advance(SeriesDeleteHandler.PENDING_INDEX_RESYNC.minusSeconds(1));
        handler.recover();
        assertEquals(1, catalog.rebuilds.get());

        clock.advance(Duration.ofSeconds(1));
        handler.recover();

        assertEquals(2, catalog.rebuilds.get());
        assertEquals(1, metric("seriesDelete.completed"));
    }

    @Test
    void pedidoDoClienteConcorrenteComARecuperacaoConcluiUmaVez() throws Exception {
        service.putPlacement("s", SeriesPlacement.active(SELF, NOW));
        CountDownLatch prepareEntered = new CountDownLatch(1);
        CountDownLatch releasePrepare = new CountDownLatch(1);
        rpc.script((node, command) -> {
            if (Commands.DELETE_PREPARE.equals(command) && prepareEntered.getCount() > 0) {
                prepareEntered.countDown();
                try {
                    releasePrepare.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return null;
        });
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<DeleteResult> client = pool.submit(() -> handler.delete(
                    new DeleteRequest("s", new DeletePrecondition(0), "op-client")));
            assertTrue(prepareEntered.await(10, TimeUnit.SECONDS));
            // A reserva do cliente já está no índice: a recuperação a acha e espera o lock de drive.
            Thread recovery = Thread.ofPlatform().start(handler::recover);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (recovery.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertEquals(Thread.State.WAITING, recovery.getState());
            releasePrepare.countDown();

            assertEquals(DeleteStatus.DELETED, client.get(10, TimeUnit.SECONDS).status());
            recovery.join(10_000);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, metric("seriesDelete.completed"));
        assertEquals(1, metric("seriesDelete.recoveryAttempts"));
        assertEquals(0, handler.recoveryBackoffSize(), "NOT_FOUND da recuperação encerra a operação");
        assertTrue(service.placementLocal("s").isEmpty());
        assertTrue(service.pendingDeletionsLocal().isEmpty());
    }

    @Test
    void falhaNoJournalNaoPulaARecuperacaoDoLiderEOReenvioTemBackoff() {
        // Líder com a própria entrada COMMITTED no journal, a reserva pendente no catálogo e todas as fases
        // falhando, inclusive o DELETE_RECOVER enviado a si mesmo.
        SeriesPlacement placement = pending("op-s", true);
        rawMap.put("s", placement);
        journal.put("s", new SeriesLifecycleJournal.Entry(placement.generationId(), 0,
                SeriesLifecycleJournal.Phase.COMMITTED, placement));
        List<Long> recoverSends = new CopyOnWriteArrayList<>();
        long start = clock.millis();
        rpc.script((node, command) -> {
            if (Commands.DELETE_RECOVER.equals(command)) recoverSends.add((clock.millis() - start) / 1000);
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "participante caiu");
        });
        List<String> warnings = new CopyOnWriteArrayList<>();
        Handler capture = pendingWarnings(warnings);
        Logger logger = Logger.getLogger(SeriesDeleteHandler.class.getName());
        logger.addHandler(capture);
        try {
            List<Long> attempts = cyclesEverySecond(10);

            assertEquals(1, metric("seriesDelete.pending"));
            assertEquals(1, catalog.rebuilds.get(), "a reconstrução acontece mesmo com o journal falhando");
            assertEquals(List.of("NGRRD_DELETE_PENDING count=1 oldestOperationId=op-s"), warnings);
            assertEquals(List.of(0L, 1L, 3L, 7L), recoverSends, "reenvio espaçado: 1, 2, 4 s");
            assertEquals(List.of(0L, 1L, 3L, 7L), attempts);
            assertEquals(1, handler.journalRecoveryBackoffSize());
        } finally {
            logger.removeHandler(capture);
        }
    }

    @Test
    void entradaDoJournalQueDeixaDeSerCandidataSaiDoBackoffDeReenvio() {
        SeriesPlacement placement = pending("op-s", true);
        journal.put("s", new SeriesLifecycleJournal.Entry(placement.generationId(), 0,
                SeriesLifecycleJournal.Phase.FINISHED, placement));
        rawMap.put("s", placement);
        failPhases();
        handler.recover();
        assertEquals(1, handler.journalRecoveryBackoffSize());

        rawMap.remove("s"); // FINISHED e sem placement: nada mais a pedir ao líder
        handler.recover();

        assertEquals(0, handler.journalRecoveryBackoffSize());
    }

    @Test
    void relogioQueRecuaNaoPrendeOBackoff() {
        service.putPlacement("s", pending("op-s", true));
        failPhases();
        cyclesEverySecond(200); // backoff no teto: próxima tentativa até 60 s à frente
        long before = metric("seriesDelete.recoveryAttempts");

        clock.advance(Duration.ofMinutes(-10));
        handler.recover();

        assertEquals(before + 1, metric("seriesDelete.recoveryAttempts"),
                "espera maior que o teto só vem de relógio que recuou: conta como vencida");
    }

    @Test
    void relogioQueRecuaRefazAReconstrucaoDoIndice() {
        handler.recover();
        assertEquals(1, catalog.rebuilds.get());

        clock.advance(Duration.ofSeconds(-1));
        handler.recover();

        assertEquals(2, catalog.rebuilds.get());
    }

    private static Handler pendingWarnings(List<String> warnings) {
        return new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage() != null
                        && record.getMessage().startsWith("NGRRD_DELETE_PENDING")) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Carga: 500 mil placements e 1 exclusão pendente. Só o primeiro ciclo (reconstrução ao assumir) percorre o
     * catálogo; os seguintes, dentro do período de reconstrução, leem só o índice. O custo por ciclo é comparado
     * ao de uma única cópia do catálogo, que era o piso do ciclo até a 8.14.
     */
    @Test
    void cicloComQuinhentosMilPlacementsEUmPendenteCustaOPendentes() throws Exception {
        int total = 500_000;
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        long populateStart = System.nanoTime();
        try {
            List<Future<?>> parts = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int slice = t;
                parts.add(pool.submit(() -> {
                    for (int i = slice; i < total; i += threads) {
                        rawMap.put("series-" + i, SeriesPlacement.active(SELF, NOW));
                    }
                }));
            }
            for (Future<?> part : parts) part.get(10, TimeUnit.MINUTES);
        } finally {
            pool.shutdownNow();
        }
        long populateMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - populateStart);
        rawMap.put("series-pending", pending("op-pending", true));
        assertEquals(total + 1, rawMap.size());
        failPhases();

        long copyNanos = Long.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            long start = System.nanoTime();
            service.placementsLocal();
            copyNanos = Math.min(copyNanos, System.nanoTime() - start);
        }

        long firstStart = System.nanoTime();
        handler.recover();
        long firstNanos = System.nanoTime() - firstStart;
        assertEquals(1, catalog.rebuilds.get());

        int cycles = (int) SeriesDeleteHandler.PENDING_INDEX_RESYNC.toSeconds() - 1;
        catalog.resetCounters();
        long steadyStart = System.nanoTime();
        for (int i = 0; i < cycles; i++) {
            clock.advance(Duration.ofSeconds(1));
            handler.recover();
        }
        long steadyNanos = System.nanoTime() - steadyStart;

        long attempts = metric("seriesDelete.recoveryAttempts");
        System.out.printf("NGRRD_DELETE_RECOVERY_LOAD placements=%d pending=1 populateMs=%d catalogCopyMs=%.3f"
                        + " firstCycleMs=%.3f steadyCycles=%d steadyAvgUs=%.1f attempts=%d strongReads=%d%n",
                total, populateMs, copyNanos / 1e6, firstNanos / 1e6, cycles, steadyNanos / 1e3 / cycles,
                attempts, catalog.strongReads.get());

        assertEquals(0, catalog.localCopies.get(), "recover() não copia o catálogo");
        assertEquals(0, catalog.rebuilds.get(), "sem reconstrução dentro do período");
        assertEquals(cycles, catalog.pendingReads.get());
        assertEquals(1, metric("seriesDelete.pending"));
        assertEquals(6, attempts, "backoff: tentativas em 0, 1, 3, 7, 15 e 31 s");
        // Leituras do catálogo proporcionais às tentativas sobre pendentes, não ao tamanho do catálogo.
        assertTrue(catalog.strongReads.get() <= 4 * (attempts - 1),
                "leituras fortes por ciclo devem ser O(pendentes): " + catalog.strongReads.get());
        assertTrue(steadyNanos / cycles < copyNanos / 10,
                "ciclo médio (" + steadyNanos / cycles + " ns) deveria ser bem menor que uma cópia do catálogo ("
                        + copyNanos + " ns)");
    }

    /** {@link Clock} controlado pelo teste. */
    private static final class MutableClock extends Clock {
        private volatile Instant instant;

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

    private static Transport fakeTransport() {
        return (Transport) Proxy.newProxyInstance(Transport.class.getClassLoader(), new Class<?>[]{Transport.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    /** {@link CatalogView} que delega ao {@link CatalogService} real e conta as leituras que importam ao custo. */
    private static final class CountingCatalog implements CatalogView {
        private final CatalogService delegate;
        final AtomicLong localCopies = new AtomicLong();
        final AtomicLong strongReads = new AtomicLong();
        final AtomicLong pendingReads = new AtomicLong();
        final AtomicLong rebuilds = new AtomicLong();

        CountingCatalog(CatalogService delegate) {
            this.delegate = delegate;
        }

        void resetCounters() {
            localCopies.set(0);
            strongReads.set(0);
            pendingReads.set(0);
            rebuilds.set(0);
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            strongReads.incrementAndGet();
            return delegate.placementStrong(seriesKey);
        }

        @Override
        public Optional<SeriesPlacement> placementLocal(String seriesKey) {
            return delegate.placementLocal(seriesKey);
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return delegate.nodeStatusStrong(nodeId);
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return delegate.nodesLocal();
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            localCopies.incrementAndGet();
            return delegate.placementsLocal();
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            delegate.putPlacement(seriesKey, placement);
        }

        @Override
        public void removePlacement(String seriesKey) {
            delegate.removePlacement(seriesKey);
        }

        @Override
        public Map<String, SeriesPlacement> pendingDeletionsLocal() {
            pendingReads.incrementAndGet();
            return delegate.pendingDeletionsLocal();
        }

        @Override
        public int rebuildPendingDeletions() {
            rebuilds.incrementAndGet();
            return delegate.rebuildPendingDeletions();
        }

        @Override
        public void notePendingDeletion(String seriesKey) {
            delegate.notePendingDeletion(seriesKey);
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            delegate.putNodeStatus(status);
        }

        @Override
        public Object placementLock(String seriesKey) {
            return delegate.placementLock(seriesKey);
        }
    }

    /**
     * {@link ClusterRpc} fake das fases da exclusão. O script devolve a resposta (ou {@code null} para o padrão
     * {@code DELETED}) ou lança a falha da fase, por nó e comando.
     */
    private static final class ScriptedRpc implements ClusterRpc {
        private volatile BiFunction<String, String, DeleteResult> script = (node, command) -> null;

        void script(BiFunction<String, String, DeleteResult> script) {
            this.script = script;
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            DeleteResult scripted = script.apply(target.value(), command);
            DeleteResult result = scripted != null ? scripted
                    : DeleteResult.of(DeleteStatus.DELETED, ((DeleteControlRequest) body).placement().ownerNodeId());
            return responseType.cast(result);
        }

        @Override
        public NodeId localId() {
            return NodeId.of(SELF);
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.of(localId());
        }
    }
}
