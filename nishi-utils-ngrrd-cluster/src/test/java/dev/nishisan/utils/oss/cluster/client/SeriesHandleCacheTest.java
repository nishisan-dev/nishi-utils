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
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadPresetResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre as regras de reaproveitamento de {@link SeriesHandleCache}: no máximo um handle principal por
 * chave; {@code open} com criação sobre um somente leitura cria um gravável novo que o substitui (o
 * antigo fica destacado e fecha localmente); {@code open} sem criar sobre um gravável devolve uma vista
 * somente leitura que nunca fecha o gravável; um handle fechado nunca é devolvido.
 */
class SeriesHandleCacheTest {

    private static final String SERIES_KEY = "device:r1/iface:eth0";
    private static final NodeId OWNER = NodeId.of("storage-a");
    private static final Duration JOIN_TIMEOUT = Duration.ofSeconds(10);
    private static final Ngrrd.OpenOptions READ_ONLY = Ngrrd.OpenOptions.defaults().withCreateIfMissing(false);

    private SeriesHandleCache cache;
    private RecordingClusterRpc rpc;
    private RecordingWriteBuffer dispatcher;
    private AtomicInteger opened;

    @BeforeEach
    void setUp() {
        cache = new SeriesHandleCache();
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        rpc.respondDefault((cmd, body) -> Commands.READ_PRESET.equals(cmd)
                ? new ReadPresetResponse(SeriesStatus.OK, OWNER.value(), Map.of("in_bps", result()), null)
                : new SeriesStatusResponse(SeriesStatus.OK, OWNER.value(), null, Boolean.TRUE));
        dispatcher = new RecordingWriteBuffer();
        opened = new AtomicInteger();
    }

    @Test
    void sequenciaAbreSemCriarFechaEAbreComCriarDevolveHandleNovoGravavel() {
        NgrrdHandle readOnly = open(false);
        readOnly.close();

        NgrrdHandle writable = open(true);

        assertNotSame(readOnly, writable);
        assertEquals(2, opened.get());
        assertSame(writable, cache.get(SERIES_KEY));
        writable.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size());
        assertEquals(List.of(Commands.OPEN, Commands.OPEN), commands(),
                "o close do handle somente leitura não envia CLOSE");
    }

    @Test
    void abrirComCriarSobreSomenteLeituraAbertoCriaGravavelNovoComAsOpcoesDeQuemPediuCriar() {
        NgrrdHandle readOnly = open(Ngrrd.OpenOptions.of(Durability.OS_CACHE, OnGeometryChange.FAIL)
                .withCreateIfMissing(false));

        NgrrdHandle writable = open(Ngrrd.OpenOptions.of(Durability.FSYNC, OnGeometryChange.MIGRATE));

        assertNotSame(readOnly, writable);
        assertEquals(2, opened.get(), "o somente leitura não é promovido: um gravável novo é aberto");
        assertSame(writable, cache.get(SERIES_KEY), "o gravável substitui o somente leitura no mapa");
        OpenRequest writableOpen = (OpenRequest) rpc.calls().get(1).body();
        assertNull(writableOpen.createIfMissing(), "o gravável abre com criação");
        assertEquals(Durability.FSYNC, writableOpen.durability());
        assertEquals(OnGeometryChange.MIGRATE, writableOpen.onGeometryChange());

        writable.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size());

        // O somente leitura antigo continua válido para quem o tem, e o close dele é local.
        assertEquals(1, readOnly.read("daily").size());
        readOnly.close();
        assertSame(writable, cache.get(SERIES_KEY), "o close do destacado não remove o gravável");
        assertTrue(((RemoteSeriesHandle) writable).isOpen());
        writable.write("in_octets", new Sample(2L, 2.0));
        assertEquals(2, dispatcher.enqueued.size());
        assertEquals(List.of(Commands.OPEN, Commands.OPEN, Commands.READ_PRESET), commands(),
                "nenhum CLOSE remoto pelo somente leitura destacado");
        assertThrows(NgrrdClusterException.class, () -> readOnly.read("daily"));
    }

    @Test
    void abrirSemCriarSobreGravavelDevolveVistaSomenteLeitura() {
        NgrrdHandle writable = open(true);

        NgrrdHandle view = open(false);

        assertNotSame(writable, view);
        assertEquals(1, opened.get(), "a vista não abre nada");
        assertEquals(SERIES_KEY, view.seriesKey());
        assertEquals(1, view.read("daily").size(), "a leitura delega ao gravável");
        IllegalStateException onWrite = assertThrows(IllegalStateException.class,
                () -> view.write("in_octets", new Sample(1L, 1.0)));
        assertTrue(onWrite.getMessage().contains("somente leitura"), onWrite.getMessage());
        assertThrows(IllegalStateException.class, view::flush);
        assertThrows(IllegalStateException.class, view::checkpoint);
        assertNotSame(view, open(false), "cada open sem criar ganha a sua vista");
        assertSame(writable, cache.get(SERIES_KEY), "a vista não entra no mapa");
    }

    @Test
    void closeDaVistaNaoFechaOGravavel() {
        NgrrdHandle writable = open(true);
        NgrrdHandle view = open(false);

        view.close();
        view.close();

        assertTrue(((RemoteSeriesHandle) writable).isOpen());
        writable.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size(), "a ingestão continua escrevendo");
        assertEquals(List.of(Commands.OPEN), commands(), "o close da vista é local");
        assertSame(writable, cache.get(SERIES_KEY));
        NgrrdClusterException closed = assertThrows(NgrrdClusterException.class, () -> view.read("daily"));
        assertEquals(ErrorCode.CLOSED, closed.code());
    }

    @Test
    void abrirSemCriarSobreSomenteLeituraAbertoDevolveOExistente() {
        NgrrdHandle readOnly = open(false);

        assertSame(readOnly, open(false));
        assertEquals(1, opened.get());
    }

    @Test
    void abrirComCriarSobreGravavelAbertoDevolveOExistente() {
        NgrrdHandle writable = open(true);

        assertSame(writable, open(true));
        assertEquals(1, opened.get());
    }

    @Test
    void handleFechadoAindaNoMapaNuncaEhDevolvido() {
        // onClose que não remove do mapa: simula um handle fechado cuja remoção ainda não aconteceu.
        NgrrdHandle stale = cache.open(SERIES_KEY, false, () -> newOpenedHandle(READ_ONLY, (key, h) -> { }));
        stale.close();
        assertSame(stale, cache.get(SERIES_KEY));

        NgrrdHandle forRead = open(false);
        assertNotSame(stale, forRead);
        assertSame(forRead, cache.get(SERIES_KEY));
        forRead.close();

        NgrrdHandle staleWritable = cache.open(SERIES_KEY, true,
                () -> newOpenedHandle(Ngrrd.OpenOptions.defaults(), (key, h) -> { }));
        staleWritable.close();
        NgrrdHandle forWrite = open(true);
        assertNotSame(staleWritable, forWrite);
        assertTrue(((RemoteSeriesHandle) forWrite).isOpen());
        assertSame(forWrite, cache.get(SERIES_KEY));
    }

    @Test
    void openQueFalhaNaoPublicaHandle() {
        assertThrows(SeriesNotFoundException.class, () -> cache.open(SERIES_KEY, false, () -> {
            throw new SeriesNotFoundException(SERIES_KEY, SeriesNotFoundException.Reason.NOT_PLACED);
        }));

        assertNull(cache.get(SERIES_KEY));
        assertEquals(0, cache.size());
    }

    @Test
    void principalRemovidoDuranteAAberturaPublicaOHandleNovoSemAbrirDeNovo() {
        NgrrdHandle readOnly = open(false);

        NgrrdHandle writable = cache.open(SERIES_KEY, true, () -> {
            // O somente leitura fecha (e sai do mapa) enquanto o gravável está sendo aberto.
            readOnly.close();
            return newOpenedHandle(Ngrrd.OpenOptions.defaults(), cache::remove);
        });

        assertSame(writable, cache.get(SERIES_KEY));
        assertEquals(2, opened.get(), "a troca condicional falhou, mas o handle já aberto é publicado");
    }

    @Test
    void gravavelPublicadoPorOutraAberturaVenceEOHandleNovoEhDescartadoSemRpc() {
        open(false);
        AtomicReference<NgrrdHandle> winner = new AtomicReference<>();
        AtomicReference<RemoteSeriesHandle> loser = new AtomicReference<>();

        NgrrdHandle result = cache.open(SERIES_KEY, true, () -> {
            RemoteSeriesHandle mine = newOpenedHandle(Ngrrd.OpenOptions.defaults(), cache::remove);
            loser.set(mine);
            // Outra abertura com criação publica um gravável antes desta terminar a troca.
            winner.set(cache.open(SERIES_KEY, true, () -> newOpenedHandle(Ngrrd.OpenOptions.defaults(),
                    cache::remove)));
            return mine;
        });

        assertSame(winner.get(), result);
        assertSame(winner.get(), cache.get(SERIES_KEY));
        assertFalse(loser.get().isOpen(), "o gravável que perdeu a publicação é descartado");
        assertTrue(commands().stream().noneMatch(Commands.CLOSE::equals),
                "o descarte é local: nenhum CLOSE fecharia a série por baixo do vencedor");
    }

    @Test
    void aberturasComCriarConcorrentesSobreSomenteLeituraRecebemOMesmoGravavel() throws InterruptedException {
        for (int iteration = 0; iteration < 50; iteration++) {
            setUp();
            open(false);
            int threads = 8;
            CountDownLatch start = new CountDownLatch(1);
            Set<NgrrdHandle> results = ConcurrentHashMap.newKeySet();
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                long ts = t;
                workers.add(new Thread(() -> {
                    awaitQuietly(start);
                    try {
                        NgrrdHandle handle = open(true);
                        handle.write("in_octets", new Sample(ts, 1.0));
                        results.add(handle);
                    } catch (RuntimeException | Error e) {
                        failures.add(e);
                    }
                }, "test-open-create-" + t));
            }
            workers.forEach(Thread::start);
            start.countDown();
            for (Thread worker : workers) {
                worker.join(JOIN_TIMEOUT.toMillis());
            }

            assertEquals(List.of(), failures, "iteração " + iteration);
            assertEquals(1, results.size(), "todas as aberturas com criação recebem o mesmo gravável");
            assertSame(results.iterator().next(), cache.get(SERIES_KEY));
            assertEquals(threads, dispatcher.enqueued.size());
        }
    }

    @Test
    void aberturaComCriarConcorrenteComCloseDoSomenteLeituraNuncaDevolveHandleFechado()
            throws InterruptedException {
        for (int iteration = 0; iteration < 300; iteration++) {
            setUp();
            NgrrdHandle readOnly = open(false);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<NgrrdHandle> created = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                awaitQuietly(start);
                readOnly.close();
            }, "test-close");
            Thread creator = new Thread(() -> {
                awaitQuietly(start);
                try {
                    created.set(open(true));
                } catch (RuntimeException | Error e) {
                    failure.set(e);
                }
            }, "test-create");
            closer.start();
            creator.start();
            start.countDown();
            closer.join(JOIN_TIMEOUT.toMillis());
            creator.join(JOIN_TIMEOUT.toMillis());

            assertNull(failure.get(), () -> "iteração falhou: " + failure.get());
            RemoteSeriesHandle result = assertInstanceOf(RemoteSeriesHandle.class, created.get());
            assertNotSame(readOnly, result);
            assertTrue(result.isOpen(), "o gravável devolvido não pode estar fechado");
            assertSame(result, cache.get(SERIES_KEY));
            assertTrue(commands().stream().noneMatch(Commands.CLOSE::equals), "o close do somente leitura é local");
        }
    }

    private NgrrdHandle open(boolean createIfMissing) {
        return open(Ngrrd.OpenOptions.defaults().withCreateIfMissing(createIfMissing));
    }

    private NgrrdHandle open(Ngrrd.OpenOptions options) {
        return cache.open(SERIES_KEY, options.createIfMissing(), () -> newOpenedHandle(options, cache::remove));
    }

    private RemoteSeriesHandle newOpenedHandle(Ngrrd.OpenOptions options,
            BiConsumer<String, RemoteSeriesHandle> onClose) {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMillis(5), Duration.ofMillis(50));
        RemoteSeriesHandle handle = new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(),
                options, new FixedPlacementLookup(), rpc, dispatcher, retry, Duration.ofSeconds(5),
                Duration.ofSeconds(5), Clock.systemUTC(), onClose, CapabilityFixtures.advertisingAll());
        handle.open();
        opened.incrementAndGet();
        return handle;
    }

    private static SeriesResult result() {
        return new SeriesResult("in_bps", "rra", ConsolidationFunction.AVERAGE, 300, List.of());
    }

    private List<String> commands() {
        return rpc.calls().stream().map(RecordingClusterRpc.Recorded::command).toList();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** {@link PlacementLookup} fake: a série sempre existe em {@link #OWNER}. */
    private static final class FixedPlacementLookup implements PlacementLookup {
        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            return SeriesPlacement.active(OWNER.value(), 0L);
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            return SeriesPlacement.active(OWNER.value(), 0L);
        }

        @Override
        public SeriesPlacement resolveExistingAtLeader(String seriesKey, Duration maxWait) {
            return SeriesPlacement.active(OWNER.value(), 0L);
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            return Optional.of(SeriesPlacement.active(OWNER.value(), 0L));
        }

        @Override
        public void invalidate(String seriesKey) {
            // sem cache neste fake
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
            // dono fixo neste fake
        }
    }

    /** {@link WriteBuffer} fake: só grava o que foi enfileirado. */
    private static final class RecordingWriteBuffer implements WriteBuffer {
        final List<SeriesWrite> enqueued = new CopyOnWriteArrayList<>();

        @Override
        public void enqueue(String ownerNodeId, SeriesWrite write) {
            enqueued.add(write);
        }

        @Override
        public void flushNodeSync(String ownerNodeId) {
            // no-op
        }

        @Override
        public void flushNodeSync(String ownerNodeId, Duration maxWait) {
            // no-op
        }
    }
}
