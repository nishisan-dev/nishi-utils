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
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre as regras de reaproveitamento de {@link SeriesHandleCache}: handle aberto sem criar é somente
 * leitura, {@code open} com criação promove um handle somente leitura em cache (nunca rebaixa um
 * gravável) e um handle fechado nunca é devolvido.
 */
class SeriesHandleCacheTest {

    private static final String SERIES_KEY = "device:r1/iface:eth0";
    private static final NodeId OWNER = NodeId.of("storage-a");
    private static final Duration JOIN_TIMEOUT = Duration.ofSeconds(10);

    private SeriesHandleCache cache;
    private RecordingClusterRpc rpc;
    private RecordingWriteBuffer dispatcher;
    private AtomicInteger opened;

    @BeforeEach
    void setUp() {
        cache = new SeriesHandleCache();
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER.value(), null));
        dispatcher = new RecordingWriteBuffer();
        opened = new AtomicInteger();
    }

    @Test
    void sequenciaAbreSemCriarFechaEAbreComCriarDevolveHandleNovoGravavel() {
        RemoteSeriesHandle readOnly = open(false);
        readOnly.close();

        RemoteSeriesHandle writable = open(true);

        assertNotSame(readOnly, writable);
        assertEquals(2, opened.get());
        assertSame(writable, cache.get(SERIES_KEY));
        writable.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size());
        assertEquals(List.of(Commands.OPEN, Commands.OPEN), commands(),
                "o close do handle somente leitura não envia CLOSE");
    }

    @Test
    void abrirComCriarSobreSomenteLeituraAbertoPromoveOMesmoHandle() {
        RemoteSeriesHandle readOnly = open(false);

        RemoteSeriesHandle writable = open(true);

        assertSame(readOnly, writable);
        assertEquals(1, opened.get(), "promoção não abre um handle novo");
        writable.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size());
        writable.close();
        assertEquals(List.of(Commands.OPEN, Commands.CLOSE), commands(), "promovido, o close envia CLOSE");
        assertNull(cache.get(SERIES_KEY));
    }

    @Test
    void abrirSemCriarSobreHandleGravavelDevolveOExistente() {
        RemoteSeriesHandle writable = open(true);

        RemoteSeriesHandle again = open(false);

        assertSame(writable, again);
        assertEquals(1, opened.get());
        again.write("in_octets", new Sample(1L, 1.0));
        assertEquals(1, dispatcher.enqueued.size(), "nunca rebaixa: o handle continua gravável");
    }

    @Test
    void abrirSemCriarSobreSomenteLeituraAbertoDevolveOExistente() {
        RemoteSeriesHandle readOnly = open(false);

        assertSame(readOnly, open(false));
        assertEquals(1, opened.get());
    }

    @Test
    void handleFechadoAindaNoMapaNuncaEhDevolvido() {
        // onClose que não remove do mapa: simula um handle fechado cuja remoção ainda não aconteceu.
        RemoteSeriesHandle stale = cache.open(SERIES_KEY, false, () -> newOpenedHandle(false, (key, h) -> { }));
        stale.close();
        assertSame(stale, cache.get(SERIES_KEY));

        RemoteSeriesHandle forRead = open(false);
        assertNotSame(stale, forRead);
        forRead.close();

        RemoteSeriesHandle staleWritable = cache.open(SERIES_KEY, true, () -> newOpenedHandle(true, (key, h) -> { }));
        staleWritable.close();
        RemoteSeriesHandle forWrite = open(true);
        assertNotSame(staleWritable, forWrite);
        assertTrue(forWrite.isOpen());
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
    void promocaoConcorrenteComCloseNuncaDevolveHandleJaFechado() throws InterruptedException {
        for (int i = 0; i < 300; i++) {
            setUp();
            RemoteSeriesHandle readOnly = open(false);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<RemoteSeriesHandle> promoted = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                awaitQuietly(start);
                readOnly.close();
            }, "test-close");
            Thread promoter = new Thread(() -> {
                awaitQuietly(start);
                try {
                    RemoteSeriesHandle handle = open(true);
                    // No instante em que é devolvido, o handle precisa estar aberto e gravável.
                    if (!handle.isOpen() && handle != readOnly) {
                        failure.set(new AssertionError("handle novo devolvido já fechado"));
                    }
                    promoted.set(handle);
                } catch (RuntimeException | Error e) {
                    failure.set(e);
                }
            }, "test-promote");
            closer.start();
            promoter.start();
            start.countDown();
            closer.join(JOIN_TIMEOUT.toMillis());
            promoter.join(JOIN_TIMEOUT.toMillis());

            assertNull(failure.get(), () -> "iteração falhou: " + failure.get());
            RemoteSeriesHandle result = promoted.get();
            long closeRpcs = commands().stream().filter(Commands.CLOSE::equals).count();
            if (result == readOnly) {
                // A promoção venceu: o close concorrente passou a fechar um handle gravável, com CLOSE.
                assertEquals(1, closeRpcs, "promovido antes do close, o close precisa ser o remoto");
                assertFalse(result.isOpen());
            } else {
                // O close venceu: a promoção falhou e o cliente abriu um handle novo, que segue aberto.
                assertTrue(result.isOpen(), "o handle novo não pode estar fechado");
                assertEquals(0, closeRpcs, "o close do handle somente leitura é local");
                assertEquals(2, opened.get());
                assertSame(result, cache.get(SERIES_KEY));
            }
        }
    }

    private RemoteSeriesHandle open(boolean createIfMissing) {
        return cache.open(SERIES_KEY, createIfMissing, () -> newOpenedHandle(createIfMissing, cache::remove));
    }

    private RemoteSeriesHandle newOpenedHandle(boolean createIfMissing, BiConsumer<String, RemoteSeriesHandle> onClose) {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMillis(5), Duration.ofMillis(50));
        RemoteSeriesHandle handle = new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(),
                Ngrrd.OpenOptions.defaults().withCreateIfMissing(createIfMissing), new FixedPlacementLookup(), rpc,
                dispatcher, retry, Duration.ofSeconds(5), Duration.ofSeconds(5), Clock.systemUTC(), onClose);
        handle.open();
        opened.incrementAndGet();
        return handle;
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
