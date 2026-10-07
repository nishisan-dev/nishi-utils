package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorageException;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link NgrrdWriter#checkpointAsync()}: ordem FIFO com as escritas, chamador nunca
 * bloqueado pela worker, writer fechado, falhas propagadas pela future e a convivência com o
 * Shutdown — nenhuma future pode ficar pendente para sempre.
 */
@Timeout(30)
class NgrrdWriterCheckpointAsyncTest {

    private static final long START_MS = 1_747_339_200L * 1000L;
    private static final String SERIES = "device:r1/iface:eth0";

    private NgrrdDefinition definition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return NgrrdYamlLoader.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8), k -> null);
        }
    }

    @Test
    void checkpointAsyncConcluiSoDepoisDasEscritasAnterioresSemBloquearOChamador() throws Exception {
        GatedStorage storage = new GatedStorage();
        ReentrantReadWriteLock seriesLock = new ReentrantReadWriteLock();
        try (NgrrdWriter writer = new NgrrdWriter(definition(), storage, SERIES, null, seriesLock)) {
            int forcesBefore = storage.forces.get();
            // Segura o read-lock da série: a worker trava no write-lock da primeira escrita.
            seriesLock.readLock().lock();
            CompletableFuture<Void> future;
            try {
                writer.write("in_octets", new Sample(START_MS, 1_000_000L));
                long started = System.nanoTime();
                future = writer.checkpointAsync();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertTrue(elapsedMs < 1_000, "checkpointAsync bloqueou o chamador por " + elapsedMs + " ms");
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> future.get(200, TimeUnit.MILLISECONDS),
                        "o checkpoint não pode concluir antes da escrita enfileirada antes dele");
            } finally {
                seriesLock.readLock().unlock();
            }
            assertDoesNotThrow(() -> future.get(10, TimeUnit.SECONDS));
            assertTrue(storage.forces.get() > forcesBefore, "a escrita anterior deve ter sido forçada pelo checkpoint");
        }
    }

    @Test
    void checkpointAsyncNaoEsperaUmForceLento() throws Exception {
        GatedStorage storage = new GatedStorage();
        try (NgrrdWriter writer = new NgrrdWriter(definition(), storage, SERIES)) {
            writer.write("in_octets", new Sample(START_MS, 1_000_000L));
            storage.gateForces();
            CompletableFuture<Void> future = writer.checkpointAsync();
            assertTrue(storage.awaitBlockedForce(), "a worker deveria estar presa no force");
            assertFalse(future.isDone());
            storage.releaseForces();
            assertDoesNotThrow(() -> future.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void writerFechadoDevolveFutureJaConcluida() throws Exception {
        NgrrdWriter writer = new NgrrdWriter(definition(), new GatedStorage(), SERIES);
        writer.close();
        CompletableFuture<Void> future = writer.checkpointAsync();
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    void falhaNoForceFalhaAFutureComAMesmaExcecaoDoCheckpointSincrono() throws Exception {
        GatedStorage storage = new GatedStorage();
        NgrrdWriter writer = new NgrrdWriter(definition(), storage, SERIES);
        try {
            writer.write("in_octets", new Sample(START_MS, 1_000_000L));
            storage.failForce = true;
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> writer.checkpointAsync().get(10, TimeUnit.SECONDS));
            assertInstanceOf(NgrrdStorageException.class, failure.getCause());
            // O checkpoint síncrono continua lançando o tipo original, sem embrulho.
            assertThrows(NgrrdStorageException.class, writer::checkpoint);
        } finally {
            storage.failForce = false;
            writer.close();
        }
    }

    @Test
    void falhaAnteriorDeEscritaFalhaAFuture() throws Exception {
        GatedStorage storage = new GatedStorage();
        try (NgrrdWriter writer = new NgrrdWriter(definition(), storage, SERIES)) {
            storage.failWrite = true;
            // Só o avanço de slot grava células: escreve passos seguidos até a falha aparecer.
            for (int i = 0; i < 5; i++) {
                try {
                    writer.write("in_octets", new Sample(START_MS + i * 300_000L, 1_000L + i * 100L));
                } catch (IllegalStateException expectedAfterFailure) {
                    break;
                }
            }
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> writer.checkpointAsync().get(10, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            storage.failWrite = false;
            ExecutionException again = assertThrows(ExecutionException.class,
                    () -> writer.checkpointAsync().get(10, TimeUnit.SECONDS),
                    "o writer envenenado continua falhando os checkpoints seguintes");
            assertInstanceOf(IllegalStateException.class, again.getCause());
        }
    }

    @Test
    void shutdownEnfileiradoDepoisDosSyncsConcluiTodasAsFutures() throws Exception {
        GatedStorage storage = new GatedStorage();
        NgrrdWriter writer = new NgrrdWriter(definition(), storage, SERIES);
        writer.write("in_octets", new Sample(START_MS, 1_000_000L));
        storage.gateForces();
        CompletableFuture<Void> first = writer.checkpointAsync();
        assertTrue(storage.awaitBlockedForce());
        writer.write("in_octets", new Sample(START_MS + 300_000L, 2_000_000L));
        CompletableFuture<Void> second = writer.checkpointAsync();
        CompletableFuture<Void> closing = CompletableFuture.runAsync(writer::close);
        assertFalse(second.isDone());
        storage.releaseForces();
        assertDoesNotThrow(() -> first.get(10, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> second.get(10, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> closing.get(10, TimeUnit.SECONDS));
    }

    @Test
    void syncQueEntraNaFilaDepoisDoShutdownConcluiComODesfechoDoCheckpointFinal() throws Exception {
        NgrrdWriter closedWithCheckpoint = new NgrrdWriter(definition(), new GatedStorage(), SERIES);
        closedWithCheckpoint.close();
        // Reproduz a corrida: a checagem de `closed` passou antes do Shutdown, o enqueue vem depois.
        reopenClosedFlag(closedWithCheckpoint);
        CompletableFuture<Void> orphan = closedWithCheckpoint.checkpointAsync();
        assertDoesNotThrow(() -> orphan.get(5, TimeUnit.SECONDS));

        NgrrdWriter closedForDeletion = new NgrrdWriter(definition(), new GatedStorage(), SERIES);
        closedForDeletion.closeForDeletion();
        reopenClosedFlag(closedForDeletion);
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> closedForDeletion.checkpointAsync().get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
    }

    @Test
    void checkpointSincronoDelegaParaAFilaEEsperaOResultado() throws Exception {
        GatedStorage storage = new GatedStorage();
        try (NgrrdWriter writer = new NgrrdWriter(definition(), storage, SERIES, null,
                new ReentrantReadWriteLock(), Durability.FSYNC)) {
            writer.write("in_octets", new Sample(START_MS, 1_000_000L));
            int forcesBefore = storage.forces.get();
            writer.checkpoint();
            assertEquals(forcesBefore + 1, storage.forces.get());
            writer.flush();
            assertEquals(forcesBefore + 1, storage.forces.get(), "sem escrita nova o flush é idle-skip");
        }
    }

    private static void reopenClosedFlag(NgrrdWriter writer) throws ReflectiveOperationException {
        Field closed = NgrrdWriter.class.getDeclaredField("closed");
        closed.setAccessible(true);
        closed.setBoolean(writer, false);
    }

    /**
     * Storage em memória com {@code force()} que pode ser travado (writer lento) ou configurado
     * para falhar, e escrita que pode falhar.
     */
    private static final class GatedStorage implements NgrrdStorage, SeriesChannelProvider {

        private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        private final AtomicInteger forces = new AtomicInteger();
        private volatile CountDownLatch forceGate;
        private final CountDownLatch forceBlocked = new CountDownLatch(1);
        volatile boolean failForce;
        volatile boolean failWrite;

        void gateForces() {
            forceGate = new CountDownLatch(1);
        }

        void releaseForces() {
            CountDownLatch gate = forceGate;
            forceGate = null;
            if (gate != null) {
                gate.countDown();
            }
        }

        boolean awaitBlockedForce() throws InterruptedException {
            return forceBlocked.await(10, TimeUnit.SECONDS);
        }

        @Override
        public void put(String key, byte[] data) {
            objects.put(key, data.clone());
        }

        @Override
        public Optional<byte[]> get(String key) {
            byte[] b = objects.get(key);
            return b == null ? Optional.empty() : Optional.of(b.clone());
        }

        @Override
        public boolean exists(String key) {
            return objects.containsKey(key);
        }

        @Override
        public void delete(String key) {
            objects.remove(key);
        }

        @Override
        public List<String> list(String prefix) {
            List<String> out = new ArrayList<>();
            for (String k : objects.keySet()) {
                if (k.startsWith(prefix)) {
                    out.add(k);
                }
            }
            return out;
        }

        @Override
        public void atomicReplace(String key, byte[] data) {
            put(key, data);
        }

        @Override
        public boolean seriesExists(String key) {
            return objects.containsKey(key);
        }

        @Override
        public SeriesChannel openSeries(String key) {
            return new MemChannel(key);
        }

        private final class MemChannel implements SeriesChannel {
            private final String key;
            private byte[] image;

            MemChannel(String key) {
                this.key = key;
                byte[] existing = objects.get(key);
                this.image = existing == null ? new byte[0] : existing.clone();
            }

            @Override
            public long size() {
                return image.length;
            }

            @Override
            public void allocate(long totalBytes) {
                int n = (int) totalBytes;
                if (image.length != n) {
                    image = Arrays.copyOf(image, n);
                }
            }

            @Override
            public byte[] readRegion(long offset, int len) {
                int off = (int) offset;
                return Arrays.copyOfRange(image, off, off + len);
            }

            @Override
            public void writeRegion(long offset, byte[] data) {
                if (failWrite) {
                    throw new NgrrdStorageException("falha simulada de escrita assíncrona");
                }
                int off = (int) offset;
                int end = off + data.length;
                if (end > image.length) {
                    image = Arrays.copyOf(image, end);
                }
                System.arraycopy(data, 0, image, off, data.length);
            }

            @Override
            public void force() {
                CountDownLatch gate = forceGate;
                if (gate != null) {
                    forceBlocked.countDown();
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new NgrrdStorageException("force interrompido");
                    }
                }
                if (failForce) {
                    throw new NgrrdStorageException("falha simulada de force()");
                }
                forces.incrementAndGet();
                objects.put(key, image.clone());
            }

            @Override
            public void close() {
                force();
            }
        }
    }
}
