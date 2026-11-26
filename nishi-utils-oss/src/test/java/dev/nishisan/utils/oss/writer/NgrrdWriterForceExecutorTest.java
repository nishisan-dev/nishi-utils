package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorageException;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o force do checkpoint fora da thread do writer ({@code checkpointForceExecutor}):
 * a fase A continua na worker, a fase B (o ticket de {@link SeriesChannel#prepareForce()})
 * roda no executor, e nenhuma future de checkpoint conclui antes de o force que a cobre
 * terminar — inclusive os Syncs coalescidos e o {@code close()}.
 *
 * <p>O canal espião registra cada chamada. Sem executor, o registro tem de ser idêntico ao
 * do writer anterior ao force destacável ({@link #GOLDEN_LOG}, capturado no commit 0ee60e4
 * com o mesmo cenário), e a imagem durável final, a mesma byte a byte ({@link #GOLDEN_SHA256}).</p>
 */
@Timeout(60)
class NgrrdWriterForceExecutorTest {

    private static final long START_MS = 1_747_339_200L * 1000L;
    private static final long STEP_MS = 300_000L;
    private static final String SERIES = "device:r1/iface:eth0";
    private static final String FORCE_THREAD_PREFIX = "ngrrd-test-force-";

    /** Chamadas ao canal no cenário {@link #runReferenceScenario}, com o writer de 0ee60e4. */
    private static final List<String> GOLDEN_LOG = List.of((""
            + "size,allocate:346104,write:0:346104,force,write:504:8,write:512:8,write:504:8,"
            + "write:138744:8,write:138752:8,write:138744:8,write:232:272,write:520:8,write:528:8,"
            + "write:520:8,write:138760:8,write:138768:8,write:138760:8,write:232:272,write:536:8,"
            + "write:544:8,write:536:8,write:138776:8,write:138784:8,write:138776:8,write:276984:8,"
            + "write:276992:8,write:276984:8,write:528:8,write:138768:8,write:276992:8,write:232:272,"
            + "force,write:536:8,write:138776:8,write:276984:8,write:528:8,write:138768:8,"
            + "write:276992:8,write:232:272,force,write:536:8,write:138776:8,write:232:272,write:552:8,"
            + "write:560:8,write:552:8,write:138792:8,write:138800:8,write:138792:8,write:276984:8,"
            + "write:528:8,write:138768:8,write:276992:8,write:232:272,force,close,force").split(","));

    /** SHA-256 da imagem durável ao fim do mesmo cenário, com o writer de 0ee60e4. */
    private static final String GOLDEN_SHA256 = "9fc2ec42222eb7ae3760479f19100899cc0b127d938f27681494665b147139f8";

    private final List<ExecutorService> pools = new ArrayList<>();

    @AfterEach
    void shutdownPools() {
        pools.forEach(ExecutorService::shutdownNow);
    }

    private NgrrdDefinition definition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return NgrrdYamlLoader.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8), k -> null);
        }
    }

    private TrackingExecutor forcePool(int threads) {
        AtomicInteger seq = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, FORCE_THREAD_PREFIX + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        pools.add(pool);
        return new TrackingExecutor(pool);
    }

    private NgrrdWriter writer(SpyStorage storage, Executor forceExecutor, NgrrdMetricsListener metrics)
            throws Exception {
        return new NgrrdWriter(definition(), storage, SERIES, metrics, new ReentrantReadWriteLock(),
                Durability.FSYNC, OnGeometryChange.FAIL, System::currentTimeMillis, true, forceExecutor);
    }

    /** Mesmo cenário usado para capturar {@link #GOLDEN_LOG} e {@link #GOLDEN_SHA256}. */
    private static void runReferenceScenario(NgrrdWriter w) {
        w.write("in_octets", new Sample(START_MS, 1_000_000L));
        w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
        w.write("out_octets", new Sample(START_MS + STEP_MS, 500_000L));
        w.write("in_octets", new Sample(START_MS + 2 * STEP_MS, 1_250_000L));
        w.checkpoint();
        w.checkpoint();
        w.write("in_octets", new Sample(START_MS + 2 * STEP_MS + 60_000L, 1_300_000L));
        w.checkpoint();
        w.write("in_octets", new Sample(START_MS + 3 * STEP_MS, 1_400_000L));
        w.close();
    }

    @Test
    void semExecutorForceInlineComoAntes() throws Exception {
        // Canal que destaca o force, mas writer sem executor: nada de prepareForce, tudo inline.
        SpyStorage detachable = new SpyStorage(true);
        runReferenceScenario(writer(detachable, null, null));
        assertEquals(GOLDEN_LOG, detachable.log, "sem executor, a ordem de chamadas ao canal é a de antes");
        assertEquals(GOLDEN_SHA256, detachable.durableSha256(), "sem executor, a imagem durável é a de antes");
        assertTrue(detachable.ticketThreads.isEmpty());
        assertTrue(detachable.forceThreads.stream().skip(1).allMatch(n -> n.startsWith("ngrrd-writer-pool-")),
                "os forces de checkpoint e de close rodam na worker: " + detachable.forceThreads);

        // Com executor e um canal que não destaca (disco local, S3): o force continua inline e a
        // sequência é a mesma, só com a chamada a prepareForce antes de cada force de checkpoint.
        SpyStorage inline = new SpyStorage(false);
        runReferenceScenario(writer(inline, forcePool(1), null));
        assertEquals(GOLDEN_LOG, inline.log.stream().filter(c -> !c.equals("prepareForce")).toList());
        // Dois checkpoints com dado novo; o idle é coalescido e o do close é sempre inline.
        assertEquals(2, Collections.frequency(inline.log, "prepareForce"));
        assertEquals(GOLDEN_SHA256, inline.durableSha256());
        assertTrue(inline.ticketThreads.isEmpty());
    }

    @Test
    void comExecutorFaseBRodaNoPool() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        TrackingExecutor pool = forcePool(1);
        try (NgrrdWriter w = writer(storage, pool, null)) {
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            w.checkpoint();
            assertEquals(1, storage.ticketThreads.size(), "o checkpoint só conclui depois do force do pool");
            assertTrue(storage.ticketThreads.get(0).startsWith(FORCE_THREAD_PREFIX),
                    "a fase B roda no executor: " + storage.ticketThreads);
            int prepare = storage.log.indexOf("prepareForce");
            assertTrue(prepare > 0 && storage.log.get(prepare - 1).equals("write:232:272"),
                    "a fase A (live-state) vem antes da preparação: " + storage.log);
            assertArrayEquals(storage.image(), storage.durable(), "o force do pool tornou durável a imagem");
        }
        // Mesmo cenário de referência: a imagem durável final é a mesma, byte a byte.
        SpyStorage reference = new SpyStorage(true);
        runReferenceScenario(writer(reference, forcePool(2), null));
        assertEquals(GOLDEN_SHA256, reference.durableSha256());
        assertEquals(2, reference.ticketThreads.size(), "os dois checkpoints com dado novo; o do close é inline");
        assertTrue(reference.ticketThreads.stream().allMatch(n -> n.startsWith(FORCE_THREAD_PREFIX)));
    }

    @Test
    void syncCoalescidoEsperaForceEmVoo() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        CoalescedCounter coalesced = new CoalescedCounter();
        try (NgrrdWriter w = writer(storage, forcePool(1), coalesced)) {
            CountDownLatch gate = storage.gateNextTicket();
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            CompletableFuture<Void> first = w.checkpointAsync();
            storage.awaitTicketStarted();
            CompletableFuture<Void> second = w.checkpointAsync();
            coalesced.await(); // a worker já processou o segundo Sync, sem escrita nova
            assertFalse(second.isDone(), "o Sync coalescido não pode concluir antes do force em voo");
            assertFalse(first.isDone());
            gate.countDown();
            assertDoesNotThrow(() -> first.get(10, TimeUnit.SECONDS));
            assertDoesNotThrow(() -> second.get(10, TimeUnit.SECONDS));
            assertEquals(1, storage.ticketThreads.size(), "o segundo Sync não forçou de novo");
        }
    }

    @Test
    void falhaNoForceRemarcaSujoEProximoSyncForca() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        TrackingExecutor pool = forcePool(1);
        try (NgrrdWriter w = writer(storage, pool, null)) {
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            storage.failNextTicket();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> w.checkpointAsync().get(10, TimeUnit.SECONDS));
            assertInstanceOf(NgrrdStorageException.class, failure.getCause(), "a falha original, sem embrulho");
            pool.awaitTasksFinished(1); // o ForceDone já está na fila do writer
            assertTrue(storage.dirty(), "o ticket remarcou o canal como sujo");

            // Sem escrita nova: o próximo Sync tem de forçar de novo, e com sucesso.
            assertDoesNotThrow(() -> w.checkpointAsync().get(10, TimeUnit.SECONDS));
            assertEquals(2, storage.ticketThreads.size(), "o Sync depois da falha forçou de novo");
            assertArrayEquals(storage.image(), storage.durable());
        }
    }

    @Test
    void syncCoalescidoEntreFalhaEForceDoneFalha() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        try (NgrrdWriter w = writer(storage, forcePool(1), null)) {
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            CountDownLatch gate = storage.gateNextTicket();
            storage.failNextTicket();
            CompletableFuture<Void> first = w.checkpointAsync();
            storage.awaitTicketStarted();
            // O callback roda no executor, entre a conclusão das futures e o enfileiramento do
            // ForceDone: o segundo Sync entra na fila antes dele, sem escrita nova.
            CompletableFuture<CompletableFuture<Void>> enqueued = first.handle((ignored, failure) -> w.checkpointAsync());
            gate.countDown();

            assertThrows(ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS));
            CompletableFuture<Void> second = enqueued.get(10, TimeUnit.SECONDS);
            assertNotNull(second);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> second.get(10, TimeUnit.SECONDS),
                    "o Sync coalescido entre a falha e o ForceDone não pode concluir com sucesso");
            assertInstanceOf(NgrrdStorageException.class, failure.getCause());
            assertEquals(1, storage.ticketThreads.size(), "o segundo Sync foi coalescido, não forçou");
        }
    }

    @Test
    void forceDoneAntigoNaoLimpaForceNovo() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        TrackingExecutor pool = forcePool(2);
        CoalescedCounter coalesced = new CoalescedCounter();
        try (NgrrdWriter w = writer(storage, pool, coalesced)) {
            CountDownLatch firstGate = storage.gateNextTicket();
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            CompletableFuture<Void> first = w.checkpointAsync();
            storage.awaitTicketStarted();

            CountDownLatch secondGate = storage.gateNextTicket();
            w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            CompletableFuture<Void> second = w.checkpointAsync();
            storage.awaitTicketStarted();

            firstGate.countDown();
            assertDoesNotThrow(() -> first.get(10, TimeUnit.SECONDS));
            pool.awaitTasksFinished(1); // ForceDone do primeiro force já na fila

            CompletableFuture<Void> third = w.checkpointAsync();
            coalesced.await(); // processado depois do ForceDone antigo
            assertFalse(third.isDone(), "o ForceDone antigo não pode ter limpado o force novo em voo");
            assertFalse(second.isDone());
            secondGate.countDown();
            assertDoesNotThrow(() -> second.get(10, TimeUnit.SECONDS));
            assertDoesNotThrow(() -> third.get(10, TimeUnit.SECONDS));
            assertEquals(2, storage.ticketThreads.size());
        }
    }

    @Test
    void escritaDuranteForceNaoSePerde() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        try (NgrrdWriter w = writer(storage, forcePool(1), null)) {
            CountDownLatch gate = storage.gateNextTicket();
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            CompletableFuture<Void> first = w.checkpointAsync();
            storage.awaitTicketStarted();
            // Escrita depois da fase A, com o force ainda em voo: vira passo e grava células.
            w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            w.write("in_octets", new Sample(START_MS + 2 * STEP_MS, 1_200_000L));
            gate.countDown();
            assertDoesNotThrow(() -> first.get(10, TimeUnit.SECONDS));

            assertDoesNotThrow(() -> w.checkpointAsync().get(10, TimeUnit.SECONDS));
            assertEquals(2, storage.ticketThreads.size(), "a escrita durante o force exige um novo force");
            assertArrayEquals(storage.image(), storage.durable(), "a escrita durante o force ficou durável");
        }
    }

    @Test
    void closeEsperaForceEmVoo() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        NgrrdWriter w = writer(storage, forcePool(1), null);
        CountDownLatch gate = storage.gateNextTicket();
        w.write("in_octets", new Sample(START_MS, 1_000_000L));
        CompletableFuture<Void> checkpoint = w.checkpointAsync();
        storage.awaitTicketStarted();

        // Sem escrita nova depois da fase A: o canal está limpo e o force do close seria no-op.
        CompletableFuture<Void> closing = CompletableFuture.runAsync(w::close);
        assertThrows(TimeoutException.class, () -> closing.get(300, TimeUnit.MILLISECONDS),
                "o close() não pode voltar com o force em voo");
        gate.countDown();
        assertDoesNotThrow(() -> closing.get(10, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> checkpoint.get(10, TimeUnit.SECONDS));

        int forced = storage.log.indexOf("ticket.done");
        int closed = storage.log.indexOf("close");
        assertTrue(forced >= 0 && forced < closed, "a faixa foi forçada antes do fechamento do canal: "
                + storage.log.subList(Math.max(0, forced - 2), storage.log.size()));
        assertArrayEquals(storage.image(), storage.durable());
    }

    @Test
    void closeComForceFalhoForcaInline() throws Exception {
        SpyStorage storage = new SpyStorage(true);
        NgrrdWriter w = writer(storage, forcePool(1), null);
        CountDownLatch gate = storage.gateNextTicket();
        storage.failNextTicket();
        w.write("in_octets", new Sample(START_MS, 1_000_000L));
        CompletableFuture<Void> checkpoint = w.checkpointAsync();
        storage.awaitTicketStarted();

        CompletableFuture<Void> closing = CompletableFuture.runAsync(w::close);
        gate.countDown();
        assertThrows(ExecutionException.class, () -> checkpoint.get(10, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> closing.get(10, TimeUnit.SECONDS));

        List<String> afterFailure = storage.log.subList(storage.log.indexOf("ticket.fail"), storage.log.size());
        int inlineForce = afterFailure.indexOf("force");
        assertTrue(inlineForce > 0 && inlineForce < afterFailure.indexOf("close"),
                "o fechamento força inline depois da falha, antes de fechar o canal: " + afterFailure);
        String closingThread = storage.forceThreads.get(storage.forceThreads.size() - 2);
        assertTrue(closingThread.startsWith("ngrrd-writer-pool-"), "force inline na worker: " + closingThread);
        assertArrayEquals(storage.image(), storage.durable(), "nada se perde com o force falho no close");
    }

    /** Executor que avisa quando cada tarefa terminou (depois de ela enfileirar o ForceDone). */
    private static final class TrackingExecutor implements Executor {
        private final Executor delegate;
        private final Semaphore finished = new Semaphore(0);

        TrackingExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable task) {
            delegate.execute(() -> {
                try {
                    task.run();
                } finally {
                    finished.release();
                }
            });
        }

        void awaitTasksFinished(int count) throws InterruptedException {
            assertTrue(finished.tryAcquire(count, 10, TimeUnit.SECONDS), "tarefas de force não terminaram");
        }
    }

    /** Listener que sinaliza cada checkpoint coalescido (prova de que a worker processou o Sync). */
    private static final class CoalescedCounter implements NgrrdMetricsListener {
        private final Semaphore coalesced = new Semaphore(0);

        @Override
        public void onCheckpointCoalesced(String seriesKey) {
            coalesced.release();
        }

        void await() throws InterruptedException {
            assertTrue(coalesced.tryAcquire(10, TimeUnit.SECONDS), "o Sync não foi coalescido");
        }
    }

    /**
     * Storage em memória com um único canal espião: registra cada chamada, guarda a imagem
     * "durável" a cada force e, quando {@code detachable}, destaca o force num ticket que
     * pode ser travado ou falhar (como o {@code BlobSeriesChannel}).
     */
    private static final class SpyStorage implements NgrrdStorage, SeriesChannelProvider {

        final List<String> log = Collections.synchronizedList(new ArrayList<>());
        final List<String> forceThreads = Collections.synchronizedList(new ArrayList<>());
        final List<String> ticketThreads = Collections.synchronizedList(new ArrayList<>());
        private final boolean detachable;
        private final Map<String, byte[]> durable = new ConcurrentHashMap<>();
        private final ConcurrentLinkedDeque<CountDownLatch> gates = new ConcurrentLinkedDeque<>();
        private final AtomicInteger failuresToInject = new AtomicInteger();
        private final Semaphore ticketStarted = new Semaphore(0);
        private volatile SpyChannel channel;

        SpyStorage(boolean detachable) {
            this.detachable = detachable;
        }

        CountDownLatch gateNextTicket() {
            CountDownLatch gate = new CountDownLatch(1);
            gates.add(gate);
            return gate;
        }

        void failNextTicket() {
            failuresToInject.incrementAndGet();
        }

        void awaitTicketStarted() throws InterruptedException {
            assertTrue(ticketStarted.tryAcquire(10, TimeUnit.SECONDS), "o force não começou no executor");
        }

        byte[] image() {
            return channel.snapshot();
        }

        byte[] durable() {
            return durable.values().iterator().next().clone();
        }

        boolean dirty() {
            return channel.dirty;
        }

        String durableSha256() throws Exception {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(durable()));
        }

        @Override
        public void put(String key, byte[] data) {
            durable.put(key, data.clone());
        }

        @Override
        public Optional<byte[]> get(String key) {
            return Optional.ofNullable(durable.get(key));
        }

        @Override
        public boolean exists(String key) {
            return durable.containsKey(key);
        }

        @Override
        public void delete(String key) {
            durable.remove(key);
        }

        @Override
        public List<String> list(String prefix) {
            return List.of();
        }

        @Override
        public void atomicReplace(String key, byte[] data) {
            put(key, data);
        }

        @Override
        public boolean seriesExists(String key) {
            return durable.containsKey(key);
        }

        @Override
        public SeriesChannel openSeries(String key) {
            SpyChannel opened = new SpyChannel(key);
            channel = opened;
            return opened;
        }

        private final class SpyChannel implements SeriesChannel {
            private final String key;
            private byte[] image = new byte[0];
            private volatile boolean dirty;

            SpyChannel(String key) {
                this.key = key;
            }

            synchronized byte[] snapshot() {
                return image.clone();
            }

            @Override
            public synchronized long size() {
                log.add("size");
                return image.length;
            }

            @Override
            public synchronized void allocate(long totalBytes) {
                log.add("allocate:" + totalBytes);
                image = Arrays.copyOf(image, (int) totalBytes);
            }

            @Override
            public synchronized byte[] readRegion(long offset, int len) {
                log.add("read:" + offset + ":" + len);
                return Arrays.copyOfRange(image, (int) offset, (int) offset + len);
            }

            @Override
            public synchronized void writeRegion(long offset, byte[] data) {
                log.add("write:" + offset + ":" + data.length);
                System.arraycopy(data, 0, image, (int) offset, data.length);
                dirty = true;
            }

            @Override
            public void force() {
                log.add("force");
                forceThreads.add(Thread.currentThread().getName());
                if (dirty) {
                    durable.put(key, snapshot());
                    dirty = false;
                }
            }

            @Override
            public ForceTicket prepareForce() {
                log.add("prepareForce");
                if (!detachable) {
                    return SeriesChannel.super.prepareForce();
                }
                if (!dirty) {
                    return ForceTicket.DONE;
                }
                dirty = false;
                CountDownLatch gate = gates.poll();
                boolean fail = failuresToInject.getAndUpdate(n -> Math.max(0, n - 1)) > 0;
                return () -> {
                    log.add("ticket.start");
                    ticketThreads.add(Thread.currentThread().getName());
                    ticketStarted.release();
                    if (gate != null) {
                        try {
                            gate.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (fail) {
                        dirty = true;
                        log.add("ticket.fail");
                        throw new NgrrdStorageException("falha simulada do force destacado");
                    }
                    durable.put(key, snapshot());
                    log.add("ticket.done");
                };
            }

            @Override
            public void close() {
                log.add("close");
                force();
            }
        }
    }
}
