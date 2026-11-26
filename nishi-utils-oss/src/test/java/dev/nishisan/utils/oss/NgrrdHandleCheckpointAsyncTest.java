package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.NgrrdHandle.CheckpointTiming;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link NgrrdHandle#checkpointAsync()} e {@link NgrrdHandle#checkpointAsyncTimed()}: o
 * padrão da interface executa o checkpoint síncrono na thread chamadora (sem medida); o handle
 * local da façade usa a fila do writer e mede os tempos.
 */
class NgrrdHandleCheckpointAsyncTest {

    private static final long START_MS = 1_747_339_200L * 1000L;

    @Test
    void padraoDaInterfaceExecutaOCheckpointSincronoNaThreadChamadora() {
        AtomicReference<Thread> checkpointThread = new AtomicReference<>();
        NgrrdHandle handle = new StubHandle(() -> checkpointThread.set(Thread.currentThread()));

        CompletableFuture<Void> future = handle.checkpointAsync();

        assertTrue(future.isDone());
        assertSame(Thread.currentThread(), checkpointThread.get());
        assertDoesNotThrow(() -> future.get());
    }

    @Test
    void padraoDaInterfaceDevolveFutureFalhaComAExcecaoDoCheckpoint() {
        IllegalStateException failure = new IllegalStateException("checkpoint remoto falhou");
        NgrrdHandle handle = new StubHandle(() -> {
            throw failure;
        });

        CompletableFuture<Void> future = handle.checkpointAsync();

        assertTrue(future.isCompletedExceptionally());
        ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
        assertSame(failure, thrown.getCause());
    }

    @Test
    void padraoDaInterfaceDevolveTimingSemMedida() {
        AtomicReference<Thread> checkpointThread = new AtomicReference<>();
        NgrrdHandle handle = new StubHandle(() -> checkpointThread.set(Thread.currentThread()));

        CompletableFuture<CheckpointTiming> timed = handle.checkpointAsyncTimed();

        assertTrue(timed.isDone());
        assertSame(Thread.currentThread(), checkpointThread.get());
        assertSame(CheckpointTiming.NONE, timed.join());
        assertEquals(new CheckpointTiming(0, 0, true), CheckpointTiming.NONE);

        IllegalStateException failure = new IllegalStateException("checkpoint remoto falhou");
        CompletableFuture<CheckpointTiming> failed = new StubHandle(() -> {
            throw failure;
        }).checkpointAsyncTimed();
        ExecutionException thrown = assertThrows(ExecutionException.class, failed::get);
        assertSame(failure, thrown.getCause());
    }

    @Test
    void handleLocalComExecutorDeForceMedeOCheckpoint(@TempDir Path base) throws Exception {
        String yaml;
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        ExecutorService pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "ngrrd-test-checkpoint-force"));
        List<String> forceThreads = new CopyOnWriteArrayList<>();
        try (BlobVolumeRegistry registry = NgrrdBlob.registry().basePath(base).shardCount(4)
                .segmentBytes(1L << 20)
                .checkpointForceExecutor(task -> pool.execute(() -> {
                    forceThreads.add(Thread.currentThread().getName());
                    task.run();
                }))
                .volume("ifaceStats").build();
             NgrrdHandle handle = Ngrrd.open(registry, NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0"),
                     yaml)) {
            handle.write("in_octets", new Sample(START_MS, 50_000L));
            handle.write("in_octets", new Sample(START_MS + 300_000L, 100_000L));
            CheckpointTiming timing = handle.checkpointAsyncTimed().get(10, TimeUnit.SECONDS);
            assertFalse(timing.coalesced());
            assertTrue(timing.queueWaitNanos() >= 0 && timing.forceNanos() >= 0, timing.toString());
            assertEquals(List.of("ngrrd-test-checkpoint-force"), forceThreads, "o force saiu da thread do writer");

            CheckpointTiming idle = handle.checkpointAsyncTimed().get(10, TimeUnit.SECONDS);
            assertTrue(idle.coalesced());
            assertEquals(0L, idle.forceNanos());
            assertEquals(1, forceThreads.size(), "o checkpoint coalescido não força");

            handle.close();
            assertSame(CheckpointTiming.NONE, handle.checkpointAsyncTimed().get(10, TimeUnit.SECONDS),
                    "handle fechado devolve future concluída, sem medida");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void handleLocalDaFachadaConcluiOCheckpointAssincronoEOsDadosFicamLegiveis(@TempDir Path base) throws Exception {
        String yaml;
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        try (BlobVolumeRegistry registry = NgrrdBlob.registry().basePath(base).shardCount(4)
                .segmentBytes(1L << 20).volume("ifaceStats").build();
             NgrrdHandle handle = Ngrrd.open(registry, NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0"),
                     yaml)) {
            long octets = 0L;
            for (int i = 0; i < 8; i++) {
                octets += 50_000L;
                handle.write("in_octets", new Sample(START_MS + i * 300_000L, octets));
                handle.write("out_octets", new Sample(START_MS + i * 300_000L, octets));
            }
            assertDoesNotThrow(() -> handle.checkpointAsync().get(10, TimeUnit.SECONDS));
            assertNotNull(handle.read("daily").get("in_bps"));
            handle.close();
            CompletableFuture<Void> afterClose = handle.checkpointAsync();
            assertTrue(afterClose.isDone() && !afterClose.isCompletedExceptionally(),
                    "handle fechado devolve future concluída, como o checkpoint síncrono");
        }
    }

    /** Implementação mínima, sem fila própria: herda o {@code checkpointAsync()} padrão. */
    private static final class StubHandle implements NgrrdHandle {
        private final Runnable onCheckpoint;

        StubHandle(Runnable onCheckpoint) {
            this.onCheckpoint = onCheckpoint;
        }

        @Override
        public String seriesKey() {
            return "stub";
        }

        @Override
        public void write(String dsName, Sample sample) {
        }

        @Override
        public void flush() {
            checkpoint();
        }

        @Override
        public void checkpoint() {
            onCheckpoint.run();
        }

        @Override
        public SeriesResult read(String dsName, ViewQuery query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, SeriesResult> read(String presetName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, SeriesResult> read(String presetName, long endExclusiveEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
