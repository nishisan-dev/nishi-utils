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

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.Sample;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pool dedicado de force do checkpoint ({@code ngrrd.checkpoint.threads}) no {@link NgrrdStorageNode}:
 * ausente por padrão, criado só com {@code threads > 0}, usado pelo force das séries e, principalmente, a
 * <b>ordem de encerramento</b>: o pool só termina depois do {@code registry.close()}, que espera os forces
 * em voo. Um único nó sem peers.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NgrrdStorageNodeForcePoolTest {

    private static final String POOL_THREAD_PREFIX = "ngrrd-checkpoint-force-";
    private static final long T0 = 1_700_000_100_000L;

    @Test
    void semThreadsNaoHaPoolNemThread(@TempDir Path tempDir) throws Exception {
        try (NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, 0))) {
            assertTrue(node.checkpointForcePool().isEmpty());
            assertFalse(poolThreadAlive(), "threads=0 não cria nenhuma thread de force");
        }
    }

    @Test
    void comThreadsOPoolExisteEOForceDoCheckpointRodaNele(@TempDir Path tempDir) throws Exception {
        NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, 2));
        try {
            assertTrue(node.checkpointForcePool().isPresent());
            openAndWrite(node, "forced");

            // Os dois workers do pool presos: o force do checkpoint fica na fila e só conclui depois do registro
            // do whenComplete, que então roda na thread do pool, nunca na do teste.
            ExecutorService pool = node.checkpointForcePool().orElseThrow();
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch blocking = new CountDownLatch(2);
            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    blocking.countDown();
                    awaitQuietly(release);
                });
            }
            assertTrue(blocking.await(10, TimeUnit.SECONDS));

            AtomicReference<String> completionThread = new AtomicReference<>();
            try {
                CompletableFuture<Void> checkpoint = node.registry().withSeries("forced", access -> {
                    CompletableFuture<Void> future = access.checkpointAsync();
                    future.whenComplete((ignored, failure) -> completionThread.set(
                            Thread.currentThread().getName()));
                    return future;
                }).orElseThrow();
                assertFalse(checkpoint.isDone(), "o force deveria estar na fila atrás dos workers presos");
            } finally {
                release.countDown();
            }
            awaitTrue(() -> completionThread.get() != null);
            assertTrue(completionThread.get().startsWith(POOL_THREAD_PREFIX),
                    "a future do checkpoint conclui na thread do pool de force: " + completionThread.get());
            awaitTrue(() -> !node.registry().isDirty("forced"));
        } finally {
            node.close();
        }
        assertTrue(node.checkpointForcePool().orElseThrow().isTerminated());
        awaitTrue(() -> !poolThreadAlive());
    }

    /**
     * Ordem de encerramento (spec 8.15.0, D.2): com o pool de uma thread ocupado e o force da série
     * enfileirado atrás dele, o {@code registry.close()} do {@code close()} do nó fica esperando esse force.
     * O pool NÃO pode ter sido encerrado nesse ponto (senão o {@code shutdownNow} descartaria as fases B de
     * writers ainda abertos, com Sync pendurado ou writer abandonado).
     * Liberado o bloqueio, o close termina rápido e só então o pool está terminado.
     */
    @Test
    void closeDoNoMantemOPoolVivoAteORegistryTerminar(@TempDir Path tempDir) throws Exception {
        NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, 1));
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> closed = new CompletableFuture<>();
        try {
            openAndWrite(node, "late");
            ExecutorService pool = node.checkpointForcePool().orElseThrow();
            CountDownLatch blocking = new CountDownLatch(1);
            pool.execute(() -> {
                blocking.countDown();
                awaitQuietly(release);
            });
            assertTrue(blocking.await(10, TimeUnit.SECONDS));

            Thread closer = new Thread(() -> {
                try {
                    node.close();
                    closed.complete(null);
                } catch (Throwable t) {
                    closed.completeExceptionally(t);
                }
            }, "test-node-closer");
            closer.setDaemon(true);
            closer.start();

            Thread.sleep(1_000);
            assertFalse(closed.isDone(), "o registry.close() deveria esperar o force preso atrás do bloqueio");
            assertFalse(pool.isShutdown(), "o pool tem que seguir vivo enquanto o registry.close() espera os forces");

            release.countDown();
            closed.get(20, TimeUnit.SECONDS);
            assertTrue(pool.isTerminated(), "o pool termina depois do registry");
        } finally {
            release.countDown();
        }
    }

    private static void openAndWrite(NgrrdStorageNode node, String key) throws IOException {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        node.registry().open(key, yaml, Ngrrd.OpenOptions.defaults());
        assertTrue(node.registry().withSeries(key, access -> {
            access.handle().write("in_octets", new Sample(T0, 1_000d));
            access.recordWrite();
            return true;
        }).orElse(false));
    }

    private static StorageNodeConfig config(Path tempDir, int threads) throws IOException {
        return StorageNodeConfig.builder()
                .nodeId("storage-solo")
                .port(allocateFreeLocalPort())
                .dataDir(tempDir.resolve("data"))
                .volumeDir(tempDir.resolve("volume"))
                .shardCount(2)
                .segmentBytes(1L << 20)
                .initialShardCapacityBytes(1L << 20)
                .bootDiscoveryWindow(Duration.ZERO)
                .localCheckpoint(new LocalCheckpointSettings(false, LocalCheckpointSettings.DEFAULT_INTERVAL,
                        LocalCheckpointSettings.defaultMaxInFlight(), threads,
                        LocalCheckpointSettings.DEFAULT_SPREAD_FRACTION))
                .build();
    }

    private static boolean poolThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().startsWith(POOL_THREAD_PREFIX) && thread.isAlive());
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condição não atingida em 10 s");
            }
            Thread.sleep(10);
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
