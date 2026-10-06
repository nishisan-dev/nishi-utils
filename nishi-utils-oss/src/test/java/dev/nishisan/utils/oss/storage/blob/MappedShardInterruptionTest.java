package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.blob.VolumeWriteMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Interrupção de threads em shards {@code PWRITE}. {@code FileChannel} é interrompível: a thread
 * interrompida fecha o canal para todas e, no JDK 21 com virtual threads, interrupções em rajada chegaram a
 * travar a escritora para sempre. A escrita {@code PWRITE} usa {@code RandomAccessFile}, que não é
 * interrompível; estes testes cravam que nada fecha, nada trava e a flag de interrupção é preservada.
 */
class MappedShardInterruptionTest {

    private static final long SEG = 64 * 1024;
    private static final UUID UUID_V = new UUID(0xDEADBEEFL, 0xCAFEBABEL);
    private static final int REGION = 4096;
    private static final long BASE = 8192L;

    private static ShardSuperblock superblock(long capacity) {
        return new ShardSuperblock(1, 0, 64, ShardSuperblock.HEADER_BYTES,
                capacity, ShardSuperblock.HEADER_BYTES, UUID_V, 1L);
    }

    private static byte[] pattern(int len, int seed) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i);
        }
        return b;
    }

    @Test
    void threadComFlagDeInterrupcaoLigadaEscreveEPreservaAFlag(@TempDir Path dir) throws Exception {
        byte[] data = pattern(REGION, 9);
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(SEG), SEG,
                VolumeWriteMode.PWRITE)) {
            AtomicBoolean flagAfterWrite = new AtomicBoolean();
            AtomicBoolean flagAfterGrow = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread interrupted = new Thread(() -> {
                try {
                    Thread.currentThread().interrupt();
                    shard.writeAt(BASE, data);
                    flagAfterWrite.set(Thread.currentThread().isInterrupted());
                    shard.grow(2 * SEG); // o map é interrompível: o shard o protege e restaura a flag
                    flagAfterGrow.set(Thread.currentThread().isInterrupted());
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            interrupted.start();
            interrupted.join();
            assertNull(failure.get(), () -> "operação com flag ligada falhou: " + failure.get());
            assertTrue(flagAfterWrite.get(), "a escrita não deve limpar a flag de interrupção");
            assertTrue(flagAfterGrow.get(), "o grow deve restaurar a flag de interrupção");

            // outra thread (sem flag) segue usando o shard depois
            byte[] other = pattern(REGION, 33);
            shard.writeAt(SEG + 100, other);
            assertArrayEquals(data, shard.readAt(BASE, data.length));
            assertArrayEquals(other, shard.readAt(SEG + 100, other.length));
        }
    }

    @Test
    void rajadasDeInterrupcaoComEscritorasVirtuaisEDePlataformaECloseConcorrenteNaoTravam(@TempDir Path root) {
        // 20 repetições, cada uma com prazo duro: um deadlock reprova em vez de pendurar a suíte
        for (int round = 0; round < 20; round++) {
            Path dir = root.resolve("round-" + round);
            dir.toFile().mkdirs();
            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> interruptionBurstRound(dir),
                    "rodada travou: deadlock sob interrupções em rajada");
        }
    }

    private void interruptionBurstRound(Path dir) throws Exception {
        int writers = 8;
        MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(SEG), SEG,
                VolumeWriteMode.PWRITE);
        AtomicBoolean closing = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicIntegerArray lastWritten = new AtomicIntegerArray(writers);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(writers);
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int id = w;
            lastWritten.set(id, -1);
            Runnable writer = () -> {
                started.countDown();
                try {
                    for (int i = 0; !stop.get(); i++) {
                        byte[] data = pattern(REGION, id * 16 + (i & 0x0F));
                        // flag própria ligada antes da escrita: a escrita não pode limpá-la
                        Thread.currentThread().interrupt();
                        try {
                            shard.writeAt(BASE + (long) id * REGION, data);
                        } catch (BlobVolumeException e) {
                            if (!closing.get()) {
                                throw e;
                            }
                            return; // fechado de propósito pelo close() concorrente
                        }
                        lastWritten.set(id, i & 0x0F);
                        if (!Thread.currentThread().isInterrupted()) {
                            throw new AssertionError("a flag de interrupção foi limpa pela escrita");
                        }
                        Thread.interrupted();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            };
            threads.add(id % 2 == 0 ? Thread.ofVirtual().unstarted(writer) : new Thread(writer));
        }
        threads.forEach(Thread::start);
        started.await();

        // interrupções em rajada, a cada poucos µs, por várias threads, sobre todas as escritoras
        AtomicBoolean interrupting = new AtomicBoolean(true);
        List<Thread> interrupters = new ArrayList<>();
        for (int k = 0; k < 3; k++) {
            Thread t = new Thread(() -> {
                while (interrupting.get()) {
                    threads.forEach(Thread::interrupt);
                    LockSupport.parkNanos(5_000L);
                }
            });
            interrupters.add(t);
            t.start();
        }
        Thread.sleep(150);

        // close() concorrente com escritas em curso e com a rajada
        closing.set(true);
        shard.close();
        stop.set(true);
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(t.isAlive(), "escritora não terminou");
        }
        interrupting.set(false);
        for (Thread t : interrupters) {
            t.join();
        }
        assertNull(failure.get(), () -> "escritora falhou: " + failure.get());

        // dados íntegros: cada região tem o padrão da última escrita concluída pela sua escritora
        try (MappedShard reopened = MappedShard.open(dir.resolve("shard-00.blob"), SEG, VolumeWriteMode.MMAP)) {
            for (int w = 0; w < writers; w++) {
                int last = lastWritten.get(w);
                if (last >= 0) {
                    assertArrayEquals(pattern(REGION, w * 16 + last),
                            reopened.readAt(BASE + (long) w * REGION, REGION), "região da escritora " + w);
                }
            }
        }
    }

    @Test
    void rajadasDeInterrupcaoNaThreadQueCresceOShardNaoQuebramEscritoras(@TempDir Path dir) throws Exception {
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            long capacity = 64 * SEG;
            try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), superblock(SEG), SEG,
                    VolumeWriteMode.PWRITE)) {
                AtomicBoolean stop = new AtomicBoolean();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread writer = Thread.ofVirtual().unstarted(() -> {
                    try {
                        byte[] data = pattern(REGION, 3);
                        while (!stop.get()) {
                            shard.writeAt(BASE, data);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
                writer.start();
                Thread grower = new Thread(() -> {
                    try {
                        for (long cap = 2 * SEG; cap <= capacity; cap += SEG) {
                            shard.grow(cap);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
                grower.start();
                // o map só roda no grow (raro e serializado); 100 µs entre interrupções já o atinge de forma
                // repetida sem tornar a thread impossível de progredir
                while (grower.isAlive()) {
                    grower.interrupt();
                    LockSupport.parkNanos(100_000L);
                }
                grower.join();
                stop.set(true);
                writer.join();
                assertNull(failure.get(), () -> "falha durante grow/escritas interrompidos: " + failure.get());
                byte[] tail = pattern(REGION, 77);
                shard.writeAt(capacity - REGION, tail);
                assertArrayEquals(tail, shard.readAt(capacity - REGION, REGION));
            }
        });
    }

    /**
     * Regressão do {@code setLength} fora do {@code writeLock}: no JDK/Unix o {@code RandomAccessFile.setLength}
     * faz {@code lseek(CUR)} + {@code ftruncate} + {@code lseek(posição antiga)}; sem o lock, esse
     * "restaura posição" pode cair entre o {@code seek} e o {@code write} de uma escritora e desviar a escrita
     * para o offset errado (corrupção silenciosa). Escritoras em regiões disjuntas conferem o conteúdo logo
     * depois de cada escrita enquanto um grower cresce o shard sem parar.
     */
    @Test
    void growConcorrenteComEscritasPosicionaisNaoDesviaEscritas(@TempDir Path root) {
        for (int round = 0; round < 4; round++) {
            Path dir = root.resolve("round-" + round);
            dir.toFile().mkdirs();
            assertTimeoutPreemptively(Duration.ofSeconds(60), () -> growVersusWritersRound(dir),
                    "rodada travou");
        }
    }

    private void growVersusWritersRound(Path dir) throws Exception {
        int writers = 6;
        int growSteps = 600;
        long smallSegment = 4096;
        // 1 KiB por região: bem menor que o segmento, para o shard crescer em muitos passos pequenos
        int region = 1024;
        ShardSuperblock sb = new ShardSuperblock(1, 0, 64, ShardSuperblock.HEADER_BYTES,
                16 * smallSegment, ShardSuperblock.HEADER_BYTES, UUID_V, 1L);
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-00.blob"), sb, smallSegment,
                VolumeWriteMode.PWRITE)) {
            AtomicBoolean growing = new AtomicBoolean(true);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                final int id = w;
                // regiões disjuntas dentro do primeiro segmento útil, longe do superblock (1 página)
                long offset = smallSegment + (long) id * region;
                Runnable writer = () -> {
                    try {
                        int i = 0;
                        while (growing.get()) {
                            byte[] data = pattern(region, id * 31 + (i++ & 0x1F));
                            shard.writeAt(offset, data);
                            assertArrayEquals(data, shard.readAt(offset, region),
                                    "escrita da escritora " + id + " desviada ou corrompida (iteração " + i + ")");
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                };
                threads.add(id % 2 == 0 ? Thread.ofVirtual().unstarted(writer) : new Thread(writer));
            }
            threads.forEach(Thread::start);
            try {
                for (int step = 1; step <= growSteps && failure.get() == null; step++) {
                    shard.grow((16 + step) * smallSegment);
                }
            } finally {
                growing.set(false);
            }
            for (Thread t : threads) {
                t.join(TimeUnit.SECONDS.toMillis(20));
                assertFalse(t.isAlive(), "escritora não terminou");
            }
            assertNull(failure.get(), () -> "corrupção ou falha com grow concorrente: " + failure.get());
        }
    }
}
