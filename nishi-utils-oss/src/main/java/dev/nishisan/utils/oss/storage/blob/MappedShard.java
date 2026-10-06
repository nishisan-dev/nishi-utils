package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.blob.VolumeWriteMode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Um shard mapeado em memória: um {@link RandomAccessFile} (1 FD) e um array de
 * {@link MappedByteBuffer} de {@code segmentBytes} cobrindo a capacidade. Acesso
 * por offset absoluto via métodos indexados (não mexem em {@code position}, logo
 * escritas em regiões disjuntas são concorrentes em {@code MMAP}); {@code force} parcial por
 * região via {@link MappedByteBuffer#force(int, int)}. Ver
 * {@code doc/oss/ngrrd-blob-volume.md} §5/§10.
 *
 * <p>A capacidade é sempre múltiplo de {@code segmentBytes}; cada segmento {@code i}
 * mapeia {@code [i*segmentBytes, (i+1)*segmentBytes)}.</p>
 *
 * <p><strong>Modo de escrita</strong> ({@link VolumeWriteMode}): a leitura é sempre via mmap.
 * Em {@code MMAP} a escrita é por {@link MappedByteBuffer#put(int, byte[], int, int)}; em
 * {@code PWRITE} é por {@link RandomAccessFile#seek(long)} + {@link RandomAccessFile#write(byte[], int, int)}
 * sob um {@code writeLock} por shard. Como o mapeamento é {@code MAP_SHARED}, ambos compartilham o mesmo
 * page cache e a leitura via mmap enxerga a escrita posicional imediatamente. Já o
 * {@link #forceRange(long, long)} usa {@code msync} da faixa, que no Linux chama
 * {@code vfs_fsync_range} sobre a faixa do arquivo e grava as páginas sujas
 * independentemente de terem sido sujas via mmap ou via {@code write()}.</p>
 *
 * <p>O {@code PWRITE} <strong>não</strong> é tão concorrente quanto o mmap: o {@code writeLock} serializa as
 * escritas do shard, o que custa pouco porque no XFS e no ext4 a escrita bufferizada já toma o lock
 * exclusivo do inode ({@code i_rwsem}) durante o {@code write()}; além disso cada {@code write()} pode
 * atualizar o {@code mtime} (mitigável com {@code lazytime}). O ganho do {@code PWRITE} é em bytes gravados
 * no disco (writeback por bloco), não em vazão de escrita.</p>
 *
 * <p><strong>Posição do arquivo.</strong> O {@code seek} muda a posição compartilhada do FD. Nada neste
 * shard depende dela: as leituras são pelo mmap (e a leitura do superblock em {@link #open(Path, long,
 * VolumeWriteMode)} faz o seu próprio {@code seek}, antes de o shard ser publicado), e
 * {@link RandomAccessFile#setLength(long)} (que salva e restaura a posição sem atomicidade) só roda sob o
 * {@code writeLock}, para nunca intercalar com um {@code seek}+{@code write}.</p>
 *
 * <p><strong>Interrupção de thread.</strong> {@link RandomAccessFile} não é um {@code InterruptibleChannel}:
 * interromper a thread que escreve (ou sincroniza, ou fecha) não fecha o FD. Por isso a escrita
 * {@code PWRITE} não usa {@link FileChannel}: o {@code write()} de um canal interrompido o fecha de vez para
 * todas as threads e, no JDK 21 com virtual threads, duas interrupções próximas numa escritora podem
 * travar para sempre (a VT fecha o canal e espera as demais threads do canal enquanto a segunda
 * {@code interrupt()} segura o lock de que ela precisa). O único uso de {@link FileChannel} é o
 * {@code map} (em {@link #create}, {@link #open} e {@link #grow(long)}), feito num {@code FileChannel}
 * <em>temporário e exclusivo</em> da chamada (aberto num {@link RandomAccessFile} próprio, fechado ao fim;
 * os {@link MappedByteBuffer} continuam válidos depois disso). Nele a flag de interrupção é limpa antes e
 * restaurada depois, e uma interrupção que feche esse canal temporário só repete o {@code map}. Como cada
 * canal temporário é usado por uma única thread, não há concorrência no mesmo canal.</p>
 */
public final class MappedShard implements AutoCloseable {

    /** Tentativas do {@code map} quando o canal temporário é fechado por uma interrupção de thread. */
    private static final int MAX_MAP_ATTEMPTS = 10;

    private final Path file;
    private final RandomAccessFile raf;
    // Serializa o seek+write do PWRITE, o setLength do grow e o close (ver "Posição do arquivo").
    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile boolean closed;
    private final long segmentBytes;
    // CopyOnWriteArrayList: grow() (sob structuralLock) adiciona segmentos enquanto
    // leituras/escritas de regiões existentes podem ocorrer concorrentemente.
    private final CopyOnWriteArrayList<MappedByteBuffer> segments;
    private final VolumeWriteMode writeMode;
    // Bytes lógicos entregues a writeAt, por caminho de escrita; contadores sem contenção no caminho quente.
    private final LongAdder mappedBytesWritten = new LongAdder();
    private final LongAdder positionalBytesWritten = new LongAdder();
    private volatile long capacity;
    private volatile ShardSuperblock superblock;

    private MappedShard(Path file, RandomAccessFile raf, long segmentBytes,
                        long capacity, List<MappedByteBuffer> segments, ShardSuperblock superblock,
                        VolumeWriteMode writeMode) {
        this.file = file;
        this.raf = raf;
        this.segmentBytes = segmentBytes;
        this.capacity = capacity;
        this.segments = new CopyOnWriteArrayList<>(segments);
        this.superblock = superblock;
        this.writeMode = writeMode;
    }

    /** Cria um shard novo em modo {@link VolumeWriteMode#MMAP}, pré-aloca o arquivo e grava o superblock. */
    public static MappedShard create(Path file, ShardSuperblock superblock, long segmentBytes) {
        return create(file, superblock, segmentBytes, VolumeWriteMode.MMAP);
    }

    /** Cria um shard novo no modo de escrita informado, pré-aloca o arquivo e grava o superblock. */
    public static MappedShard create(Path file, ShardSuperblock superblock, long segmentBytes,
                                     VolumeWriteMode writeMode) {
        Objects.requireNonNull(file, "file é obrigatório");
        Objects.requireNonNull(superblock, "superblock é obrigatório");
        Objects.requireNonNull(writeMode, "writeMode é obrigatório");
        long capacity = superblock.shardCapacityBytes();
        if (segmentBytes <= 0 || capacity < segmentBytes || capacity % segmentBytes != 0) {
            throw new BlobVolumeException("capacidade (" + capacity + ") deve ser múltiplo positivo de segmentBytes ("
                    + segmentBytes + ")");
        }
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file.toFile(), "rw");
            raf.setLength(capacity);
            List<MappedByteBuffer> segments = mapSegments(file, capacity, segmentBytes, 0);
            MappedShard shard = new MappedShard(file, raf, segmentBytes, capacity, segments, superblock, writeMode);
            shard.writeSuperblock(superblock);
            return shard;
        } catch (IOException e) {
            closeQuietly(raf);
            throw new BlobVolumeException("falha ao criar shard " + file, e);
        } catch (RuntimeException e) {
            closeQuietly(raf);
            throw e;
        }
    }

    /** Abre um shard existente em modo {@link VolumeWriteMode#MMAP}, lê e valida o superblock e mapeia os segmentos. */
    public static MappedShard open(Path file, long segmentBytes) {
        return open(file, segmentBytes, VolumeWriteMode.MMAP);
    }

    /** Abre um shard existente no modo de escrita informado, lê e valida o superblock e mapeia os segmentos. */
    public static MappedShard open(Path file, long segmentBytes, VolumeWriteMode writeMode) {
        Objects.requireNonNull(file, "file é obrigatório");
        Objects.requireNonNull(writeMode, "writeMode é obrigatório");
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file.toFile(), "rw");
            byte[] head = new byte[ShardSuperblock.BYTES];
            raf.seek(0L);
            raf.readFully(head);
            ShardSuperblock sb = ShardSuperblock.decode(head);
            long capacity = sb.shardCapacityBytes();
            if (segmentBytes <= 0 || capacity < segmentBytes || capacity % segmentBytes != 0) {
                throw new BlobVolumeException("shard " + file + " com capacidade incompatível com segmentBytes");
            }
            if (raf.length() < capacity) {
                throw new BlobVolumeException("shard " + file + " menor que a capacidade declarada");
            }
            List<MappedByteBuffer> segments = mapSegments(file, capacity, segmentBytes, 0);
            return new MappedShard(file, raf, segmentBytes, capacity, segments, sb, writeMode);
        } catch (IOException e) {
            closeQuietly(raf);
            throw new BlobVolumeException("falha ao abrir shard " + file, e);
        } catch (RuntimeException e) {
            closeQuietly(raf);
            throw e;
        }
    }

    public ShardSuperblock superblock() {
        return superblock;
    }

    public long capacity() {
        return capacity;
    }

    /** Modo de escrita deste shard. */
    public VolumeWriteMode writeMode() {
        return writeMode;
    }

    /** Bytes lógicos entregues a {@link #writeAt(long, byte[])} desde a abertura do shard (soma dos dois caminhos). */
    public long bytesWritten() {
        return mappedBytesWritten.sum() + positionalBytesWritten.sum();
    }

    /** Bytes lógicos gravados pelo caminho do mmap ({@link VolumeWriteMode#MMAP}). */
    public long mappedBytesWritten() {
        return mappedBytesWritten.sum();
    }

    /** Bytes lógicos gravados pelo caminho posicional ({@link VolumeWriteMode#PWRITE}). */
    public long positionalBytesWritten() {
        return positionalBytesWritten.sum();
    }

    /** Reescreve o superblock (primeira página) e o torna durável. */
    public void writeSuperblock(ShardSuperblock sb) {
        writeAt(0L, sb.encode());
        forceRange(0L, ShardSuperblock.BYTES);
        this.superblock = sb;
    }

    /** Lê {@code len} bytes a partir de {@code offset} (cruza segmentos se preciso). */
    public byte[] readAt(long offset, int len) {
        checkBounds(offset, len);
        byte[] out = new byte[len];
        long pos = offset;
        int done = 0;
        while (done < len) {
            int segIdx = (int) (pos / segmentBytes);
            int local = (int) (pos % segmentBytes);
            int chunk = (int) Math.min(len - done, segmentBytes - local);
            segments.get(segIdx).get(local, out, done, chunk);
            done += chunk;
            pos += chunk;
        }
        return out;
    }

    /**
     * Escreve {@code data} a partir de {@code offset} (cruza segmentos se preciso). Em
     * {@link VolumeWriteMode#MMAP} grava pelo mapeamento; em {@link VolumeWriteMode#PWRITE}
     * grava por {@code seek}+{@code write} no {@link RandomAccessFile}, sob o {@code writeLock} do shard.
     */
    public void writeAt(long offset, byte[] data) {
        checkBounds(offset, data.length);
        if (writeMode == VolumeWriteMode.PWRITE) {
            writePositional(offset, data);
            positionalBytesWritten.add(data.length);
        } else {
            writeMapped(offset, data);
            mappedBytesWritten.add(data.length);
        }
    }

    private void writeMapped(long offset, byte[] data) {
        long pos = offset;
        int done = 0;
        while (done < data.length) {
            int segIdx = (int) (pos / segmentBytes);
            int local = (int) (pos % segmentBytes);
            int chunk = (int) Math.min(data.length - done, segmentBytes - local);
            segments.get(segIdx).put(local, data, done, chunk);
            done += chunk;
            pos += chunk;
        }
    }

    private void writePositional(long offset, byte[] data) {
        writeLock.lock();
        try {
            if (closed) {
                throw new BlobVolumeException("shard " + file + " já foi fechado");
            }
            raf.seek(offset);
            raf.write(data, 0, data.length);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao escrever no shard " + file + " (offset=" + offset
                    + ", len=" + data.length + ")", e);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Sincroniza ({@code msync}) apenas as páginas que cobrem {@code [offset, offset+len)}.
     * Vale para os dois modos de escrita: no Linux o {@code msync} chama {@code vfs_fsync_range}
     * sobre a faixa do arquivo, gravando as páginas sujas venham elas do mmap ou de {@code write()}.
     */
    public void forceRange(long offset, long len) {
        checkBounds(offset, len);
        long pos = offset;
        long remaining = len;
        while (remaining > 0) {
            int segIdx = (int) (pos / segmentBytes);
            int local = (int) (pos % segmentBytes);
            int chunk = (int) Math.min(remaining, segmentBytes - local);
            segments.get(segIdx).force(local, chunk);
            pos += chunk;
            remaining -= chunk;
        }
    }

    /**
     * Aumenta a capacidade (em múltiplos de segmento), estende o arquivo e mapeia novos segmentos. O
     * {@code setLength} roda sob o {@code writeLock} (ver "Posição do arquivo"); o {@code sync} usa
     * {@link java.io.FileDescriptor#sync()}, que não é interrompível.
     */
    public void grow(long newCapacity) {
        long target = roundUpToSegment(newCapacity);
        if (target <= capacity) {
            return;
        }
        try {
            writeLock.lock();
            try {
                if (closed) {
                    throw new BlobVolumeException("shard " + file + " já foi fechado");
                }
                raf.setLength(target);
            } finally {
                writeLock.unlock();
            }
            raf.getFD().sync();
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao crescer shard " + file + " para " + target, e);
        }
        List<MappedByteBuffer> extra = mapSegments(file, target, segmentBytes, segments.size());
        segments.addAll(extra);
        this.capacity = target;
    }

    /** Sincroniza ({@code fsync}) e fecha o arquivo; idempotente. Espera uma escrita {@code PWRITE} em curso. */
    @Override
    public void close() {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            BlobVolumeException failure = null;
            try {
                raf.getFD().sync();
            } catch (IOException e) {
                failure = new BlobVolumeException("falha ao sincronizar shard " + file, e);
            }
            try {
                raf.close();
            } catch (IOException e) {
                BlobVolumeException closeFailure = new BlobVolumeException("falha ao fechar shard " + file, e);
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            writeLock.unlock();
        }
    }

    private long roundUpToSegment(long bytes) {
        return ((bytes + segmentBytes - 1) / segmentBytes) * segmentBytes;
    }

    private void checkBounds(long offset, long len) {
        if (offset < 0 || len < 0 || offset + len > capacity) {
            throw new BlobVolumeException("acesso fora da capacidade do shard " + file
                    + " (offset=" + offset + ", len=" + len + ", capacity=" + capacity + ")");
        }
    }

    /**
     * Mapeia os segmentos {@code [fromIndex, capacity/segmentBytes)} num {@link FileChannel} temporário e
     * exclusivo desta chamada. {@link FileChannel#map} é interrompível: limpa-se a flag de interrupção antes
     * (e restaura-se depois) e, se uma interrupção ainda assim fechar o canal temporário, o {@code map} é
     * repetido num canal novo, até {@value #MAX_MAP_ATTEMPTS} tentativas. Os mapeamentos sobrevivem ao
     * fechamento do canal.
     */
    private static List<MappedByteBuffer> mapSegments(Path file, long capacity, long segmentBytes,
                                                      int fromIndex) {
        boolean interrupted = Thread.interrupted();
        try {
            IOException last = null;
            for (int attempt = 0; attempt < MAX_MAP_ATTEMPTS; attempt++) {
                try (RandomAccessFile mapFile = new RandomAccessFile(file.toFile(), "rw");
                     FileChannel channel = mapFile.getChannel()) {
                    int total = (int) (capacity / segmentBytes);
                    List<MappedByteBuffer> segments = new ArrayList<>(total - fromIndex);
                    for (int i = fromIndex; i < total; i++) {
                        segments.add(channel.map(MapMode.READ_WRITE, (long) i * segmentBytes, segmentBytes));
                    }
                    return segments;
                } catch (ClosedChannelException e) {
                    // ClosedByInterruptException deixa a flag ligada: limpa para não fechar o canal seguinte
                    interrupted |= Thread.interrupted();
                    last = e;
                } catch (IOException e) {
                    throw new BlobVolumeException("falha ao mapear shard " + file, e);
                }
            }
            throw new BlobVolumeException("falha ao mapear shard " + file
                    + ": canal fechado repetidamente por interrupção (" + MAX_MAP_ATTEMPTS + " tentativas)", last);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void closeQuietly(RandomAccessFile raf) {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ignored) {
                // melhor esforço: já estamos propagando a falha original
            }
        }
    }
}
