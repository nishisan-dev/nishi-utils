package dev.nishisan.utils.oss.storage.blob;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Um shard mapeado em memória: um {@link FileChannel} (1 FD) e um array de
 * {@link MappedByteBuffer} de {@code segmentBytes} cobrindo a capacidade. Acesso
 * por offset absoluto via métodos indexados (não mexem em {@code position}, logo
 * escritas em regiões disjuntas são concorrentes); {@code force} parcial por
 * região via {@link MappedByteBuffer#force(int, int)}. Ver
 * {@code doc/oss/ngrrd-blob-volume.md} §5/§10.
 *
 * <p>A capacidade é sempre múltiplo de {@code segmentBytes}; cada segmento {@code i}
 * mapeia {@code [i*segmentBytes, (i+1)*segmentBytes)}.</p>
 */
public final class MappedShard implements AutoCloseable {

    private final Path file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final long segmentBytes;
    private final List<MappedByteBuffer> segments;
    private long capacity;
    private ShardSuperblock superblock;

    private MappedShard(Path file, RandomAccessFile raf, FileChannel channel, long segmentBytes,
                        long capacity, List<MappedByteBuffer> segments, ShardSuperblock superblock) {
        this.file = file;
        this.raf = raf;
        this.channel = channel;
        this.segmentBytes = segmentBytes;
        this.capacity = capacity;
        this.segments = segments;
        this.superblock = superblock;
    }

    /** Cria um shard novo, pré-aloca o arquivo e grava o superblock. */
    public static MappedShard create(Path file, ShardSuperblock superblock, long segmentBytes) {
        Objects.requireNonNull(file, "file é obrigatório");
        Objects.requireNonNull(superblock, "superblock é obrigatório");
        long capacity = superblock.shardCapacityBytes();
        if (segmentBytes <= 0 || capacity < segmentBytes || capacity % segmentBytes != 0) {
            throw new BlobVolumeException("capacidade (" + capacity + ") deve ser múltiplo positivo de segmentBytes ("
                    + segmentBytes + ")");
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
            raf.setLength(capacity);
            FileChannel channel = raf.getChannel();
            List<MappedByteBuffer> segments = mapSegments(channel, capacity, segmentBytes, 0);
            MappedShard shard = new MappedShard(file, raf, channel, segmentBytes, capacity, segments, superblock);
            shard.writeSuperblock(superblock);
            return shard;
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao criar shard " + file, e);
        }
    }

    /** Abre um shard existente, lê e valida o superblock e mapeia os segmentos. */
    public static MappedShard open(Path file, long segmentBytes) {
        Objects.requireNonNull(file, "file é obrigatório");
        try {
            RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
            FileChannel channel = raf.getChannel();
            byte[] head = new byte[ShardSuperblock.BYTES];
            readFully(channel, head, 0);
            ShardSuperblock sb = ShardSuperblock.decode(head);
            long capacity = sb.shardCapacityBytes();
            if (segmentBytes <= 0 || capacity < segmentBytes || capacity % segmentBytes != 0) {
                throw new BlobVolumeException("shard " + file + " com capacidade incompatível com segmentBytes");
            }
            if (channel.size() < capacity) {
                throw new BlobVolumeException("shard " + file + " menor que a capacidade declarada");
            }
            List<MappedByteBuffer> segments = mapSegments(channel, capacity, segmentBytes, 0);
            return new MappedShard(file, raf, channel, segmentBytes, capacity, segments, sb);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao abrir shard " + file, e);
        }
    }

    public ShardSuperblock superblock() {
        return superblock;
    }

    public long capacity() {
        return capacity;
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

    /** Escreve {@code data} a partir de {@code offset} (cruza segmentos se preciso). */
    public void writeAt(long offset, byte[] data) {
        checkBounds(offset, data.length);
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

    /** Sincroniza ({@code msync}) apenas as páginas que cobrem {@code [offset, offset+len)}. */
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

    /** Aumenta a capacidade (em múltiplos de segmento), estende o arquivo e mapeia novos segmentos. */
    public void grow(long newCapacity) {
        long target = roundUpToSegment(newCapacity);
        if (target <= capacity) {
            return;
        }
        try {
            raf.setLength(target);
            channel.force(true);
            List<MappedByteBuffer> extra = mapSegments(channel, target, segmentBytes, segments.size());
            segments.addAll(extra);
            this.capacity = target;
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao crescer shard " + file + " para " + target, e);
        }
    }

    @Override
    public void close() {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao sincronizar shard " + file, e);
        } finally {
            try {
                raf.close();
            } catch (IOException e) {
                throw new BlobVolumeException("falha ao fechar shard " + file, e);
            }
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

    private static List<MappedByteBuffer> mapSegments(FileChannel channel, long capacity,
                                                      long segmentBytes, int fromIndex) throws IOException {
        int total = (int) (capacity / segmentBytes);
        List<MappedByteBuffer> segments = new ArrayList<>(total - fromIndex);
        for (int i = fromIndex; i < total; i++) {
            segments.add(channel.map(MapMode.READ_WRITE, (long) i * segmentBytes, segmentBytes));
        }
        return segments;
    }

    private static void readFully(FileChannel channel, byte[] dst, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(dst);
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new IOException("EOF inesperado lendo shard em " + pos);
            }
            pos += n;
        }
    }
}
