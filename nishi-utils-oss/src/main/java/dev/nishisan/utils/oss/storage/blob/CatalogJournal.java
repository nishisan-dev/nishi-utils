package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.blob.CatalogEntry.State;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Journal append-only do catálogo ({@code catalog.wal}). Cada mutação estrutural
 * (ALLOC/FREE) é gravada como {@code u32 frameLen | payload} e tornada durável
 * (fsync) antes de ser publicada em memória. O replay tolera cauda truncada ou
 * corrompida (trunca o arquivo até o último registro íntegro), espelhando o
 * padrão de {@code NMapPersistence}. Ver {@code doc/oss/ngrrd-blob-volume.md} §7.
 */
public final class CatalogJournal implements AutoCloseable {

    private static final byte[] MAGIC = "NCWA".getBytes(StandardCharsets.US_ASCII);
    private static final int ENTRY_VERSION = 1;
    private static final int MIN_PAYLOAD = 4 + 2 + 1 + 1 + 8 + 2 + 0 + 4 + 8 + 8 + 8 + 4;
    private static final int MAX_PAYLOAD = 1 << 20;

    private final FileChannel channel;

    public CatalogJournal(Path walPath) {
        Objects.requireNonNull(walPath, "walPath é obrigatório");
        try {
            this.channel = FileChannel.open(walPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao abrir WAL do catálogo: " + walPath, e);
        }
    }

    /** Operação registrada no WAL. */
    public enum WalOp {
        ALLOC((byte) 1),
        FREE((byte) 2);

        private final byte code;

        WalOp(byte code) {
            this.code = code;
        }

        byte code() {
            return code;
        }

        static WalOp fromCode(byte code) {
            return switch (code) {
                case 1 -> ALLOC;
                case 2 -> FREE;
                default -> throw new BlobVolumeException("opType de WAL desconhecido: " + code);
            };
        }
    }

    /** Registro decodificado do WAL (a {@code state} da entrada deriva da operação). */
    public record WalRecord(WalOp op, long generation, CatalogEntry entry) {
    }

    public void appendAlloc(long generation, CatalogEntry entry) {
        append(new WalRecord(WalOp.ALLOC, generation, entry));
    }

    public void appendFree(long generation, CatalogEntry entry) {
        append(new WalRecord(WalOp.FREE, generation, entry));
    }

    private void append(WalRecord record) {
        byte[] payload = encodePayload(record);
        ByteBuffer frame = ByteBuffer.allocate(4 + payload.length);
        frame.putInt(payload.length);
        frame.put(payload);
        frame.flip();
        try {
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao gravar no WAL do catálogo", e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao fechar WAL do catálogo", e);
        }
    }

    private static byte[] encodePayload(WalRecord record) {
        byte[] key = record.entry().key().getBytes(StandardCharsets.UTF_8);
        int len = MIN_PAYLOAD + key.length;
        ByteBuffer b = ByteBuffer.allocate(len);
        b.put(MAGIC);
        b.putShort((short) ENTRY_VERSION);
        b.put(record.op().code());
        b.put((byte) 0); // reserved
        b.putLong(record.generation());
        b.putShort((short) key.length);
        b.put(key);
        b.putInt(record.entry().shardId());
        b.putLong(record.entry().regionOffset());
        b.putLong(record.entry().regionBytes());
        b.putLong(record.entry().objectBytes());
        byte[] image = b.array();
        b.putInt(BlobCodecs.crc32(image, 0, len - 4));
        return image;
    }

    /**
     * Lê todos os registros íntegros do WAL na ordem de gravação. Trunca o arquivo
     * removendo qualquer cauda truncada/corrompida. Arquivo ausente → lista vazia.
     */
    public static List<WalRecord> replay(Path walPath) {
        Objects.requireNonNull(walPath, "walPath é obrigatório");
        if (!Files.exists(walPath)) {
            return List.of();
        }
        try (FileChannel ch = FileChannel.open(walPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long size = ch.size();
            long pos = 0;
            long goodEnd = 0;
            List<WalRecord> out = new ArrayList<>();
            while (pos + 4 <= size) {
                int frameLen = readInt(ch, pos);
                if (frameLen < MIN_PAYLOAD || frameLen > MAX_PAYLOAD || pos + 4 + frameLen > size) {
                    break; // cauda truncada
                }
                byte[] payload = read(ch, pos + 4, frameLen);
                WalRecord record = decodePayload(payload);
                if (record == null) {
                    break; // cauda corrompida
                }
                out.add(record);
                pos += 4 + frameLen;
                goodEnd = pos;
            }
            if (goodEnd < size) {
                ch.truncate(goodEnd);
            }
            return out;
        } catch (IOException e) {
            throw new BlobVolumeException("falha ao reproduzir WAL do catálogo: " + walPath, e);
        }
    }

    private static WalRecord decodePayload(byte[] payload) {
        if (payload.length < MIN_PAYLOAD || !BlobCodecs.magicMatches(payload, MAGIC)) {
            return null;
        }
        if (ByteBuffer.wrap(payload).getInt(payload.length - 4) != BlobCodecs.crc32(payload, 0, payload.length - 4)) {
            return null;
        }
        ByteBuffer b = ByteBuffer.wrap(payload);
        b.position(MAGIC.length);
        int version = Short.toUnsignedInt(b.getShort());
        if (version != ENTRY_VERSION) {
            return null;
        }
        WalOp op = WalOp.fromCode(b.get());
        b.get(); // reserved
        long generation = b.getLong();
        int keyLen = Short.toUnsignedInt(b.getShort());
        if (keyLen != payload.length - (MIN_PAYLOAD)) {
            return null;
        }
        byte[] key = new byte[keyLen];
        b.get(key);
        int shardId = b.getInt();
        long regionOffset = b.getLong();
        long regionBytes = b.getLong();
        long objectBytes = b.getLong();
        State state = op == WalOp.ALLOC ? State.LIVE : State.DELETED;
        return new WalRecord(op, generation, new CatalogEntry(
                new String(key, StandardCharsets.UTF_8), shardId, regionOffset, regionBytes, objectBytes, state));
    }

    private static int readInt(FileChannel ch, long pos) throws IOException {
        return ByteBuffer.wrap(read(ch, pos, 4)).getInt();
    }

    private static byte[] read(FileChannel ch, long pos, int len) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(len);
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) {
                throw new IOException("EOF inesperado no WAL em " + p);
            }
            p += n;
        }
        return buf.array();
    }
}
