package dev.nishisan.utils.oss.storage.blob;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Codec do snapshot do catálogo ({@code catalog.bin}): header + entradas de
 * tamanho variável + trailer CRC. Big-endian. Ver
 * {@code doc/oss/ngrrd-blob-volume.md} §6.
 *
 * <p>Stateless e thread-safe.</p>
 */
public final class BlobCatalogCodec {

    static final byte[] MAGIC = "NGRRCTLG".getBytes(StandardCharsets.US_ASCII);
    public static final int FORMAT_VERSION = 1;
    private static final int HEADER_BYTES = 64;
    private static final int HEADER_CRC_OFFSET = 60;
    private static final int TRAILER_BYTES = 4;

    private BlobCatalogCodec() {
    }

    /** Snapshot decodificado do catálogo. */
    public record Snapshot(int shardCount, UUID volumeUuid, long generation, List<CatalogEntry> entries) {
    }

    /** Serializa o catálogo completo. */
    public static byte[] encode(int shardCount, UUID volumeUuid, long generation, List<CatalogEntry> entries) {
        Objects.requireNonNull(volumeUuid, "volumeUuid é obrigatório");
        Objects.requireNonNull(entries, "entries é obrigatório");

        List<byte[]> keyBytes = new ArrayList<>(entries.size());
        int total = HEADER_BYTES + TRAILER_BYTES;
        for (CatalogEntry e : entries) {
            byte[] kb = e.key().getBytes(StandardCharsets.UTF_8);
            if (kb.length > 0xFFFF) {
                throw new BlobVolumeException("catalogKey excede 65535 bytes: " + e.key());
            }
            keyBytes.add(kb);
            total += 2 + kb.length + 4 + 8 + 8 + 8 + 1 + 4;
        }

        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.put(MAGIC);
        buf.putShort((short) FORMAT_VERSION);
        buf.putShort((short) 0); // reserved
        buf.putInt(shardCount);
        buf.putLong(volumeUuid.getMostSignificantBits());
        buf.putLong(volumeUuid.getLeastSignificantBits());
        buf.putLong(generation);
        buf.putInt(entries.size());
        buf.put(new byte[16]); // reserved -> posição 60
        byte[] image = buf.array();
        buf.putInt(BlobCodecs.crc32(image, 0, HEADER_CRC_OFFSET)); // header CRC -> posição 64

        for (int i = 0; i < entries.size(); i++) {
            CatalogEntry e = entries.get(i);
            byte[] kb = keyBytes.get(i);
            int entryStart = buf.position();
            buf.putShort((short) kb.length);
            buf.put(kb);
            buf.putInt(e.shardId());
            buf.putLong(e.regionOffset());
            buf.putLong(e.regionBytes());
            buf.putLong(e.objectBytes());
            buf.put(e.state().code());
            int entryLen = buf.position() - entryStart;
            buf.putInt(BlobCodecs.crc32(image, entryStart, entryLen));
        }

        buf.putInt(BlobCodecs.crc32(image, HEADER_BYTES, buf.position() - HEADER_BYTES));
        return image;
    }

    /** Decodifica e valida header CRC, CRC por entrada e trailer CRC. */
    public static Snapshot decode(byte[] image) {
        Objects.requireNonNull(image, "image é obrigatório");
        if (image.length < HEADER_BYTES + TRAILER_BYTES) {
            throw new BlobVolumeException("catalog.bin truncado: " + image.length);
        }
        if (!BlobCodecs.magicMatches(image, MAGIC)) {
            throw new BlobVolumeException("catalog.bin com MAGIC inválido");
        }
        ByteBuffer buf = ByteBuffer.wrap(image);
        if (buf.getInt(HEADER_CRC_OFFSET) != BlobCodecs.crc32(image, 0, HEADER_CRC_OFFSET)) {
            throw new BlobVolumeException("catalog.bin com header CRC inválido");
        }
        int trailerOffset = image.length - TRAILER_BYTES;
        if (buf.getInt(trailerOffset) != BlobCodecs.crc32(image, HEADER_BYTES, trailerOffset - HEADER_BYTES)) {
            throw new BlobVolumeException("catalog.bin com trailer CRC inválido");
        }

        buf.position(MAGIC.length);
        int formatVersion = Short.toUnsignedInt(buf.getShort());
        if (formatVersion != FORMAT_VERSION) {
            throw new BlobVolumeException("catalog.bin com formatVersion não suportado: " + formatVersion);
        }
        buf.getShort(); // reserved
        int shardCount = buf.getInt();
        UUID volumeUuid = new UUID(buf.getLong(), buf.getLong());
        long generation = buf.getLong();
        int entryCount = buf.getInt();

        List<CatalogEntry> entries = new ArrayList<>(Math.max(0, entryCount));
        buf.position(HEADER_BYTES);
        try {
            for (int i = 0; i < entryCount; i++) {
                int entryStart = buf.position();
                int keyLen = Short.toUnsignedInt(buf.getShort());
                byte[] kb = new byte[keyLen];
                buf.get(kb);
                int shardId = buf.getInt();
                long regionOffset = buf.getLong();
                long regionBytes = buf.getLong();
                long objectBytes = buf.getLong();
                byte stateCode = buf.get();
                int entryLen = buf.position() - entryStart;
                int storedCrc = buf.getInt();
                if (storedCrc != BlobCodecs.crc32(image, entryStart, entryLen)) {
                    throw new BlobVolumeException("catalog.bin com CRC inválido na entrada " + i);
                }
                entries.add(new CatalogEntry(new String(kb, StandardCharsets.UTF_8),
                        shardId, regionOffset, regionBytes, objectBytes, CatalogEntry.State.fromCode(stateCode)));
            }
        } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
            throw new BlobVolumeException("catalog.bin com entradas malformadas", e);
        }
        if (buf.position() != trailerOffset) {
            throw new BlobVolumeException("catalog.bin: entryCount inconsistente com o conteúdo");
        }
        return new Snapshot(shardCount, volumeUuid, generation, List.copyOf(entries));
    }
}
