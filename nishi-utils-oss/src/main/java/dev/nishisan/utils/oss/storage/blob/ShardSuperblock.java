package dev.nishisan.utils.oss.storage.blob;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Superblock de um shard ({@code shard-NN.blob}), ocupando a primeira página
 * (4096 bytes). A {@code data region} começa em {@link #HEADER_BYTES}. Codec
 * big-endian de tamanho fixo. Ver {@code doc/oss/ngrrd-blob-volume.md} §5.
 *
 * <p>{@code bumpCursor} é apenas um hint: o valor autoritativo é rederivado do
 * catálogo na reabertura. {@code shardCapacityBytes} é durável (reflete o
 * {@code setLength} do arquivo).</p>
 */
public record ShardSuperblock(
        int formatVersion,
        int shardId,
        int shardCount,
        long headerBytes,
        long shardCapacityBytes,
        long bumpCursor,
        UUID volumeUuid,
        long generation) {

    static final byte[] MAGIC = "NGRRBLOB".getBytes(StandardCharsets.US_ASCII);
    public static final int FORMAT_VERSION = 1;
    /** Tamanho do superblock (= 1 página) em bytes. */
    public static final int BYTES = 4096;
    /** Offset do início da data region (= tamanho do superblock). */
    public static final long HEADER_BYTES = 4096;
    private static final int CRC_OFFSET = 4092;

    public ShardSuperblock {
        Objects.requireNonNull(volumeUuid, "volumeUuid é obrigatório");
    }

    /** Serializa para os {@value #BYTES} bytes do superblock. */
    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(BYTES);
        buf.put(MAGIC);
        buf.putShort((short) formatVersion);
        buf.putShort((short) 0); // reserved
        buf.putInt(shardId);
        buf.putInt(shardCount);
        buf.putInt(0); // reserved
        buf.putLong(headerBytes);
        buf.putLong(shardCapacityBytes);
        buf.putLong(bumpCursor);
        buf.putLong(volumeUuid.getMostSignificantBits());
        buf.putLong(volumeUuid.getLeastSignificantBits());
        buf.putLong(generation);
        // resto da página fica em zeros (reserved)
        byte[] image = buf.array();
        ByteBuffer.wrap(image).putInt(CRC_OFFSET, BlobCodecs.crc32(image, 0, CRC_OFFSET));
        return image;
    }

    /** Decodifica e valida magic, versão e CRC. */
    public static ShardSuperblock decode(byte[] image) {
        Objects.requireNonNull(image, "image é obrigatório");
        if (image.length < BYTES) {
            throw new BlobVolumeException("superblock truncado: " + image.length + " < " + BYTES);
        }
        if (!BlobCodecs.magicMatches(image, MAGIC)) {
            throw new BlobVolumeException("superblock com MAGIC inválido");
        }
        ByteBuffer buf = ByteBuffer.wrap(image);
        if (buf.getInt(CRC_OFFSET) != BlobCodecs.crc32(image, 0, CRC_OFFSET)) {
            throw new BlobVolumeException("superblock com CRC inválido");
        }
        buf.position(MAGIC.length);
        int formatVersion = Short.toUnsignedInt(buf.getShort());
        if (formatVersion != FORMAT_VERSION) {
            throw new BlobVolumeException("superblock com formatVersion não suportado: " + formatVersion);
        }
        buf.getShort(); // reserved
        int shardId = buf.getInt();
        int shardCount = buf.getInt();
        buf.getInt(); // reserved
        long headerBytes = buf.getLong();
        long shardCapacityBytes = buf.getLong();
        long bumpCursor = buf.getLong();
        long uuidMsb = buf.getLong();
        long uuidLsb = buf.getLong();
        long generation = buf.getLong();
        return new ShardSuperblock(formatVersion, shardId, shardCount, headerBytes,
                shardCapacityBytes, bumpCursor, new UUID(uuidMsb, uuidLsb), generation);
    }
}
