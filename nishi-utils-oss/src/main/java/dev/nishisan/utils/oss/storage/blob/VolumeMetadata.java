package dev.nishisan.utils.oss.storage.blob;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Metadados imutáveis de um blob volume ({@code volume.meta}). Codec big-endian
 * de tamanho fixo. Ver {@code doc/oss/ngrrd-blob-volume.md} §4.
 */
public record VolumeMetadata(
        int formatVersion,
        int shardCount,
        UUID volumeUuid,
        long segmentBytes,
        int routingAlgorithm,
        int routingVersion,
        long generation) {

    static final byte[] MAGIC = "NGRRVOL1".getBytes(StandardCharsets.US_ASCII);
    public static final int FORMAT_VERSION = 1;
    /** Tamanho total do {@code volume.meta} em bytes. */
    public static final int BYTES = 56;
    private static final int CRC_OFFSET = 52;

    public VolumeMetadata {
        Objects.requireNonNull(volumeUuid, "volumeUuid é obrigatório");
    }

    /** Serializa para os {@value #BYTES} bytes do {@code volume.meta}. */
    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(BYTES);
        buf.put(MAGIC);
        buf.putShort((short) formatVersion);
        buf.putShort((short) 0); // reserved
        buf.putInt(shardCount);
        buf.putLong(volumeUuid.getMostSignificantBits());
        buf.putLong(volumeUuid.getLeastSignificantBits());
        buf.putLong(segmentBytes);
        buf.putShort((short) routingAlgorithm);
        buf.putShort((short) routingVersion);
        buf.putLong(generation);
        byte[] image = buf.array();
        ByteBuffer.wrap(image).putInt(CRC_OFFSET, BlobCodecs.crc32(image, 0, CRC_OFFSET));
        return image;
    }

    /** Decodifica e valida magic, versão e CRC. */
    public static VolumeMetadata decode(byte[] image) {
        Objects.requireNonNull(image, "image é obrigatório");
        if (image.length < BYTES) {
            throw new BlobVolumeException("volume.meta truncado: " + image.length + " < " + BYTES);
        }
        if (!BlobCodecs.magicMatches(image, MAGIC)) {
            throw new BlobVolumeException("volume.meta com MAGIC inválido");
        }
        ByteBuffer buf = ByteBuffer.wrap(image);
        if (buf.getInt(CRC_OFFSET) != BlobCodecs.crc32(image, 0, CRC_OFFSET)) {
            throw new BlobVolumeException("volume.meta com CRC inválido");
        }
        buf.position(MAGIC.length);
        int formatVersion = Short.toUnsignedInt(buf.getShort());
        if (formatVersion != FORMAT_VERSION) {
            throw new BlobVolumeException("volume.meta com formatVersion não suportado: " + formatVersion);
        }
        buf.getShort(); // reserved
        int shardCount = buf.getInt();
        long uuidMsb = buf.getLong();
        long uuidLsb = buf.getLong();
        long segmentBytes = buf.getLong();
        int routingAlgorithm = Short.toUnsignedInt(buf.getShort());
        int routingVersion = Short.toUnsignedInt(buf.getShort());
        long generation = buf.getLong();
        return new VolumeMetadata(formatVersion, shardCount, new UUID(uuidMsb, uuidLsb),
                segmentBytes, routingAlgorithm, routingVersion, generation);
    }
}
