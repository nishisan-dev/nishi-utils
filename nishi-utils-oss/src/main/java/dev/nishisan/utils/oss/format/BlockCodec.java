package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Codec de bloco binário NGRD. Serializa e desserializa o par
 * {@link BlockHeader} + payload {@code double[]} usando o layout descrito em
 * {@link dev.nishisan.utils.oss.format} com CRC32 calculado sobre
 * header (excluindo o próprio CRC) + payload.
 *
 * <p>Stateless e thread-safe.</p>
 */
public final class BlockCodec {

    private static final ConsolidationFunction[] CFS = ConsolidationFunction.values();
    private static final DataSourceType[] DSS = DataSourceType.values();

    private BlockCodec() {
    }

    /**
     * Serializa {@code header + payload} em um array {@code byte[]} pronto para
     * persistir. O CRC32 é gerado automaticamente.
     *
     * @throws IllegalArgumentException se {@code payload.length != header.rows()}
     */
    public static byte[] encode(BlockHeader header, double[] payload) {
        Objects.requireNonNull(header, "header é obrigatório");
        Objects.requireNonNull(payload, "payload é obrigatório");
        if (payload.length != header.rows()) {
            throw new IllegalArgumentException(
                    "payload.length=" + payload.length + " incompatível com header.rows=" + header.rows());
        }

        int payloadBytes = header.rows() * Double.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(BlockHeader.HEADER_BYTES + payloadBytes)
                .order(ByteOrder.BIG_ENDIAN);

        writeHeaderNoCrc(buffer, header);
        for (double v : payload) {
            buffer.putDouble(v);
        }

        byte[] bytes = buffer.array();
        int crc = computeCrc(bytes);
        ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(BlockHeader.HEADER_BYTES_NO_CRC, crc);

        return bytes;
    }

    /**
     * Desserializa um bloco previamente produzido por {@link #encode(BlockHeader, double[])}.
     *
     * @throws NgrrdFormatException se o magic for inválido, a versão não for
     *         suportada, o tamanho estiver inconsistente ou o CRC32 divergir
     */
    public static EncodedBlock decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes é obrigatório");
        if (bytes.length < BlockHeader.HEADER_BYTES) {
            throw new NgrrdFormatException("Bloco truncado: " + bytes.length + " bytes < HEADER_BYTES");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        int magic = buffer.getInt();
        if (magic != BlockHeader.MAGIC) {
            throw new NgrrdFormatException(
                    String.format("MAGIC inválido: esperado 0x%08X, lido 0x%08X", BlockHeader.MAGIC, magic));
        }

        int version = Short.toUnsignedInt(buffer.getShort());
        if (version != BlockHeader.CURRENT_VERSION) {
            throw new NgrrdFormatException(
                    "Versão de formato não suportada: " + version + " (suportada: " + BlockHeader.CURRENT_VERSION + ")");
        }

        int flags = Short.toUnsignedInt(buffer.getShort());
        int stepSec = buffer.getInt();
        int cfOrdinal = Byte.toUnsignedInt(buffer.get());
        int dsOrdinal = Byte.toUnsignedInt(buffer.get());
        long blockStartEpoch = buffer.getLong();
        int rows = buffer.getInt();
        int storedCrc = buffer.getInt();

        if (cfOrdinal >= CFS.length) {
            throw new NgrrdFormatException("CF ordinal desconhecido: " + cfOrdinal);
        }
        if (dsOrdinal >= DSS.length) {
            throw new NgrrdFormatException("DS ordinal desconhecido: " + dsOrdinal);
        }

        int expectedTotal = BlockHeader.HEADER_BYTES + rows * Double.BYTES;
        if (bytes.length != expectedTotal) {
            throw new NgrrdFormatException(
                    "Tamanho inconsistente: bytes.length=" + bytes.length + " esperado=" + expectedTotal);
        }

        int actualCrc = computeCrc(bytes);
        if (actualCrc != storedCrc) {
            throw new NgrrdFormatException(
                    String.format("CRC32 divergente: armazenado=0x%08X calculado=0x%08X", storedCrc, actualCrc));
        }

        double[] payload = new double[rows];
        for (int i = 0; i < rows; i++) {
            payload[i] = buffer.getDouble();
        }

        BlockHeader header = new BlockHeader(
                version, flags, stepSec, CFS[cfOrdinal], DSS[dsOrdinal], blockStartEpoch, rows);
        return new EncodedBlock(header, payload, storedCrc);
    }

    private static void writeHeaderNoCrc(ByteBuffer buffer, BlockHeader header) {
        buffer.putInt(BlockHeader.MAGIC);
        buffer.putShort((short) header.version());
        buffer.putShort((short) header.flags());
        buffer.putInt(header.stepSec());
        buffer.put((byte) header.cf().ordinal());
        buffer.put((byte) header.ds().ordinal());
        buffer.putLong(header.blockStartEpoch());
        buffer.putInt(header.rows());
        // 4 bytes reservados para CRC32 — preenchidos depois.
        buffer.putInt(0);
    }

    /**
     * CRC32 calculado sobre os primeiros {@link BlockHeader#HEADER_BYTES_NO_CRC}
     * bytes seguidos do payload completo, ignorando o slot do próprio CRC.
     */
    private static int computeCrc(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, BlockHeader.HEADER_BYTES_NO_CRC);
        int payloadOffset = BlockHeader.HEADER_BYTES;
        int payloadLength = bytes.length - payloadOffset;
        if (payloadLength > 0) {
            crc.update(bytes, payloadOffset, payloadLength);
        }
        return (int) crc.getValue();
    }
}
