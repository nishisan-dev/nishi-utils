package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockCodecTest {

    private static BlockHeader sampleHeader(int rows) {
        return new BlockHeader(
                BlockHeader.CURRENT_VERSION,
                BlockHeader.flags(false, false, false),
                300,
                ConsolidationFunction.AVERAGE,
                DataSourceType.COUNTER,
                1_747_339_200L,
                rows);
    }

    @Test
    void roundTripPreservaHeaderEPayload() {
        BlockHeader header = sampleHeader(5);
        double[] payload = {1.0, 2.5, Double.NaN, -3.0, 0.0};

        byte[] encoded = BlockCodec.encode(header, payload);
        EncodedBlock decoded = BlockCodec.decode(encoded);

        assertEquals(header, decoded.header());
        assertArrayEquals(payload, decoded.payload());
        assertEquals(BlockHeader.HEADER_BYTES + payload.length * Double.BYTES, encoded.length);
    }

    @Test
    void roundTripComPayloadVazio() {
        BlockHeader header = sampleHeader(0);
        double[] payload = new double[0];

        byte[] encoded = BlockCodec.encode(header, payload);
        EncodedBlock decoded = BlockCodec.decode(encoded);

        assertEquals(header, decoded.header());
        assertEquals(0, decoded.payload().length);
        assertEquals(BlockHeader.HEADER_BYTES, encoded.length);
    }

    @Test
    void preservaNaNQuandoCelulaMissing() {
        BlockHeader header = sampleHeader(3);
        double[] payload = {Double.NaN, Double.NaN, Double.NaN};

        EncodedBlock decoded = BlockCodec.decode(BlockCodec.encode(header, payload));

        for (double v : decoded.payload()) {
            assertTrue(Double.isNaN(v));
        }
    }

    @Test
    void preservaFlagsBitsIndividualmente() {
        BlockHeader header = new BlockHeader(
                BlockHeader.CURRENT_VERSION,
                BlockHeader.flags(false, true, true),
                300,
                ConsolidationFunction.MAX,
                DataSourceType.GAUGE,
                1_747_339_200L,
                2);
        EncodedBlock decoded = BlockCodec.decode(BlockCodec.encode(header, new double[]{1, 2}));

        assertTrue(decoded.header().partial());
        assertTrue(decoded.header().agg());
        assertEquals(ConsolidationFunction.MAX, decoded.header().cf());
        assertEquals(DataSourceType.GAUGE, decoded.header().ds());
    }

    @Test
    void rejeitaPayloadDeTamanhoIncompativel() {
        BlockHeader header = sampleHeader(5);
        assertThrows(IllegalArgumentException.class,
                () -> BlockCodec.encode(header, new double[]{1.0, 2.0}));
    }

    @Test
    void rejeitaMagicCorrompido() {
        BlockHeader header = sampleHeader(2);
        byte[] encoded = BlockCodec.encode(header, new double[]{1, 2});
        encoded[0] = 'X';

        NgrrdFormatException ex = assertThrows(NgrrdFormatException.class,
                () -> BlockCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("MAGIC"));
    }

    @Test
    void rejeitaVersaoNaoSuportada() {
        BlockHeader header = sampleHeader(2);
        byte[] encoded = BlockCodec.encode(header, new double[]{1, 2});
        // Sobrescreve VER (bytes 4 e 5) com 99
        encoded[4] = 0;
        encoded[5] = 99;

        NgrrdFormatException ex = assertThrows(NgrrdFormatException.class,
                () -> BlockCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("Versão"));
    }

    @Test
    void rejeitaBlocoTruncado() {
        BlockHeader header = sampleHeader(2);
        byte[] encoded = BlockCodec.encode(header, new double[]{1, 2});
        byte[] truncated = new byte[encoded.length - 5];
        System.arraycopy(encoded, 0, truncated, 0, truncated.length);

        assertThrows(NgrrdFormatException.class, () -> BlockCodec.decode(truncated));
    }

    @Test
    void rejeitaPayloadCorrompidoViaCrc() {
        BlockHeader header = sampleHeader(3);
        byte[] encoded = BlockCodec.encode(header, new double[]{1, 2, 3});
        // Mutação de 1 byte no payload (após o header) — CRC deve detectar.
        int mutationIndex = BlockHeader.HEADER_BYTES + 4;
        encoded[mutationIndex] = (byte) (encoded[mutationIndex] ^ 0x01);

        NgrrdFormatException ex = assertThrows(NgrrdFormatException.class,
                () -> BlockCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("CRC32"));
    }

    @Test
    void rejeitaMutacaoNoHeaderViaCrc() {
        BlockHeader header = sampleHeader(2);
        byte[] encoded = BlockCodec.encode(header, new double[]{42, 7});
        // Flipa um bit do campo STEP — header íntegro mas inconsistente com CRC
        encoded[8] = (byte) (encoded[8] ^ 0x02);

        assertThrows(NgrrdFormatException.class, () -> BlockCodec.decode(encoded));
    }

    @Test
    void crcDiferenteParaPayloadsDiferentes() {
        BlockHeader header = sampleHeader(2);
        EncodedBlock a = BlockCodec.decode(BlockCodec.encode(header, new double[]{1, 2}));
        EncodedBlock b = BlockCodec.decode(BlockCodec.encode(header, new double[]{1, 3}));
        assertNotEquals(a.crc32(), b.crc32());
    }
}
