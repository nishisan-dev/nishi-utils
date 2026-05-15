package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockHeaderTest {

    @Test
    void magicEhAsciiNgrd() {
        assertEquals(0x4E475244, BlockHeader.MAGIC);
        byte[] expected = {'N', 'G', 'R', 'D'};
        assertEquals(expected[0], (byte) (BlockHeader.MAGIC >>> 24));
        assertEquals(expected[1], (byte) (BlockHeader.MAGIC >>> 16));
        assertEquals(expected[2], (byte) (BlockHeader.MAGIC >>> 8));
        assertEquals(expected[3], (byte) BlockHeader.MAGIC);
    }

    @Test
    void headerBytesEhVintEOitoEsemCrcVinteESeis() {
        assertEquals(30, BlockHeader.HEADER_BYTES);
        assertEquals(26, BlockHeader.HEADER_BYTES_NO_CRC);
    }

    @Test
    void flagsBuilderCombinaBitsCorretamente() {
        int flags = BlockHeader.flags(true, false, true);
        BlockHeader header = new BlockHeader(
                1, flags, 60, ConsolidationFunction.AVERAGE, DataSourceType.COUNTER, 0L, 10);
        assertTrue(header.compressed());
        assertFalse(header.partial());
        assertTrue(header.agg());
    }

    @Test
    void rejeitaBlockStartEpochNaoAlinhadoAoStep() {
        assertThrows(IllegalArgumentException.class, () -> new BlockHeader(
                1, 0, 60, ConsolidationFunction.LAST, DataSourceType.GAUGE, 65L, 1));
    }

    @Test
    void rejeitaVersionForaDoIntervaloUint16() {
        assertThrows(IllegalArgumentException.class, () -> new BlockHeader(
                0, 0, 60, ConsolidationFunction.LAST, DataSourceType.GAUGE, 0L, 1));
        assertThrows(IllegalArgumentException.class, () -> new BlockHeader(
                0x10000, 0, 60, ConsolidationFunction.LAST, DataSourceType.GAUGE, 0L, 1));
    }

    @Test
    void rejeitaStepInvalido() {
        assertThrows(IllegalArgumentException.class, () -> new BlockHeader(
                1, 0, 0, ConsolidationFunction.LAST, DataSourceType.GAUGE, 0L, 1));
    }

    @Test
    void rejeitaRowsNegativos() {
        assertThrows(IllegalArgumentException.class, () -> new BlockHeader(
                1, 0, 60, ConsolidationFunction.LAST, DataSourceType.GAUGE, 0L, -1));
    }
}
