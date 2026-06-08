package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.cluster.transport.codec.Lz4FrameCompressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the LZ4 block wrap/unwrap round-trip, the frame header layout
 * (marker + original length) and the guards on malformed frames.
 */
class Lz4FrameCompressorTest {

    @Test
    void shouldRoundTripTypicalPayload() throws Exception {
        byte[] original = ("{\"topic\":\"orders\",\"value\":\"" + "x".repeat(2000) + "\"}")
                .getBytes(StandardCharsets.UTF_8);

        byte[] frame = Lz4FrameCompressor.wrap(original);

        assertEquals(Lz4FrameCompressor.LZ4_JSON_MARKER, frame[0], "first byte must be the LZ4 marker");
        assertTrue(frame.length < original.length, "highly repetitive payload should compress smaller");

        byte[] restored = Lz4FrameCompressor.unwrap(frame);
        assertArrayEquals(original, restored);
    }

    @Test
    void shouldRoundTripEmptyPayload() throws Exception {
        byte[] frame = Lz4FrameCompressor.wrap(new byte[0]);
        byte[] restored = Lz4FrameCompressor.unwrap(frame);
        assertEquals(0, restored.length);
    }

    @Test
    void shouldRoundTripSmallPayload() throws Exception {
        byte[] original = "hi".getBytes(StandardCharsets.UTF_8);
        byte[] restored = Lz4FrameCompressor.unwrap(Lz4FrameCompressor.wrap(original));
        assertArrayEquals(original, restored);
    }

    @Test
    void shouldRoundTripMultiMegabytePayload() throws Exception {
        // ~4 MiB of semi-compressible data (repeating blocks): exercises the snapshot-sized path.
        byte[] block = new byte[1024];
        new Random(42).nextBytes(block);
        byte[] original = new byte[4 * 1024 * 1024];
        for (int off = 0; off < original.length; off += block.length) {
            System.arraycopy(block, 0, original, off, Math.min(block.length, original.length - off));
        }

        byte[] frame = Lz4FrameCompressor.wrap(original);
        byte[] restored = Lz4FrameCompressor.unwrap(frame);
        assertArrayEquals(original, restored);
    }

    @Test
    void shouldRoundTripIncompressiblePayload() throws Exception {
        // Random bytes barely compress; round-trip must still be exact.
        byte[] original = new byte[8192];
        new Random(7).nextBytes(original);

        byte[] restored = Lz4FrameCompressor.unwrap(Lz4FrameCompressor.wrap(original));
        assertArrayEquals(original, restored);
    }

    @Test
    void unwrapShouldRejectTruncatedFrame() {
        assertThrows(IOException.class, () -> Lz4FrameCompressor.unwrap(new byte[]{0x10, 0x00}));
    }

    @Test
    void unwrapShouldRejectWrongMarker() {
        byte[] frame = Lz4FrameCompressor.wrap("payload".getBytes(StandardCharsets.UTF_8));
        byte[] tampered = Arrays.copyOf(frame, frame.length);
        tampered[0] = 0x00; // not the LZ4 marker
        assertThrows(IOException.class, () -> Lz4FrameCompressor.unwrap(tampered));
    }

    @Test
    void unwrapShouldRejectOutOfBoundsDeclaredLength() {
        // marker + originalLength = Integer.MAX_VALUE (way past MAX_DECOMPRESSED_SIZE)
        byte[] frame = new byte[]{0x10, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x00, 0x00};
        assertThrows(IOException.class, () -> Lz4FrameCompressor.unwrap(frame));
    }
}
