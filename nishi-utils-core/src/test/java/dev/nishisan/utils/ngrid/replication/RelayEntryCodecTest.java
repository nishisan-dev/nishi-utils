package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class RelayEntryCodecTest {

    @Test
    void roundTripPreservesAllFields() {
        UUID op = UUID.randomUUID();
        byte[] payload = "hello-payload".getBytes(StandardCharsets.UTF_8);
        RelayEntry entry = new RelayEntry(100L, 4242L, "queue:orders:eu", op, payload);

        RelayEntry decoded = RelayEntryCodec.decode(RelayEntryCodec.encode(entry));

        assertEquals(100L, decoded.epoch());
        assertEquals(4242L, decoded.sequence());
        assertEquals("queue:orders:eu", decoded.topic());
        assertEquals(op, decoded.operationId());
        assertArrayEquals(payload, decoded.payloadBytes());
    }

    @Test
    void roundTripHandlesEmptyPayloadAndUnicodeTopicWithColon() {
        UUID op = UUID.randomUUID();
        RelayEntry entry = new RelayEntry(0L, 0L, "tópico-çãö:λ", op, new byte[0]);

        RelayEntry decoded = RelayEntryCodec.decode(RelayEntryCodec.encode(entry));

        assertEquals("tópico-çãö:λ", decoded.topic());
        assertEquals(0, decoded.payloadBytes().length);
        assertEquals(op, decoded.operationId());
    }

    @Test
    void roundTripHandlesExtremeNumericValuesAndLargePayload() {
        UUID op = new UUID(Long.MIN_VALUE, Long.MAX_VALUE);
        byte[] big = new byte[64 * 1024];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i % 251);
        }
        RelayEntry entry = new RelayEntry(Long.MAX_VALUE, Long.MIN_VALUE, "t", op, big);

        RelayEntry decoded = RelayEntryCodec.decode(RelayEntryCodec.encode(entry));

        assertEquals(Long.MAX_VALUE, decoded.epoch());
        assertEquals(Long.MIN_VALUE, decoded.sequence());
        assertEquals(op, decoded.operationId());
        assertArrayEquals(big, decoded.payloadBytes());
    }

    @Test
    void decodeRejectsUnknownVersion() {
        byte[] frame = RelayEntryCodec.encode(new RelayEntry(1L, 1L, "t", UUID.randomUUID(), new byte[] { 1 }));
        frame[0] = 9; // corrupt the version byte
        assertThrows(IllegalArgumentException.class, () -> RelayEntryCodec.decode(frame));
    }

    @Test
    void recordRejectsNullArguments() {
        UUID op = UUID.randomUUID();
        assertThrows(NullPointerException.class, () -> new RelayEntry(1L, 1L, null, op, new byte[0]));
        assertThrows(NullPointerException.class, () -> new RelayEntry(1L, 1L, "t", null, new byte[0]));
        assertThrows(NullPointerException.class, () -> new RelayEntry(1L, 1L, "t", op, null));
    }
}
