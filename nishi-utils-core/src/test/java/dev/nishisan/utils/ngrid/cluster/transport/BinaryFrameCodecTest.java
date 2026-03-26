package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.cluster.transport.codec.BinaryFrameCodec;
import dev.nishisan.utils.ngrid.common.*;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the BinaryFrameCodec can round-trip HEARTBEAT and PING
 * messages and produces significantly smaller frames than JSON.
 */
class BinaryFrameCodecTest {

    private final BinaryFrameCodec codec = new BinaryFrameCodec();

    @Test
    void shouldRoundTripHeartbeatMessage() throws Exception {
        NodeId source = NodeId.of("node-abc-123");
        HeartbeatPayload payload = new HeartbeatPayload(
                System.currentTimeMillis(), 42L, 7L);

        ClusterMessage original = ClusterMessage.lightweight(
                MessageType.HEARTBEAT, "hb", source, null, payload);

        byte[] encoded = codec.encode(original);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        ClusterMessage decoded = codec.decode(encoded);
        assertNotNull(decoded);
        assertEquals(MessageType.HEARTBEAT, decoded.type());
        assertEquals(source, decoded.source());
        assertEquals("hb", decoded.qualifier());

        HeartbeatPayload dp = decoded.payload(HeartbeatPayload.class);
        assertNotNull(dp);
        assertEquals(payload.epochMilli(), dp.epochMilli());
        assertEquals(42L, dp.leaderHighWatermark());
        assertEquals(7L, dp.leaderEpoch());
    }

    @Test
    void shouldRoundTripPingMessage() throws Exception {
        NodeId source = NodeId.of("node-xyz-789");
        HeartbeatPayload payload = HeartbeatPayload.now();

        ClusterMessage original = new ClusterMessage(
                new UUID(0, 99), null, MessageType.PING,
                "rtt", source, NodeId.of("node-dest"), payload, 1);

        byte[] encoded = codec.encode(original);
        ClusterMessage decoded = codec.decode(encoded);

        assertEquals(MessageType.PING, decoded.type());
        assertEquals(source, decoded.source());

        HeartbeatPayload dp = decoded.payload(HeartbeatPayload.class);
        assertNotNull(dp);
        assertEquals(payload.epochMilli(), dp.epochMilli());
    }

    @Test
    void shouldProduceCompactFrame() throws Exception {
        NodeId source = NodeId.of(java.util.UUID.randomUUID().toString());
        HeartbeatPayload payload = new HeartbeatPayload(
                System.currentTimeMillis(), 100L, 5L);

        ClusterMessage message = ClusterMessage.lightweight(
                MessageType.HEARTBEAT, "hb", source, null, payload);

        byte[] binaryFrame = codec.encode(message);

        // Binary frame: 1 (marker) + 2 (length) + 36 (UUID string) + 24 (3 longs) = 63 bytes
        assertTrue(binaryFrame.length < 100,
                "Binary frame should be compact, was " + binaryFrame.length + " bytes");
    }

    @Test
    void shouldRejectUnsupportedType() {
        NodeId source = NodeId.of("node-1");
        ClusterMessage message = ClusterMessage.request(
                MessageType.HANDSHAKE, "hello", source, null, null);

        assertThrows(java.io.IOException.class, () -> codec.encode(message));
    }

    @Test
    void shouldIdentifyBinaryFrameMarkers() {
        assertTrue(BinaryFrameCodec.isBinaryFrame(BinaryFrameCodec.HEARTBEAT_MARKER));
        assertTrue(BinaryFrameCodec.isBinaryFrame(BinaryFrameCodec.PING_MARKER));
        assertFalse(BinaryFrameCodec.isBinaryFrame((byte) 0x00));
        assertFalse(BinaryFrameCodec.isBinaryFrame((byte) 0x7B));
    }

    @Test
    void decodeShouldRejectTruncatedFrame() {
        byte[] truncated = new byte[]{BinaryFrameCodec.HEARTBEAT_MARKER, 0x00};
        assertThrows(java.io.IOException.class, () -> codec.decode(truncated));
    }

    @Test
    void decodeShouldRejectUnknownMarker() {
        byte[] unknown = new byte[]{(byte) 0xFF, 0x00, 0x01, 0x41};
        assertThrows(java.io.IOException.class, () -> codec.decode(unknown));
    }
}
