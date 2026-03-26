package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.cluster.transport.codec.BinaryFrameCodec;
import dev.nishisan.utils.ngrid.cluster.transport.codec.CompositeMessageCodec;
import dev.nishisan.utils.ngrid.common.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the CompositeMessageCodec correctly dispatches between
 * BinaryFrameCodec (for HEARTBEAT/PING) and JacksonMessageCodec (for all other types).
 */
class CompositeMessageCodecTest {

    private final CompositeMessageCodec codec = new CompositeMessageCodec();

    @Test
    void heartbeatShouldUseBinaryCodec() throws Exception {
        NodeId source = NodeId.of("node-1");
        HeartbeatPayload payload = new HeartbeatPayload(
                System.currentTimeMillis(), 10L, 2L);
        ClusterMessage message = ClusterMessage.lightweight(
                MessageType.HEARTBEAT, "hb", source, null, payload);

        byte[] encoded = codec.encode(message);
        assertNotNull(encoded);
        // First byte should be the binary HEARTBEAT marker
        assertEquals(BinaryFrameCodec.HEARTBEAT_MARKER, encoded[0]);

        ClusterMessage decoded = codec.decode(encoded);
        assertEquals(MessageType.HEARTBEAT, decoded.type());
        assertEquals(source, decoded.source());

        HeartbeatPayload dp = decoded.payload(HeartbeatPayload.class);
        assertEquals(10L, dp.leaderHighWatermark());
        assertEquals(2L, dp.leaderEpoch());
    }

    @Test
    void pingShouldUseBinaryCodec() throws Exception {
        NodeId source = NodeId.of("node-2");
        HeartbeatPayload payload = HeartbeatPayload.now();
        ClusterMessage message = ClusterMessage.lightweight(
                MessageType.PING, "rtt", source, NodeId.of("node-3"), payload);

        byte[] encoded = codec.encode(message);
        assertEquals(BinaryFrameCodec.PING_MARKER, encoded[0]);

        ClusterMessage decoded = codec.decode(encoded);
        assertEquals(MessageType.PING, decoded.type());
        assertEquals(source, decoded.source());
    }

    @Test
    void handshakeShouldUseJacksonCodec() throws Exception {
        NodeId source = NodeId.of("node-1");
        NodeId dest = NodeId.of("node-2");
        NodeInfo sourceInfo = new NodeInfo(source, "127.0.0.1", 5000);
        NodeInfo destInfo = new NodeInfo(dest, "127.0.0.2", 5001);

        HandshakePayload payload = new HandshakePayload(
                sourceInfo,
                java.util.Set.of(sourceInfo, destInfo),
                java.util.Map.of(dest, 1.5));

        ClusterMessage message = ClusterMessage.request(
                MessageType.HANDSHAKE, "hello", source, dest, payload);

        byte[] encoded = codec.encode(message);
        assertNotNull(encoded);
        // First byte should be the JSON marker (0x00)
        assertEquals(0x00, encoded[0]);

        ClusterMessage decoded = codec.decode(encoded);
        assertEquals(MessageType.HANDSHAKE, decoded.type());
        assertEquals(source, decoded.source());
        assertEquals(dest, decoded.destination());
    }

    @Test
    void shouldDecodeLegacyJsonWithoutMarker() throws Exception {
        // Simulate a legacy JSON frame (starts with '{')
        NodeId source = NodeId.of("node-1");
        HeartbeatPayload payload = new HeartbeatPayload(
                System.currentTimeMillis(), -1L, 0L);
        ClusterMessage message = ClusterMessage.request(
                MessageType.HEARTBEAT, "hb", source, null, payload);

        // Encode with JacksonMessageCodec directly (no marker)
        dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec jacksonCodec =
                new dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec();
        byte[] legacyJson = jacksonCodec.encode(message);
        assertTrue(legacyJson.length > 0);
        // Standard Jackson JSON starts with '{'
        assertEquals((byte) '{', legacyJson[0]);

        // CompositeCodec should decode legacy JSON without marker via fallback
        ClusterMessage decoded = codec.decode(legacyJson);
        assertNotNull(decoded);
        assertEquals(MessageType.HEARTBEAT, decoded.type());
        assertEquals(source, decoded.source());
    }

    @Test
    void shouldRejectEmptyFrame() {
        assertThrows(java.io.IOException.class, () -> codec.decode(new byte[0]));
        assertThrows(java.io.IOException.class, () -> codec.decode(null));
    }

    @Test
    void binaryFrameShouldBeSmallerThanJson() throws Exception {
        NodeId source = NodeId.of(java.util.UUID.randomUUID().toString());
        HeartbeatPayload payload = new HeartbeatPayload(
                System.currentTimeMillis(), 100L, 5L);
        ClusterMessage message = ClusterMessage.lightweight(
                MessageType.HEARTBEAT, "hb", source, null, payload);

        byte[] binaryEncoded = codec.encode(message);

        // Encode same with Jackson for comparison
        dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec jacksonCodec =
                new dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec();
        byte[] jsonEncoded = jacksonCodec.encode(message);

        assertTrue(binaryEncoded.length < jsonEncoded.length,
                String.format("Binary (%d bytes) should be smaller than JSON (%d bytes)",
                        binaryEncoded.length, jsonEncoded.length));
    }
}
