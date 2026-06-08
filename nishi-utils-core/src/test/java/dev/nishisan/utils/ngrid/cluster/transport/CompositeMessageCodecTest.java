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

    // ── LZ4 compression (marker 0x10) ────────────────────────────────────────

    /** Builds a HANDSHAKE message whose JSON payload is large and highly compressible. */
    private static ClusterMessage largeCompressibleMessage() {
        NodeId source = NodeId.of("node-compress");
        // A peer set with many repetitive entries inflates the JSON well past the threshold.
        java.util.Set<NodeInfo> peers = new java.util.HashSet<>();
        for (int i = 0; i < 60; i++) {
            peers.add(new NodeInfo(NodeId.of("node-aaaaaaaaaaaaaaaa-" + i), "10.0.0." + i, 5000 + i));
        }
        NodeInfo local = new NodeInfo(source, "10.0.0.1", 5000);
        peers.add(local);
        HandshakePayload payload = new HandshakePayload(local, peers, java.util.Map.of());
        return ClusterMessage.request(MessageType.HANDSHAKE, "hello", source, null, payload);
    }

    @Test
    void shouldCompressLargeJsonWhenEnabled() throws Exception {
        CompositeMessageCodec compressing = new CompositeMessageCodec();
        compressing.setCompressOutput(true);
        ClusterMessage message = largeCompressibleMessage();

        byte[] plain = codec.encode(message);                 // default codec: compression off
        byte[] encoded = compressing.encode(message);

        assertEquals(dev.nishisan.utils.ngrid.cluster.transport.codec.Lz4FrameCompressor.LZ4_JSON_MARKER,
                encoded[0], "large compressible JSON should be LZ4-framed");
        assertTrue(encoded.length < plain.length, "compressed frame should be smaller than plain JSON");

        ClusterMessage decoded = compressing.decode(encoded);
        assertEquals(MessageType.HANDSHAKE, decoded.type());
        assertEquals(message.source(), decoded.source());
    }

    @Test
    void shouldNotCompressBelowThreshold() throws Exception {
        CompositeMessageCodec compressing = new CompositeMessageCodec();
        compressing.setCompressOutput(true);

        // A tiny PEER_UPDATE-style message: well below the 512-byte default threshold.
        NodeId source = NodeId.of("n1");
        HeartbeatPayload payload = new HeartbeatPayload(1L, 2L, 3L);
        ClusterMessage small = ClusterMessage.request(MessageType.SYNC_REQUEST, "s", source, null, payload);

        byte[] encoded = compressing.encode(small);
        assertEquals(0x00, encoded[0], "payload below threshold must stay uncompressed (0x00)");
        assertEquals(MessageType.SYNC_REQUEST, compressing.decode(encoded).type());
    }

    @Test
    void shouldNotCompressWhenDisabled() throws Exception {
        // Default codec has compression disabled; even a large payload stays 0x00.
        byte[] encoded = codec.encode(largeCompressibleMessage());
        assertEquals(0x00, encoded[0], "compression disabled must always produce a 0x00 frame");
    }

    @Test
    void heartbeatStaysBinaryEvenWhenCompressionEnabled() throws Exception {
        CompositeMessageCodec compressing = new CompositeMessageCodec();
        compressing.setCompressOutput(true);
        ClusterMessage hb = ClusterMessage.lightweight(
                MessageType.HEARTBEAT, "hb", NodeId.of("n1"), null, new HeartbeatPayload(1L, 2L, 3L));

        byte[] encoded = compressing.encode(hb);
        assertEquals(BinaryFrameCodec.HEARTBEAT_MARKER, encoded[0],
                "HEARTBEAT must remain a binary frame, never compressed");
    }

    @Test
    void compressedFrameIsReadableByAnotherCodecInstance() throws Exception {
        // Encoder with compression on, decoder is a different instance with compression OFF:
        // proves decode is independent of the outbound flag (a peer always reads what it gets).
        CompositeMessageCodec encoder = new CompositeMessageCodec();
        encoder.setCompressOutput(true);
        CompositeMessageCodec decoder = new CompositeMessageCodec(); // compression off

        ClusterMessage message = largeCompressibleMessage();
        byte[] encoded = encoder.encode(message);
        assertEquals(dev.nishisan.utils.ngrid.cluster.transport.codec.Lz4FrameCompressor.LZ4_JSON_MARKER,
                encoded[0]);

        ClusterMessage decoded = decoder.decode(encoded);
        assertEquals(MessageType.HANDSHAKE, decoded.type());
        assertEquals(message.source(), decoded.source());
    }

    @Test
    void compressionEnabledDecoderStillReadsLegacyAndPlainFrames() throws Exception {
        // A "new" node (compression on) must still read plain 0x00 and legacy '{' frames
        // produced by an "old" node that never compresses.
        CompositeMessageCodec newNode = new CompositeMessageCodec();
        newNode.setCompressOutput(true);

        ClusterMessage message = largeCompressibleMessage();

        // Plain 0x00 frame from an old node (composite without compression).
        byte[] plain = new CompositeMessageCodec().encode(message);
        assertEquals(0x00, plain[0]);
        assertEquals(MessageType.HANDSHAKE, newNode.decode(plain).type());

        // Legacy raw-JSON frame ('{') without any marker.
        byte[] legacy = new dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec()
                .encode(message);
        assertEquals((byte) '{', legacy[0]);
        assertEquals(MessageType.HANDSHAKE, newNode.decode(legacy).type());
    }
}
