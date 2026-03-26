package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the JacksonMessageCodec can round-trip all message types
 * used by the cluster protocol.
 */
class JacksonMessageCodecTest {

    private final JacksonMessageCodec codec = new JacksonMessageCodec();

    @Test
    void shouldRoundTripHandshakeMessage() throws Exception {
        NodeId sourceId = NodeId.of("node-1");
        NodeId destId = NodeId.of("node-2");
        NodeInfo sourceInfo = new NodeInfo(sourceId, "127.0.0.1", 5000);
        NodeInfo destInfo = new NodeInfo(destId, "127.0.0.2", 5001);

        Set<NodeInfo> knownPeers = Set.of(sourceInfo, destInfo);
        Map<NodeId, Double> latencies = Map.of(destId, 1.5);
        HandshakePayload payload = new HandshakePayload(sourceInfo, knownPeers, latencies);

        ClusterMessage message = ClusterMessage.request(
                MessageType.HANDSHAKE, "handshake", sourceId, destId, payload);

        byte[] encoded = codec.encode(message);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        ClusterMessage decoded = codec.decode(encoded);
        assertNotNull(decoded);
        assertEquals(MessageType.HANDSHAKE, decoded.type());
        assertEquals(sourceId, decoded.source());
        assertEquals(destId, decoded.destination());

        HandshakePayload dp = decoded.payload(HandshakePayload.class);
        assertNotNull(dp);
        assertEquals(sourceInfo.nodeId(), dp.local().nodeId());
        assertEquals(2, dp.peers().size());
    }

    @Test
    void shouldRoundTripHeartbeatMessage() throws Exception {
        NodeId sourceId = NodeId.of("node-1");
        NodeId destId = NodeId.of("node-2");
        HeartbeatPayload payload = new HeartbeatPayload(
                System.currentTimeMillis(), 42L, 1L);

        ClusterMessage message = ClusterMessage.request(
                MessageType.HEARTBEAT, "heartbeat", sourceId, destId, payload);

        byte[] encoded = codec.encode(message);
        ClusterMessage decoded = codec.decode(encoded);

        assertEquals(MessageType.HEARTBEAT, decoded.type());
        HeartbeatPayload dp = decoded.payload(HeartbeatPayload.class);
        assertNotNull(dp);
        assertEquals(42L, dp.leaderHighWatermark());
        assertEquals(1L, dp.leaderEpoch());
    }

    @Test
    void shouldRoundTripReplicationPayload() throws Exception {
        NodeId sourceId = NodeId.of("node-1");
        NodeId destId = NodeId.of("node-2");
        UUID opId = UUID.randomUUID();
        ReplicationPayload payload = new ReplicationPayload(opId, 1L, 1L, "test-topic", "test-data");

        ClusterMessage message = ClusterMessage.request(
                MessageType.REPLICATION_REQUEST, "replicate", sourceId, destId, payload);

        byte[] encoded = codec.encode(message);
        ClusterMessage decoded = codec.decode(encoded);

        assertEquals(MessageType.REPLICATION_REQUEST, decoded.type());
        ReplicationPayload dp = decoded.payload(ReplicationPayload.class);
        assertNotNull(dp);
        assertEquals(opId, dp.operationId());
        assertEquals("test-topic", dp.topic());
        assertEquals("test-data", dp.data());
    }

    @Test
    void shouldRoundTripClientRequestPayload() throws Exception {
        NodeId sourceId = NodeId.of("node-1");
        NodeId destId = NodeId.of("node-2");
        UUID requestId = UUID.randomUUID();
        ClientRequestPayload payload = new ClientRequestPayload(requestId, "queue:offer", "hello");

        ClusterMessage message = ClusterMessage.request(
                MessageType.CLIENT_REQUEST, "queue:offer", sourceId, destId, payload);

        byte[] encoded = codec.encode(message);
        ClusterMessage decoded = codec.decode(encoded);

        assertEquals(MessageType.CLIENT_REQUEST, decoded.type());
        ClientRequestPayload dp = decoded.payload(ClientRequestPayload.class);
        assertNotNull(dp);
        assertEquals(requestId, dp.requestId());
        assertEquals("queue:offer", dp.command());
        assertEquals("hello", dp.body());
    }
}
