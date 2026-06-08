package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the compression-capability negotiation field on {@link HandshakePayload},
 * including backward compatibility with legacy payloads that never serialized it.
 */
class HandshakePayloadTest {

    private final ObjectMapper mapper = JacksonMessageCodec.createDefaultMapper();

    @Test
    void convenienceConstructorsDefaultToSupported() {
        NodeInfo local = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
        assertTrue(new HandshakePayload(local, Set.of(local)).supportsCompression());
        assertTrue(new HandshakePayload(local, Set.of(local), Map.of()).supportsCompression());
    }

    @Test
    void shouldRoundTripSupportsCompressionTrue() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
        HandshakePayload payload = new HandshakePayload(local, Set.of(local), Map.of(), true);

        byte[] json = mapper.writeValueAsBytes(payload);
        HandshakePayload decoded = mapper.readValue(json, HandshakePayload.class);

        assertTrue(decoded.supportsCompression());
        assertEquals(local, decoded.local());
    }

    @Test
    void shouldRoundTripSupportsCompressionFalse() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
        HandshakePayload payload = new HandshakePayload(local, Set.of(local), Map.of(), false);

        HandshakePayload decoded = mapper.readValue(mapper.writeValueAsBytes(payload), HandshakePayload.class);
        assertFalse(decoded.supportsCompression());
    }

    @Test
    void legacyPayloadWithoutFieldDeserializesAsUnsupported() throws Exception {
        // Simulate an older node's handshake JSON by serializing a real payload and stripping
        // the field (robust against the exact NodeInfo/NodeId serialization format).
        NodeInfo local = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
        ObjectNode node = (ObjectNode) mapper.readTree(
                mapper.writeValueAsBytes(new HandshakePayload(local, Set.of(local), Map.of(), true)));
        node.remove("supportsCompression");
        assertFalse(node.has("supportsCompression"));

        HandshakePayload decoded = mapper.treeToValue(node, HandshakePayload.class);
        assertFalse(decoded.supportsCompression(),
                "missing field must default to false so peers do not compress towards a legacy node");
        assertEquals(NodeId.of("n1"), decoded.local().nodeId());
    }
}
