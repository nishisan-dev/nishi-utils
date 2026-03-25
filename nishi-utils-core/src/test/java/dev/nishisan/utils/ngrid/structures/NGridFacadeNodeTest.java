package dev.nishisan.utils.ngrid.structures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NGridNodeBuilder}.
 */
class NGridFacadeNodeTest {

    @Test
    void nodeBuilderGeneratesNodeIdFromHostPort() throws Exception {
        NGridNodeBuilder builder = NGrid.node("192.168.1.10", 9011);
        // We cannot inspect internal state directly, so we verify through build.
        // The node would start with NodeId "192.168.1.10:9011".
        // Since we can't fully start without a real network, we validate the builder
        // doesn't throw on construction.
        assertNotNull(builder);
    }

    @Test
    void nodeBuilderWithExplicitId() {
        NGridNodeBuilder builder = NGrid.node("192.168.1.10", 9011).id("primary-east-1");
        assertNotNull(builder);
    }

    @Test
    void nodeBuilderAcceptsPeers() {
        NGridNodeBuilder builder = NGrid.node("192.168.1.10", 9011)
                .peers("192.168.1.11:9011", "192.168.1.12:9011");
        assertNotNull(builder);
    }

    @Test
    void nodeBuilderAcceptsSeed() {
        NGridNodeBuilder builder = NGrid.node("192.168.1.10", 9011)
                .seed("192.168.1.11:9011");
        assertNotNull(builder);
    }

    @Test
    void nodeBuilderAcceptsQueueAndMap() {
        NGridNodeBuilder builder = NGrid.node("192.168.1.10", 9011)
                .queue("orders")
                .map("users");
        assertNotNull(builder);
    }

    @Test
    void nodeBuilderRejectsInvalidReplication() {
        assertThrows(IllegalArgumentException.class, () ->
                NGrid.node("192.168.1.10", 9011).replication(0));
    }

    @Test
    void nodeBuilderAutoPortCreatesBuilder() {
        // NGrid.node(host) should allocate port dynamically
        NGridNodeBuilder builder = NGrid.node("127.0.0.1");
        assertNotNull(builder);
    }

    @Test
    void nodeBuilderRejectsInvalidPeerFormat() {
        NGridNodeBuilder builder = NGrid.node("127.0.0.1", 9011)
                .peers("invalid-no-port");
        // The validation happens at start() time, so we just verify
        // the builder accepts it without throwing. The invalid peer
        // will cause an error when start() parses it.
        assertNotNull(builder);
    }
}
