package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the compression defaults and builder validation on {@link TcpTransportConfig}.
 */
class TransportCompressionConfigTest {

    private static NodeInfo local() {
        return new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
    }

    @Test
    void compressionIsEnabledByDefaultWith512Threshold() {
        TcpTransportConfig config = TcpTransportConfig.builder(local()).build();
        assertTrue(config.compressionEnabled());
        assertEquals(512, config.compressionMinSize());
    }

    @Test
    void builderOverridesCompressionSettings() {
        TcpTransportConfig config = TcpTransportConfig.builder(local())
                .compressionEnabled(false)
                .compressionMinSize(1024)
                .build();
        assertFalse(config.compressionEnabled());
        assertEquals(1024, config.compressionMinSize());
    }

    @Test
    void negativeMinSizeIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> TcpTransportConfig.builder(local()).compressionMinSize(-1));
    }
}
