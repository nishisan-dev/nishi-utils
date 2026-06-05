package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.MapConfig;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the resolution of leader-local by-reference (issue #115): a per-map
 * override wins over the global default. Uses a single-node cluster so the local
 * node is always the leader.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class DistributedMapByReferenceConfigTest {

    static final class Box implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int x;

        @SuppressWarnings("unused")
        Box() {
        }

        Box(int x) {
            this.x = x;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Box b && b.x == x;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(x);
        }
    }

    private NGridNode node;

    @AfterEach
    void tearDown() throws IOException {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void perMapOverrideWinsOverGlobalDefault() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo info = new NodeInfo(NodeId.of("cfg-1"), "127.0.0.1", port);
        Path dir = Files.createTempDirectory("byref-config");

        node = new NGridNode(NGridConfig.builder(info)
                .dataDirectory(dir)
                .replicationFactor(1)
                .mapLeaderLocalByReference(true) // global default ON
                .addMap(MapConfig.builder("explicit-off")
                        .leaderLocalByReference(false) // per-map override OFF
                        .build())
                .build());
        node.start();
        awaitLeadership(node);

        // Lazily-created map (no per-map config): inherits the global default (ON).
        DistributedMap<String, Box> inheritsGlobal = node.getMap("inherits-global", String.class, Box.class);
        Box v1 = new Box(1);
        inheritsGlobal.put("k", v1);
        assertSame(v1, inheritsGlobal.getOptional("k").orElseThrow(),
                "map without override must inherit the global by-reference default (ON)");

        // Configured map with an explicit override OFF: stores a copy.
        DistributedMap<String, Box> overrideOff = node.getMap("explicit-off", String.class, Box.class);
        Box v2 = new Box(2);
        overrideOff.put("k", v2);
        Box stored = overrideOff.getOptional("k").orElseThrow();
        assertNotSame(v2, stored, "per-map override OFF must win over the global default");
        assertEquals(v2, stored);
    }

    private static void awaitLeadership(NGridNode node) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("single node did not become leader in time");
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }
}
