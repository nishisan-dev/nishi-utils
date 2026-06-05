package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster-level tests for leader-local by-reference (issue #115) using the
 * {@link NGrid#local(int)} facade. Verifies that the leader preserves the original
 * instance while followers receive a deserialized, type-faithful copy.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class DistributedMapByReferenceClusterTest {

    /** Plain POJO without Jackson annotations; Serializable for completeness. */
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

    private NGridCluster cluster;

    @AfterEach
    void tearDown() throws IOException {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void leaderKeepsReferenceWhileFollowerGetsCopy() throws Exception {
        cluster = NGrid.local(3)
                .map("ref-map", true) // leader-local by-reference enabled
                .replication(2)
                .start();

        NGridNode leaderNode = cluster.leader().orElseThrow(() -> new AssertionError("no leader"));
        NGridNode followerNode = cluster.nodes().stream()
                .filter(n -> !n.coordinator().isLeader())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no follower"));

        DistributedMap<String, Box> leaderMap = leaderNode.getMap("ref-map", String.class, Box.class);
        DistributedMap<String, Box> followerMap = followerNode.getMap("ref-map", String.class, Box.class);

        Box value = new Box(7);
        leaderMap.put("k", value);

        // Leader: identity preserved (same instance back).
        assertSame(value, leaderMap.getOptional("k").orElseThrow(),
                "leader must return the original instance in by-reference mode");

        // Follower: its LOCAL replicated state holds a type-faithful copy (equal
        // content, different instance). Read EVENTUAL so it does not route to the
        // leader but reads the locally applied replica.
        Box onFollower = awaitLocalValue(followerMap, "k");
        assertInstanceOf(Box.class, onFollower);
        assertNotSame(value, onFollower, "follower must hold a deserialized copy, not the leader's instance");
        assertEquals(value, onFollower, "follower copy must equal the leader value by content");
    }

    private static Box awaitLocalValue(DistributedMap<String, Box> map, String key) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var v = map.getOptional(key, Consistency.EVENTUAL);
            if (v.isPresent()) {
                return v.get();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("key '" + key + "' not replicated to follower in time");
    }
}
