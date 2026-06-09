package dev.nishisan.utils.ngrid.structures;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link DistributedMap#destroy()} replication across
 * an in-memory NGrid cluster.
 */
@Tag("resilience")
class DistributedMapDestroyTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void destroyShouldReplicateToAllNodes() throws Exception {
        try (NGridCluster cluster = NGrid.local(3)
                .map("destroy-test")
                .start()) {

            DistributedMap<String, String> m0 = cluster.map("destroy-test", String.class, String.class);
            DistributedMap<String, String> m1 = cluster.map(1, "destroy-test", String.class, String.class);
            DistributedMap<String, String> m2 = cluster.map(2, "destroy-test", String.class, String.class);

            // Write some data
            m0.put("key1", "value1");
            m0.put("key2", "value2");
            m0.put("key3", "value3");

            // Verify data is visible on followers. RELAY_STREAM replicates by async pull (followers poll
            // the leader op-log), so visibility is eventual — await convergence instead of asserting it
            // synchronously.
            awaitTrue(() -> Optional.of("value1").equals(m1.getOptional("key1")),
                    "Node 1 should see value1 after replication");
            awaitTrue(() -> Optional.of("value2").equals(m2.getOptional("key2")),
                    "Node 2 should see value2 after replication");

            // Destroy from node 0 (may or may not be leader — the method routes correctly)
            m0.destroy();

            // Verify data is cleared on all remaining map instances (await async pull of the destroy).
            awaitTrue(m1::isEmpty, "Node 1 should have empty map after destroy");
            awaitTrue(m2::isEmpty, "Node 2 should have empty map after destroy");
        }
    }

    /** Polls {@code condition} until true or the timeout elapses, then asserts it (eventual consistency). */
    private static void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void destroyFromFollowerShouldReplicateToAllNodes() throws Exception {
        try (NGridCluster cluster = NGrid.local(2)
                .map("destroy-follower")
                .start()) {

            DistributedMap<String, String> m0 = cluster.map("destroy-follower", String.class, String.class);
            DistributedMap<String, String> m1 = cluster.map(1, "destroy-follower", String.class, String.class);

            m0.put("a", "1");
            m0.put("b", "2");

            // Determine which node is NOT the leader and destroy from there
            boolean node0IsLeader = cluster.node(0).coordinator().isLeader();
            DistributedMap<String, String> followerMap = node0IsLeader ? m1 : m0;
            DistributedMap<String, String> leaderMap = node0IsLeader ? m0 : m1;

            followerMap.destroy();

            // Both sides should be empty
            assertTrue(leaderMap.isEmpty(), "Leader should have empty map after destroy from follower");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void destroyLocalShouldOnlyAffectCallingNode() throws Exception {
        try (NGridCluster cluster = NGrid.local(2)
                .map("destroy-local")
                .start()) {

            DistributedMap<String, String> m0 = cluster.map("destroy-local", String.class, String.class);
            DistributedMap<String, String> m1 = cluster.map(1, "destroy-local", String.class, String.class);

            m0.put("x", "100");

            // destroyLocal() should only affect node 1
            m1.destroyLocal();

            // Node 0 should still have data (no replication of destroy)
            assertEquals(Optional.of("100"), m0.getOptional("x"), "Node 0 should still have data after destroyLocal on node 1");
        }
    }
}
