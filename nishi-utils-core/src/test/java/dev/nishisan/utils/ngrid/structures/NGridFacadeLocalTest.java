package dev.nishisan.utils.ngrid.structures;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link NGrid#local(int)} facade.
 */
class NGridFacadeLocalTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void localClusterFormsAndElectsLeader() throws Exception {
        try (NGridCluster cluster = NGrid.local(2)
                .queue("orders")
                .start()) {

            assertEquals(2, cluster.size());
            assertTrue(cluster.leader().isPresent(), "Leader should be elected");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void localClusterQueueReplicates() throws Exception {
        try (NGridCluster cluster = NGrid.local(2)
                .queue("orders")
                .start()) {

            DistributedQueue<String> q0 = cluster.queue("orders", String.class);
            DistributedQueue<String> q1 = cluster.queue(1, "orders", String.class);

            q0.offer("pedido-1");
            q0.offer("pedido-2");

            Optional<String> peek = q1.peek();
            assertTrue(peek.isPresent());
            assertEquals("pedido-1", peek.get());

            Optional<String> polled = q1.poll();
            assertTrue(polled.isPresent());
            assertEquals("pedido-1", polled.get());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void localClusterMapReplicates() throws Exception {
        try (NGridCluster cluster = NGrid.local(2)
                .map("users")
                .start()) {

            DistributedMap<String, String> m0 = cluster.map("users", String.class, String.class);
            DistributedMap<String, String> m1 = cluster.map(1, "users", String.class, String.class);

            m0.put("u1", "Alice");
            Optional<String> fetched = m1.get("u1");
            assertTrue(fetched.isPresent());
            assertEquals("Alice", fetched.get());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void localClusterWithThreeNodes() throws Exception {
        try (NGridCluster cluster = NGrid.local(3)
                .queue("events")
                .map("sessions")
                .replication(2)
                .start()) {

            assertEquals(3, cluster.size());
            assertTrue(cluster.leader().isPresent());

            cluster.queue("events", String.class).offer("evt-1");
            cluster.map("sessions", String.class, String.class).put("s1", "token-abc");

            assertEquals("evt-1", cluster.queue(2, "events", String.class).peek().orElseThrow());
            assertEquals("token-abc", cluster.map(1, "sessions", String.class, String.class).get("s1").orElseThrow());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void localClusterCloseReleasesResources() throws Exception {
        NGridCluster cluster = NGrid.local(2)
                .queue("q")
                .start();

        assertDoesNotThrow(cluster::close);
    }

    @Test
    void localClusterRejectsZeroNodes() {
        assertThrows(IllegalArgumentException.class, () -> NGrid.local(0));
    }
}
