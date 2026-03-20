package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.queue.NQueueHeaders;
import dev.nishisan.utils.queue.NQueueRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that validate end-to-end propagation of {@code key} and
 * {@code NQueueHeaders} (V3 record format) through the NGrid replication stack.
 *
 * <h3>Verification strategy</h3>
 * After the follower acknowledges receiving the record via {@code peek()},
 * the test reads the raw V3 metadata directly from the follower's internal
 * {@link dev.nishisan.utils.queue.NQueue} by drilling through
 * {@code DistributedQueue.queueService().queue().peekRecord()}. This avoids
 * the need for post-shutdown file inspection (which is fragile) and guarantees
 * that the V3 binary fields reach the in-process follower log unchanged.
 */
class QueueKeyHeadersIntegrationTest {

        // ─── cluster helpers ──────────────────────────────────────────────────────

        private static int allocateFreeLocalPort() throws IOException {
                return allocateFreeLocalPort(Set.of());
        }

        private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
                for (int attempt = 0; attempt < 50; attempt++) {
                        try (ServerSocket s = new ServerSocket()) {
                                s.setReuseAddress(true);
                                s.bind(new InetSocketAddress("127.0.0.1", 0));
                                int port = s.getLocalPort();
                                if (port > 0 && !avoid.contains(port))
                                        return port;
                        }
                }
                throw new IOException("Unable to allocate a free local port");
        }

        /** Waits until both nodes see each other and agree on a leader. */
        private void awaitClusterStability(NGridNode a, NGridNode b,
                        NodeInfo infoA, NodeInfo infoB) {
                long deadline = System.currentTimeMillis() + 20_000;
                while (System.currentTimeMillis() < deadline) {
                        boolean leadersAgree = a.coordinator().leaderInfo().isPresent()
                                        && a.coordinator().leaderInfo().equals(b.coordinator().leaderInfo());
                        boolean allMembers = a.coordinator().activeMembers().size() == 2
                                        && b.coordinator().activeMembers().size() == 2;
                        boolean connected = a.transport().isConnected(infoB.nodeId())
                                        && b.transport().isConnected(infoA.nodeId());
                        if (leadersAgree && allMembers && connected)
                                return;
                        try {
                                Thread.sleep(100);
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(e);
                        }
                }
                throw new IllegalStateException("Cluster did not stabilize within 20 s");
        }

        /** Retries an assertion up to 10 s before failing. */
        private void assertEventually(Runnable assertion) {
                long deadline = System.currentTimeMillis() + 10_000;
                Throwable last = null;
                while (System.currentTimeMillis() < deadline) {
                        try {
                                assertion.run();
                                return;
                        } catch (AssertionError | Exception e) {
                                last = e;
                                try {
                                        Thread.sleep(200);
                                } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        throw new RuntimeException(ie);
                                }
                        }
                }
                if (last instanceof AssertionError ae)
                        throw ae;
                throw new RuntimeException(last);
        }

        /**
         * Waits until the follower's internal NQueue has at least one record with
         * metadata.
         */
        private NQueueRecord awaitFollowerRecord(DistributedQueue<String> followerQueue) {
                final NQueueRecord[] result = { null };
                assertEventually(() -> {
                        try {
                                Optional<NQueueRecord> rec = followerQueue.queueService().queue().peekRecord();
                                assertTrue(rec.isPresent(), "Follower NQueue must contain at least one V3 record");
                                result[0] = rec.get();
                        } catch (IOException e) {
                                throw new RuntimeException(e);
                        }
                });
                return result[0];
        }

        // =========================================================================
        // Test 1 — key only: byte[] key survives offer → replication →
        // NQueue.peekRecord()
        // =========================================================================

        /**
         * Offers a record with a routing key (no headers) on the leader and verifies
         * that the key bytes are stored verbatim in the V3 binary header on the
         * follower's in-process {@link dev.nishisan.utils.queue.NQueue}.
         */
        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void testKeyIsReplicatedAndStoredInV3FormatOnFollower() throws Exception {
                final String queueName = "kh-key-queue";

                int portL = allocateFreeLocalPort();
                int portF = allocateFreeLocalPort(Set.of(portL));
                NodeInfo infoL = new NodeInfo(NodeId.of("kh-leader-1"), "127.0.0.1", portL);
                NodeInfo infoF = new NodeInfo(NodeId.of("kh-follower-1"), "127.0.0.1", portF);

                Path baseDir = Files.createTempDirectory("ngrid-kh-key-test");

                try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                                .addPeer(infoF)
                                .dataDirectory(Files.createDirectories(baseDir.resolve("leader")))
                                .replicationQuorum(1)
                                .heartbeatInterval(Duration.ofMillis(150))
                                .build());
                                NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                                                .addPeer(infoL)
                                                .dataDirectory(Files.createDirectories(baseDir.resolve("follower")))
                                                .replicationQuorum(1)
                                                .heartbeatInterval(Duration.ofMillis(150))
                                                .build())) {

                        leader.start();
                        follower.start();
                        awaitClusterStability(leader, follower, infoL, infoF);

                        NGridNode actualLeader = leader.coordinator().isLeader() ? leader : follower;
                        NGridNode actualFollower = leader.coordinator().isLeader() ? follower : leader;

                        DistributedQueue<String> q = actualLeader.getQueue(queueName, String.class);
                        DistributedQueue<String> qFollower = actualFollower.getQueue(queueName, String.class);

                        byte[] key = "order-event".getBytes(StandardCharsets.UTF_8);
                        q.offer(key, "payload-1");

                        // Wait for follower to receive the record and then read the raw V3 record
                        NQueueRecord record = awaitFollowerRecord(qFollower);

                        assertNotNull(record.meta().getKey(), "V3 record must have a non-null key");
                        assertArrayEquals("order-event".getBytes(StandardCharsets.UTF_8),
                                        record.meta().getKey(),
                                        "Key must survive the offer → replicate → follower NQueue round-trip");
                }
        }

        // =========================================================================
        // Test 2 — headers: all entries survive replication intact
        // =========================================================================

        /**
         * Offers a record with three custom headers and verifies that all three are
         * preserved byte-for-byte in the follower's in-process V3 NQueue record.
         */
        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void testHeadersAreReplicatedIntact() throws Exception {
                final String queueName = "kh-headers-queue";

                int portL = allocateFreeLocalPort();
                int portF = allocateFreeLocalPort(Set.of(portL));
                NodeInfo infoL = new NodeInfo(NodeId.of("kh-leader-2"), "127.0.0.1", portL);
                NodeInfo infoF = new NodeInfo(NodeId.of("kh-follower-2"), "127.0.0.1", portF);

                Path baseDir = Files.createTempDirectory("ngrid-kh-headers-test");

                NQueueHeaders headers = NQueueHeaders.empty()
                                .add("source", "checkout-service".getBytes(StandardCharsets.UTF_8))
                                .add("trace-id", "abc-123-xyz".getBytes(StandardCharsets.UTF_8))
                                .add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));

                try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                                .addPeer(infoF)
                                .dataDirectory(Files.createDirectories(baseDir.resolve("leader")))
                                .replicationQuorum(1)
                                .heartbeatInterval(Duration.ofMillis(150))
                                .build());
                                NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                                                .addPeer(infoL)
                                                .dataDirectory(Files.createDirectories(baseDir.resolve("follower")))
                                                .replicationQuorum(1)
                                                .heartbeatInterval(Duration.ofMillis(150))
                                                .build())) {

                        leader.start();
                        follower.start();
                        awaitClusterStability(leader, follower, infoL, infoF);

                        NGridNode actualLeader = leader.coordinator().isLeader() ? leader : follower;
                        NGridNode actualFollower = leader.coordinator().isLeader() ? follower : leader;

                        DistributedQueue<String> q = actualLeader.getQueue(queueName, String.class);
                        DistributedQueue<String> qFollower = actualFollower.getQueue(queueName, String.class);

                        q.offer("checkout".getBytes(StandardCharsets.UTF_8), headers, "checkout-payload");

                        NQueueRecord record = awaitFollowerRecord(qFollower);
                        NQueueHeaders stored = record.meta().getHeaders();

                        assertNotNull(stored, "Headers must not be null after V3 round-trip");
                        assertEquals(3, stored.size(), "All 3 headers must survive replication");
                        assertArrayEquals("checkout-service".getBytes(StandardCharsets.UTF_8),
                                        stored.get("source").orElse(null), "Header 'source' must match");
                        assertArrayEquals("abc-123-xyz".getBytes(StandardCharsets.UTF_8),
                                        stored.get("trace-id").orElse(null), "Header 'trace-id' must match");
                        assertArrayEquals("application/json".getBytes(StandardCharsets.UTF_8),
                                        stored.get("content-type").orElse(null), "Header 'content-type' must match");
                }
        }

        // =========================================================================
        // Test 3 — regression: plain offer() produces V3 record with null key + empty
        // headers
        // =========================================================================

        /**
         * Regression guard. A plain {@code offer(value)} must produce a valid V3 record
         * with a {@code null} key and an empty (non-null) {@link NQueueHeaders} on the
         * follower.
         */
        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void testSimpleOfferWithoutKeyOrHeadersRemainsCompatible() throws Exception {
                final String queueName = "kh-nokey-queue";

                int portL = allocateFreeLocalPort();
                int portF = allocateFreeLocalPort(Set.of(portL));
                NodeInfo infoL = new NodeInfo(NodeId.of("kh-leader-3"), "127.0.0.1", portL);
                NodeInfo infoF = new NodeInfo(NodeId.of("kh-follower-3"), "127.0.0.1", portF);

                Path baseDir = Files.createTempDirectory("ngrid-kh-nokey-test");

                try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                                .addPeer(infoF)
                                .dataDirectory(Files.createDirectories(baseDir.resolve("leader")))
                                .replicationQuorum(1)
                                .heartbeatInterval(Duration.ofMillis(150))
                                .build());
                                NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                                                .addPeer(infoL)
                                                .dataDirectory(Files.createDirectories(baseDir.resolve("follower")))
                                                .replicationQuorum(1)
                                                .heartbeatInterval(Duration.ofMillis(150))
                                                .build())) {

                        leader.start();
                        follower.start();
                        awaitClusterStability(leader, follower, infoL, infoF);

                        NGridNode actualLeader = leader.coordinator().isLeader() ? leader : follower;
                        NGridNode actualFollower = leader.coordinator().isLeader() ? follower : leader;

                        DistributedQueue<String> q = actualLeader.getQueue(queueName, String.class);
                        DistributedQueue<String> qFollower = actualFollower.getQueue(queueName, String.class);

                        q.offer("simple-message");

                        NQueueRecord record = awaitFollowerRecord(qFollower);

                        // A plain offer() without key should produce either null or an empty byte[]
                        // key.
                        // The V3 format uses an empty byte[0] as the canonical "no key" sentinel.
                        byte[] storedKey = record.meta().getKey();
                        assertTrue(storedKey == null || storedKey.length == 0,
                                        "Key must be null or empty for plain offer() — was: "
                                                        + (storedKey == null ? "null"
                                                                        : "byte[" + storedKey.length + "]"));
                        NQueueHeaders stored = record.meta().getHeaders();
                        assertNotNull(stored, "Headers must never be null");
                        assertEquals(0, stored.size(), "Headers must be empty for plain offer()");
                }
        }

        // =========================================================================
        // Test 4 — FIFO ordering preserved with mixed keyed and plain offers
        // =========================================================================

        /**
         * Verifies FIFO ordering across {@code offer(key, value)},
         * {@code offer(key, headers, value)}, and plain {@code offer(value)} calls.
         */
        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void testFifoOrderPreservedWithMixedKeyAndNoKeyOffers() throws Exception {
                final String queueName = "kh-fifo-queue";

                int portL = allocateFreeLocalPort();
                int portF = allocateFreeLocalPort(Set.of(portL));
                NodeInfo infoL = new NodeInfo(NodeId.of("kh-leader-4"), "127.0.0.1", portL);
                NodeInfo infoF = new NodeInfo(NodeId.of("kh-follower-4"), "127.0.0.1", portF);

                Path baseDir = Files.createTempDirectory("ngrid-kh-fifo-test");

                try (NGridNode leader = new NGridNode(NGridConfig.builder(infoL)
                                .addPeer(infoF)
                                .dataDirectory(Files.createDirectories(baseDir.resolve("leader")))
                                .replicationQuorum(1)
                                .heartbeatInterval(Duration.ofMillis(150))
                                .build());
                                NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
                                                .addPeer(infoL)
                                                .dataDirectory(Files.createDirectories(baseDir.resolve("follower")))
                                                .replicationQuorum(1)
                                                .heartbeatInterval(Duration.ofMillis(150))
                                                .build())) {

                        leader.start();
                        follower.start();
                        awaitClusterStability(leader, follower, infoL, infoF);

                        NGridNode actualLeader = leader.coordinator().isLeader() ? leader : follower;
                        NGridNode actualFollower = leader.coordinator().isLeader() ? follower : leader;

                        DistributedQueue<String> q = actualLeader.getQueue(queueName, String.class);
                        DistributedQueue<String> qFollower = actualFollower.getQueue(queueName, String.class);

                        NQueueHeaders h = NQueueHeaders.empty()
                                        .add("env", "prod".getBytes(StandardCharsets.UTF_8));

                        q.offer("msg-0");
                        q.offer("order-key".getBytes(StandardCharsets.UTF_8), "msg-1");
                        q.offer("order-key".getBytes(StandardCharsets.UTF_8), h, "msg-2");
                        q.offer("msg-3");

                        // Follower must see at least the first record before we poll
                        assertEventually(() -> assertTrue(qFollower.peek().isPresent(),
                                        "Follower must see the replicated records"));

                        // Poll from leader and verify FIFO order
                        assertEquals("msg-0", q.poll().orElseThrow(), "Record 0 must be msg-0");
                        assertEquals("msg-1", q.poll().orElseThrow(), "Record 1 must be msg-1");
                        assertEquals("msg-2", q.poll().orElseThrow(), "Record 2 must be msg-2");
                        assertEquals("msg-3", q.poll().orElseThrow(), "Record 3 must be msg-3");
                }
        }
}
