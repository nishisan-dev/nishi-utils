/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.RelayStreamFetchPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fase 3 — o líder serve {@code RELAY_STREAM_FETCH} a partir do op-log durável (binlog): um run
 * estritamente contíguo a partir da sequência pedida, vazio quando o follower está em dia, e sinal
 * de {@code needSnapshot} quando o follower está abaixo do piso de retenção.
 */
class RelayStreamFetchProtocolTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeId LEADER = NodeId.of("zzz-leader");
    private static final NodeId PEER = NodeId.of("aaa-peer");

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("relay-stream-fetch-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void leaderServesContiguousRunFromOpLog() throws Exception {
        try (Leader l = new Leader(ReplicationConfig.builder(1)
                .strictConsistency(false).leaderLocalApply(false)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .operationTimeout(Duration.ofSeconds(5)).dataDirectory(tempDir).build())) {

            for (int i = 0; i < 5; i++) {
                l.manager.replicate(TOPIC, bytes("op-" + i)).get(2, TimeUnit.SECONDS);
            }
            Thread.sleep(200); // op-log append runs just after the commit future completes

            RelayStreamBatchPayload batch = l.fetch(1L, 10);
            assertFalse(batch.needSnapshot());
            assertEquals(1L, batch.fromSequence());
            assertEquals(5, batch.frames().size(), "should serve the full contiguous run [1..5]");
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1L, RelayEntryCodec.decode(batch.frames().get(i)).sequence(),
                        "frames must be contiguous and ascending from fromSequence");
            }
            assertEquals(1L, batch.oldestSequence());
        }
    }

    @Test
    void fetchAboveWatermarkReturnsEmptyWithoutSnapshot() throws Exception {
        try (Leader l = new Leader(ReplicationConfig.builder(1)
                .strictConsistency(false).leaderLocalApply(false)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .operationTimeout(Duration.ofSeconds(5)).dataDirectory(tempDir).build())) {

            l.manager.replicate(TOPIC, bytes("op-1")).get(2, TimeUnit.SECONDS);
            Thread.sleep(200);

            RelayStreamBatchPayload batch = l.fetch(100L, 10);
            assertTrue(batch.frames().isEmpty(), "caught-up follower gets an empty run");
            assertFalse(batch.needSnapshot(), "being ahead of the log is not a snapshot condition");
        }
    }

    @Test
    void fetchBelowRetainedWindowSignalsSnapshot() throws Exception {
        // Tiny op-log retention (1 entry/segment, keep 2) so the oldest sequence advances past 1.
        try (Leader l = new Leader(ReplicationConfig.builder(1)
                .strictConsistency(false).leaderLocalApply(false)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .resendLogSegmentMaxEntries(1).resendLogMaxEntries(2)
                .operationTimeout(Duration.ofSeconds(5)).dataDirectory(tempDir).build())) {

            for (int i = 0; i < 6; i++) {
                l.manager.replicate(TOPIC, bytes("op-" + i)).get(2, TimeUnit.SECONDS);
            }
            Thread.sleep(300);

            RelayStreamBatchPayload batch = l.fetch(1L, 10);
            assertTrue(batch.oldestSequence() > 1L, "retention should have evicted the low sequences");
            assertTrue(batch.needSnapshot(), "a follower below the retained window must snapshot");
            assertTrue(batch.frames().isEmpty());
        }
    }

    // ---- leader harness ----

    private final class Leader implements AutoCloseable {
        final RecordingTransport transport;
        final ClusterCoordinator coordinator;
        final ReplicationManager manager;

        Leader(ReplicationConfig config) {
            NodeInfo leaderNode = new NodeInfo(LEADER, "127.0.0.1", 0);
            NodeInfo peerNode = new NodeInfo(PEER, "127.0.0.1", 0);
            this.transport = new RecordingTransport(leaderNode, List.of(peerNode));
            this.coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
            coordinator.start();
            transport.simulatePeerConnected(peerNode);
            this.manager = new ReplicationManager(transport, coordinator, config);
            manager.registerHandler(TOPIC, (operationId, payload) -> {
            });
            manager.start();
        }

        RelayStreamBatchPayload fetch(long from, int maxBatch) throws InterruptedException {
            transport.clearSent();
            transport.deliverToListeners(ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream",
                    PEER, LEADER, new RelayStreamFetchPayload(TOPIC, from, maxBatch)));
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline) {
                for (ClusterMessage m : transport.getSentMessages()) {
                    if (m.type() == MessageType.RELAY_STREAM_BATCH) {
                        return m.payload(RelayStreamBatchPayload.class);
                    }
                }
                Thread.sleep(25);
            }
            throw new AssertionError("leader did not answer RELAY_STREAM_FETCH with a RELAY_STREAM_BATCH");
        }

        @Override
        public void close() {
            try {
                manager.close();
            } catch (Exception ignored) {
            }
            try {
                coordinator.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo p : peers) {
                connected.put(p.nodeId(), true);
            }
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }

        @Override
        public void addListener(TransportListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public void send(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            sent.add(message);
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return Boolean.TRUE.equals(connected.get(nodeId));
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return isConnected(nodeId);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            for (TransportListener l : listeners) {
                l.onPeerConnected(peer);
            }
        }

        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
        }

        List<ClusterMessage> getSentMessages() {
            return List.copyOf(sent);
        }

        void clearSent() {
            sent.clear();
        }

        @Override
        public void close() throws IOException {
        }
    }
}
