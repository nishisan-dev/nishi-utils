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

package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #169, ponta a ponta: um cliente ngrrd (nó NGrid inelegível a líder, prioridade 0, com
 * {@code peers(...)} listando todos os storages como {@code host:port}) que religa com o mesmo id
 * não pode derrubar os links entre os storages nem falhar requests storage→storage em voo.
 * <p>
 * O cliente começa com os storages sob ids provisórios ({@code host:port}); antes da correção esses
 * aliases eram propagados no handshake, cada storage trocava o id canônico do vizinho pelo alias,
 * discava de novo para ele e o desempate de conexão duplicada derrubava o link original —
 * "Disconnect confirmed for storage-..." e {@code PeerDisconnectedException} no request em voo.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClientRejoinMeshStabilityTest {

    private static final String SLOW_COMMAND = "test.slow-chunk";
    private static final int RESTARTS = 3;

    private final List<NGridNode> storages = new ArrayList<>();
    private NGridNode client;

    @AfterEach
    void tearDown() {
        closeQuietly(client);
        storages.forEach(ClientRejoinMeshStabilityTest::closeQuietly);
    }

    @Test
    void clientRejoiningWithSeedAliasesDoesNotDropStorageLinks(@TempDir Path tempDir) throws Exception {
        List<String> ids = List.of("storage-a", "storage-b", "storage-c");
        List<Integer> ports = new ArrayList<>();
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < ids.size() + 1; i++) {
            int port = allocateFreeLocalPort(used);
            used.add(port);
            ports.add(port);
        }
        List<String> storageAddresses = ports.subList(0, ids.size()).stream().map(p -> "127.0.0.1:" + p).toList();
        int clientPort = ports.get(ids.size());

        for (int i = 0; i < ids.size(); i++) {
            List<String> others = new ArrayList<>(storageAddresses);
            others.remove(i);
            storages.add(NGrid.node("127.0.0.1", ports.get(i))
                    .id(ids.get(i))
                    .roles("storage")
                    .dataDir(tempDir.resolve(ids.get(i)))
                    .peers(others.toArray(new String[0]))
                    .start());
        }
        awaitStorageMesh();

        // Disconnects of storage peers, attributed per observing storage (exact, via the listener)...
        List<String> storageLinkDrops = new CopyOnWriteArrayList<>();
        for (NGridNode storage : storages) {
            NodeId self = storage.transport().local().nodeId();
            storage.transport().addListener(new TransportListener() {
                public void onPeerConnected(NodeInfo peer) { }
                public void onPeerDisconnected(NodeId peer) {
                    if (peer.value().startsWith("storage-")) {
                        storageLinkDrops.add(self.value() + " -> " + peer.value());
                    }
                }
                public void onMessage(ClusterMessage message) { }
            });
        }
        // ...and the transport log line seen in production (all transports of this JVM).
        DisconnectLogCapture logCapture = new DisconnectLogCapture();
        Logger transportLogger = Logger.getLogger(TcpTransport.class.getName());
        transportLogger.addHandler(logCapture);

        NGridNode storageA = storages.get(0);
        NGridNode storageC = storages.get(2);
        AtomicReference<CountDownLatch> releaseReply = new AtomicReference<>(new CountDownLatch(0));
        storageC.transport().addListener(slowResponder(storageC, releaseReply));
        List<String> outcomes = new ArrayList<>();
        try {
            for (int round = 1; round <= RESTARTS; round++) {
                // A migration-chunk-like request from storage-a to storage-c stays in flight while
                // the client (re)joins; it is answered only after the client left again.
                CountDownLatch release = new CountDownLatch(1);
                releaseReply.set(release);
                CompletableFuture<ClusterMessage> pending = storageA.transport().sendAndAwait(
                        ClusterMessage.request(MessageType.CLIENT_REQUEST, SLOW_COMMAND,
                                storageA.transport().local().nodeId(), storageC.transport().local().nodeId(),
                                new ClientRequestPayload(UUID.randomUUID(), SLOW_COMMAND, "round-" + round)));

                client = startClient(tempDir, clientPort, storageAddresses);
                awaitClientConnected(client);
                Thread.sleep(2_000); // several heartbeat rounds carrying the client's gossip

                // The client's own shutdown legitimately confirms its links to the storages
                // ("... on client-1"); its reader threads finish right after close() returns.
                logCapture.pause();
                client.close();
                client = null;
                Thread.sleep(500);
                logCapture.resume();

                release.countDown();
                outcomes.add(outcome(pending));
            }

            assertAll(
                    () -> assertTrue(storageLinkDrops.isEmpty(),
                            "links entre storages caíram enquanto o cliente religava: " + storageLinkDrops),
                    () -> assertTrue(logCapture.records().isEmpty(),
                            "log 'Disconnect confirmed for storage-' durante os religamentos: " + logCapture.records()),
                    () -> assertEquals(List.of("ok", "ok", "ok"), outcomes,
                            "requests storage-a -> storage-c em voo deveriam completar"));
            awaitStorageMesh();
        } finally {
            releaseReply.get().countDown();
            transportLogger.removeHandler(logCapture);
        }
    }

    private static NGridNode startClient(Path tempDir, int port, List<String> storageAddresses) throws IOException {
        // Same shape as DefaultNgrrdClusterClient.connect(...) in nishi-utils-ngrrd-cluster.
        return NGrid.node("127.0.0.1", port)
                .id("client-1")
                .priority(0)
                .roles("client", NodeInfo.ROLE_LEADER_INELIGIBLE)
                .dataDir(tempDir.resolve("client-1"))
                .peers(storageAddresses.toArray(new String[0]))
                .start();
    }

    private static TransportListener slowResponder(NGridNode self, AtomicReference<CountDownLatch> releaseReply) {
        return new TransportListener() {
            public void onPeerConnected(NodeInfo peer) { }
            public void onPeerDisconnected(NodeId peer) { }
            public void onMessage(ClusterMessage message) {
                if (message.type() != MessageType.CLIENT_REQUEST || !SLOW_COMMAND.equals(message.qualifier())) {
                    return;
                }
                ClientRequestPayload request = message.payload(ClientRequestPayload.class);
                CountDownLatch release = releaseReply.get();
                Thread.ofVirtual().start(() -> {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    self.transport().send(ClusterMessage.response(message,
                            new ClientResponsePayload(request.requestId(), true, "stored", null)));
                });
            }
        };
    }

    private void awaitStorageMesh() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (storageMeshConnected() && storagesAgreeOnLeader()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("malha entre storages não convergiu: " + storageMeshState());
    }

    private void awaitClientConnected(NGridNode node) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = storages.stream()
                    .allMatch(s -> node.transport().isConnected(s.transport().local().nodeId()));
            if (all && node.coordinator().leaderInfo().isPresent()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("cliente não conectou a todos os storages");
    }

    private boolean storageMeshConnected() {
        for (NGridNode from : storages) {
            for (NGridNode to : storages) {
                if (from != to && !from.transport().isConnected(to.transport().local().nodeId())) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean storagesAgreeOnLeader() {
        var leader = storages.get(0).coordinator().leaderInfo();
        return leader.isPresent() && storages.stream().allMatch(s -> leader.equals(s.coordinator().leaderInfo()));
    }

    private String storageMeshState() {
        StringBuilder sb = new StringBuilder();
        for (NGridNode from : storages) {
            for (NGridNode to : storages) {
                if (from != to) {
                    sb.append(from.transport().local().nodeId().value()).append("->")
                            .append(to.transport().local().nodeId().value())
                            .append(from.transport().isConnected(to.transport().local().nodeId()) ? " ok; " : " DOWN; ");
                }
            }
        }
        return sb.toString();
    }

    private static String outcome(CompletableFuture<ClusterMessage> pending) {
        try {
            ClusterMessage response = pending.get(10, TimeUnit.SECONDS);
            return response.payload(ClientResponsePayload.class).success() ? "ok" : "error-response";
        } catch (ExecutionException e) {
            return String.valueOf(e.getCause());
        } catch (Exception e) {
            return e.toString();
        }
    }

    private static void closeQuietly(NGridNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (IOException ignored) {
        }
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (!avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }

    /** Captures "Disconnect confirmed for storage-..." records while not paused. */
    private static final class DisconnectLogCapture extends Handler {
        private final List<String> records = new CopyOnWriteArrayList<>();
        private volatile boolean paused;

        DisconnectLogCapture() {
            setLevel(Level.ALL);
        }

        void pause() {
            paused = true;
        }

        void resume() {
            paused = false;
        }

        List<String> records() {
            return records;
        }

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (!paused && message != null && message.startsWith("Disconnect confirmed for storage-")) {
                records.add(message);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
