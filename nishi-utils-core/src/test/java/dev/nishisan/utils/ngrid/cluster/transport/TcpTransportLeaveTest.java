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

package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.PeerUpdatePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Esquecimento de membros efêmeros (clientes inelegíveis a líder) no transporte: um id esquecido sai
 * de {@code knownPeers} de vez, não é mais discado, fica protegido por um tombstone contra readmissão
 * de segunda mão (gossip, mensagem retransmitida, socket sem handshake) e volta só por um handshake
 * direto (nova encarnação).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class TcpTransportLeaveTest {

    private static final Duration RECONNECT = Duration.ofMillis(150);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final Set<Integer> usedPorts = new HashSet<>();

    @AfterEach
    void tearDown() {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }

    @Test
    void forgottenPeerIsNotDialedNorReAddedByGossipAndComesBackByDirectHandshake() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT));
        RecordingListener events = listen(storage);
        // Listening port nobody binds: the storage (lower id) is the designated dialer of the client.
        NodeInfo client = info("z-client", freePort(), true);

        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "cliente não conectou");

        assertTrue(storage.forget(client.nodeId()));
        AtomicInteger dialsToClient = countDials(storage, client.nodeId());

        assertFalse(knows(storage, client.nodeId()), "o id esquecido deveria sair de peers()");
        assertTrue(storage.isDeparted(client.nodeId()));
        assertFalse(storage.isConnected(client.nodeId()));

        // A third node that still lists the client: in its handshake and in a PEER_UPDATE.
        NodeInfo third = info("m-storage", freePort(), false);
        RawPeer thirdLink = raw(storage);
        thirdLink.send(handshake(third, storage.local(), Set.of(client)));
        awaitTrue(() -> storage.isConnected(third.nodeId()), "terceiro nó não conectou");
        thirdLink.send(ClusterMessage.request(MessageType.PEER_UPDATE, "peer-update", third.nodeId(), null,
                new PeerUpdatePayload(Set.of(client, third), Map.of(client.nodeId(), 1.0))));

        Thread.sleep(RECONNECT.toMillis() * 6);
        assertFalse(knows(storage, client.nodeId()), "gossip de terceiros não pode readmitir o id esquecido");
        assertEquals(0, dialsToClient.get(), "o id esquecido não pode ser discado de novo");
        assertEquals(List.of("left:z-client"), events.peerEvents(client.nodeId()),
                "a saída é reportada uma vez, como onPeerLeft, sem disconnect repetido");

        // New incarnation: direct handshake with the same id.
        RawPeer reborn = raw(storage);
        reborn.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "a nova encarnação deveria ser aceita");
        assertTrue(knows(storage, client.nodeId()));
        assertFalse(storage.isDeparted(client.nodeId()), "o handshake direto limpa o tombstone");
    }

    @Test
    void relayedMessagesAndHandshakelessSocketsOfAForgottenPeerAreDropped() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false)));
        RecordingListener events = listen(storage);
        NodeInfo client = info("z-client", 0, true);
        NodeInfo relay = info("m-storage", freePort(), false);

        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        RawPeer relayLink = raw(storage);
        relayLink.send(handshake(relay, storage.local(), Set.of(client)));
        awaitTrue(() -> storage.isConnected(client.nodeId()) && storage.isConnected(relay.nodeId()),
                "peers não conectaram");

        storage.forget(client.nodeId());

        relayLink.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "relayed-from-departed",
                client.nodeId(), storage.local().nodeId(), "late"));
        RawPeer handshakeless = raw(storage);
        handshakeless.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "inferred-departed",
                client.nodeId(), storage.local().nodeId(), "late"));
        relayLink.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "from-relay",
                relay.nodeId(), storage.local().nodeId(), "control"));

        awaitTrue(() -> events.qualifiers().contains("from-relay"), "mensagem de controle não chegou");
        Thread.sleep(300);
        assertFalse(events.qualifiers().contains("relayed-from-departed"),
                "mensagem retransmitida de um id esquecido deveria ser descartada");
        assertFalse(events.qualifiers().contains("inferred-departed"),
                "socket sem handshake identificado como id esquecido não pode ser publicado");
        assertFalse(storage.isConnected(client.nodeId()));
        assertFalse(knows(storage, client.nodeId()));
    }

    @Test
    void forgettingAPeerFailsItsPendingResponses() throws Exception {
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .requestTimeout(Duration.ofSeconds(30)));
        NodeInfo client = info("z-client", 0, true);
        RawPeer clientLink = raw(storage);
        clientLink.send(handshake(client, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(client.nodeId()), "cliente não conectou");

        CompletableFuture<ClusterMessage> pending = storage.sendAndAwait(ClusterMessage.request(
                MessageType.CLIENT_REQUEST, "never-answered", storage.local().nodeId(), client.nodeId(), "x"));
        storage.forget(client.nodeId());

        ExecutionException failure = assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
        assertInstanceOf(PeerDisconnectedException.class, failure.getCause());
    }

    /**
     * Saída sem LEAVE (kill -9, OOM, perda de rede): o peer efêmero é esquecido depois de
     * {@code departedPeerForgetAfter} sem conexão; um peer elegível a líder nunca é esquecido assim, e
     * um efêmero ainda conectado também não.
     */
    @Test
    void ephemeralPeerDisconnectedBeyondTheForgetWindowIsForgottenButVotersAreKept() throws Exception {
        Duration forgetAfter = Duration.ofMillis(600);
        TcpTransport storage = start(TcpTransportConfig.builder(info("a-storage", freePort(), false))
                .reconnectInterval(RECONNECT)
                .connectTimeout(Duration.ofMillis(200))
                .departedPeerForgetAfter(forgetAfter));
        RecordingListener events = listen(storage);
        NodeInfo killedClient = info("z-client", freePort(), true);
        NodeInfo liveClient = info("y-client", 0, true);
        NodeInfo voter = info("m-storage", freePort(), false);

        RawPeer killed = raw(storage);
        killed.send(handshake(killedClient, storage.local(), Set.of()));
        RawPeer live = raw(storage);
        live.send(handshake(liveClient, storage.local(), Set.of()));
        RawPeer voterLink = raw(storage);
        voterLink.send(handshake(voter, storage.local(), Set.of()));
        awaitTrue(() -> storage.isConnected(killedClient.nodeId()) && storage.isConnected(liveClient.nodeId())
                && storage.isConnected(voter.nodeId()), "peers não conectaram");

        long killedAt = System.currentTimeMillis();
        killed.close();
        voterLink.close();

        awaitTrue(() -> !knows(storage, killedClient.nodeId()), "cliente morto sem LEAVE não foi esquecido");
        long forgottenAfterMs = System.currentTimeMillis() - killedAt;
        assertTrue(forgottenAfterMs >= forgetAfter.toMillis(),
                "esquecido cedo demais: " + forgottenAfterMs + " ms < " + forgetAfter);
        assertTrue(storage.isDeparted(killedClient.nodeId()));
        assertTrue(events.peerEvents(killedClient.nodeId()).contains("left:z-client"));

        Thread.sleep(forgetAfter.toMillis() * 2);
        assertTrue(knows(storage, voter.nodeId()), "um peer elegível a líder nunca é esquecido por timeout");
        assertFalse(storage.isDeparted(voter.nodeId()));
        assertTrue(knows(storage, liveClient.nodeId()), "um efêmero conectado não pode ser esquecido");
    }

    // ---- helpers ----

    static NodeInfo info(String id, int port, boolean ephemeral) {
        Set<String> roles = ephemeral ? Set.of("client", NodeInfo.ROLE_LEADER_INELIGIBLE) : Set.of("storage");
        return new NodeInfo(NodeId.of(id), "127.0.0.1", port, roles, 0);
    }

    static ClusterMessage handshake(NodeInfo self, NodeInfo target, Set<NodeInfo> peers) {
        return ClusterMessage.request(MessageType.HANDSHAKE, "hello", self.nodeId(), target.nodeId(),
                new HandshakePayload(self, peers, Map.of(), false, true));
    }

    private TcpTransport start(TcpTransportConfig.Builder builder) {
        TcpTransport transport = new TcpTransport(builder.build());
        closeables.add(transport);
        transport.start();
        return transport;
    }

    private RawPeer raw(TcpTransport target) throws IOException {
        RawPeer peer = new RawPeer(target.local().host(), target.local().port());
        closeables.add(peer);
        return peer;
    }

    private static RecordingListener listen(TcpTransport transport) {
        RecordingListener listener = new RecordingListener();
        transport.addListener(listener);
        return listener;
    }

    private static AtomicInteger countDials(TcpTransport transport, NodeId target) {
        AtomicInteger dials = new AtomicInteger();
        transport.setBeforeDialHook(id -> {
            if (id.equals(target)) {
                dials.incrementAndGet();
            }
        });
        return dials;
    }

    static boolean knows(Transport transport, NodeId id) {
        return transport.peers().stream().anyMatch(p -> p.nodeId().equals(id));
    }

    static void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError(message);
    }

    private int freePort() throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (usedPorts.add(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }

    static final class RecordingListener implements TransportListener {
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final List<String> qualifiers = new CopyOnWriteArrayList<>();

        @Override
        public void onPeerConnected(NodeInfo peer) {
            events.add("connected:" + peer.nodeId().value());
        }

        @Override
        public void onPeerDisconnected(NodeId peerId) {
            events.add("disconnected:" + peerId.value());
        }

        @Override
        public void onPeerLeft(NodeId peerId) {
            events.add("left:" + peerId.value());
        }

        @Override
        public void onMessage(ClusterMessage message) {
            qualifiers.add(message.qualifier());
        }

        /** Connection-lifecycle events of {@code id} after its first connection. */
        List<String> peerEvents(NodeId id) {
            List<String> matching = new ArrayList<>();
            for (String event : events) {
                if (event.endsWith(":" + id.value()) && !event.startsWith("connected:")) {
                    matching.add(event);
                }
            }
            return matching;
        }

        List<String> qualifiers() {
            return qualifiers;
        }
    }
}
