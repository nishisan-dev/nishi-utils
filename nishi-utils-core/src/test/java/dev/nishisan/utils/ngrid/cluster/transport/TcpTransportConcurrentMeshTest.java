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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Guarda de regressão para o invariante de malha determinística introduzido pela
 * correção da issue #117: peers iniciados concorrentemente com configuração
 * full-mesh convergem para uma malha <b>direta</b> e <b>estável</b> (sem proxy,
 * sem flapping), independente da ordem de boot.
 *
 * <p>
 * <b>Escopo:</b> em loopback (localhost) o <i>simultaneous open</i> completa sem
 * {@code RST}/half-open, então este teste valida o <i>invariante</i> garantido
 * por {@code registerLiveConnection} (reconciliação determinística por NodeId) —
 * ele não dispara a manifestação dependente de rede real do bug. A reprodução
 * fiel do failover quebrado (rede com latência, hub como SPOF) é coberta pelo IT
 * Docker {@code NGridConcurrentMeshFailoverIT}.
 */
class TcpTransportConcurrentMeshTest {

    @Test
    void closeAlsoClosesAcceptedSocketsWithoutAPeerIdentity() throws Exception {
        int port = allocateFreeLocalPort(Set.of());
        NodeInfo local = new NodeInfo(NodeId.of("closing-server"), "127.0.0.1", port);
        var received = new CountDownLatch(1);
        try (TcpTransport server = new TcpTransport(meshConfig(local))) {
            server.addListener(new TransportListener() {
                public void onPeerConnected(NodeInfo peer) { }
                public void onPeerDisconnected(NodeId peer) { }
                public void onMessage(dev.nishisan.utils.ngrid.common.ClusterMessage message) { received.countDown(); }
            });
            server.start();
            try (var socket = new java.net.Socket(local.host(), port)) {
                socket.setSoTimeout(2_000);
                // A frame without source/handshake proves acceptance without publishing a peer.
                var frame = dev.nishisan.utils.ngrid.common.ClusterMessage.request(
                        dev.nishisan.utils.ngrid.common.MessageType.CLIENT_REQUEST, "unidentified",
                        null, local.nodeId(), "probe");
                byte[] bytes = new dev.nishisan.utils.ngrid.cluster.transport.codec.CompositeMessageCodec(1024).encode(frame);
                var out = new java.io.DataOutputStream(socket.getOutputStream());
                out.writeInt(bytes.length);
                out.write(bytes);
                out.flush();
                assertTrue(received.await(5, TimeUnit.SECONDS));
                server.close();
                assertEquals(-1, socket.getInputStream().read(), "shutdown must close unidentified sockets too");
            }
        }
    }

    @Test
    void dialAlreadyInProgressCannotPublishAConnectionAfterClose() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        NodeInfo a = new NodeInfo(NodeId.of("a-closing"), "127.0.0.1", portA);
        NodeInfo b = new NodeInfo(NodeId.of("z-peer"), "127.0.0.1", portB);
        TcpTransport client = new TcpTransport(meshConfig(a, b));
        TcpTransport server = new TcpTransport(meshConfig(b));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var dialer = new java.util.concurrent.atomic.AtomicReference<Thread>();
        client.setBeforeDialHook(ignored -> {
            dialer.compareAndSet(null, Thread.currentThread());
            entered.countDown();
            // Simulate a connect that returns only AFTER shutdown's bounded wait.
            boolean released = false;
            while (!released) {
                try { release.await(); released = true; }
                catch (InterruptedException expectedDuringClose) { }
            }
        });
        try {
            server.start();
            client.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            client.close();
            release.countDown();
            dialer.get().join(5_000);
            assertTrue(!dialer.get().isAlive(), "the outstanding dial must terminate");
            assertTrue(!client.isConnected(b.nodeId()), "shutdown must fence an already-started dial");
        } finally {
            release.countDown();
            closeQuietly(client, server);
        }
    }

    @Test
    void seedAliasBecomesCanonicalWithoutClosingItsOwnConnection() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        NodeInfo a = new NodeInfo(NodeId.of("a-client"), "127.0.0.1", portA);
        NodeInfo b = new NodeInfo(NodeId.of("z-storage"), "127.0.0.1", portB);
        NodeInfo seed = new NodeInfo(NodeId.of("127.0.0.1:" + portB), b.host(), b.port());
        TcpTransport client = new TcpTransport(meshConfig(a, seed));
        TcpTransport server = new TcpTransport(meshConfig(b));
        var dials = new java.util.concurrent.atomic.AtomicInteger();
        client.setBeforeDialHook(ignored -> dials.incrementAndGet());
        try {
            server.start();
            client.start();
            awaitDiscovery(client, b.nodeId());
            awaitFullDirectMesh(List.of(client, server), List.of(a, b), Duration.ofSeconds(10));
            assertEquals(1, dials.get(), "learning the seed's real ID must preserve the established socket");
            assertTrue(client.peers().stream().noneMatch(peer -> peer.nodeId().equals(seed.nodeId())));
        } finally {
            closeQuietly(client, server);
        }
    }

    /**
     * Issue #169: um nó que entra com seeds {@code host:port} ainda não resolvidos propaga esses
     * aliases no handshake. O storage que o recebe não pode trocar o peer canônico já verificado
     * pelo alias — senão disca de novo para o mesmo processo sob outra chave, o outro lado fecha a
     * conexão original pelo desempate e o request em voo falha com {@link PeerDisconnectedException}.
     */
    @Test
    void joiningPeerWithUnresolvedSeedAliasKeepsEstablishedLinks() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portC = allocateFreeLocalPort(Set.of(portA));
        int portClient = allocateFreeLocalPort(Set.of(portA, portC));
        NodeInfo a = new NodeInfo(NodeId.of("storage-a"), "127.0.0.1", portA);
        NodeInfo c = new NodeInfo(NodeId.of("storage-c"), "127.0.0.1", portC);
        NodeInfo client = new NodeInfo(NodeId.of("client-1"), "127.0.0.1", portClient);
        NodeInfo aliasA = new NodeInfo(NodeId.of("127.0.0.1:" + portA), a.host(), a.port());
        NodeInfo aliasC = new NodeInfo(NodeId.of("127.0.0.1:" + portC), c.host(), c.port());

        TcpTransport storageA = new TcpTransport(meshConfig(a, c));
        TcpTransport storageC = new TcpTransport(meshConfig(c, a));
        TcpTransport clientTransport = new TcpTransport(meshConfig(client, aliasA, aliasC));
        var releaseReply = new CountDownLatch(1);
        var releaseClientDial = new CountDownLatch(1);
        Set<NodeId> dialedByA = ConcurrentHashMap.newKeySet();
        storageA.setBeforeDialHook(dialedByA::add);
        storageC.addListener(slowResponder(storageC, releaseReply));
        clientTransport.setBeforeDialHook(id -> {
            if (id.equals(aliasC.nodeId())) {
                awaitUninterruptibly(releaseClientDial);
            }
        });
        try {
            storageA.start();
            storageC.start();
            awaitFullDirectMesh(List.of(storageA, storageC), List.of(a, c), Duration.ofSeconds(10));
            dialedByA.clear();

            var pending = storageA.sendAndAwait(ClusterMessage.request(
                    MessageType.CLIENT_REQUEST, "slow", a.nodeId(), c.nodeId(),
                    "chunk"));

            clientTransport.start();
            awaitDiscovery(storageA, client.nodeId());
            List<String> violations = sampleLinkWhileBroadcasting(storageA, c.nodeId(), aliasC.nodeId(),
                    Duration.ofSeconds(2));

            releaseReply.countDown();
            String pendingOutcome = awaitOutcome(pending);
            assertAll(
                    () -> assertTrue(violations.isEmpty(),
                            "storage-a perdeu o link canônico para storage-c: " + violations),
                    () -> assertFalse(dialedByA.contains(aliasC.nodeId()),
                            "storage-a discou o alias de seed de outro nó: " + dialedByA),
                    () -> assertEquals("ok", pendingOutcome, "request em voo a->c deveria completar"));

            releaseClientDial.countDown();
            awaitFullDirectMesh(List.of(storageA, storageC, clientTransport), List.of(a, c, client),
                    Duration.ofSeconds(15));
        } finally {
            releaseReply.countDown();
            releaseClientDial.countDown();
            closeQuietly(clientTransport, storageA, storageC);
        }
    }

    /**
     * O nó que entra aprende o id canônico de um seed por gossip de outro nó antes de a resposta do
     * handshake desse seed chegar. Ele não pode discar de novo para o mesmo processo: a conexão ao
     * alias já leva até lá. A discagem duplicada era resolvida pelo desempate de forma diferente em
     * cada ponta (cada uma registra as duas conexões em ordem diferente) e derrubava o link.
     */
    @Test
    void seedLinkStillResolvingServesCanonicalPeerWithoutRedial() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portC = allocateFreeLocalPort(Set.of(portA));
        int portClient = allocateFreeLocalPort(Set.of(portA, portC));
        NodeInfo a = new NodeInfo(NodeId.of("storage-a"), "127.0.0.1", portA);
        NodeInfo c = new NodeInfo(NodeId.of("storage-c"), "127.0.0.1", portC);
        NodeInfo client = new NodeInfo(NodeId.of("client-1"), "127.0.0.1", portClient);
        NodeInfo aliasA = new NodeInfo(NodeId.of("127.0.0.1:" + portA), a.host(), a.port());
        NodeInfo aliasC = new NodeInfo(NodeId.of("127.0.0.1:" + portC), c.host(), c.port());

        TcpTransport storageA = new TcpTransport(meshConfig(a, c));
        TcpTransport storageC = new TcpTransport(meshConfig(c, a));
        TcpTransport clientTransport = new TcpTransport(meshConfig(client, aliasA, aliasC));
        var holdingSeedReply = new CountDownLatch(1);
        var releaseSeedReply = new CountDownLatch(1);
        var seenFromC = new java.util.concurrent.atomic.AtomicInteger();
        clientTransport.setHandshakeIdentityHook(id -> {
            if (id.equals(c.nodeId()) && seenFromC.incrementAndGet() == 1) {
                holdingSeedReply.countDown();
                awaitUninterruptibly(releaseSeedReply);
            }
        });
        Set<NodeId> dialedByClient = ConcurrentHashMap.newKeySet();
        clientTransport.setBeforeDialHook(dialedByClient::add);
        List<NodeId> clientLostPeers = new java.util.concurrent.CopyOnWriteArrayList<>();
        clientTransport.addListener(new TransportListener() {
            public void onPeerConnected(NodeInfo peer) { }
            public void onPeerDisconnected(NodeId peer) { clientLostPeers.add(peer); }
            public void onMessage(ClusterMessage message) { }
        });
        try {
            storageA.start();
            storageC.start();
            awaitFullDirectMesh(List.of(storageA, storageC), List.of(a, c), Duration.ofSeconds(10));

            clientTransport.start();
            assertTrue(holdingSeedReply.await(5, TimeUnit.SECONDS), "resposta do seed C não chegou");
            // storage-a's handshake reply teaches the client storage-c's canonical id.
            awaitDiscovery(clientTransport, c.nodeId());
            Thread.sleep(1_000); // past the 100 ms dial scheduled for a newly learned peer
            assertFalse(dialedByClient.contains(c.nodeId()),
                    "o cliente discou de novo para storage-c apesar da conexão ao seed aberta: " + dialedByClient);

            releaseSeedReply.countDown();
            awaitFullDirectMesh(List.of(storageA, storageC, clientTransport), List.of(a, c, client),
                    Duration.ofSeconds(15));
            assertTrue(clientLostPeers.isEmpty(), "o cliente perdeu links durante a entrada: " + clientLostPeers);
        } finally {
            releaseSeedReply.countDown();
            closeQuietly(clientTransport, storageA, storageC);
        }
    }

    /** Variante: o alias chega a storage-a por PEER_UPDATE de um terceiro, não por handshake. */
    @Test
    void seedAliasGossipedByPeerUpdateKeepsEstablishedLinks() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portC = allocateFreeLocalPort(Set.of(portA));
        NodeInfo a = new NodeInfo(NodeId.of("storage-a"), "127.0.0.1", portA);
        NodeInfo c = new NodeInfo(NodeId.of("storage-c"), "127.0.0.1", portC);
        NodeInfo aliasC = new NodeInfo(NodeId.of("127.0.0.1:" + portC), c.host(), c.port());
        NodeInfo gossiper = new NodeInfo(NodeId.of("gossip-x"), "127.0.0.1", 1);

        TcpTransport storageA = new TcpTransport(meshConfig(a, c));
        TcpTransport storageC = new TcpTransport(meshConfig(c, a));
        var releaseReply = new CountDownLatch(1);
        Set<NodeId> dialedByA = ConcurrentHashMap.newKeySet();
        storageA.setBeforeDialHook(dialedByA::add);
        storageC.addListener(slowResponder(storageC, releaseReply));
        try {
            storageA.start();
            storageC.start();
            awaitFullDirectMesh(List.of(storageA, storageC), List.of(a, c), Duration.ofSeconds(10));
            dialedByA.clear();

            var pending = storageA.sendAndAwait(ClusterMessage.request(
                    MessageType.CLIENT_REQUEST, "slow", a.nodeId(), c.nodeId(),
                    "chunk"));

            try (RawPeer raw = new RawPeer(a.host(), a.port())) {
                raw.send(ClusterMessage.request(
                        MessageType.HANDSHAKE, "hello", gossiper.nodeId(),
                        a.nodeId(), new HandshakePayload(
                                gossiper, Set.of(), Map.of(), false, true)));
                awaitDiscovery(storageA, gossiper.nodeId());
                raw.send(ClusterMessage.request(
                        MessageType.PEER_UPDATE, "peer-update", gossiper.nodeId(),
                        a.nodeId(), new PeerUpdatePayload(
                                Set.of(gossiper, aliasC), Map.of())));
                List<String> violations = sampleLinkWhileBroadcasting(storageA, c.nodeId(), aliasC.nodeId(),
                        Duration.ofSeconds(2));

                releaseReply.countDown();
                String pendingOutcome = awaitOutcome(pending);
                assertAll(
                        () -> assertTrue(violations.isEmpty(),
                                "storage-a perdeu o link canônico para storage-c: " + violations),
                        () -> assertFalse(dialedByA.contains(aliasC.nodeId()),
                                "storage-a discou o alias de seed de outro nó: " + dialedByA),
                        () -> assertEquals("ok", pendingOutcome, "request em voo a->c deveria completar"));
            }
        } finally {
            releaseReply.countDown();
            closeQuietly(storageA, storageC);
        }
    }

    /**
     * Um seed ainda não resolvido ({@code host:port} sem handshake direto) não entra no handshake
     * nem no PEER_UPDATE: quem o recebesse trocaria o id canônico daquele processo pelo alias.
     * Um peer inicial já verificado por handshake continua sendo propagado.
     */
    @Test
    void unresolvedSeedAliasIsNotGossiped() throws Exception {
        int portT = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portT));
        int deadPort = allocateFreeLocalPort(Set.of(portT, portB));
        NodeInfo t = new NodeInfo(NodeId.of("t-node"), "127.0.0.1", portT);
        NodeInfo b = new NodeInfo(NodeId.of("z-peer"), "127.0.0.1", portB);
        NodeInfo deadAlias = new NodeInfo(NodeId.of("127.0.0.1:" + deadPort), "127.0.0.1", deadPort);
        NodeInfo observer = new NodeInfo(NodeId.of("raw-observer"), "127.0.0.1", 1);
        TcpTransport transport = new TcpTransport(meshConfig(t, b, deadAlias));
        TcpTransport peerB = new TcpTransport(meshConfig(b));
        try {
            peerB.start();
            transport.start();
            awaitFullDirectMesh(List.of(transport, peerB), List.of(t, b), Duration.ofSeconds(10));
            assertTrue(transport.peers().stream().anyMatch(p -> p.nodeId().equals(deadAlias.nodeId())),
                    "precondição: o alias não resolvido segue conhecido localmente");

            try (RawPeer raw = new RawPeer(t.host(), t.port())) {
                raw.send(ClusterMessage.request(MessageType.HANDSHAKE, "hello", observer.nodeId(), t.nodeId(),
                        new HandshakePayload(observer, Set.of(), Map.of(), false, true)));
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && !(receivedType(raw, MessageType.HANDSHAKE)
                        && receivedType(raw, MessageType.PEER_UPDATE))) {
                    Thread.sleep(50);
                }
                List<Set<NodeInfo>> gossiped = new ArrayList<>();
                for (ClusterMessage message : raw.received()) {
                    if (message.type() == MessageType.HANDSHAKE) {
                        gossiped.add(message.payload(HandshakePayload.class).peers());
                    } else if (message.type() == MessageType.PEER_UPDATE) {
                        gossiped.add(message.payload(PeerUpdatePayload.class).peers());
                    }
                }
                assertTrue(receivedType(raw, MessageType.HANDSHAKE), "handshake de resposta não chegou");
                assertTrue(receivedType(raw, MessageType.PEER_UPDATE), "PEER_UPDATE não chegou");
                for (Set<NodeInfo> peers : gossiped) {
                    assertFalse(peers.stream().anyMatch(p -> p.nodeId().equals(deadAlias.nodeId())),
                            "alias de seed não resolvido foi propagado: " + peers);
                    assertTrue(peers.stream().anyMatch(p -> p.nodeId().equals(b.nodeId())),
                            "peer inicial verificado deveria ser propagado: " + peers);
                }
            }
        } finally {
            closeQuietly(transport, peerB);
        }
    }

    /**
     * O lock de conexão por peer serializa as discagens para ele. Se o disconnect o descartasse,
     * uma discagem ainda em curso seguiria segurando o lock antigo enquanto a próxima criaria um
     * novo — duas discagens simultâneas para o mesmo peer. O lock vive enquanto o peer é conhecido.
     */
    @Test
    void peerConnectionLockSurvivesDisconnect() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        NodeInfo a = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", portA);
        NodeInfo b = new NodeInfo(NodeId.of("b-node"), "127.0.0.1", portB);
        TcpTransport transportA = new TcpTransport(meshConfig(a, b));
        TcpTransport transportB = new TcpTransport(meshConfig(b, a));
        var disconnected = new CountDownLatch(1);
        transportA.addListener(new TransportListener() {
            public void onPeerConnected(NodeInfo peer) { }
            public void onPeerDisconnected(NodeId peer) {
                if (peer.equals(b.nodeId())) {
                    disconnected.countDown();
                }
            }
            public void onMessage(ClusterMessage message) { }
        });
        try {
            transportA.start();
            transportB.start();
            awaitFullDirectMesh(List.of(transportA, transportB), List.of(a, b), Duration.ofSeconds(10));
            var lockBefore = transportA.connectionLockFor(b.nodeId());

            transportB.close();
            assertTrue(disconnected.await(5, TimeUnit.SECONDS), "disconnect de b-node não foi confirmado");

            assertSame(lockBefore, transportA.connectionLockFor(b.nodeId()),
                    "o disconnect não pode trocar o lock de conexão do peer");
        } finally {
            closeQuietly(transportA, transportB);
        }
    }

    private static boolean receivedType(RawPeer raw, MessageType type) {
        return raw.received().stream().anyMatch(m -> m.type() == type);
    }

    /** Responde ao request {@code slow} só depois que {@code release} abrir. */
    private static TransportListener slowResponder(TcpTransport self, CountDownLatch release) {
        return new TransportListener() {
            public void onPeerConnected(NodeInfo peer) { }
            public void onPeerDisconnected(NodeId peer) { }
            public void onMessage(ClusterMessage message) {
                if ("slow".equals(message.qualifier())
                        && message.type() == MessageType.CLIENT_REQUEST) {
                    Thread.ofVirtual().start(() -> {
                        awaitUninterruptibly(release);
                        self.send(ClusterMessage.response(message, "done"));
                    });
                }
            }
        };
    }

    /**
     * Emula heartbeats de {@code from} (broadcast a cada ~100 ms) e registra toda amostra em que o
     * peer canônico some de {@code peers()}, o alias aparece, ou o link direto cai.
     */
    private static List<String> sampleLinkWhileBroadcasting(TcpTransport from, NodeId canonical, NodeId alias,
                                                            Duration window) throws InterruptedException {
        List<String> violations = new ArrayList<>();
        long start = System.currentTimeMillis();
        int tick = 0;
        while (System.currentTimeMillis() - start < window.toMillis()) {
            from.broadcast(ClusterMessage.request(
                    MessageType.CLIENT_REQUEST, "tick", from.local().nodeId(), null,
                    "tick-" + tick++));
            Thread.sleep(100);
            boolean knowsCanonical = from.peers().stream().anyMatch(p -> p.nodeId().equals(canonical));
            boolean knowsAlias = from.peers().stream().anyMatch(p -> p.nodeId().equals(alias));
            boolean connected = from.isConnected(canonical);
            if (!knowsCanonical || knowsAlias || !connected) {
                violations.add("t+" + (System.currentTimeMillis() - start) + "ms canonical=" + knowsCanonical
                        + " alias=" + knowsAlias + " connected=" + connected);
            }
        }
        return violations;
    }

    private static String awaitOutcome(CompletableFuture<?> pending) {
        try {
            pending.get(10, TimeUnit.SECONDS);
            return "ok";
        } catch (ExecutionException e) {
            return e.getCause().toString();
        } catch (Exception e) {
            return e.toString();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Três transports full-mesh iniciados o mais simultaneamente possível devem
     * convergir para uma malha totalmente direta (6 conexões direcionais) e
     * mantê-la estável, sem recorrer a proxy.
     */
    @Test
    void concurrentlyStartedPeersFormFullDirectMesh() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));

        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "127.0.0.1", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "127.0.0.1", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "127.0.0.1", portC);

        // Full-mesh: cada nó conhece os outros dois desde o boot.
        TcpTransport transA = new TcpTransport(meshConfig(infoA, infoB, infoC));
        TcpTransport transB = new TcpTransport(meshConfig(infoB, infoA, infoC));
        TcpTransport transC = new TcpTransport(meshConfig(infoC, infoA, infoB));

        try {
            startConcurrently(transA, transB, transC);

            // 1) Descoberta mútua (sanity)
            awaitDiscovery(transA, infoB.nodeId(), infoC.nodeId());
            awaitDiscovery(transB, infoA.nodeId(), infoC.nodeId());
            awaitDiscovery(transC, infoA.nodeId(), infoB.nodeId());

            // 2) Malha direta total e estável
            awaitFullDirectMesh(
                    List.of(transA, transB, transC),
                    List.of(infoA, infoB, infoC),
                    Duration.ofSeconds(20));

            // 3) Rotas devem ser DIRECT (nextHop == destino), nunca via proxy
            assertDirectRoute(transA, infoB.nodeId());
            assertDirectRoute(transA, infoC.nodeId());
            assertDirectRoute(transB, infoA.nodeId());
            assertDirectRoute(transB, infoC.nodeId());
            assertDirectRoute(transC, infoA.nodeId());
            assertDirectRoute(transC, infoB.nodeId());
        } finally {
            closeQuietly(transA, transB, transC);
        }
    }

    private static TcpTransportConfig meshConfig(NodeInfo local, NodeInfo... peers) {
        TcpTransportConfig.Builder builder = TcpTransportConfig.builder(local)
                .reconnectInterval(Duration.ofMillis(500))
                .routeProbeInterval(Duration.ofSeconds(1))
                .connectTimeout(Duration.ofSeconds(1));
        for (NodeInfo peer : peers) {
            builder.addPeer(peer);
        }
        return builder.build();
    }

    /** Inicia os transports o mais simultaneamente possível para forçar o simultaneous open. */
    private static void startConcurrently(TcpTransport... transports) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(transports.length);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(transports.length);
        for (TcpTransport t : transports) {
            Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    go.await();
                    t.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS), "threads de start não ficaram prontas");
        go.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "transports não iniciaram a tempo");
    }

    private static void awaitDiscovery(TcpTransport transport, NodeId... targets) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = true;
            for (NodeId target : targets) {
                if (transport.peers().stream().noneMatch(p -> p.nodeId().equals(target))) {
                    all = false;
                    break;
                }
            }
            if (all) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Descoberta incompleta em " + transport.local().nodeId());
    }

    /**
     * Aguarda a malha direta total e exige que ela permaneça estável por uma
     * janela, para descartar estados transitórios.
     */
    private static void awaitFullDirectMesh(List<TcpTransport> transports,
                                            List<NodeInfo> infos,
                                            Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            // Exige malha direta total E que ela permaneça estável por ~2s, para
            // descartar estados transitórios (flapping) de convergência.
            if (allDirectlyConnected(transports, infos)
                    && stableFor(transports, infos, Duration.ofSeconds(2))) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Malha direta total não convergiu/estabilizou dentro de " + timeout
                + ".\nEstado final:\n  " + meshState(transports, infos));
    }

    private static boolean stableFor(List<TcpTransport> transports,
                                     List<NodeInfo> infos,
                                     Duration window) throws InterruptedException {
        long end = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < end) {
            if (!allDirectlyConnected(transports, infos)) {
                return false;
            }
            Thread.sleep(50);
        }
        return true;
    }

    /** Verdadeiro quando todos os pares direcionais possuem conexão direta aberta. */
    private static boolean allDirectlyConnected(List<TcpTransport> transports, List<NodeInfo> infos) {
        for (int i = 0; i < transports.size(); i++) {
            for (int j = 0; j < infos.size(); j++) {
                if (i != j && !transports.get(i).isConnected(infos.get(j).nodeId())) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String meshState(List<TcpTransport> transports, List<NodeInfo> infos) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < transports.size(); i++) {
            for (int j = 0; j < infos.size(); j++) {
                if (i == j) {
                    continue;
                }
                boolean connected = transports.get(i).isConnected(infos.get(j).nodeId());
                sb.append(infos.get(i).nodeId().value())
                        .append(" -> ")
                        .append(infos.get(j).nodeId().value())
                        .append(connected ? " [direct] " : " [MISSING] ");
            }
        }
        return sb.toString();
    }

    private static void assertDirectRoute(TcpTransport transport, NodeId target) {
        Optional<NodeId> hop = transport.getRouter().nextHop(target);
        assertTrue(hop.isPresent(),
                "Sem rota de " + transport.local().nodeId() + " para " + target);
        assertEquals(target, hop.get(),
                "Rota de " + transport.local().nodeId() + " para " + target
                        + " deveria ser DIRECT, mas é via " + hop.get());
    }

    private static void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws java.io.IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (java.net.ServerSocket socket = new java.net.ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new java.io.IOException("Unable to allocate a free local port");
    }
}
