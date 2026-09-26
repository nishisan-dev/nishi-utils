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

import dev.nishisan.utils.ngrid.cluster.transport.codec.CompositeMessageCodec;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.PeerUpdatePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handshake de conexões a seeds configurados como {@code host:port} (alias provisório), no formato do
 * cliente ngrrd. Se qualquer frame chega antes do handshake numa conexão discada, o storage infere a
 * identidade do discador pelo {@code source} e nunca responde o handshake; o cliente fica com o link
 * preso ao alias, {@code isConnected(<id canônico>)} falso para sempre, e não sobe ("nenhum storage node
 * alcançável via transporte").
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class SeedAliasHandshakeTest {

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

    /** V1: heartbeat enviado à chave do alias entre a publicação da conexão e o handshake. */
    @Test
    void heartbeatToTheAliasBeforeTheHandshakeStillLetsTheClientReachTheCanonicalId() throws Exception {
        runSeedAliasScenario(false);
    }

    /** V2: mensagem ao id canônico reaproveita o socket do alias antes do handshake. */
    @Test
    void messageToTheCanonicalIdOverTheAliasSocketStillLetsTheClientReachIt() throws Exception {
        runSeedAliasScenario(true);
    }

    /** O handshake é o primeiro frame de uma conexão discada, mesmo com envio concorrente pela chave publicada. */
    @Test
    void dialedConnectionSendsTheHandshakeFirst() throws Exception {
        ServerSocket seedServer = new ServerSocket();
        closeables.add(seedServer);
        seedServer.bind(new InetSocketAddress("127.0.0.1", 0));
        int seedPort = seedServer.getLocalPort();
        usedPorts.add(seedPort);
        NodeInfo client = node("client-1", freePort());
        NodeInfo alias = node("127.0.0.1:" + seedPort, seedPort);
        TcpTransport transport = transport(client, alias);
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        closeables.add(release::countDown);
        holdAfterPublish(transport, alias.nodeId(), published, release);
        transport.start();
        Socket accepted = seedServer.accept();
        closeables.add(accepted);
        assertTrue(published.await(5, TimeUnit.SECONDS), "conexão ao seed não foi publicada");

        transport.send(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", client.nodeId(), alias.nodeId(),
                HeartbeatPayload.now()));
        release.countDown();

        accepted.setSoTimeout(5_000);
        DataInputStream in = new DataInputStream(accepted.getInputStream());
        assertEquals(MessageType.HANDSHAKE, readFrame(in).type(), "primeiro frame da conexão discada");
        assertEquals(MessageType.HEARTBEAT, readFrame(in).type(), "o frame retido segue depois do handshake");
    }

    /**
     * Se o envio do handshake de uma conexão discada falha, a conexão não pode ficar publicada e aberta
     * com todos os frames retidos pela trava pré-handshake para sempre: ela é fechada e o peer volta a ser
     * discado, agora com o handshake como primeiro frame.
     */
    @Test
    void failedHandshakeSendClosesTheDialedConnection() throws Exception {
        ServerSocket seedServer = new ServerSocket();
        closeables.add(seedServer);
        seedServer.bind(new InetSocketAddress("127.0.0.1", 0));
        int seedPort = seedServer.getLocalPort();
        usedPorts.add(seedPort);
        NodeInfo client = node("client-1", freePort());
        NodeInfo alias = node("127.0.0.1:" + seedPort, seedPort);
        TcpTransport transport = transport(client, alias);
        AtomicBoolean failNext = new AtomicBoolean(true);
        transport.setAfterPublishHook(id -> {
            if (id.equals(alias.nodeId()) && failNext.getAndSet(false)) {
                throw new IllegalStateException("simulated handshake send failure");
            }
        });
        transport.start();
        Socket first = seedServer.accept();
        closeables.add(first);

        first.setSoTimeout(5_000);
        assertEquals(-1, first.getInputStream().read(),
                "a conexão discada cujo handshake falhou ficou aberta, retendo os frames");

        // The next send redials the peer (client-1 sorts after the alias, so the reconnect loop does not).
        transport.send(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", client.nodeId(), alias.nodeId(),
                HeartbeatPayload.now()));
        seedServer.setSoTimeout(5_000);
        Socket second = seedServer.accept();
        closeables.add(second);
        second.setSoTimeout(5_000);
        DataInputStream in = new DataInputStream(second.getInputStream());
        assertEquals(MessageType.HANDSHAKE, readFrame(in).type(), "o redial não enviou o handshake primeiro");
        assertEquals(MessageType.HEARTBEAT, readFrame(in).type(), "o frame do redial não foi entregue");
    }

    /** Um frame anterior ao handshake não pode calar a resposta do handshake numa conexão aceita. */
    @Test
    void acceptedSocketAnswersTheFirstHandshakeEvenAfterTheIdentityWasInferred() throws Exception {
        NodeInfo server = node("storage-z", freePort());
        NodeInfo dialer = node("client-raw", freePort());
        TcpTransport transport = transport(server);
        transport.start();
        RawPeer raw = new RawPeer(server.host(), server.port());
        closeables.add(raw);

        raw.send(ClusterMessage.request(MessageType.PEER_UPDATE, "peer-update", dialer.nodeId(), server.nodeId(),
                new PeerUpdatePayload(Set.of(dialer), Map.of())));
        Thread.sleep(200);
        raw.send(ClusterMessage.request(MessageType.HANDSHAKE, "hello", dialer.nodeId(), server.nodeId(),
                new HandshakePayload(dialer, Set.of(), Map.of(), false, true)));

        awaitTrue(() -> raw.received().stream().anyMatch(m -> m.type() == MessageType.HANDSHAKE),
                "o handshake do discador ficou sem resposta");
    }

    /**
     * Defesa em profundidade: um socket de alias cujo handshake nunca é respondido não pode substituir o
     * dial do id canônico indefinidamente — depois de um prazo limitado o id canônico é discado.
     */
    @Test
    void aliasSocketWithoutHandshakeReplyDoesNotReplaceTheCanonicalDialForever() throws Exception {
        ServerSocket silentSeed = new ServerSocket();
        closeables.add(silentSeed);
        silentSeed.bind(new InetSocketAddress("127.0.0.1", 0));
        int seedPort = silentSeed.getLocalPort();
        usedPorts.add(seedPort);
        List<Socket> accepted = new java.util.concurrent.CopyOnWriteArrayList<>();
        closeables.add(() -> accepted.forEach(socket -> {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best-effort cleanup
            }
        }));
        Thread.ofVirtual().start(() -> {
            try {
                while (true) {
                    accepted.add(silentSeed.accept()); // accepts, reads nothing, never answers
                }
            } catch (IOException ignored) {
                // server closed
            }
        });
        NodeInfo client = node("client-1", freePort());
        NodeInfo alias = node("127.0.0.1:" + seedPort, seedPort);
        NodeInfo canonical = node("storage-c", seedPort);
        TcpTransport transport = transport(client, Duration.ofMillis(300), alias);
        AtomicInteger canonicalDials = new AtomicInteger();
        transport.setBeforeDialHook(id -> {
            if (id.equals(canonical.nodeId())) {
                canonicalDials.incrementAndGet();
            }
        });
        transport.start();
        awaitTrue(() -> !accepted.isEmpty() && transport.isConnected(alias.nodeId()),
                "o cliente não discou o alias do seed");

        transport.addPeer(canonical); // the canonical id of that address, learned from gossip
        awaitTrue(() -> {
            transport.send(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", client.nodeId(),
                    canonical.nodeId(), HeartbeatPayload.now()));
            return canonicalDials.get() > 0;
        }, "o id canônico nunca foi discado: o socket do alias sem handshake o substituiu indefinidamente");
    }

    /**
     * Depois que o handshake resolve o id canônico de um seed discado por alias, nenhuma chave antiga
     * (o alias) continua apontando para a mesma conexão — mesmo quando o alias já tinha saído de
     * knownPeers por gossip antes da resposta.
     */
    @Test
    void handshakeLeavesNoAliasKeyPointingToTheResolvedConnection() throws Exception {
        NodeInfo a = node("storage-a", freePort());
        NodeInfo c = node("storage-c", freePort());
        NodeInfo client = node("client-1", freePort());
        NodeInfo aliasA = node("127.0.0.1:" + a.port(), a.port());
        NodeInfo aliasC = node("127.0.0.1:" + c.port(), c.port());
        TcpTransport storageA = transport(a, c);
        TcpTransport storageC = transport(c, a);
        TcpTransport clientT = transport(client, aliasA, aliasC);
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        closeables.add(release::countDown);
        holdAfterPublish(clientT, aliasC.nodeId(), published, release);
        storageA.start();
        storageC.start();
        awaitMesh(List.of(storageA, storageC), List.of(a, c));
        clientT.start();
        assertTrue(published.await(5, TimeUnit.SECONDS), "alias de storage-c não foi publicado");
        // storage-c's canonical id arrives by gossip (storage-a) before the alias link's handshake reply.
        awaitTrue(() -> TcpTransportLeaveTest.knows(clientT, c.nodeId()), "cliente não aprendeu storage-c");
        release.countDown();

        awaitMesh(List.of(storageA, storageC, clientT), List.of(a, c, client));
        awaitTrue(() -> clientT.outboundQueueDepths().keySet().equals(Set.of(a.nodeId(), c.nodeId())),
                "chaves antigas seguem apontando para conexões já resolvidas");
        assertFalse(clientT.isConnected(aliasA.nodeId()) || clientT.isConnected(aliasC.nodeId()),
                "alias ainda publicado: " + clientT.outboundQueueDepths().keySet());
    }

    // ---- helpers ----

    /**
     * Client seeded with the two storages as {@code host:port} aliases; its dial to storage-c is held
     * right after publication, while a heartbeat is sent (to the alias, or to storage-c's canonical id
     * learned from storage-a). The full mesh must still converge.
     */
    private void runSeedAliasScenario(boolean viaCanonical) throws Exception {
        NodeInfo a = node("storage-a", freePort());
        NodeInfo c = node("storage-c", freePort());
        NodeInfo client = node("client-1", freePort());
        NodeInfo aliasA = node("127.0.0.1:" + a.port(), a.port());
        NodeInfo aliasC = node("127.0.0.1:" + c.port(), c.port());
        TcpTransport storageA = transport(a, c);
        TcpTransport storageC = transport(c, a);
        TcpTransport clientT = transport(client, aliasA, aliasC);
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        closeables.add(release::countDown);
        holdAfterPublish(clientT, aliasC.nodeId(), published, release);
        storageA.start();
        storageC.start();
        awaitMesh(List.of(storageA, storageC), List.of(a, c));
        clientT.start();
        assertTrue(published.await(5, TimeUnit.SECONDS), "alias de storage-c não foi publicado");

        NodeId destination = aliasC.nodeId();
        if (viaCanonical) {
            awaitTrue(() -> TcpTransportLeaveTest.knows(clientT, c.nodeId()),
                    "cliente não aprendeu storage-c via storage-a");
            destination = c.nodeId();
        }
        clientT.send(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", client.nodeId(), destination,
                HeartbeatPayload.now()));
        Thread.sleep(200); // the heartbeat is handed over before the handshake is queued
        release.countDown();

        awaitMesh(List.of(storageA, storageC, clientT), List.of(a, c, client));
    }

    private static void holdAfterPublish(TcpTransport transport, NodeId target, CountDownLatch published,
            CountDownLatch release) {
        transport.setAfterPublishHook(id -> {
            if (id.equals(target) && published.getCount() > 0) {
                published.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private static ClusterMessage readFrame(DataInputStream in) throws IOException {
        byte[] frame = in.readNBytes(in.readInt());
        return new CompositeMessageCodec(1024).decode(frame);
    }

    static NodeInfo node(String id, int port) {
        return new NodeInfo(NodeId.of(id), "127.0.0.1", port);
    }

    TcpTransport transport(NodeInfo local, NodeInfo... peers) {
        return transport(local, Duration.ofSeconds(1), peers);
    }

    TcpTransport transport(NodeInfo local, Duration connectTimeout, NodeInfo... peers) {
        TcpTransportConfig.Builder builder = TcpTransportConfig.builder(local)
                .reconnectInterval(Duration.ofMillis(500))
                .routeProbeInterval(Duration.ofSeconds(1))
                .connectTimeout(connectTimeout);
        for (NodeInfo peer : peers) {
            builder.addPeer(peer);
        }
        TcpTransport transport = new TcpTransport(builder.build());
        closeables.add(transport);
        return transport;
    }

    static void awaitMesh(List<TcpTransport> transports, List<NodeInfo> infos) throws InterruptedException {
        try {
            awaitTrue(() -> {
                for (int i = 0; i < transports.size(); i++) {
                    for (int j = 0; j < infos.size(); j++) {
                        if (i != j && !transports.get(i).isConnected(infos.get(j).nodeId())) {
                            return false;
                        }
                    }
                }
                return true;
            }, "malha não convergiu");
        } catch (AssertionError e) {
            throw new AssertionError("malha não convergiu: " + describe(transports, infos), e);
        }
    }

    private static String describe(List<TcpTransport> transports, List<NodeInfo> infos) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < transports.size(); i++) {
            sb.append(infos.get(i).nodeId().value()).append(" keys=")
                    .append(transports.get(i).outboundQueueDepths().keySet()).append("; ");
        }
        return sb.toString();
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

    int freePort() throws IOException {
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
}
