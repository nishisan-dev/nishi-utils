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
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Issue #169 (H3): as respostas pendentes são globais, não por conexão. Quando a conexão rastreada
 * de um peer cai enquanto outra conexão já identificada como o mesmo peer está aberta (ainda não
 * publicada — por exemplo, no meio do próprio handshake), o disconnect não pode falhar os requests
 * em voo: a outra conexão assume o peer e a resposta chega por ela.
 */
class PendingResponseDisconnectGraceTest {

    @Test
    void pendingResponseSurvivesWhenAnotherIdentifiedConnectionTakesOver() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo local = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", port);
        // Port 0: a non-listening peer, so the transport never dials it on its own.
        NodeInfo remote = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", 0);
        var secondHandshakeHeld = new CountDownLatch(1);
        var releaseSecondHandshake = new CountDownLatch(1);
        var handshakes = new AtomicInteger();
        TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local)
                .requestTimeout(Duration.ofSeconds(15))
                .build());
        transport.setHandshakeIdentityHook(id -> {
            if (id.equals(remote.nodeId()) && handshakes.incrementAndGet() == 2) {
                secondHandshakeHeld.countDown();
                awaitUninterruptibly(releaseSecondHandshake);
            }
        });
        try {
            transport.start();
            try (RawPeer first = new RawPeer(local.host(), port);
                 RawPeer second = new RawPeer(local.host(), port)) {
                first.send(handshake(remote, local));
                awaitTrue(() -> transport.isConnected(remote.nodeId()), "primeira conexão não foi publicada");

                CompletableFuture<ClusterMessage> pending = transport.sendAndAwait(ClusterMessage.request(
                        MessageType.CLIENT_REQUEST, "slow", local.nodeId(), remote.nodeId(), "chunk"));
                awaitTrue(() -> requestOn(first).isPresent(), "request não chegou pela primeira conexão");
                ClusterMessage request = requestOn(first).orElseThrow();

                // Second connection identifies itself as the same peer; its handshake is held right
                // after the identity is known, before the connection is published.
                second.send(handshake(remote, local));
                assertTrue(secondHandshakeHeld.await(5, TimeUnit.SECONDS), "segundo handshake não chegou");

                first.close();
                // Either the disconnect failed the request, or the second connection now carries the
                // peer's traffic (probes sent to z-node arrive through it).
                awaitTrue(() -> {
                    transport.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "probe", local.nodeId(),
                            remote.nodeId(), "probe"));
                    return pending.isDone() || second.received().stream()
                            .anyMatch(m -> "probe".equals(m.qualifier()));
                }, "disconnect da primeira conexão não foi processado");
                assertFalse(pending.isCompletedExceptionally(),
                        "o disconnect falhou o request em voo apesar de outra conexão aberta do mesmo peer: "
                                + outcome(pending));

                releaseSecondHandshake.countDown();
                second.send(ClusterMessage.response(request, "done"));
                ClusterMessage response = pending.get(5, TimeUnit.SECONDS);
                assertEquals(MessageType.CLIENT_RESPONSE, response.type());
                assertEquals("done", response.payload(String.class));
            }
        } finally {
            releaseSecondHandshake.countDown();
            transport.close();
        }
    }

    /**
     * B5: quando o desempate do simultaneous-open fecha a conexão que já estava publicada, os frames
     * ainda enfileirados nela (atrás de um frame em escrita) eram descartados em silêncio e o request
     * esperava o requestTimeout inteiro. Eles devem ser redrenados, em ordem, para a conexão vencedora.
     */
    @Test
    void requestQueuedOnALosingConnectionIsDeliveredOverTheWinner() throws Exception {
        int portLocal = allocateFreeLocalPort();
        int portRemote = allocateFreeLocalPort();
        // z-node > a-node: the inbound connection from a-node wins over z-node's own dialed one.
        NodeInfo local = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", portLocal);
        NodeInfo remote = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", portRemote);
        var fillerReached = new CountDownLatch(1);
        var releaseFiller = new CountDownLatch(1);
        TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local)
                .addPeer(remote)
                .requestTimeout(Duration.ofSeconds(15))
                .reconnectInterval(Duration.ofSeconds(30))
                .build());
        transport.setBeforeWriteHook(message -> {
            if ("filler".equals(message.qualifier())) {
                fillerReached.countDown();
                awaitUninterruptibly(releaseFiller);
            }
        });
        // a-node's listener: accepts z-node's dial and never answers (its handshake stays pending).
        try (ServerSocket remoteListener = new ServerSocket()) {
            remoteListener.setReuseAddress(true);
            remoteListener.bind(new InetSocketAddress("127.0.0.1", portRemote));
            transport.start();
            try (var dialed = remoteListener.accept()) {
                awaitTrue(() -> transport.isConnected(remote.nodeId()), "conexão discada não foi publicada");

                // Hold the dialed connection's writer on a filler frame; the request queues behind it.
                transport.send(ClusterMessage.request(MessageType.CLIENT_REQUEST, "filler", local.nodeId(),
                        remote.nodeId(), "x"));
                assertTrue(fillerReached.await(5, TimeUnit.SECONDS), "writer não chegou ao filler");
                CompletableFuture<ClusterMessage> pending = transport.sendAndAwait(ClusterMessage.request(
                        MessageType.CLIENT_REQUEST, "slow", local.nodeId(), remote.nodeId(), "chunk"));

                // Simultaneous open: a-node's inbound handshake wins the tie-break; the dialed one closes.
                try (RawPeer winner = new RawPeer(local.host(), portLocal)) {
                    winner.send(handshake(remote, local));
                    awaitTrue(() -> winner.received().stream().anyMatch(m -> m.type() == MessageType.HANDSHAKE),
                            "handshake de resposta não chegou pela conexão vencedora");
                    releaseFiller.countDown();

                    awaitTrue(() -> requestOn(winner).isPresent() || pending.isDone(),
                            "request enfileirado na conexão perdedora nem foi redrenado nem falhou rápido");
                    assertFalse(pending.isDone(), "o request nunca saiu: deveria ter sido redrenado, não falhado: "
                            + outcome(pending));
                    ClusterMessage request = requestOn(winner).orElseThrow();
                    winner.send(ClusterMessage.response(request, "done"));
                    assertEquals("done", pending.get(5, TimeUnit.SECONDS).payload(String.class));
                }
            }
        } finally {
            releaseFiller.countDown();
            transport.close();
        }
    }

    private static ClusterMessage handshake(NodeInfo self, NodeInfo target) {
        return ClusterMessage.request(MessageType.HANDSHAKE, "hello", self.nodeId(), target.nodeId(),
                new HandshakePayload(self, Set.of(), Map.of(), false, true));
    }

    private static Optional<ClusterMessage> requestOn(RawPeer peer) {
        return peer.received().stream()
                .filter(m -> m.type() == MessageType.CLIENT_REQUEST && "slow".equals(m.qualifier()))
                .findFirst();
    }

    private static String outcome(CompletableFuture<?> future) {
        try {
            return String.valueOf(future.getNow(null));
        } catch (Exception e) {
            return e.getCause() != null ? e.getCause().toString() : e.toString();
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError(message);
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

    private static int allocateFreeLocalPort() throws java.io.IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }
}
