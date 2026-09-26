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

import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.UndeliverablePayload;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A request routed through a relay to a peer that is gone must fail fast. The relay holds no direct
 * connection to the destination, so it answers {@link MessageType#UNDELIVERABLE} and the caller's
 * pending request completes exceptionally at once — instead of waiting out the request timeout, which
 * is what every strong read and every RPC still addressed to a just-dead leader used to do (tens of
 * seconds per attempt while the gossip-based proxy route to the dead node lingered).
 */
class UndeliverableRequestIntegrationTest {

    @Test
    void requestRelayedTowardsADeadPeerFailsFast() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", portC);

        // A long request timeout: the fail-fast must come from the relay's notice, not from it.
        TcpTransportConfig confA = TcpTransportConfig.builder(infoA).addPeer(infoB)
                .requestTimeout(Duration.ofSeconds(60)).build();
        TcpTransportConfig confB = TcpTransportConfig.builder(infoB).build();
        TcpTransportConfig confC = TcpTransportConfig.builder(infoC).addPeer(infoB).build();

        try (TcpTransport transA = new TcpTransport(confA);
             TcpTransport transB = new TcpTransport(confB);
             TcpTransport transC = new TcpTransport(confC)) {
            transB.start();
            transC.start();
            transA.start();
            await(() -> transA.peers().stream().anyMatch(p -> p.nodeId().equals(infoC.nodeId())), "A discovers C");
            await(() -> transA.isConnected(infoB.nodeId()) && transB.isConnected(infoC.nodeId()), "mesh up");

            // C dies (closing it here; the try-with-resources close at the end is idempotent). A still
            // routes to C via B: the gossip route survives the death.
            transC.close();
            // send() prefers an open direct socket even when the router says PROXY.
            // Wait for both readers to observe EOF before testing the relay-only path.
            await(() -> !transB.isConnected(infoC.nodeId()) && !transA.isConnected(infoC.nodeId()),
                    "A and B see C gone");
            // Since 8.8.0 B reports at once that it lost its link to C, so A would dial C directly. Wait
            // until that report has been processed by A (B is no longer a relay candidate for C)...
            await(() -> {
                transA.getRouter().markDirectFailure(infoC.nodeId());
                return Optional.of(infoC.nodeId()).equals(transA.getRouter().nextHop(infoC.nodeId()));
            }, "A processed B's report that it lost C");
            // ...then re-inject B's earlier report (as if its PEER_UPDATE were still in flight): the
            // fail-fast under test is the relay's notice for a STALE route to C via B.
            transA.getRouter().updateReachability(infoB.nodeId(), Set.of(infoC), Map.of(), Set.of(infoC.nodeId()));
            transA.getRouter().markDirectFailure(infoC.nodeId());
            Optional<NodeId> hop = transA.getRouter().nextHop(infoC.nodeId());
            assertTrue(hop.isPresent());
            assertEquals(infoB.nodeId(), hop.get(), "A must route to C via B");

            ClusterMessage request = ClusterMessage.request(MessageType.CLIENT_REQUEST, "dead-peer-request",
                    infoA.nodeId(), infoC.nodeId(), "ping");
            long start = System.currentTimeMillis();
            CompletableFuture<ClusterMessage> future = transA.sendAndAwait(request);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> future.get(10, TimeUnit.SECONDS),
                    "the request must fail, not wait out the 60 s request timeout");
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 10_000, "must fail fast, took " + elapsed + " ms");
            assertTrue(String.valueOf(failure.getCause().getMessage()).contains("undeliverable"),
                    "failure must come from the relay's notice: " + failure.getCause());
        }
    }

    /**
     * B6: um request roteado por um relay (próximo salto) deve falhar rápido quando o RELAY cai.
     * failPendingResponsesTo casava só o destino final, então o request esperava o requestTimeout.
     */
    @Test
    void requestRoutedThroughARelayFailsFastWhenTheRelayDisconnects() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", portC);
        TcpTransportConfig confA = TcpTransportConfig.builder(infoA).addPeer(infoB)
                .requestTimeout(Duration.ofSeconds(60)).build();
        TcpTransportConfig confB = TcpTransportConfig.builder(infoB).build();
        TcpTransportConfig confC = TcpTransportConfig.builder(infoC).addPeer(infoB).build();

        try (TcpTransport transA = new TcpTransport(confA);
             TcpTransport transB = new TcpTransport(confB);
             TcpTransport transC = new TcpTransport(confC)) {
            transB.start();
            transC.start();
            transA.start();
            await(() -> transA.isConnected(infoB.nodeId()) && transB.isConnected(infoC.nodeId()), "mesh up");
            // B reports C as connected: once that report reached A, B is a relay candidate for C.
            await(() -> {
                transA.getRouter().markDirectFailure(infoC.nodeId());
                return Optional.of(infoB.nodeId()).equals(transA.getRouter().nextHop(infoC.nodeId()));
            }, "A routes to C via B");

            // C never answers (no listener). The request is forwarded by B and stays pending on A.
            CompletableFuture<ClusterMessage> future = transA.sendAndAwait(ClusterMessage.request(
                    MessageType.CLIENT_REQUEST, "via-relay", infoA.nodeId(), infoC.nodeId(), "ping"));
            Thread.sleep(200);
            transB.close();

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> future.get(10, TimeUnit.SECONDS),
                    "the request must fail when its relay disconnects, not wait out the 60 s timeout");
            assertInstanceOf(PeerDisconnectedException.class, failure.getCause());
        }
    }

    /**
     * B6: o destino morre depois de o relay já ter encaminhado o request — nenhum UNDELIVERABLE volta.
     * O relay não rastreia os requests que encaminhou; o que ele reporta, ao confirmar a queda do
     * destino, é o novo conjunto de peers conectados (PEER_UPDATE). O remetente falha na hora os
     * requests pendentes roteados por esse relay para um destino que ele deixou de alcançar.
     */
    @Test
    void requestForwardedByARelayFailsFastWhenTheRelayLosesTheDestination() throws Exception {
        int portA = allocateFreeLocalPort(Set.of());
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", portC);
        TcpTransportConfig confA = TcpTransportConfig.builder(infoA).addPeer(infoB)
                .requestTimeout(Duration.ofSeconds(60)).build();
        TcpTransportConfig confB = TcpTransportConfig.builder(infoB).build();
        TcpTransportConfig confC = TcpTransportConfig.builder(infoC).addPeer(infoB).build();
        CountDownLatch releaseDials = new CountDownLatch(1);
        CountDownLatch receivedByC = new CountDownLatch(1);

        try (TcpTransport transA = new TcpTransport(confA);
             TcpTransport transB = new TcpTransport(confB);
             TcpTransport transC = new TcpTransport(confC)) {
            // No direct A-C link ever forms (as when the relay is used for real): dials are held in
            // both directions (C would dial A on a PEER_UPDATE broadcast of its own).
            transA.setBeforeDialHook(holdDialsTo(infoC.nodeId(), releaseDials));
            transC.setBeforeDialHook(holdDialsTo(infoA.nodeId(), releaseDials));
            transC.addListener(new TransportListener() {
                public void onPeerConnected(NodeInfo peer) { }
                public void onPeerDisconnected(NodeId peer) { }
                public void onMessage(ClusterMessage message) {
                    if ("via-relay".equals(message.qualifier())) {
                        receivedByC.countDown();
                    }
                }
            });
            transB.start();
            transC.start();
            transA.start();
            await(() -> transA.isConnected(infoB.nodeId()) && transB.isConnected(infoC.nodeId()), "mesh up");
            await(() -> {
                transA.getRouter().markDirectFailure(infoC.nodeId());
                return Optional.of(infoB.nodeId()).equals(transA.getRouter().nextHop(infoC.nodeId()));
            }, "A routes to C via B");

            CompletableFuture<ClusterMessage> future = transA.sendAndAwait(ClusterMessage.request(
                    MessageType.CLIENT_REQUEST, "via-relay", infoA.nodeId(), infoC.nodeId(), "ping"));
            assertTrue(receivedByC.await(10, TimeUnit.SECONDS), "B must have forwarded the request to C");
            assertTrue(!transA.isConnected(infoC.nodeId()), "precondition: A has no direct link to C");

            transC.close();

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> future.get(10, TimeUnit.SECONDS),
                    "the request must fail once the relay reports it lost C, not wait out the 60 s timeout");
            assertTrue(String.valueOf(failure.getCause().getMessage()).contains(infoB.nodeId().value()),
                    "the failure must name the relay: " + failure.getCause());
        } finally {
            releaseDials.countDown();
        }
    }

    /**
     * B10 (a): o UNDELIVERABLE de uma RESPOSTA vai para quem respondeu, que não tem pendência alguma
     * com aquele messageId — a pendência está no requester, chaveada pelo correlationId. O respondedor
     * deve repassar a notícia ao requester (pela rota que restar; aqui, discando-o diretamente) para
     * que ele falhe o request na hora em vez de esperar o timeout.
     */
    @Test
    void responderForwardsAResponseUndeliverableNoticeToTheRequester() throws Exception {
        int portP = allocateFreeLocalPort(Set.of());
        int portQ = allocateFreeLocalPort(Set.of(portP));
        // z-node > q-node: P is not the designated initiator, so it does not dial the requester's
        // listener on gossip (that dial would win the tie-break over the raw requester link below).
        NodeInfo responder = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", portP);
        NodeInfo requester = new NodeInfo(NodeId.of("q-node"), "127.0.0.1", portQ);
        NodeInfo relay = new NodeInfo(NodeId.of("r-node"), "127.0.0.1", 1);
        AtomicReference<ClusterMessage> received = new AtomicReference<>();
        JacksonMessageCodec json = new JacksonMessageCodec();

        try (TcpTransport transP = new TcpTransport(TcpTransportConfig.builder(responder).build());
             ServerSocket requesterListener = new ServerSocket()) {
            requesterListener.setReuseAddress(true);
            requesterListener.bind(new InetSocketAddress("127.0.0.1", portQ));
            requesterListener.setSoTimeout(5_000);
            transP.addListener(new TransportListener() {
                public void onPeerConnected(NodeInfo peer) { }
                public void onPeerDisconnected(NodeId peer) { }
                public void onMessage(ClusterMessage message) {
                    if ("rpc".equals(message.qualifier())) {
                        received.set(message);
                    }
                }
            });
            transP.start();
            try (RawPeer relayLink = new RawPeer(responder.host(), portP)) {
                // The relay holds a link to the requester (so P will route the response through it).
                relayLink.send(ClusterMessage.request(MessageType.HANDSHAKE, "hello", relay.nodeId(), responder.nodeId(),
                        new HandshakePayload(relay, Set.of(requester), Map.of(), false, true, true,
                                Set.of(requester.nodeId()))));
                await(() -> transP.isConnected(relay.nodeId()), "relay connected");
                ClusterMessage request;
                try (RawPeer requesterLink = new RawPeer(responder.host(), portP)) {
                    requesterLink.send(ClusterMessage.request(MessageType.HANDSHAKE, "hello", requester.nodeId(),
                            responder.nodeId(), new HandshakePayload(requester, Set.of(), Map.of(), false, true, true,
                                    Set.of())));
                    await(() -> transP.isConnected(requester.nodeId()), "requester connected");
                    request = ClusterMessage.request(MessageType.CLIENT_REQUEST, "rpc", requester.nodeId(),
                            responder.nodeId(), "ping");
                    requesterLink.send(request);
                    await(() -> received.get() != null, "P received the request");
                }
                // The requester's direct link dropped before P answers: P routes the response via the relay.
                await(() -> !transP.isConnected(requester.nodeId()), "requester link gone");
                await(() -> {
                    transP.getRouter().markDirectFailure(requester.nodeId());
                    return Optional.of(relay.nodeId()).equals(transP.getRouter().nextHop(requester.nodeId()));
                }, "P routes to the requester via the relay");
                transP.send(ClusterMessage.response(received.get(), "done"));
                await(() -> relayLink.received().stream().anyMatch(m -> m.type() == MessageType.CLIENT_RESPONSE),
                        "relay received the response");
                ClusterMessage response = relayLink.received().stream()
                        .filter(m -> m.type() == MessageType.CLIENT_RESPONSE).findFirst().orElseThrow();

                // The relay lost the requester meanwhile: it reports the RESPONSE as undeliverable to P.
                relayLink.send(ClusterMessage.lightweight(MessageType.UNDELIVERABLE, "undeliverable", relay.nodeId(),
                        responder.nodeId(), new UndeliverablePayload(response.messageId(), requester.nodeId(),
                                response.correlationId().orElse(null))));

                // P must tell the requester (dialing it directly, the relay being no longer a candidate).
                ClusterMessage notice = awaitUndeliverable(requesterListener, json);
                assertTrue(notice != null, "P should have forwarded the notice to the requester");
                UndeliverablePayload payload = notice.payload(UndeliverablePayload.class);
                assertEquals(request.messageId(), payload.messageId(),
                        "the notice must be keyed by the requester's pending request (correlation id)");
                assertEquals(responder.nodeId(), payload.destination(),
                        "the notice must name the request's destination, so the requester's pending matches");
            }
        }
    }

    /**
     * Accepts connections on {@code listener} until one delivers an UNDELIVERABLE frame (returned) or the
     * accept times out (null). Earlier dials closed by the transport (e.g. a PEER_UPDATE broadcast that
     * lost the tie-break) end in EOF and are skipped.
     */
    private static ClusterMessage awaitUndeliverable(ServerSocket listener, JacksonMessageCodec json)
            throws IOException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Socket fromP;
            try {
                fromP = listener.accept();
            } catch (java.net.SocketTimeoutException e) {
                return null;
            }
            try (fromP) {
                fromP.setSoTimeout(5_000);
                DataInputStream in = new DataInputStream(fromP.getInputStream());
                while (true) {
                    int length = in.readInt();
                    byte[] data = in.readNBytes(length);
                    int offset = data.length > 0 && data[0] == 0x00 ? 1 : 0;
                    if (data.length - offset > 0 && data[offset] == '{') {
                        ClusterMessage frame = json.decode(Arrays.copyOfRange(data, offset, data.length));
                        if (frame.type() == MessageType.UNDELIVERABLE) {
                            return frame;
                        }
                    }
                }
            } catch (java.io.EOFException | java.net.SocketTimeoutException closedOrSilent) {
                // this connection carried no notice; wait for the next one
            }
        }
        return null;
    }

    /** A dial hook that holds every dial to {@code target} until {@code release} opens. */
    private static java.util.function.Consumer<NodeId> holdDialsTo(NodeId target, CountDownLatch release) {
        return id -> {
            if (id.equals(target)) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("condition not met in time: " + what);
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("localhost", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port");
    }
}
