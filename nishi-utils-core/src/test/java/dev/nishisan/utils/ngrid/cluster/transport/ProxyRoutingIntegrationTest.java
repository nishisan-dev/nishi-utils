package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProxyRoutingIntegrationTest {

    @Test
    void shouldRouteViaProxyWhenDirectLinkFails() throws Exception {
        int portA = allocateFreeLocalPort();
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));

        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", portC);

        // A knows B, C knows B. B acts as the hub initially.
        TcpTransportConfig confA = TcpTransportConfig.builder(infoA).addPeer(infoB).build();
        TcpTransportConfig confB = TcpTransportConfig.builder(infoB).build(); // B waits
        TcpTransportConfig confC = TcpTransportConfig.builder(infoC).addPeer(infoB).build();

        try (TcpTransport transA = new TcpTransport(confA);
             TcpTransport transB = new TcpTransport(confB);
             TcpTransport transC = new TcpTransport(confC)) {

            transB.start();
            transC.start();
            transA.start();

            // Wait for full mesh (A should discover C via B)
            waitForDiscovery(transA, infoC.nodeId());
            waitForDiscovery(transC, infoA.nodeId());
            waitForConnected(transA, infoB.nodeId());
            waitForConnected(transB, infoA.nodeId());
            waitForConnected(transB, infoC.nodeId());
            waitForConnected(transC, infoB.nodeId());
            // The direct A-C link must have settled before the failure is simulated: a handshake on
            // that link completing after markDirectFailure promotes the route back to DIRECT.
            waitForStableLink(transA, transC);

            System.out.println("Mesh converged.");

            // Verify Direct Route first
            Optional<NodeId> hopBefore = transA.getRouter().nextHop(infoC.nodeId());
            assertTrue(hopBefore.isPresent());
            assertEquals(infoC.nodeId(), hopBefore.get(), "Should initially be a direct route");

            // SIMULATE FAILURE: Mark C as unreachable from A directly
            System.out.println("Simulating failure A->C...");
            transA.getRouter().markDirectFailure(infoC.nodeId());

            // Verify Router updated to Proxy (likely via B, as it's the only other peer)
            Optional<NodeId> hopAfter = transA.getRouter().nextHop(infoC.nodeId());
            assertTrue(hopAfter.isPresent());
            assertEquals(infoB.nodeId(), hopAfter.get(), "Route should have switched to Proxy via B");

            // SEND MESSAGE: A -> C (will go A -> B -> C)
            CountDownLatch latch = new CountDownLatch(1);
            transC.addListener(new TransportListener() {
                @Override
                public void onPeerConnected(NodeInfo peer) {}
                @Override
                public void onPeerDisconnected(NodeId peerId) {}
                @Override
                public void onMessage(ClusterMessage message) {
                    if ("proxy-test".equals(message.qualifier())) {
                        System.out.println("Node C received message from " + message.source());
                        latch.countDown();
                    }
                }
            });

            ClusterMessage msg = ClusterMessage.request(MessageType.CLIENT_REQUEST, "proxy-test", infoA.nodeId(), infoC.nodeId(), "proxy-ping");
            waitForProxyRoute(transA, infoC.nodeId(), infoB.nodeId());
            transA.send(msg);

            // Assert delivery
            boolean received = latch.await(5, TimeUnit.SECONDS);
            assertTrue(received, "Message should have been delivered to C via proxy");
        }
    }

    /**
     * B1: um peer que apenas lista o alvo no handshake (gossip) mas declara não ter conexão viva com
     * ele não pode ser escolhido como relay — todo nó segue listando um líder morto por muito tempo.
     */
    @Test
    void relayWithoutLiveLinkToTheTargetIsNotChosen() throws Exception {
        int portA = allocateFreeLocalPort();
        int deadPort = allocateFreeLocalPort(Set.of(portA));
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "127.0.0.1", portA);
        NodeInfo relay = new NodeInfo(NodeId.of("node-relay"), "127.0.0.1", 1);
        NodeInfo target = new NodeInfo(NodeId.of("node-target"), "127.0.0.1", deadPort); // nobody listens

        try (TcpTransport transA = new TcpTransport(TcpTransportConfig.builder(infoA).build())) {
            transA.start();
            try (RawPeer relayLink = new RawPeer(infoA.host(), portA)) {
            // The relay knows the target (peer list) but holds no connection to it (connected = {}).
            relayLink.send(ClusterMessage.request(MessageType.HANDSHAKE, "hello", relay.nodeId(), infoA.nodeId(),
                    new HandshakePayload(relay, Set.of(target), Map.of(), false, true, true, Set.of())));
            waitForConnected(transA, relay.nodeId());
            waitForDiscovery(transA, target.nodeId());

            transA.getRouter().markDirectFailure(target.nodeId());

            assertEquals(Optional.of(target.nodeId()), transA.getRouter().nextHop(target.nodeId()),
                    "um relay sem link vivo ao alvo não pode ser escolhido");
            assertFalse(transA.isProxied(target.nodeId()));
            }
        }
    }

    /**
     * B1: quando o alvo morre, o relay deixa de reportá-lo como conectado e a rota do remetente volta
     * a DIRECT (isProxied falso) — em vez de ficar PROXY para sempre, entregando UNDELIVERABLE a cada
     * request e concedendo ao coordenador a carência de "alcançável via proxy" a um nó morto.
     */
    @Test
    void targetDeathReturnsTheRouteToDirectAndClearsIsProxied() throws Exception {
        int portA = allocateFreeLocalPort();
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", portB);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", portC);
        TcpTransportConfig confA = TcpTransportConfig.builder(infoA).addPeer(infoB).build();
        TcpTransportConfig confB = TcpTransportConfig.builder(infoB).build();
        TcpTransportConfig confC = TcpTransportConfig.builder(infoC).addPeer(infoB).build();

        try (TcpTransport transA = new TcpTransport(confA);
             TcpTransport transB = new TcpTransport(confB);
             TcpTransport transC = new TcpTransport(confC)) {
            transB.start();
            transC.start();
            transA.start();
            waitForDiscovery(transA, infoC.nodeId());
            waitForConnected(transA, infoB.nodeId());
            waitForConnected(transB, infoC.nodeId());
            waitForStableLink(transA, transC);

            transA.getRouter().markDirectFailure(infoC.nodeId());
            waitForProxyRoute(transA, infoC.nodeId(), infoB.nodeId());
            assertTrue(transA.isProxied(infoC.nodeId()), "precondição: rota via proxy B");

            transC.close();

            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline
                    && (transA.isProxied(infoC.nodeId())
                        || !Optional.of(infoC.nodeId()).equals(transA.getRouter().nextHop(infoC.nodeId())))) {
                Thread.sleep(50);
            }
            assertFalse(transA.isProxied(infoC.nodeId()), "o alvo morreu: a rota não pode seguir PROXY");
            assertEquals(Optional.of(infoC.nodeId()), transA.getRouter().nextHop(infoC.nodeId()),
                    "sem relay com link vivo, a rota volta a DIRECT");
        }
    }

    /**
     * B2: um membro inelegível a líder (cliente) não serve de relay entre storages, mesmo que tenha
     * link vivo com o destino — o tráfego storage↔storage não pode depender de um cliente efêmero.
     */
    @Test
    void leaderIneligiblePeerIsNeverChosenAsRelay() throws Exception {
        int portA = allocateFreeLocalPort();
        int portB = allocateFreeLocalPort(Set.of(portA));
        int portC = allocateFreeLocalPort(Set.of(portA, portB));
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", portA);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", portB,
                Set.of("client", NodeInfo.ROLE_LEADER_INELIGIBLE));
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", portC);
        TcpTransportConfig confA = TcpTransportConfig.builder(infoA).addPeer(infoB).build();
        TcpTransportConfig confB = TcpTransportConfig.builder(infoB).build();
        TcpTransportConfig confC = TcpTransportConfig.builder(infoC).addPeer(infoB).build();

        try (TcpTransport transA = new TcpTransport(confA);
             TcpTransport transB = new TcpTransport(confB);
             TcpTransport transC = new TcpTransport(confC)) {
            transB.start();
            transC.start();
            transA.start();
            waitForDiscovery(transA, infoC.nodeId());
            waitForConnected(transA, infoB.nodeId());
            waitForConnected(transB, infoC.nodeId());
            waitForStableLink(transA, transC);

            transA.getRouter().markDirectFailure(infoC.nodeId());

            assertEquals(Optional.of(infoC.nodeId()), transA.getRouter().nextHop(infoC.nodeId()),
                    "um peer inelegível a líder não pode ser relay");
            assertFalse(transA.isProxied(infoC.nodeId()));
        }
    }

    private void waitForDiscovery(TcpTransport transport, NodeId target) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            if (transport.peers().stream().anyMatch(p -> p.nodeId().equals(target))) {
                return;
            }
            Thread.sleep(100);
        }
        throw new RuntimeException("Peer " + target + " not discovered by " + transport.local().nodeId());
    }

    private void waitForConnected(TcpTransport transport, NodeId target) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            if (transport.isConnected(target)) {
                return;
            }
            Thread.sleep(50);
        }
        throw new RuntimeException("Peer " + target + " not connected by " + transport.local().nodeId());
    }

    /**
     * Waits until both endpoints report the direct link as connected and it stays so for a quiet
     * window (no simultaneous-open reshuffle or late handshake still in flight).
     */
    private void waitForStableLink(TcpTransport left, TcpTransport right) throws InterruptedException {
        NodeId leftId = left.local().nodeId();
        NodeId rightId = right.local().nodeId();
        long stableForMs = 500;
        long start = System.currentTimeMillis();
        long connectedSince = -1;
        while (System.currentTimeMillis() - start < 10_000) {
            boolean connected = left.isConnected(rightId) && right.isConnected(leftId);
            long now = System.currentTimeMillis();
            if (!connected) {
                connectedSince = -1;
            } else if (connectedSince < 0) {
                connectedSince = now;
            } else if (now - connectedSince >= stableForMs) {
                return;
            }
            Thread.sleep(25);
        }
        throw new RuntimeException("Direct link " + leftId + " <-> " + rightId + " did not settle");
    }

    private void waitForProxyRoute(TcpTransport transport, NodeId target, NodeId expectedProxy)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            Optional<NodeId> hop = transport.getRouter().nextHop(target);
            if (hop.isPresent() && hop.get().equals(expectedProxy)) {
                return;
            }
            Thread.sleep(50);
        }
        throw new RuntimeException("Proxy route not established from " + transport.local().nodeId()
                + " to " + target + " via " + expectedProxy);
    }

    private static int allocateFreeLocalPort() throws java.io.IOException {
        return allocateFreeLocalPort(Set.of());
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
