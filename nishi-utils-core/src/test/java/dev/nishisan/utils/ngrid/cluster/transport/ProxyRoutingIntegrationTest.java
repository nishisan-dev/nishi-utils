package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

            ClusterMessage msg = ClusterMessage.request(MessageType.PING, "proxy-test", infoA.nodeId(), infoC.nodeId(), HeartbeatPayload.now());
            waitForProxyRoute(transA, infoC.nodeId(), infoB.nodeId());
            transA.send(msg);

            // Assert delivery
            boolean received = latch.await(5, TimeUnit.SECONDS);
            assertTrue(received, "Message should have been delivered to C via proxy");
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
