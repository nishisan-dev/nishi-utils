package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProxyRoutingIntegrationTest {

    @Test
    void shouldRouteViaProxyWhenDirectLinkFails() throws Exception {
        // Setup 3 Nodes: A -> 9010, B -> 9011, C -> 9012
        NodeInfo infoA = new NodeInfo(NodeId.of("node-a"), "localhost", 9010);
        NodeInfo infoB = new NodeInfo(NodeId.of("node-b"), "localhost", 9011);
        NodeInfo infoC = new NodeInfo(NodeId.of("node-c"), "localhost", 9012);

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
}
