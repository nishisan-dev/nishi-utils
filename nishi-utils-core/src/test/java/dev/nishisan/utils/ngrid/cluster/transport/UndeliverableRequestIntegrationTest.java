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
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
