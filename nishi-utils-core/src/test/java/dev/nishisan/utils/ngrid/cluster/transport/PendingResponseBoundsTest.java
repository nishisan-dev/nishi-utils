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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * B10 (b): a tabela de respostas pendentes não pode vazar. Com {@code requestTimeout <= 0} nenhuma
 * tarefa de timeout era agendada e futures abandonados ficavam em {@code pendingResponses} para sempre;
 * um future cancelado pelo chamador também ficava lá até o timeout.
 */
class PendingResponseBoundsTest {

    @Test
    void cancellingTheFutureRemovesThePendingEntry() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo local = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", port);
        NodeInfo remote = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", 0);
        TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local)
                .requestTimeout(Duration.ofMinutes(5)).build());
        try {
            transport.start();
            try (RawPeer peer = new RawPeer(local.host(), port)) {
                peer.send(handshake(remote, local));
                awaitTrue(() -> transport.isConnected(remote.nodeId()), "peer não conectou");
                CompletableFuture<ClusterMessage> pending = transport.sendAndAwait(ClusterMessage.request(
                        MessageType.CLIENT_REQUEST, "never-answered", local.nodeId(), remote.nodeId(), "x"));
                assertEquals(1, transport.pendingResponseCount(), "precondição: request pendente");

                pending.cancel(true);

                awaitTrue(() -> transport.pendingResponseCount() == 0,
                        "um future cancelado pelo chamador deveria sair de pendingResponses");
            }
        } finally {
            transport.close();
        }
    }

    @Test
    void disabledRequestTimeoutIsStillBoundedByTheHardLimit() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo local = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", port);
        NodeInfo remote = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", 0);
        TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local)
                .requestTimeout(Duration.ZERO).build());
        Duration hardBoundBefore = TcpTransport.pendingResponseHardBound;
        TcpTransport.pendingResponseHardBound = Duration.ofMillis(300);
        try {
            transport.start();
            try (RawPeer peer = new RawPeer(local.host(), port)) {
                peer.send(handshake(remote, local));
                awaitTrue(() -> transport.isConnected(remote.nodeId()), "peer não conectou");
                CompletableFuture<ClusterMessage> pending = transport.sendAndAwait(ClusterMessage.request(
                        MessageType.CLIENT_REQUEST, "never-answered", local.nodeId(), remote.nodeId(), "x"));

                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> pending.get(5, TimeUnit.SECONDS),
                        "sem requestTimeout o future deveria falhar pelo limite máximo, não ficar para sempre");
                assertInstanceOf(TimeoutException.class, failure.getCause());
                assertEquals(0, transport.pendingResponseCount());
            }
        } finally {
            TcpTransport.pendingResponseHardBound = hardBoundBefore;
            transport.close();
        }
    }

    private static ClusterMessage handshake(NodeInfo self, NodeInfo target) {
        return ClusterMessage.request(MessageType.HANDSHAKE, "hello", self.nodeId(), target.nodeId(),
                new HandshakePayload(self, Set.of(), Map.of(), false, true, true));
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

    private static int allocateFreeLocalPort() throws java.io.IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }
}
