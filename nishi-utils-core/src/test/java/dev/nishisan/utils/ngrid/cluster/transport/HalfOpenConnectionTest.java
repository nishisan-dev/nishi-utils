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
import dev.nishisan.utils.ngrid.common.PeerUpdatePayload;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B8: uma conexão half-open (o peer sumiu sem FIN/RST: host morto, NAT, cabo) ficava "conectada" para
 * sempre, com a fila de saída crescendo sem limite. O leitor passa a ter SO_TIMEOUT = janela de
 * silêncio (max(departedPeerForgetAfter, 3 × routeProbeInterval)): como todo link vivo carrega o
 * gossip periódico de peers conectados a cada routeProbeInterval, silêncio por essa janela significa
 * peer morto e a conexão é fechada. Keepalive TCP fica ligado como segunda linha.
 */
class HalfOpenConnectionTest {

    @Test
    void silentPeerIsClosedAfterTheReadIdleWindow() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo local = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", port);
        NodeInfo remote = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", 0);
        TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local)
                .departedPeerForgetAfter(Duration.ofMillis(300))
                .routeProbeInterval(Duration.ofMillis(100))
                .reconnectInterval(Duration.ofSeconds(10))
                .build());
        CountDownLatch disconnected = new CountDownLatch(1);
        transport.addListener(new TransportListener() {
            public void onPeerConnected(NodeInfo peer) { }
            public void onPeerDisconnected(NodeId peer) {
                if (peer.equals(remote.nodeId())) {
                    disconnected.countDown();
                }
            }
            public void onMessage(ClusterMessage message) { }
        });
        try {
            transport.start();
            try (Socket silent = new Socket()) {
                silent.connect(new InetSocketAddress(local.host(), port), 5_000);
                silent.setSoTimeout(5_000);
                byte[] hello = new JacksonMessageCodec().encode(ClusterMessage.request(MessageType.HANDSHAKE, "hello",
                        remote.nodeId(), local.nodeId(), new HandshakePayload(remote, Set.of(), Map.of(), false, true, true)));
                DataOutputStream out = new DataOutputStream(silent.getOutputStream());
                out.writeInt(hello.length);
                out.write(hello);
                out.flush();
                awaitTrue(() -> transport.isConnected(remote.nodeId()), "peer não conectou");
                long connectedAt = System.currentTimeMillis();

                // The peer never sends anything again (half-open). It still ACKs at the TCP level, so
                // only application-level silence can reveal it.
                assertTrue(disconnected.await(5, TimeUnit.SECONDS),
                        "a conexão half-open deveria ser fechada após a janela de silêncio");
                long silentForMs = System.currentTimeMillis() - connectedAt;
                assertTrue(silentForMs >= 250, "fechou cedo demais (" + silentForMs + " ms): a janela deve ser respeitada");
                assertFalse(transport.isConnected(remote.nodeId()));
                InputStream in = silent.getInputStream();
                while (in.read() != -1) {
                    // drain the handshake reply and gossip until EOF
                }
                assertEquals(-1, in.read(), "o socket do peer deveria ver o fechamento");
            }
        } finally {
            transport.close();
        }
    }

    @Test
    void peerThatKeepsTalkingStaysConnected() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo local = new NodeInfo(NodeId.of("a-node"), "127.0.0.1", port);
        NodeInfo remote = new NodeInfo(NodeId.of("z-node"), "127.0.0.1", 0);
        TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local)
                .departedPeerForgetAfter(Duration.ofMillis(300))
                .routeProbeInterval(Duration.ofMillis(100))
                .reconnectInterval(Duration.ofSeconds(10))
                .build());
        try {
            transport.start();
            try (RawPeer chatty = new RawPeer(local.host(), port)) {
                chatty.send(ClusterMessage.request(MessageType.HANDSHAKE, "hello", remote.nodeId(), local.nodeId(),
                        new HandshakePayload(remote, Set.of(), Map.of(), false, true, true)));
                awaitTrue(() -> transport.isConnected(remote.nodeId()), "peer não conectou");
                long end = System.currentTimeMillis() + 1_500;
                while (System.currentTimeMillis() < end) {
                    chatty.send(ClusterMessage.request(MessageType.PEER_UPDATE, "peer-update", remote.nodeId(),
                            local.nodeId(), new PeerUpdatePayload(Set.of(remote), Map.of())));
                    Thread.sleep(100);
                    assertTrue(transport.isConnected(remote.nodeId()), "um peer que fala não pode ser fechado");
                }
            }
        } finally {
            transport.close();
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

    private static int allocateFreeLocalPort() throws java.io.IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }
}
