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

package dev.nishisan.utils.ngrid.config;

import dev.nishisan.utils.ngrid.cluster.transport.codec.CompositeMessageCodec;
import dev.nishisan.utils.ngrid.cluster.transport.codec.MessageCodec;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandshakePayload;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item 5 (coverage) — isolates {@code NGridNode.performAutodiscover} building its raw HANDSHAKE
 * message with the full local {@link NodeInfo} (roles + priority), independent of the seed's own
 * (unrelated) {@code TcpTransport} handshake. A fake seed — a bare {@link ServerSocket} speaking
 * only enough of the wire protocol to read the very first frame — captures the
 * {@link HandshakePayload#local()} the client sends, without ever completing the config-fetch
 * round trip (irrelevant here). This isolates the fix at {@code NGridNode.java:197-203} from the
 * end-to-end tests, whose final observed state is also (redundantly) guaranteed by the real
 * {@code TcpTransport} handshake performed later in {@code startServices()}.
 */
class NGridAutodiscoverHandshakeCarriesRolesTest {

    @Test
    @Timeout(30)
    void performAutodiscoverHandshakeCarriesRolesAndPriority(@TempDir Path tempDir) throws Exception {
        ServerSocket fakeSeed = new ServerSocket();
        fakeSeed.setReuseAddress(true);
        fakeSeed.bind(new InetSocketAddress("127.0.0.1", 0));
        int seedPort = fakeSeed.getLocalPort();

        AtomicReference<NodeInfo> capturedLocal = new AtomicReference<>();
        CountDownLatch handshakeCaptured = new CountDownLatch(1);
        Thread seedThread = new Thread(() -> {
            try (Socket socket = fakeSeed.accept()) {
                MessageCodec codec = new CompositeMessageCodec();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int length = dis.readInt();
                byte[] frame = dis.readNBytes(length);
                ClusterMessage handshakeMessage = codec.decode(frame);
                HandshakePayload payload = handshakeMessage.payload(HandshakePayload.class);
                capturedLocal.set(payload.local());
                handshakeCaptured.countDown();
                // Deliberately never answers the CONFIG_FETCH_REQUEST that follows: the client's
                // autodiscover will time out and NGridNode.start() will fail — irrelevant here, the
                // handshake we needed to capture was already sent as the very first frame.
            } catch (IOException ignored) {
                // The client closes the socket after its own timeout — expected.
            }
        }, "fake-seed-accept");
        seedThread.setDaemon(true);
        seedThread.start();

        Path clientConfigFile = tempDir.resolve("client.yaml");
        NGridYamlConfig clientConfig = new NGridYamlConfig();

        NodeIdentityConfig clientIdentity = new NodeIdentityConfig();
        clientIdentity.setId("client");
        clientIdentity.setHost("127.0.0.1");
        clientIdentity.setPort(0);
        clientIdentity.setPriority(100);
        clientIdentity.setRoles(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE));
        NodeIdentityConfig.DirsConfig dirs = new NodeIdentityConfig.DirsConfig();
        dirs.setBase(tempDir.resolve("client-data").toString());
        clientIdentity.setDirs(dirs);
        clientConfig.setNode(clientIdentity);

        AutodiscoverConfig auto = new AutodiscoverConfig();
        auto.setEnabled(true);
        auto.setSecret("whatever-the-fake-seed-ignores-anyway");
        auto.setSeed("127.0.0.1:" + seedPort);
        clientConfig.setAutodiscover(auto);

        NGridConfigLoader.save(clientConfigFile, clientConfig);

        NGridNode client = new NGridNode(clientConfigFile);
        try {
            assertThrows(IllegalStateException.class, client::start,
                    "the fake seed never answers CONFIG_FETCH_REQUEST, so autodiscover should time out"
                            + " and start() should fail — the handshake must already have been sent"
                            + " by then");
        } finally {
            fakeSeed.close();
        }

        assertTrue(handshakeCaptured.await(5, TimeUnit.SECONDS),
                "the fake seed should have received the client's raw HANDSHAKE frame");
        NodeInfo local = capturedLocal.get();
        assertEquals(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), local.roles(),
                "the raw HANDSHAKE built in performAutodiscover must carry the client's real roles");
        assertEquals(100, local.priority(),
                "the raw HANDSHAKE built in performAutodiscover must carry the client's real priority");
    }
}
