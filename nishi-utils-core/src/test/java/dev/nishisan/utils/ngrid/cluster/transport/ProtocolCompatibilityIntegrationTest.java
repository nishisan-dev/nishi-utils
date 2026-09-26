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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire compatibility of the 8.3.0 relay changes with older nodes, exercised through a raw socket
 * that speaks the legacy JSON framing:
 * <ul>
 *   <li>a handshake without {@code supportsUndeliverable} decodes as {@code false}, and a relay never
 *       sends {@code UNDELIVERABLE} to such a peer (an old node cannot decode the enum value);</li>
 *   <li>a message whose {@code MessageType} this node does not know is dropped WITHOUT closing the
 *       connection — only framing/I/O errors do.</li>
 * </ul>
 */
class ProtocolCompatibilityIntegrationTest {

    private final JacksonMessageCodec json = new JacksonMessageCodec();

    @Test
    void handshakeWithoutTheFieldMeansNoSupport() throws Exception {
        NodeInfo old = new NodeInfo(NodeId.of("old-node"), "localhost", 1);
        byte[] encoded = json.encode(ClusterMessage.request(MessageType.HANDSHAKE, "hello", old.nodeId(), null,
                new HandshakePayload(old, Set.of(), Map.of(), false, false)));
        String legacy = new String(encoded, StandardCharsets.UTF_8).replace(",\"supportsUndeliverable\":false", "");
        assertFalse(legacy.contains("supportsUndeliverable"), "precondition: field removed from the wire form");
        HandshakePayload decoded = json.decode(legacy.getBytes(StandardCharsets.UTF_8)).payload(HandshakePayload.class);
        assertFalse(decoded.supportsUndeliverable(), "an old handshake must not announce the notice");
        assertFalse(decoded.supportsCompression());
    }

    @Test
    void handshakeWithoutSupportsLeaveMeansNoSupport() throws Exception {
        NodeInfo old = new NodeInfo(NodeId.of("old-node"), "localhost", 1);
        byte[] encoded = json.encode(ClusterMessage.request(MessageType.HANDSHAKE, "hello", old.nodeId(), null,
                new HandshakePayload(old, Set.of(), Map.of(), true, true, false)));
        String legacy = new String(encoded, StandardCharsets.UTF_8).replace(",\"supportsLeave\":false", "");
        assertFalse(legacy.contains("supportsLeave"), "precondition: field removed from the wire form");
        HandshakePayload decoded = json.decode(legacy.getBytes(StandardCharsets.UTF_8)).payload(HandshakePayload.class);
        assertFalse(decoded.supportsLeave(), "an old handshake must not announce LEAVE support");
        assertTrue(decoded.supportsUndeliverable());
    }

    /**
     * PEER_UPDATE {@code departed} (8.7.0): an older node decodes the payload with its own two-field
     * shape and simply ignores the new property; a newer node reading an old update sees no departures.
     */
    @Test
    void peerUpdateDepartedIsIgnoredByOldNodesAndAbsentFromTheirUpdates() throws Exception {
        NodeInfo peer = new NodeInfo(NodeId.of("storage-1"), "localhost", 1);
        byte[] encoded = json.encode(ClusterMessage.request(MessageType.PEER_UPDATE, "peer-update", peer.nodeId(),
                null, new PeerUpdatePayload(Set.of(peer), Map.of(), Map.of(NodeId.of("client-1"), 60_000L))));
        com.fasterxml.jackson.databind.ObjectMapper mapper = JacksonMessageCodec.createDefaultMapper();
        com.fasterxml.jackson.databind.node.ObjectNode payload =
                (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(encoded).get("payload");
        assertTrue(payload.has("departed"), "precondition: the new field is on the wire");
        payload.remove("@class");

        LegacyPeerUpdatePayload legacy = mapper.treeToValue(payload, LegacyPeerUpdatePayload.class);
        assertTrue(legacy.peers.contains(peer), "an old node still reads the peer list");

        payload.remove("departed");
        PeerUpdatePayload fromOldNode = mapper.treeToValue(payload, PeerUpdatePayload.class);
        assertTrue(fromOldNode.departed().isEmpty(), "an update without the field reports no departure");
        assertTrue(fromOldNode.peers().contains(peer));
    }

    /** The PEER_UPDATE payload shape of nodes older than 8.7.0 (no {@code departed}). */
    static final class LegacyPeerUpdatePayload {
        final Set<NodeInfo> peers;
        final Map<NodeId, Double> latencies;

        @com.fasterxml.jackson.annotation.JsonCreator
        LegacyPeerUpdatePayload(@com.fasterxml.jackson.annotation.JsonProperty("peers") Set<NodeInfo> peers,
                @com.fasterxml.jackson.annotation.JsonProperty("latencies") Map<NodeId, Double> latencies) {
            this.peers = peers;
            this.latencies = latencies;
        }
    }

    @Test
    void oldPeerNeverReceivesUndeliverableAndUnknownTypeDoesNotCloseTheConnection() throws Exception {
        int port = allocateFreeLocalPort();
        NodeInfo local = new NodeInfo(NodeId.of("new-node"), "localhost", port);
        NodeInfo dead = new NodeInfo(NodeId.of("dead-node"), "localhost", allocateFreeLocalPort());
        List<ClusterMessage> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch afterBogus = new CountDownLatch(1);
        try (TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(local).addPeer(dead).build())) {
            transport.addListener(new TransportListener() {
                @Override
                public void onPeerConnected(NodeInfo peer) {
                }

                @Override
                public void onPeerDisconnected(NodeId peerId) {
                }

                @Override
                public void onMessage(ClusterMessage message) {
                    delivered.add(message);
                    if ("after-bogus".equals(message.qualifier())) {
                        afterBogus.countDown();
                    }
                }
            });
            transport.start();

            NodeInfo oldInfo = new NodeInfo(NodeId.of("old-node"), "localhost", 1);
            try (RawPeer old = new RawPeer(port)) {
                // Legacy handshake: no supportsUndeliverable field at all.
                byte[] hs = json.encode(ClusterMessage.request(MessageType.HANDSHAKE, "hello", oldInfo.nodeId(),
                        local.nodeId(), new HandshakePayload(oldInfo, Set.of(), Map.of(), false, false)));
                old.writeFrame(new String(hs, StandardCharsets.UTF_8)
                        .replace(",\"supportsUndeliverable\":false", "").getBytes(StandardCharsets.UTF_8));

                // 1. A request the new node must relay to a peer it cannot reach: no notice may come back.
                old.writeFrame(json.encode(ClusterMessage.request(MessageType.CLIENT_REQUEST, "to-dead",
                        oldInfo.nodeId(), dead.nodeId(), "ping")));
                Thread.sleep(1_000);
                assertTrue(old.received().stream().noneMatch(m -> m.type() == MessageType.UNDELIVERABLE),
                        "an old peer must never receive UNDELIVERABLE: " + old.received());

                // 2. A message of a type this node does not know: dropped, connection kept.
                byte[] valid = json.encode(ClusterMessage.request(MessageType.CLIENT_REQUEST, "bogus",
                        oldInfo.nodeId(), local.nodeId(), "x"));
                old.writeFrame(new String(valid, StandardCharsets.UTF_8)
                        .replace("\"CLIENT_REQUEST\"", "\"TYPE_FROM_THE_FUTURE\"").getBytes(StandardCharsets.UTF_8));
                old.writeFrame(json.encode(ClusterMessage.request(MessageType.CLIENT_REQUEST, "after-bogus",
                        oldInfo.nodeId(), local.nodeId(), "still here")));
                assertTrue(afterBogus.await(5, TimeUnit.SECONDS),
                        "the connection must survive an undecodable message; delivered=" + delivered);
                assertTrue(transport.isConnected(oldInfo.nodeId()), "connection must still be tracked as open");
            }
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("localhost", 0));
            return socket.getLocalPort();
        }
    }
}
