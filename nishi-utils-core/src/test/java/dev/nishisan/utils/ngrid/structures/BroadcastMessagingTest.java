package dev.nishisan.utils.ngrid.structures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.nishisan.utils.ngrid.cluster.transport.codec.CompositeMessageCodec;
import dev.nishisan.utils.ngrid.common.BroadcastMessagePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Coverage for the user-level broadcast messaging API (broadcastMessage / onMsgBroadcasted): the
 * USER_BROADCAST frame round-trips through the codec carrying the producer identity, and a message
 * broadcast by one node is delivered to every node — including the producer (loopback).
 */
class BroadcastMessagingTest {

    @Test
    void userBroadcastRoundTripsThroughCodecWithProducerIdentity() throws Exception {
        CompositeMessageCodec codec = new CompositeMessageCodec();
        NodeId producer = NodeId.of("node-1");
        ClusterMessage message = ClusterMessage.request(MessageType.USER_BROADCAST, "broadcast",
                producer, null, new BroadcastMessagePayload("hello-cluster"));

        ClusterMessage decoded = codec.decode(codec.encode(message));

        assertEquals(MessageType.USER_BROADCAST, decoded.type());
        assertEquals(producer, decoded.source(), "producer identity must survive the round-trip");
        assertEquals("hello-cluster", decoded.payload(BroadcastMessagePayload.class).body());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void broadcastIsDeliveredToEveryNodeIncludingProducer() throws Exception {
        try (NGridCluster cluster = NGrid.local(2).start()) {
            NGridNode producer = cluster.node(0);
            NGridNode other = cluster.node(1);
            NodeId producerId = producer.transport().local().nodeId();

            CopyOnWriteArrayList<String> producerSeen = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<NodeId> producerFrom = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<String> otherSeen = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<NodeId> otherFrom = new CopyOnWriteArrayList<>();

            producer.addBroadcastListener((from, msg) -> {
                producerFrom.add(from);
                producerSeen.add(msg);
            });
            other.addBroadcastListener((from, msg) -> {
                otherFrom.add(from);
                otherSeen.add(msg);
            });

            producer.broadcastMessage("coordinate-now");

            // Remote node must receive it...
            awaitContains(otherSeen, "coordinate-now", 10_000);
            // ...and the producer too (loopback).
            awaitContains(producerSeen, "coordinate-now", 10_000);

            assertEquals(producerId, otherFrom.get(0), "remote must see the producer identity");
            assertEquals(producerId, producerFrom.get(0), "loopback must carry the producer identity");
            assertTrue(otherSeen.contains("coordinate-now") && producerSeen.contains("coordinate-now"));
        }
    }

    private static void awaitContains(CopyOnWriteArrayList<String> list, String value, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!list.contains(value)) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("broadcast not delivered: expected '" + value + "' in " + list);
            }
            Thread.sleep(50);
        }
    }
}
