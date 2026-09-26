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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OutboundChannel} (issue #113, restored in 8.8.0 after the 5.0.0 regression):
 * the outbound queue of a connection bounds the recoverable data plane ({@code RELAY_STREAM_BATCH},
 * re-fetched by the follower) to the configured capacity, dropping the OLDEST data frame when full,
 * while control frames (HEARTBEAT, PING, requests/responses, ...) are never dropped nor counted.
 * Depth and drop counters feed {@code outboundQueueDepths()}/{@code outboundDropped()}.
 */
class OutboundChannelTest {

    private static final NodeId SOURCE = NodeId.of("leader");
    private static final NodeId DEST = NodeId.of("follower");

    private static ClusterMessage message(MessageType type, int seq) {
        return ClusterMessage.request(type, "ctl", SOURCE, DEST, "payload-" + seq);
    }

    private static String payload(ClusterMessage message) {
        return message.payload(String.class);
    }

    @Test
    void boundedChannelDropsTheOldestDataFrameWhenFull() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(2);
        assertTrue(channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 1)));
        assertTrue(channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 2)));
        assertEquals(2, channel.dataDepth());

        assertTrue(channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 3)), "the newest frame is kept");

        assertEquals(2, channel.dataDepth(), "the depth never exceeds the capacity");
        assertEquals(1, channel.droppedCount(), "one frame was dropped");
        assertEquals(2, channel.capacity());
        assertEquals("payload-2", payload(channel.poll(1, TimeUnit.SECONDS)), "the OLDEST data frame is the one dropped");
        assertEquals("payload-3", payload(channel.poll(1, TimeUnit.SECONDS)));
        assertEquals(0, channel.dataDepth(), "polling a data frame decrements the depth");
    }

    @Test
    void controlFramesAreNeverDroppedNorCountedAndKeepTheirOrder() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(1);
        assertTrue(channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 1))); // fills the data quota
        List<MessageType> control = List.of(MessageType.HEARTBEAT, MessageType.PING, MessageType.HANDSHAKE,
                MessageType.PEER_UPDATE, MessageType.CLIENT_REQUEST, MessageType.CLIENT_RESPONSE,
                MessageType.SYNC_REQUEST, MessageType.SYNC_RESPONSE, MessageType.RELAY_STREAM_FETCH,
                MessageType.UNDELIVERABLE, MessageType.LEAVE);
        int seq = 2;
        for (MessageType type : control) {
            assertTrue(channel.enqueue(message(type, seq++)), type + " must never be dropped");
        }

        assertEquals(1, channel.dataDepth(), "control frames do not count towards the data quota");
        assertEquals(0, channel.droppedCount());
        assertEquals(MessageType.RELAY_STREAM_BATCH, channel.poll(1, TimeUnit.SECONDS).type());
        for (MessageType type : control) {
            assertEquals(type, channel.poll(1, TimeUnit.SECONDS).type(), "FIFO order is preserved");
        }
    }

    @Test
    void unboundedChannelNeverDrops() {
        OutboundChannel channel = new OutboundChannel(0);
        int total = 1000;
        for (int i = 0; i < total; i++) {
            assertTrue(channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, i)), "capacity 0 never drops");
        }
        assertEquals(total, channel.dataDepth(), "depth still reports the queued data frames");
        assertEquals(0, channel.droppedCount(), "nothing is ever dropped");
        assertEquals(0, channel.capacity(), "capacity reports unbounded (0)");
    }

    @Test
    void drainingHandsOverTheFramesAndResetsTheDepth() {
        OutboundChannel channel = new OutboundChannel(4);
        channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 1));
        channel.enqueue(message(MessageType.HEARTBEAT, 2));
        channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 3));
        List<ClusterMessage> drained = new ArrayList<>();

        assertEquals(3, channel.drainTo(drained));

        assertEquals(List.of("payload-1", "payload-2", "payload-3"), drained.stream().map(OutboundChannelTest::payload).toList());
        assertEquals(0, channel.dataDepth(), "drained frames no longer count");
        assertTrue(channel.enqueue(message(MessageType.RELAY_STREAM_BATCH, 4)));
        assertEquals(1, channel.dataDepth());
    }

    @Test
    void pollReturnsMessagesInFifoOrder() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(0);
        channel.enqueue(message(MessageType.CLIENT_REQUEST, 1));
        channel.enqueue(message(MessageType.HEARTBEAT, 2));

        ClusterMessage first = channel.poll(1, TimeUnit.SECONDS);
        ClusterMessage second = channel.poll(1, TimeUnit.SECONDS);
        assertEquals(MessageType.CLIENT_REQUEST, first.type());
        assertEquals(MessageType.HEARTBEAT, second.type());
        assertEquals(0, channel.dataDepth());
    }

    @Test
    void pollTimeoutReturnsNull() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(0);
        assertNull(channel.poll(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void negativeCapacityIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new OutboundChannel(-1));
    }
}
