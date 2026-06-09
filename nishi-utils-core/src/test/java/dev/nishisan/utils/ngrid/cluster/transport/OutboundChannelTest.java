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

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OutboundChannel}.
 *
 * <p>
 * Since 5.0.0 (RELAY_STREAM) the leader no longer pushes replication traffic, so the legacy
 * per-connection replication drop policy was removed along with the push protocol. The channel is
 * now a plain unbounded FIFO; these tests assert that it never drops, always accepts, and that the
 * depth/dropped metrics report a stable zero.
 * </p>
 */
class OutboundChannelTest {

    private static final NodeId SOURCE = NodeId.of("leader");
    private static final NodeId DEST = NodeId.of("follower");

    private static ClusterMessage message(MessageType type, int seq) {
        return ClusterMessage.request(type, "ctl", SOURCE, DEST, "payload-" + seq);
    }

    @Test
    void enqueueAlwaysAcceptsAndNeverDrops() {
        OutboundChannel channel = new OutboundChannel(0);
        int total = 1000;
        for (int i = 0; i < total; i++) {
            assertTrue(channel.enqueue(message(MessageType.CLIENT_REQUEST, i)), "channel never drops");
        }
        assertEquals(0, channel.dataDepth(), "depth metric is a stable zero (no data plane)");
        assertEquals(0, channel.droppedCount(), "nothing is ever dropped");
        assertEquals(0, channel.capacity(), "capacity reports unbounded (0)");
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
