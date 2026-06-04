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
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OutboundChannel}, the per-connection outbound buffer with
 * the drop+catch-up backpressure policy (issue #113).
 *
 * <p>
 * These tests are the primary acceptance gate for "leader memory stays stable":
 * they prove that the number of pending replication messages is formally bounded
 * by the configured capacity, that control traffic is never throttled, and that
 * the unbounded (default) mode preserves the legacy never-drop behaviour.
 * </p>
 */
class OutboundChannelTest {

    private static final NodeId SOURCE = NodeId.of("leader");
    private static final NodeId DEST = NodeId.of("follower");

    private static ClusterMessage replication(int seq) {
        return ClusterMessage.request(MessageType.REPLICATION_REQUEST, "rep", SOURCE, DEST, "payload-" + seq);
    }

    private static ClusterMessage control(MessageType type) {
        return ClusterMessage.lightweight(type, "ctl", SOURCE, DEST, "ctl-payload");
    }

    @Test
    void capacityBoundsReplicationDepth() {
        int capacity = 10;
        int total = 25;
        OutboundChannel channel = new OutboundChannel(capacity);

        int accepted = 0;
        for (int i = 0; i < total; i++) {
            if (channel.enqueue(replication(i))) {
                accepted++;
            }
        }

        assertEquals(capacity, accepted, "only capacity messages should be accepted");
        assertEquals(capacity, channel.dataDepth(), "depth must never exceed capacity");
        assertEquals(total - capacity, channel.droppedCount(), "excess replication must be dropped");
    }

    @Test
    void enqueueReturnsFalseWhenDropping() {
        OutboundChannel channel = new OutboundChannel(1);
        assertTrue(channel.enqueue(replication(1)), "first replication fits");
        assertFalse(channel.enqueue(replication(2)), "second replication is dropped");
    }

    @Test
    void controlTrafficIsNeverDroppedNorCounted() {
        OutboundChannel channel = new OutboundChannel(2);
        // Saturate the replication capacity.
        assertTrue(channel.enqueue(replication(1)));
        assertTrue(channel.enqueue(replication(2)));
        assertEquals(2, channel.dataDepth());

        // Control / non-replication traffic always flows and never counts.
        for (MessageType type : new MessageType[]{
                MessageType.HEARTBEAT, MessageType.PING, MessageType.CLIENT_REQUEST,
                MessageType.SYNC_REQUEST, MessageType.REPLICATION_ACK, MessageType.SEQUENCE_RESEND_RESPONSE}) {
            assertTrue(channel.enqueue(control(type)), type + " must never be dropped");
        }

        assertEquals(2, channel.dataDepth(), "control traffic must not affect replication depth");
        assertEquals(0, channel.droppedCount(), "control traffic must not be counted as dropped");
    }

    @Test
    void unboundedModeNeverDropsButStillMeasures() {
        OutboundChannel channel = new OutboundChannel(0); // unbounded / default
        int total = 1000;
        for (int i = 0; i < total; i++) {
            assertTrue(channel.enqueue(replication(i)), "unbounded mode never drops");
        }
        assertEquals(total, channel.dataDepth(), "depth is still tracked when unbounded");
        assertEquals(0, channel.droppedCount(), "unbounded mode drops nothing");
    }

    @Test
    void pollDecrementsOnlyForReplication() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(5);
        channel.enqueue(replication(1));
        channel.enqueue(control(MessageType.HEARTBEAT));
        channel.enqueue(replication(2));
        assertEquals(2, channel.dataDepth());

        // First out is the replication message (FIFO) -> depth drops.
        ClusterMessage first = channel.poll(1, TimeUnit.SECONDS);
        assertEquals(MessageType.REPLICATION_REQUEST, first.type());
        assertEquals(1, channel.dataDepth());

        // Next is the heartbeat -> depth unchanged.
        ClusterMessage second = channel.poll(1, TimeUnit.SECONDS);
        assertEquals(MessageType.HEARTBEAT, second.type());
        assertEquals(1, channel.dataDepth());

        // Last replication -> depth back to zero.
        ClusterMessage third = channel.poll(1, TimeUnit.SECONDS);
        assertEquals(MessageType.REPLICATION_REQUEST, third.type());
        assertEquals(0, channel.dataDepth());
    }

    @Test
    void pollTimeoutReturnsNull() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(1);
        assertEquals(null, channel.poll(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void freeingSpaceAllowsNewReplication() throws InterruptedException {
        OutboundChannel channel = new OutboundChannel(1);
        assertTrue(channel.enqueue(replication(1)));
        assertFalse(channel.enqueue(replication(2)), "full -> dropped");

        channel.poll(1, TimeUnit.SECONDS); // drain -> frees one slot
        assertEquals(0, channel.dataDepth());
        assertTrue(channel.enqueue(replication(3)), "slot freed -> accepted");
    }

    @Test
    void negativeCapacityIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new OutboundChannel(-1));
    }

    /**
     * Fast producers + a single slow consumer (the WAN-saturated scenario): the
     * pending replication depth stays bounded (soft limit: capacity + at most the
     * number of concurrent producers), every enqueue is either accepted or
     * dropped, and accepted == drained once everything settles. The consumer
     * sleeps per drained message so it is guaranteed to fall behind the producers,
     * making drops deterministic.
     */
    @Test
    @Timeout(30)
    void concurrentProducersStayBoundedAndAccountForEveryMessage() throws InterruptedException {
        int capacity = 50;
        int producers = 8;
        int perProducer = 1000;
        OutboundChannel channel = new OutboundChannel(capacity);

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger dropped = new AtomicInteger();
        AtomicInteger drained = new AtomicInteger();
        AtomicInteger maxObservedDepth = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch start = new CountDownLatch(1);

        Thread consumer = Thread.ofVirtual().start(() -> {
            try {
                start.await();
                while (running.get() || channel.dataDepth() > 0) {
                    maxObservedDepth.accumulateAndGet(channel.dataDepth(), Math::max);
                    ClusterMessage m = channel.poll(20, TimeUnit.MILLISECONDS);
                    if (m != null && m.type() == MessageType.REPLICATION_REQUEST) {
                        drained.incrementAndGet();
                        Thread.sleep(1); // slow writer: cannot keep up with producers
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread[] threads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            threads[p] = Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        if (channel.enqueue(replication(i))) {
                            accepted.incrementAndGet();
                        } else {
                            dropped.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        for (Thread t : threads) {
            t.join();
        }
        running.set(false);
        consumer.join();

        int totalAttempts = producers * perProducer;
        assertEquals(totalAttempts, accepted.get() + dropped.get(),
                "every enqueue must be accounted as accepted or dropped");
        assertTrue(dropped.get() > 0, "slow consumer must force drops");
        assertEquals(accepted.get(), drained.get(),
                "every accepted replication must eventually be drained");
        assertEquals(0, channel.dataDepth(), "queue fully drained at the end");
        assertTrue(maxObservedDepth.get() <= capacity + producers,
                "soft bound: depth <= capacity + concurrent producers, observed " + maxObservedDepth.get());
    }
}
