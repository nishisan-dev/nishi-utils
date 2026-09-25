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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.*;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RemoteSeriesRetryBudgetTest {
    private final TestClock clock = new TestClock();
    private final List<Duration> rpcBudgets = new ArrayList<>();
    private final List<Duration> lookupBudgets = new ArrayList<>();
    private final List<String> commands = new ArrayList<>();
    private String owner = "a";
    private boolean initialOpen = true;
    private Responder responder;

    private interface Responder {
        SeriesStatusResponse respond(String command, Duration timeout);
    }

    @Test
    void redirectReopenAndLookupShareTheOriginalBudget() {
        RemoteSeriesHandle handle = handle(0);
        responder = (command, timeout) -> {
            if (command.equals(Commands.OPEN)) {
                clock.advance(timeout.toMillis());
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "slow reopen");
            }
            if (owner.equals("a")) {
                clock.advance(40);
                return status(SeriesStatus.WRONG_OWNER, "b");
            }
            clock.advance(30);
            return status(SeriesStatus.NOT_OPEN, "b");
        };

        NgrrdClusterException failure = assertThrows(NgrrdClusterException.class, handle::checkpoint);

        assertEquals(ErrorCode.TIMEOUT, failure.code());
        assertEquals(List.of(Commands.CHECKPOINT, Commands.CHECKPOINT, Commands.OPEN), commands);
        assertEquals(List.of(Duration.ofMillis(100), Duration.ofMillis(60), Duration.ofMillis(30)), rpcBudgets);
        assertEquals(List.of(Duration.ofMillis(30)), lookupBudgets);
        assertEquals(100, clock.millis());
    }

    @Test
    void drainingPendingWritesAlsoConsumesTheOperationBudget() {
        RemoteSeriesHandle handle = handle(80);
        responder = (command, timeout) -> status(SeriesStatus.OK, owner);

        handle.checkpoint();

        assertEquals(List.of(Duration.ofMillis(20)), rpcBudgets);
    }

    @Test
    void noRpcStartsAfterTheWriteBarrierExhaustsTheBudget() {
        RemoteSeriesHandle handle = handle(100);
        assertEquals(ErrorCode.TIMEOUT,
                assertThrows(NgrrdClusterException.class, handle::flush).code());
        assertTrue(commands.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(value = SeriesStatus.class, names = {"NOT_OPEN", "WRONG_OWNER", "MIGRATING"})
    void persistentRetryableStatusesEventuallyFailWithinTheBudget(SeriesStatus status) {
        RemoteSeriesHandle handle = handle(0);
        responder = (command, timeout) -> {
            if (command.equals(Commands.OPEN)) return status(SeriesStatus.OK, owner);
            clock.advance(Math.min(40, timeout.toMillis()));
            return status(status, owner);
        };

        NgrrdClusterException failure = assertThrows(NgrrdClusterException.class, handle::checkpoint);

        assertEquals(status == SeriesStatus.MIGRATING ? ErrorCode.MIGRATING
                : status == SeriesStatus.WRONG_OWNER ? ErrorCode.WRONG_OWNER : ErrorCode.TIMEOUT, failure.code());
        assertEquals(100, clock.millis());
        assertTrue(commands.size() <= 5, "reopens must not reset the budget");
        assertTrue(rpcBudgets.stream().allMatch(d -> d.toMillis() > 0 && d.toMillis() <= 100));
    }

    @Test
    void permanentStorageErrorDuringReopenIsPreserved() {
        RemoteSeriesHandle handle = handle(0);
        responder = (command, timeout) -> command.equals(Commands.OPEN)
                ? new SeriesStatusResponse(SeriesStatus.ERROR, owner, "storage failure")
                : status(SeriesStatus.NOT_OPEN, owner);

        NgrrdClusterException failure = assertThrows(NgrrdClusterException.class, handle::checkpoint);

        assertEquals(ErrorCode.REMOTE_ERROR, failure.code());
        assertEquals("storage failure", failure.getMessage());
        assertEquals(List.of(Commands.CHECKPOINT, Commands.OPEN), commands);
    }

    @Test
    void interruptionStopsRecoveryAndPreservesInterruptFlag() {
        RemoteSeriesHandle handle = handle(0);
        responder = (command, timeout) -> status(SeriesStatus.NOT_OPEN, owner);
        Thread.currentThread().interrupt();
        try {
            assertEquals(ErrorCode.CLOSED,
                    assertThrows(NgrrdClusterException.class, handle::checkpoint).code());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(List.of(Commands.CHECKPOINT), commands);
        } finally {
            Thread.interrupted();
        }
    }

    private RemoteSeriesHandle handle(long writeDrainMillis) {
        var lookup = new PlacementLookup() {
            public SeriesPlacement resolve(String key, String hash) { return SeriesPlacement.active(owner, 0); }
            public SeriesPlacement resolve(String key, String hash, GeometryDescriptor geometry, Duration maxWait) {
                if (!initialOpen) lookupBudgets.add(maxWait);
                return resolve(key, hash);
            }
            public SeriesPlacement resolveExisting(String key, Duration maxWait) { return resolve(key, null); }
            public Optional<SeriesPlacement> placementCached(String key) { return Optional.of(resolve(key, null)); }
            public void invalidate(String key) { }
            public void noteOwner(String key, String newOwner) { owner = newOwner; }
        };
        var rpc = new ClusterRpc() {
            public <R> R call(NodeId target, String command, Object body, Class<R> type) {
                throw new AssertionError("RPC must have an explicit budget");
            }
            public <R> R call(NodeId target, String command, Object body, Class<R> type, Duration timeout) {
                if (initialOpen) return type.cast(status(SeriesStatus.OK, owner));
                commands.add(command);
                rpcBudgets.add(timeout);
                return type.cast(responder.respond(command, timeout));
            }
            public NodeId localId() { return NodeId.of("client"); }
            public Optional<NodeId> leaderId() { return Optional.of(NodeId.of("leader")); }
        };
        var buffer = new WriteBuffer() {
            public void enqueue(String owner, SeriesWrite write) { }
            public void flushNodeSync(String owner) { throw new AssertionError("unbounded flush"); }
            public void flushNodeSync(String owner, Duration maxWait) {
                assertEquals(Duration.ofMillis(100), maxWait);
                clock.advance(writeDrainMillis);
            }
        };
        var handle = new RemoteSeriesHandle("series", "yaml", "hash", Map.of(), Ngrrd.OpenOptions.defaults(),
                lookup, rpc, buffer, new RetryPolicy(Duration.ofMillis(100), Duration.ofMillis(1), Duration.ofMillis(2)),
                Duration.ofSeconds(5), Duration.ofSeconds(5), clock, (key, handle2) -> { });
        handle.open();
        initialOpen = false;
        return handle;
    }

    private static SeriesStatusResponse status(SeriesStatus status, String owner) {
        return new SeriesStatusResponse(status, owner, null);
    }

    private static final class TestClock extends Clock {
        private long now;
        void advance(long millis) { now += millis; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return Instant.ofEpochMilli(now); }
        public long millis() { return now; }
    }
}
