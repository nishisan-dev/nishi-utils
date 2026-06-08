package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.nishisan.utils.queue.NQueue;

class RelayStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void relayForIsIdempotentPerTopicAndSeparatePerTopic() {
        try (RelayStore store = new RelayStore(tempDir, Duration.ofMinutes(10))) {
            NQueue<byte[]> a1 = store.relayFor("queue:orders");
            NQueue<byte[]> a2 = store.relayFor("queue:orders");
            NQueue<byte[]> b = store.relayFor("map:profiles");

            assertSame(a1, a2, "same topic must reuse the same relay instance");
            assertNotSame(a1, b, "distinct topics must get distinct relays");
        }
    }

    @Test
    void framesRoundTripThroughTheRelay() throws Exception {
        try (RelayStore store = new RelayStore(tempDir, Duration.ofMinutes(10))) {
            NQueue<byte[]> relay = store.relayFor("queue:orders");
            UUID op = UUID.randomUUID();
            RelayEntry entry = new RelayEntry(7L, 11L, "queue:orders", op,
                    "frame-payload".getBytes(StandardCharsets.UTF_8));

            relay.offer(RelayEntryCodec.encode(entry));

            RelayEntry head = RelayEntryCodec.decode(relay.peek().orElseThrow());
            assertEquals(op, head.operationId());
            assertEquals(11L, head.sequence());
            assertEquals("queue:orders", head.topic());

            relay.poll();
            assertEquals(0L, relay.size(), "relay must drain to empty after poll");
        }
    }

    @Test
    void relayIsDurableAcrossReopen() throws Exception {
        UUID op = UUID.randomUUID();
        try (RelayStore store = new RelayStore(tempDir, Duration.ofMinutes(10))) {
            store.relayFor("queue:orders").offer(RelayEntryCodec.encode(
                    new RelayEntry(1L, 1L, "queue:orders", op, new byte[] { 9 })));
        }

        try (RelayStore reopened = new RelayStore(tempDir, Duration.ofMinutes(10))) {
            NQueue<byte[]> relay = reopened.relayFor("queue:orders");
            assertEquals(1L, relay.size(), "frame must survive a clean close + reopen");
            assertEquals(op, RelayEntryCodec.decode(relay.peek().orElseThrow()).operationId());
        }
    }

    @Test
    void framesAreDurableUnderEachRelayDurabilityPolicy() throws Exception {
        for (RelayDurability mode : RelayDurability.values()) {
            Path dir = tempDir.resolve("dur-" + mode);
            Files.createDirectories(dir);
            UUID op = UUID.randomUUID();

            try (RelayStore store = new RelayStore(dir, Duration.ofMinutes(10), mode, Duration.ofMillis(50))) {
                store.relayFor("queue:orders").offer(RelayEntryCodec.encode(
                        new RelayEntry(1L, 1L, "queue:orders", op, new byte[] { 7 })));
                if (mode == RelayDurability.GROUP_COMMIT) {
                    Thread.sleep(150); // allow a group-commit tick to force the relay
                }
            }

            try (RelayStore reopened = new RelayStore(dir, Duration.ofMinutes(10), mode, Duration.ofMillis(50))) {
                NQueue<byte[]> relay = reopened.relayFor("queue:orders");
                assertEquals(1L, relay.size(), "frame must be durable under " + mode);
                assertEquals(op, RelayEntryCodec.decode(relay.peek().orElseThrow()).operationId());
            }
        }
    }

    @Test
    void cleanShutdownMarkerDistinguishesCrashFromCleanRestart() throws Exception {
        Path relayDir = tempDir.resolve("relay");

        // Cold start: relay dir absent -> not unclean (nothing to bootstrap).
        assertFalse(RelayStore.isUncleanRestart(relayDir));

        // Prior run left state but no marker -> unclean (crash).
        Files.createDirectories(relayDir);
        assertTrue(RelayStore.isUncleanRestart(relayDir));

        // A clean shutdown writes the marker -> next start is clean.
        RelayStore.writeCleanMarker(relayDir);
        assertFalse(RelayStore.isUncleanRestart(relayDir));

        // Startup consumes the marker -> a subsequent crash (no new marker) is detected again.
        RelayStore.consumeCleanMarker(relayDir);
        assertTrue(RelayStore.isUncleanRestart(relayDir));
    }

    @Test
    void expireAfterWriteDropsAgedUnappliedEntriesOnPeek() throws Exception {
        // The relay-log TTL (MySQL relay-log expiry analog) discards entries older than the TTL at
        // peek time even though they were never applied — forcing the snapshot bootstrap path.
        try (RelayStore store = new RelayStore(tempDir, Duration.ofMinutes(10),
                RelayDurability.OS_MANAGED, Duration.ofSeconds(1), Duration.ofMillis(100))) {
            NQueue<byte[]> relay = store.relayFor("queue:orders");
            relay.offer(RelayEntryCodec.encode(
                    new RelayEntry(1L, 1L, "queue:orders", UUID.randomUUID(), new byte[] { 1 })));
            assertEquals(1L, relay.size(), "entry is present right after offer");

            Thread.sleep(250); // age well past the 100ms TTL
            assertTrue(relay.peek().isEmpty(),
                    "an un-applied entry older than the TTL must be discarded on peek");
            assertEquals(0L, relay.size(), "expired entry must be dropped from the relay");
        }
    }

    @Test
    void disabledExpireAfterWriteRetainsUnappliedEntries() throws Exception {
        // Default (Duration.ZERO) disables the TTL: un-applied entries are never dropped by age.
        try (RelayStore store = new RelayStore(tempDir, Duration.ofMinutes(10),
                RelayDurability.OS_MANAGED, Duration.ofSeconds(1), Duration.ZERO)) {
            NQueue<byte[]> relay = store.relayFor("queue:orders");
            relay.offer(RelayEntryCodec.encode(
                    new RelayEntry(1L, 1L, "queue:orders", UUID.randomUUID(), new byte[] { 1 })));
            Thread.sleep(150);
            assertTrue(relay.peek().isPresent(), "with TTL disabled the entry must survive");
            assertEquals(1L, relay.size());
        }
    }

    @Test
    void dirNameSanitizesAndDisambiguates() {
        assertFalse(RelayStore.dirName("queue:orders").contains(":"), "':' must be sanitized");
        assertFalse(RelayStore.dirName("map/profiles").contains("/"), "'/' must be sanitized");
        assertNotEquals(RelayStore.dirName("queue:orders"), RelayStore.dirName("queue/orders"),
                "topics that sanitize to the same string must stay disambiguated by hash");
    }
}
