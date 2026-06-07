package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

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
    void dirNameSanitizesAndDisambiguates() {
        assertFalse(RelayStore.dirName("queue:orders").contains(":"), "':' must be sanitized");
        assertFalse(RelayStore.dirName("map/profiles").contains("/"), "'/' must be sanitized");
        assertNotEquals(RelayStore.dirName("queue:orders"), RelayStore.dirName("queue/orders"),
                "topics that sanitize to the same string must stay disambiguated by hash");
    }
}
