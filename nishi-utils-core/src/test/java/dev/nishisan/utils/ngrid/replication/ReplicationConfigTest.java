package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;

class ReplicationConfigTest {

    private static ReplicationConfig.Builder builder() {
        return ReplicationConfig.builder(1).dataDirectory(Path.of("target", "replication-config-test"));
    }

    @Test
    void followerIngestModeDefaultsInlineAndBuilderPropagates() {
        ReplicationConfig def = ReplicationConfig.builder(1)
                .dataDirectory(Path.of("target", "replication-config-test"))
                .build();
        assertEquals(FollowerIngestMode.INLINE, def.followerIngestMode(),
                "follower ingest mode must default to INLINE (4.3.0 behavior preserved)");

        ReplicationConfig relay = ReplicationConfig.builder(1)
                .dataDirectory(Path.of("target", "replication-config-test"))
                .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                .build();
        assertEquals(FollowerIngestMode.RELAY_LOG, relay.followerIngestMode(),
                "builder must propagate the configured follower ingest mode");

        assertThrows(NullPointerException.class,
                () -> ReplicationConfig.builder(1).followerIngestMode(null),
                "null mode must be rejected");
    }

    @Test
    void relayDurabilityDefaultsOsManagedAndBuilderPropagates() {
        assertEquals(RelayDurability.OS_MANAGED, builder().build().relayDurability(),
                "relay durability must default to OS_MANAGED");
        assertEquals(RelayDurability.ALWAYS,
                builder().relayDurability(RelayDurability.ALWAYS).build().relayDurability());

        assertThrows(NullPointerException.class, () -> ReplicationConfig.builder(1).relayDurability(null));
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationConfig.builder(1).relayGroupCommitInterval(Duration.ZERO),
                "zero group-commit interval must be rejected");
    }
}
