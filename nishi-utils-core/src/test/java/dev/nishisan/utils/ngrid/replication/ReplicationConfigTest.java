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
    void followerIngestModeDefaultsRelayStreamAndBuilderPropagates() {
        ReplicationConfig def = ReplicationConfig.builder(1)
                .dataDirectory(Path.of("target", "replication-config-test"))
                .build();
        assertEquals(FollowerIngestMode.RELAY_STREAM, def.followerIngestMode(),
                "follower ingest mode must default to RELAY_STREAM (the single, definitive mode)");

        ReplicationConfig relay = ReplicationConfig.builder(1)
                .dataDirectory(Path.of("target", "replication-config-test"))
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .build();
        assertEquals(FollowerIngestMode.RELAY_STREAM, relay.followerIngestMode(),
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

    @Test
    void binlogSegmentRetentionKnobsDefaultDisabledAndBuilderPropagates() {
        ReplicationConfig def = builder().build();
        assertEquals(0L, def.resendLogSegmentMaxBytes(),
                "byte-based rolling must default to disabled (current behavior preserved)");
        assertEquals(0, def.resendLogMaxSegments(),
                "segment-count retention must default to disabled");

        ReplicationConfig tuned = builder()
                .resendLogSegmentMaxBytes(10L * 1024 * 1024 * 1024) // 10GB
                .resendLogMaxSegments(10)
                .build();
        assertEquals(10L * 1024 * 1024 * 1024, tuned.resendLogSegmentMaxBytes(),
                "builder must propagate the per-segment byte cap");
        assertEquals(10, tuned.resendLogMaxSegments(),
                "builder must propagate the retained segment-count cap");
    }

    @Test
    void binlogSegmentRetentionKnobsRejectNegatives() {
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationConfig.builder(1).resendLogSegmentMaxBytes(-1L));
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationConfig.builder(1).resendLogMaxSegments(-1));
    }

    @Test
    void relayExpireAfterWriteDefaultsDisabledAndBuilderPropagates() {
        assertEquals(Duration.ZERO, builder().build().relayExpireAfterWrite(),
                "relay TTL must default to disabled (Duration.ZERO)");
        assertEquals(Duration.ofMinutes(30),
                builder().relayExpireAfterWrite(Duration.ofMinutes(30)).build().relayExpireAfterWrite());

        assertThrows(NullPointerException.class,
                () -> ReplicationConfig.builder(1).relayExpireAfterWrite(null));
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationConfig.builder(1).relayExpireAfterWrite(Duration.ofSeconds(-1)));
    }
}
