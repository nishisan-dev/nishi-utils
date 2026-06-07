package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class ReplicationConfigTest {

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
}
