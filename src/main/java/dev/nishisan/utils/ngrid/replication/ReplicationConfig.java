package dev.nishisan.utils.ngrid.replication;

import java.util.Objects;

/**
 * Configuration for the replication manager.
 */
public final class ReplicationConfig {
    private final int quorum;

    private ReplicationConfig(int quorum) {
        this.quorum = quorum;
    }

    public static ReplicationConfig of(int quorum) {
        if (quorum < 1) {
            throw new IllegalArgumentException("Quorum must be >= 1");
        }
        return new ReplicationConfig(quorum);
    }

    public int quorum() {
        return quorum;
    }
}
