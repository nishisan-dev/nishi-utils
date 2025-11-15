package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration container used to bootstrap an {@link NGridNode} instance.
 */
public final class NGridConfig {
    private final NodeInfo local;
    private final Set<NodeInfo> peers;
    private final int replicationQuorum;
    private final Path queueDirectory;
    private final String queueName;

    private NGridConfig(Builder builder) {
        this.local = builder.local;
        this.peers = Collections.unmodifiableSet(new HashSet<>(builder.peers));
        this.replicationQuorum = builder.replicationQuorum;
        this.queueDirectory = builder.queueDirectory;
        this.queueName = builder.queueName;
    }

    public NodeInfo local() {
        return local;
    }

    public Set<NodeInfo> peers() {
        return peers;
    }

    public int replicationQuorum() {
        return replicationQuorum;
    }

    public Path queueDirectory() {
        return queueDirectory;
    }

    public String queueName() {
        return queueName;
    }

    public static Builder builder(NodeInfo local) {
        return new Builder(local);
    }

    public static final class Builder {
        private final NodeInfo local;
        private final Set<NodeInfo> peers = new HashSet<>();
        private int replicationQuorum = 2;
        private Path queueDirectory;
        private String queueName = "ngrid";

        private Builder(NodeInfo local) {
            this.local = Objects.requireNonNull(local, "local");
        }

        public Builder addPeer(NodeInfo peer) {
            if (!peer.nodeId().equals(local.nodeId())) {
                peers.add(peer);
            }
            return this;
        }

        public Builder replicationQuorum(int quorum) {
            if (quorum < 1) {
                throw new IllegalArgumentException("Quorum must be >= 1");
            }
            this.replicationQuorum = quorum;
            return this;
        }

        public Builder queueDirectory(Path directory) {
            this.queueDirectory = Objects.requireNonNull(directory, "directory");
            return this;
        }

        public Builder queueName(String name) {
            this.queueName = Objects.requireNonNull(name, "name");
            return this;
        }

        public NGridConfig build() {
            if (queueDirectory == null) {
                throw new IllegalStateException("Queue directory must be specified");
            }
            return new NGridConfig(this);
        }
    }
}
