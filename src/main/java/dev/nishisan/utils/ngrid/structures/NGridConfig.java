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

package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.map.MapPersistenceMode;

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
    private final Path mapDirectory;
    private final String mapName;
    private final MapPersistenceMode mapPersistenceMode;

    private NGridConfig(Builder builder) {
        this.local = builder.local;
        this.peers = Collections.unmodifiableSet(new HashSet<>(builder.peers));
        this.replicationQuorum = builder.replicationQuorum;
        this.queueDirectory = builder.queueDirectory;
        this.queueName = builder.queueName;
        this.mapDirectory = builder.mapDirectory != null ? builder.mapDirectory : builder.queueDirectory.resolve("maps");
        this.mapName = builder.mapName;
        this.mapPersistenceMode = builder.mapPersistenceMode;
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

    public Path mapDirectory() {
        return mapDirectory;
    }

    public String mapName() {
        return mapName;
    }

    public MapPersistenceMode mapPersistenceMode() {
        return mapPersistenceMode;
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
        private Path mapDirectory;
        private String mapName = "map";
        private MapPersistenceMode mapPersistenceMode = MapPersistenceMode.DISABLED;

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

        public Builder mapDirectory(Path directory) {
            this.mapDirectory = Objects.requireNonNull(directory, "directory");
            return this;
        }

        public Builder mapName(String name) {
            this.mapName = Objects.requireNonNull(name, "name");
            return this;
        }

        public Builder mapPersistenceMode(MapPersistenceMode mode) {
            this.mapPersistenceMode = Objects.requireNonNull(mode, "mode");
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
