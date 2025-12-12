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

package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Utility factory for a reusable leader election service built on top of NGrid's
 * {@link ClusterCoordinator}.
 */
public final class LeaderElectionUtils {

    private LeaderElectionUtils() {
    }

    /**
     * Creates a leader election service.
     *
     * <p><strong>Note:</strong> {@link ClusterCoordinator#close()} shuts down the provided scheduler.
     * Provide a dedicated {@link ScheduledExecutorService} for this service.
     */
    public static LeaderElectionService create(Transport transport,
                                               ClusterCoordinatorConfig config,
                                               ScheduledExecutorService scheduler) {
        return new LeaderElectionService(transport, config, scheduler);
    }

    /**
     * Leader election service wrapper that exposes leader-related information and listener hooks.
     */
    public static final class LeaderElectionService implements Closeable {
        private final Transport transport;
        private final ClusterCoordinator coordinator;
        private final Set<LeaderElectionListener> electionListeners = new CopyOnWriteArraySet<>();

        private final LeadershipListener internalLeadershipListener = this::notifyLeadershipChange;

        private LeaderElectionService(Transport transport,
                                      ClusterCoordinatorConfig config,
                                      ScheduledExecutorService scheduler) {
            this.transport = Objects.requireNonNull(transport, "transport");
            Objects.requireNonNull(config, "config");
            Objects.requireNonNull(scheduler, "scheduler");
            this.coordinator = new ClusterCoordinator(transport, config, scheduler);
        }

        public void start() {
            coordinator.addLeadershipListener(internalLeadershipListener);
            coordinator.start();
        }

        public boolean isLeader() {
            return coordinator.isLeader();
        }

        public Optional<NodeInfo> leaderInfo() {
            return coordinator.leaderInfo();
        }

        public Collection<NodeInfo> activeMembers() {
            return coordinator.activeMembers();
        }

        /**
         * Adds a listener that is notified when the local node gains or loses leadership.
         */
        public void addLeaderElectionListener(LeaderElectionListener listener) {
            electionListeners.add(Objects.requireNonNull(listener, "listener"));
        }

        public void removeLeaderElectionListener(LeaderElectionListener listener) {
            electionListeners.remove(listener);
        }

        /**
         * Adds a raw coordinator listener (notified on any leader id change).
         */
        public void addLeadershipListener(LeadershipListener listener) {
            coordinator.addLeadershipListener(Objects.requireNonNull(listener, "listener"));
        }

        public void removeLeadershipListener(LeadershipListener listener) {
            coordinator.removeLeadershipListener(listener);
        }

        private void notifyLeadershipChange(NodeId newLeader) {
            boolean nowLeader = newLeader != null && newLeader.equals(transport.local().nodeId());
            for (LeaderElectionListener listener : electionListeners) {
                listener.onLeadershipChanged(nowLeader, newLeader);
            }
        }

        @Override
        public void close() throws IOException {
            coordinator.removeLeadershipListener(internalLeadershipListener);
            coordinator.close();
        }
    }
}


