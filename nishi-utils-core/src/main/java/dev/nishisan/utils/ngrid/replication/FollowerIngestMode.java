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

package dev.nishisan.utils.ngrid.replication;

/**
 * Selects how a <b>follower</b> ingests replicated operations. This is a
 * follower-side knob, orthogonal to the leader-side {@link ReplicationConfig#leaderLocalApply()}.
 */
public enum FollowerIngestMode {

    /**
     * Legacy path: each {@code REPLICATION_REQUEST} is applied inline, with
     * out-of-order sequences held in an in-memory buffer until their turn. Under a
     * sustained gap the buffer is capped and the follower falls back to a full
     * snapshot — the failure mode the relay-log replaces.
     */
    INLINE,

    /**
     * Relay-log path (#124): each {@code REPLICATION_REQUEST} is first persisted to
     * an on-disk relay (one NQueue per topic) and applied by a separate consumer at
     * its own pace, decoupling reception (durable, never drops) from application
     * (own rhythm). A lagging follower catches up from the relay instead of
     * resetting state and reinstalling a growing snapshot.
     */
    RELAY_LOG
}
