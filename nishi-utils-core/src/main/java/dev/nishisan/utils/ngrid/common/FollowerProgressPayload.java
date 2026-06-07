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

package dev.nishisan.utils.ngrid.common;

/**
 * Follower → leader report of apply progress (#129). The leader uses it to know how far a joining
 * follower has caught up, so it can release the leader-pause-on-join quiesce gate once the follower
 * is within the configured lag threshold. Sent periodically while a node is a follower.
 *
 * @param appliedSequence the follower's last applied global sequence
 * @param epoch           the leader epoch the follower currently tracks (fencing/staleness context)
 */
public record FollowerProgressPayload(
                long appliedSequence,
                long epoch) {
}
