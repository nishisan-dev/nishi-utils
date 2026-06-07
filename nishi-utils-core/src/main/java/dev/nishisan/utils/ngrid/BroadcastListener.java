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

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Listener for user-level broadcast messages exchanged between nodes via the
 * {@code broadcastMessage} API. Lets code that coordinates over the cluster react to small,
 * fire-and-forget messages from any node (including the local one, see loopback below).
 *
 * <p>Delivery is best-effort: not ordered, not durable, and not guaranteed (a peer that is
 * unreachable at send time simply misses it). For guaranteed, ordered delivery use a replicated
 * queue instead.
 *
 * <p>The callback is invoked on a worker thread; keep it non-blocking. With loopback enabled the
 * producer also receives its own message (with {@code producer} equal to the local node id).
 */
@FunctionalInterface
public interface BroadcastListener {

    /**
     * Invoked when a broadcast message is received.
     *
     * @param producer the node that produced the message
     * @param message  the message body
     */
    void onMsgBroadcasted(NodeId producer, String message);
}
