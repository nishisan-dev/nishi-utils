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
 * Durability policy for the follower relay-log's on-disk tail (#124), analogous to
 * MySQL's {@code sync_relay_log}. It governs only how often received-and-persisted
 * entries are forced to disk — it does <b>not</b> affect crash-safety against duplicate
 * apply (that is handled structurally by the apply-frontier/bootstrap logic, analogous to
 * MySQL's crash-safe relay-log info).
 *
 * <p>Whatever tail is lost in a crash is re-fetched from the leader's resend op-log (#122),
 * so the choice is a throughput-vs-loss-window trade-off, not a correctness one.
 */
public enum RelayDurability {

    /**
     * No explicit fsync: the OS flushes the relay on its own schedule (the relay queue runs
     * with {@code withFsync(false)}). Fastest; the loss window of not-yet-flushed tail is
     * recovered by the leader's resend (#122). The default.
     */
    OS_MANAGED,

    /**
     * Group commit: the relay runs without per-op fsync, but a periodic task forces it to
     * disk every {@code relayGroupCommitInterval}, bounding the crash loss window without an
     * fsync per operation.
     */
    GROUP_COMMIT,

    /**
     * Fsync on every operation (the relay queue runs with {@code withFsync(true)}). Strongest
     * local durability, lowest throughput.
     */
    ALWAYS
}
