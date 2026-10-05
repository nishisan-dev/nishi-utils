/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Per-handler registry of the state captured for in-flight snapshot transfers (8.10.1). Internal to the
 * NGrid replication handlers ({@code MapClusterService}, {@code QueueClusterService}); not meant for
 * application code.
 *
 * <p>A transfer opens its session on chunk {@code 0} (capturing a consistent view of the handler state)
 * and later chunks read that same view. A session is dropped when released, when idle for longer than
 * the time-to-live (a requester that vanished mid-transfer), or when the registry is full and a newer
 * session evicts the least recently used one. A later chunk of a dropped session gets nothing; the
 * requester's stuck-sync janitor then restarts the transfer from chunk {@code 0}.
 *
 * <p>Thread-safe: every operation runs under the registry monitor and does no I/O (the capture itself
 * runs outside the monitor).
 *
 * @param <S> the captured state type
 */
public final class SnapshotSessionRegistry<S> {

    /** Default idle time-to-live of a session, well above the requester's 15 s stuck-sync janitor. */
    public static final long DEFAULT_TTL_MS = 60_000L;
    /** Default bound on concurrent sessions per handler. */
    public static final int DEFAULT_MAX_SESSIONS = 8;

    private record Entry<S>(S state, long lastAccessMs) {
    }

    private final long ttlMs;
    private final int maxSessions;
    private final LongSupplier clock;
    private final LinkedHashMap<String, Entry<S>> sessions = new LinkedHashMap<>(16, 0.75f, true);

    /** Registry with the default time-to-live and capacity, on the wall clock. */
    public SnapshotSessionRegistry() {
        this(DEFAULT_TTL_MS, DEFAULT_MAX_SESSIONS, System::currentTimeMillis);
    }

    /**
     * @param ttlMs       idle time-to-live of a session, in milliseconds
     * @param maxSessions maximum number of concurrent sessions (at least 1)
     * @param clock       wall clock, in milliseconds
     */
    public SnapshotSessionRegistry(long ttlMs, int maxSessions, LongSupplier clock) {
        this.ttlMs = Math.max(1L, ttlMs);
        this.maxSessions = Math.max(1, maxSessions);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Opens (or re-opens) a session with freshly captured state: a new chunk {@code 0} for the same
     * session identifier replaces the previous capture.
     *
     * @param sessionId the session identifier
     * @param capture   captures the consistent view (called outside the registry monitor)
     * @return the captured state
     */
    public S open(String sessionId, Supplier<S> capture) {
        Objects.requireNonNull(sessionId, "sessionId");
        S state = capture.get();
        synchronized (this) {
            long now = clock.getAsLong();
            expire(now);
            sessions.remove(sessionId);
            while (sessions.size() >= maxSessions) {
                String eldest = sessions.keySet().iterator().next();
                sessions.remove(eldest);
            }
            sessions.put(sessionId, new Entry<>(state, now));
        }
        return state;
    }

    /**
     * Returns the state of a live session and refreshes its idle timer.
     *
     * @param sessionId the session identifier
     * @return the captured state, or {@code null} when the session is unknown, expired or evicted
     */
    public synchronized S get(String sessionId) {
        long now = clock.getAsLong();
        expire(now);
        Entry<S> entry = sessions.get(sessionId);
        if (entry == null) {
            return null;
        }
        sessions.put(sessionId, new Entry<>(entry.state(), now));
        return entry.state();
    }

    /**
     * Drops a session.
     *
     * @param sessionId the session identifier
     */
    public synchronized void release(String sessionId) {
        sessions.remove(sessionId);
    }

    /**
     * Number of live sessions (expired ones are dropped first). For tests and diagnostics.
     *
     * @return the live session count
     */
    public synchronized int size() {
        expire(clock.getAsLong());
        return sessions.size();
    }

    private void expire(long now) {
        sessions.values().removeIf(entry -> now - entry.lastAccessMs() > ttlMs);
    }
}
