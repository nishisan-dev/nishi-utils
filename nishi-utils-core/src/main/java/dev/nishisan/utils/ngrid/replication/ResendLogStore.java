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

import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Owns the leader-side resend op-log (#127): one durable {@link ResendLog} per topic, each a
 * segmented, sequence-indexed append-only log governed by a temporal retention window. It is the
 * disk-backed tail of the hybrid resend store — the {@code ReplicationManager} keeps a small,
 * count-capped hot cache on the heap for the freshest deltas and falls back here for the deep,
 * time-governed window, so a large backlog window costs disk (cheap, auto-compacted by segment
 * drop) instead of heap (the cause of the re-snapshot death spiral under load).
 *
 * <p>Mirrors {@link RelayStore}'s role/shape: lazy per-topic open, filesystem-safe directory names,
 * and a single {@link #close()} that releases every open log.
 */
final class ResendLogStore implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(ResendLogStore.class.getName());

    private final Path baseDir;
    private final int segmentMaxEntries;
    private final Duration segmentMaxAge;
    private final Duration retentionTime;
    private final long maxEntries;
    private final boolean fsync;
    private final Map<String, ResendLog> byTopic = new ConcurrentHashMap<>();

    ResendLogStore(Path baseDir, int segmentMaxEntries, Duration segmentMaxAge, Duration retentionTime,
            long maxEntries, boolean fsync) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir");
        this.segmentMaxEntries = segmentMaxEntries;
        this.segmentMaxAge = segmentMaxAge == null ? Duration.ZERO : segmentMaxAge;
        this.retentionTime = retentionTime == null ? Duration.ZERO : retentionTime;
        this.maxEntries = maxEntries;
        this.fsync = fsync;
    }

    /** Returns the resend log for {@code topic}, opening it lazily on first use (idempotent). */
    ResendLog logFor(String topic) {
        return byTopic.computeIfAbsent(topic, this::open);
    }

    private ResendLog open(String topic) {
        return new ResendLog(baseDir.resolve(dirName(topic)), segmentMaxEntries, segmentMaxAge, retentionTime,
                maxEntries, fsync);
    }

    /**
     * Maps a (possibly path-unsafe) topic such as {@code "queue:orders"} to a stable,
     * filesystem-safe directory name, disambiguated by the topic's hash so distinct topics that
     * sanitize to the same string still get separate directories. Mirrors {@code RelayStore.dirName}.
     */
    static String dirName(String topic) {
        String safe = topic.replaceAll("[^a-zA-Z0-9._-]", "_");
        return safe + "-" + Integer.toHexString(topic.hashCode());
    }

    /** Appends a committed operation to the topic's resend log; returns false if the append failed. */
    boolean append(String topic, long sequence, long timestamp, byte[] frame) {
        return logFor(topic).append(sequence, timestamp, frame);
    }

    /** Reads present entries with sequence in {@code [from, to]} for the topic, ascending. */
    List<RelayEntry> read(String topic, long from, long to) {
        return logFor(topic).read(from, to);
    }

    /** Current number of entries retained on disk for the topic (0 if none open). */
    long size(String topic) {
        ResendLog log = byTopic.get(topic);
        return log == null ? 0L : log.size();
    }

    @Override
    public void close() {
        for (Map.Entry<String, ResendLog> entry : byTopic.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to close resend log for topic " + entry.getKey(), e);
            }
        }
        byTopic.clear();
    }
}
