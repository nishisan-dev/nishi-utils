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

import dev.nishisan.utils.queue.NQueue;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Owns the follower relay-log: one durable {@code NQueue<byte[]>} per topic, each
 * holding {@link RelayEntryCodec}-encoded frames in arrival order (#124).
 *
 * <p>Every relay queue is opened with the durability invariants the replay model
 * requires:
 * <ul>
 * <li>{@code shortCircuit=false} — no in-RAM hand-off that bypasses persistence;</li>
 * <li>{@code memoryBuffer=false} — {@code peek} reads the durable log, not the stager;</li>
 * <li>{@code TIME_BASED} retention with {@code retentionClampToConsumer=true} — old
 * entries are reclaimed, but never ahead of the apply cursor (an over-retention backlog
 * is surfaced to the consumer and resolved by bootstrap, not silently dropped);</li>
 * <li>{@code fsync=false} — sustains the leader's throughput; the small loss window of
 * not-yet-fsynced tail entries is recovered by the leader's resend op-log (#122).</li>
 * </ul>
 */
final class RelayStore implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(RelayStore.class.getName());

    /** Marker file written on a clean shutdown; its absence on start signals a crash. */
    static final String CLEAN_MARKER = ".clean-shutdown";

    /**
     * True when {@code relayDir} holds state from a prior run but no clean-shutdown marker —
     * i.e. the previous shutdown was a crash, so the coalesced apply frontier may be stale and the
     * follower must bootstrap rather than risk re-applying (duplicating) the non-idempotent queue.
     */
    static boolean isUncleanRestart(Path relayDir) {
        return Files.exists(relayDir) && !Files.exists(relayDir.resolve(CLEAN_MARKER));
    }

    /** Removes the clean-shutdown marker on startup; from now until the next clean stop we are dirty. */
    static void consumeCleanMarker(Path relayDir) {
        try {
            Files.deleteIfExists(relayDir.resolve(CLEAN_MARKER));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to clear relay clean-shutdown marker", e);
        }
    }

    /** Writes the clean-shutdown marker on graceful stop, after the apply frontier is flushed. */
    static void writeCleanMarker(Path relayDir) {
        try {
            Files.createDirectories(relayDir);
            Files.write(relayDir.resolve(CLEAN_MARKER), new byte[0]);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write relay clean-shutdown marker", e);
        }
    }

    private final Path baseDir;
    private final Duration retention;
    private final RelayDurability durability;
    private final Map<String, NQueue<byte[]>> byTopic = new ConcurrentHashMap<>();
    private final ScheduledExecutorService syncExecutor;

    RelayStore(Path baseDir, Duration retention) {
        this(baseDir, retention, RelayDurability.OS_MANAGED, Duration.ofSeconds(1));
    }

    RelayStore(Path baseDir, Duration retention, RelayDurability durability, Duration groupCommitInterval) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir");
        this.retention = retention == null ? Duration.ZERO : retention;
        this.durability = durability == null ? RelayDurability.OS_MANAGED : durability;
        if (this.durability == RelayDurability.GROUP_COMMIT) {
            long intervalMs = Math.max(1L, (groupCommitInterval == null ? Duration.ofSeconds(1) : groupCommitInterval)
                    .toMillis());
            this.syncExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ngrid-relay-sync");
                t.setDaemon(true);
                return t;
            });
            this.syncExecutor.scheduleWithFixedDelay(this::syncAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        } else {
            this.syncExecutor = null;
        }
    }

    /**
     * Returns the relay queue for {@code topic}, opening it lazily on first use. Idempotent:
     * subsequent calls for the same topic return the same instance.
     */
    NQueue<byte[]> relayFor(String topic) {
        return byTopic.computeIfAbsent(topic, this::open);
    }

    private NQueue<byte[]> open(String topic) {
        try {
            NQueue.Options options = NQueue.Options.defaults()
                    .withFsync(durability == RelayDurability.ALWAYS)
                    .withShortCircuit(false)
                    .withMemoryBuffer(false)
                    .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                    .withRetentionTime(retention)
                    .withRetentionClampToConsumer(true)
                    .withCompactionInterval(Duration.ofSeconds(30));
            return NQueue.open(baseDir, dirName(topic), options);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open relay for topic " + topic, e);
        }
    }

    /**
     * Maps a (possibly path-unsafe) topic such as {@code "queue:orders"} to a stable,
     * filesystem-safe directory name, disambiguated by the topic's hash so distinct topics
     * that sanitize to the same string still get separate directories.
     */
    static String dirName(String topic) {
        String safe = topic.replaceAll("[^a-zA-Z0-9._-]", "_");
        return safe + "-" + Integer.toHexString(topic.hashCode());
    }

    /** Forces all open relays to disk (group-commit tick). */
    private void syncAll() {
        for (Map.Entry<String, NQueue<byte[]>> entry : byTopic.entrySet()) {
            try {
                entry.getValue().sync();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Relay group-commit sync failed for topic " + entry.getKey(), e);
            }
        }
    }

    @Override
    public void close() {
        if (syncExecutor != null) {
            syncExecutor.shutdownNow();
        }
        for (Map.Entry<String, NQueue<byte[]>> entry : byTopic.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to close relay for topic " + entry.getKey(), e);
            }
        }
        byTopic.clear();
    }
}
