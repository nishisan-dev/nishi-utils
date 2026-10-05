package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.*;
import dev.nishisan.utils.oss.cluster.catalog.*;
import dev.nishisan.utils.oss.cluster.protocol.*;
import dev.nishisan.utils.oss.cluster.rpc.*;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.format.SeriesMetadataReader;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.*;

/** All methods touching a series execute under registry.operationLock, before the handle lock. */
public final class SeriesLifecycleService {
    private static final Logger LOG = Logger.getLogger(SeriesLifecycleService.class.getName());
    private final SeriesLifecycleJournal journal;
    private final BlobVolume volume;
    private final CatalogView catalog;
    private final SeriesHandleRegistry registry;
    private final String self, prefix;
    private final Clock clock;
    private final long interval;
    private final LongAdder deleted = new LongAdder();
    private final LongAdder receiptForces = new LongAdder();
    private final Map<DeleteStatus, LongAdder> outcomes = new java.util.concurrent.ConcurrentHashMap<>();

    public SeriesLifecycleService(SeriesLifecycleJournal journal, BlobVolume volume, CatalogView catalog,
            SeriesHandleRegistry registry, String self, String prefix, Clock clock, long interval) {
        if (interval <= 0) throw new IllegalArgumentException("receipt interval must be positive");
        this.journal = journal; this.volume = volume; this.catalog = catalog; this.registry = registry;
        this.self = self; this.prefix = prefix; this.clock = clock; this.interval = interval;
    }
    String objectKey(String key) { return SeriesObjectKeys.objectKey(prefix, key); }
    public SeriesLifecycleJournal journal() { return journal; }
    public long seriesDeleted() { return deleted.sum(); }
    public Map<DeleteStatus, Long> refusalCounts() {
        Map<DeleteStatus, Long> result = new EnumMap<>(DeleteStatus.class);
        outcomes.forEach((status, count) -> result.put(status, count.sum()));
        return Map.copyOf(result);
    }
    public Map<String, Long> metrics() {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("seriesDeleted", deleted.sum());
        outcomes.forEach((status, count) -> metrics.put("seriesDelete." + status.name(), count.sum()));
        metrics.put("receiptFsyncs", receiptForces.sum());
        metrics.put("lifecycleFsyncs", journal.fsyncCount());
        long count = 0, bytes = 0;
        for (var entry : journal.snapshot().entrySet()) {
            if (entry.getValue().phase() != QUARANTINED) continue;
            try (var guard = CoordinationLocks.acquire(registry.operationLock(entry.getKey()))) {
                var current = journal.get(entry.getKey());
                if (current.phase() == QUARANTINED && volume.storage().exists(objectKey(entry.getKey()))) {
                    count++;
                    try (var channel = volume.storage().openSeries(objectKey(entry.getKey()))) { bytes += channel.size(); }
                }
            }
        }
        metrics.put("quarantinedSeries", count);
        metrics.put("quarantinedBytes", bytes);
        return Map.copyOf(metrics);
    }

    public void record(DeleteStatus status) {
        if (status != DeleteStatus.DELETED && status != DeleteStatus.NOT_FOUND)
            outcomes.computeIfAbsent(status, ignored -> new LongAdder()).increment();
    }

    /** Never treats a failed strong read as an absent placement. */
    public SeriesInspectResponse inspect(String key) {
        try (var guard = CoordinationLocks.acquire(registry.operationLock(key))) {
            boolean exists = volume.storage().exists(objectKey(key));
            var entry = journal.get(key);
            var placement = catalog.placementStrong(key);
            if (exists && placement.isEmpty() && (entry == null || entry.phase() != COMMITTED)) {
                quarantine(key, entry);
                entry = journal.get(key);
            }
            if (exists && placement.isPresent() && entry != null && entry.phase() == ACTIVE
                    && !placement.get().generationId().equals(entry.generationId())) {
                quarantine(key, entry);
                entry = journal.get(key);
            }
            return new SeriesInspectResponse(exists, exists && entry != null && entry.phase() == QUARANTINED,
                    entry == null ? null : entry.generationId(), entry != null && (entry.phase() == PREPARED || entry.phase() == COMMITTED));
        }
    }

    public void quarantine(String key, SeriesLifecycleJournal.Entry old) {
        if (old != null && old.phase() == QUARANTINED) return;
        journal.put(key, new SeriesLifecycleJournal.Entry(old == null ? null : old.generationId(),
                old == null ? 0 : old.receivedThrough(), QUARANTINED, old == null ? null : old.placement()));
        LOG.warning("NGRRD_SERIES_QUARANTINED série=" + key + " nó=" + self
                + " motivo=NO_PLACEMENT ação='ngrrd-admin reconcile " + self + " --adopt'");
    }

    /** Local gate is cheap on the steady write path. Tokens fence delayed requests after recreation. */
    public SeriesStatus gate(String key, String generation) {
        var entry = journal.get(key);
        if (entry == null) return null;
        if (entry.phase() == QUARANTINED) return SeriesStatus.QUARANTINED;
        if (entry.phase() == PREPARED || entry.phase() == COMMITTED) return SeriesStatus.MIGRATING;
        if (generation != null && (entry.phase() == DELETED || entry.phase() == FINISHED || !generation.equals(entry.generationId())))
            return SeriesStatus.SERIES_DELETED;
        if (generation == null && (entry.phase() == DELETED || entry.phase() == FINISHED)) return SeriesStatus.SERIES_DELETED;
        return null;
    }

    public String ownerHint(String key) {
        return catalog.placementStrong(key).map(SeriesPlacement::ownerNodeId).orElse(null);
    }

    public String message(SeriesStatus status, String key) {
        return status == SeriesStatus.QUARANTINED
                ? "QUARANTINED série=" + key + " nó=" + self + "; execute ngrrd-admin reconcile " + self + " --adopt"
                : status + " série=" + key + " nó=" + self;
    }

    /** Explicit OPEN can bind a fresh generation, but may never adopt quarantined bytes. */
    public SeriesStatus beforeOpen(String key, String generation) {
        var current = catalog.placementStrong(key);
        var entry = journal.get(key);
        if (entry != null && entry.phase() == QUARANTINED) return SeriesStatus.QUARANTINED;
        if (current.isEmpty()) {
            if (volume.storage().exists(objectKey(key))) { quarantine(key, entry); return SeriesStatus.QUARANTINED; }
            return generation != null ? SeriesStatus.SERIES_DELETED : SeriesStatus.WRONG_OWNER;
        }
        var placement = current.get();
        if (placement.deletion() != null || (entry != null && (entry.phase() == COMMITTED || entry.phase() == PREPARED)))
            return SeriesStatus.MIGRATING;
        if (generation != null && !generation.equals(placement.generationId())) return SeriesStatus.SERIES_DELETED;
        if (entry != null && !placement.generationId().equals(entry.generationId())
                && volume.storage().exists(objectKey(key))) {
            quarantine(key, entry);
            return SeriesStatus.QUARANTINED;
        }
        if (!placement.isOwnedBy(self)) return SeriesStatus.WRONG_OWNER;
        if (entry != null && (entry.phase() == DELETED || entry.phase() == FINISHED) && placement.generationId().equals(entry.generationId()))
            return SeriesStatus.SERIES_DELETED;
        if (entry == null || !placement.generationId().equals(entry.generationId()) || (entry.phase() == DELETED || entry.phase() == FINISHED))
            initialize(key, placement);
        return null;
    }

    private SeriesLifecycleJournal.Entry initialEntry(String key, SeriesPlacement placement) {
        long timestamp = clock.millis();
        if (volume.storage().exists(objectKey(key))) {
            try (var channel = volume.storage().openSeries(objectKey(key))) {
                long persisted = SeriesMetadataReader.lastUpdate(channel);
                if (persisted > 0) timestamp = persisted;
            }
        }
        var old = journal.get(key);
        long upper = SeriesLifecycleJournal.upperBound(timestamp, interval);
        if (old != null && old.phase() == QUARANTINED) upper = Math.max(upper, old.receivedThrough());
        return new SeriesLifecycleJournal.Entry(placement.generationId(),
                upper, ACTIVE, null);
    }
    public void initialize(String key, SeriesPlacement placement) { journal.put(key, initialEntry(key, placement)); }

    /** Caller holds the sorted operation locks of all series in this batch. */
    public void receipts(Collection<String> keys) {
        Map<String, SeriesLifecycleJournal.Entry> updates = new LinkedHashMap<>();
        long upper = SeriesLifecycleJournal.upperBound(clock.millis(), interval);
        for (String key : keys) {
            var entry = journal.get(key);
            if (entry == null) {
                var placement = catalog.placementStrong(key).orElseThrow(() -> new IllegalStateException("no placement: " + key));
                entry = initialEntry(key, placement);
            }
            if (entry.phase() != ACTIVE) throw new IllegalStateException("series not active: " + key);
            if (upper > entry.receivedThrough() || journal.get(key) == null)
                updates.put(key, new SeriesLifecycleJournal.Entry(entry.generationId(), Math.max(upper, entry.receivedThrough()), ACTIVE, null));
        }
        journal.putAll(updates);
        if (!updates.isEmpty()) receiptForces.increment();
    }

    public DeleteResult prepare(DeleteControlRequest request) {
        String key = request.seriesKey(); var placement = request.placement();
        try (var guard = CoordinationLocks.acquire(registry.operationLock(key))) {
            verifyReservation(request, false);
            if (registry.isMigrating(key) || registry.isCopying(key)) return DeleteResult.of(DeleteStatus.REFUSED_MIGRATING, placement.ownerNodeId());
            var entry = journal.get(key);
            if (entry != null && entry.phase() == QUARANTINED)
                return DeleteResult.error(ErrorCode.QUARANTINED, message(SeriesStatus.QUARANTINED, key));
            if (entry != null && entry.phase() == DELETED && entry.generationId().equals(placement.generationId()))
                return DeleteResult.of(DeleteStatus.DELETED, placement.ownerNodeId());
            if (entry != null && entry.phase() == ACTIVE && !placement.generationId().equals(entry.generationId())
                    && volume.storage().exists(objectKey(key))) {
                quarantine(key, entry);
                return DeleteResult.error(ErrorCode.QUARANTINED, "catalog generation differs from durable volume: " + key);
            }
            if (entry == null || !placement.generationId().equals(entry.generationId())) {
                entry = initialEntry(key, placement);
            }
            if (entry.phase() == COMMITTED || entry.phase() == DELETED || entry.phase() == FINISHED)
                return DeleteResult.of(DeleteStatus.DELETED, placement.ownerNodeId());
            if (placement.isOwnedBy(self) && entry.receivedThrough() >= placement.deletion().lastWriteBefore()) {
                return new DeleteResult(DeleteStatus.REFUSED_RECENT_WRITE, self, entry.receivedThrough(), null, null);
            }
            journal.put(key, new SeriesLifecycleJournal.Entry(placement.generationId(), entry.receivedThrough(), PREPARED, placement));
            registry.invalidateOwnership(key);
            return DeleteResult.of(DeleteStatus.DELETED, placement.ownerNodeId());
        }
    }

    private void verifyReservation(DeleteControlRequest request, boolean requireCommit) {
        var proposed = request.placement();
        var current = catalog.placementStrong(request.seriesKey()).orElseThrow(() -> new IllegalStateException("deletion reservation missing"));
        if (!current.generationId().equals(proposed.generationId()) || current.deletion() == null
                || !current.deletion().operationId().equals(proposed.deletion().operationId())
                || (requireCommit && !current.deletion().committed()))
            throw new IllegalStateException("stale deletion operation");
    }

    public DeleteResult commit(DeleteControlRequest request) {
        try (var guard = CoordinationLocks.acquire(registry.operationLock(request.seriesKey()))) {
            verifyReservation(request, true);
            var entry = journal.get(request.seriesKey());
            if (entry == null || !request.placement().generationId().equals(entry.generationId()))
                throw new IllegalStateException("local deletion preparation missing");
            if (entry.phase() != DELETED && entry.phase() != FINISHED)
                journal.put(request.seriesKey(), new SeriesLifecycleJournal.Entry(entry.generationId(), entry.receivedThrough(), COMMITTED, request.placement()));
            return DeleteResult.of(DeleteStatus.DELETED, request.placement().ownerNodeId());
        }
    }

    public DeleteResult apply(DeleteControlRequest request) {
        String key = request.seriesKey();
        try (var guard = CoordinationLocks.acquire(registry.operationLock(key))) {
            verifyReservation(request, true);
            var entry = journal.get(key);
            if (entry == null || !request.placement().generationId().equals(entry.generationId())
                    || (entry.phase() != COMMITTED && entry.phase() != DELETED && entry.phase() != FINISHED))
                throw new IllegalStateException("local deletion commit missing");
            if (entry.phase() != DELETED && entry.phase() != FINISHED) {
                registry.closeForDeletion(key);
                volume.storage().delete(objectKey(key));
                if (volume.storage().exists(objectKey(key))) throw new IllegalStateException("series still exists after delete");
                journal.put(key, new SeriesLifecycleJournal.Entry(entry.generationId(), entry.receivedThrough(), DELETED, request.placement()));
                if (request.placement().isOwnedBy(self)) {
                    deleted.increment();
                    LOG.info("NGRRD_SERIES_DELETED série=" + key + " dono=" + self);
                }
            }
            registry.invalidateOwnership(key);
            return DeleteResult.of(DeleteStatus.DELETED, request.placement().ownerNodeId());
        }
    }

    public DeleteResult abort(DeleteControlRequest request) {
        String key = request.seriesKey();
        try (var guard = CoordinationLocks.acquire(registry.operationLock(key))) {
            var entry = journal.get(key);
            if (entry == null || entry.phase() != PREPARED || !entry.generationId().equals(request.placement().generationId())
                    || !entry.placement().deletion().operationId().equals(request.placement().deletion().operationId()))
                return DeleteResult.of(DeleteStatus.NOT_FOUND, self);
            var current = catalog.placementStrong(key);
            if (current.isPresent() && current.get().deletion() != null) throw new IllegalStateException("reservation not cancelled");
            journal.put(key, new SeriesLifecycleJournal.Entry(entry.generationId(), entry.receivedThrough(), ACTIVE, null));
            registry.invalidateOwnership(key);
            return DeleteResult.of(DeleteStatus.NOT_FOUND, self);
        }
    }

    public DeleteResult finish(DeleteControlRequest request) {
        String key = request.seriesKey();
        try (var guard = CoordinationLocks.acquire(registry.operationLock(key))) {
            var entry = journal.get(key);
            if (entry != null && entry.phase() == DELETED && entry.generationId().equals(request.placement().generationId())
                    && entry.placement() != null && entry.placement().deletion() != null
                    && entry.placement().deletion().operationId().equals(request.placement().deletion().operationId()))
                journal.put(key, new SeriesLifecycleJournal.Entry(entry.generationId(), entry.receivedThrough(), FINISHED, entry.placement()));
            registry.invalidateOwnership(key);
            return DeleteResult.of(DeleteStatus.DELETED, self);
        }
    }

    public ReconcileResponse reconcile(ReconcileRequest request, ClusterRpc rpc) {
        Map<String, String> results = new LinkedHashMap<>(); long bytes = 0;
        for (String object : volume.storage().list(SeriesObjectKeys.prefixWithSlash(prefix))) {
            var keyOpt = SeriesObjectKeys.seriesKeyOf(object, prefix);
            if (keyOpt.isEmpty() || (request.seriesKey() != null && !request.seriesKey().equals(keyOpt.get()))) continue;
            String key = keyOpt.get();
            try (var guard = CoordinationLocks.acquire(registry.operationLock(key))) {
                if (!inspect(key).quarantined()) continue;
                try (var channel = volume.storage().openSeries(object)) { bytes += channel.size(); }
                if (request.action() == ReconcileRequest.Action.REPORT) { results.put(key, "QUARANTINED"); continue; }
                var current = catalog.placementStrong(key);
                if (request.action() == ReconcileRequest.Action.ADOPT && current.isPresent()
                        && current.get().isOwnedBy(self) && current.get().state() == PlacementState.ACTIVE
                        && current.get().deletion() == null && !registry.isMigrating(key) && !registry.isCopying(key)) {
                    initialize(key, current.get());
                    registry.invalidateOwnership(key);
                    results.put(key, "ADOPTED");
                    continue;
                }
                boolean confirmedOtherCopy = false;
                if (request.action() == ReconcileRequest.Action.PURGE && current.isPresent()
                        && !current.get().isOwnedBy(self) && current.get().state() == PlacementState.ACTIVE
                        && current.get().deletion() == null) {
                    confirmedOtherCopy = rpc.call(NodeId.of(current.get().ownerNodeId()), Commands.SERIES_EXISTS,
                            new SeriesExistsRequest(key), SeriesExistsResponse.class).exists();
                    // A failed or changed authoritative placement never authorizes local collection.
                    confirmedOtherCopy &= current.equals(catalog.placementStrong(key));
                }
                if ((current.isPresent() && !confirmedOtherCopy) || registry.isMigrating(key) || registry.isCopying(key)) {
                    results.put(key, "REFUSED_PLACED_OR_MIGRATING"); continue;
                }
                if (request.action() == ReconcileRequest.Action.PURGE) {
                    registry.closeForDeletion(key);
                    volume.storage().delete(object);
                    if (volume.storage().exists(object)) throw new IllegalStateException("orphan still exists");
                    var old = journal.get(key);
                    journal.put(key, new SeriesLifecycleJournal.Entry(old.generationId(), old.receivedThrough(), DELETED, null));
                    results.put(key, "PURGED");
                } else {
                    NodeId leader = rpc.leaderId().orElseThrow(() -> new IllegalStateException("no leader"));
                    PlaceResponse placed = rpc.call(leader, Commands.PLACE,
                            new PlaceRequest(key, null, self, null, null, true), PlaceResponse.class);
                    if (placed.status() != SeriesStatus.OK || !placed.placement().isOwnedBy(self)) {
                        results.put(key, "REFUSED_PLACEMENT"); continue;
                    }
                    initialize(key, placed.placement());
                    registry.invalidateOwnership(key);
                    results.put(key, "ADOPTED");
                }
            } catch (RuntimeException e) { results.put(key, "ERROR: " + e.getMessage()); }
        }
        return new ReconcileResponse(results, bytes);
    }
}
