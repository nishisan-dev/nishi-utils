package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.*;
import dev.nishisan.utils.oss.cluster.catalog.*;
import dev.nishisan.utils.oss.cluster.protocol.*;
import dev.nishisan.utils.oss.cluster.rpc.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.logging.*;
import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.*;

/** Leader coordinates prepare/commit/apply; local phases are durable and generation fenced. */
public final class SeriesDeleteHandler extends RequestHandlerSupport implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(SeriesDeleteHandler.class.getName());
    private final CatalogView catalog;
    private final ClusterRpc rpc;
    private final SeriesLifecycleService lifecycle;
    private final BooleanSupplier leader, synced;
    private final Clock clock;
    private final Object[] driveLocks = java.util.stream.IntStream.range(0, 256).mapToObj(i -> new Object()).toArray();
    private final ScheduledExecutorService recovery = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "ngrrd-series-deletion-recovery"); thread.setDaemon(true); return thread;
    });
    private volatile boolean closed;

    public SeriesDeleteHandler(Transport transport, CatalogView catalog, ClusterRpc rpc,
            SeriesLifecycleService lifecycle, BooleanSupplier leader, BooleanSupplier synced, Clock clock) {
        super(transport, Set.of(Commands.SERIES_DELETE, Commands.SERIES_DELETE_BATCH, Commands.DELETE_PREPARE,
                Commands.DELETE_COMMIT, Commands.DELETE_APPLY, Commands.DELETE_ABORT, Commands.DELETE_FINISH,
                Commands.DELETE_RECOVER, Commands.SERIES_INSPECT, Commands.RECONCILE));
        this.catalog = catalog; this.rpc = rpc; this.lifecycle = lifecycle;
        this.leader = leader; this.synced = synced; this.clock = clock;
    }
    public void start() { recovery.scheduleWithFixedDelay(this::recover, 1, 1, TimeUnit.SECONDS); }

    @Override protected Object handle(String command, Object body, NodeId source) {
        return switch (command) {
            case Commands.SERIES_DELETE -> delete((DeleteRequest) body);
            case Commands.SERIES_DELETE_BATCH -> {
                Map<String, DeleteResult> results = new LinkedHashMap<>();
                for (var request : ((DeleteBatchRequest) body).requests()) results.put(request.seriesKey(), delete(request));
                yield new DeleteBatchResponse(results);
            }
            case Commands.DELETE_PREPARE -> lifecycle.prepare((DeleteControlRequest) body);
            case Commands.DELETE_COMMIT -> lifecycle.commit((DeleteControlRequest) body);
            case Commands.DELETE_APPLY -> lifecycle.apply((DeleteControlRequest) body);
            case Commands.DELETE_ABORT -> lifecycle.abort((DeleteControlRequest) body);
            case Commands.DELETE_FINISH -> lifecycle.finish((DeleteControlRequest) body);
            case Commands.DELETE_RECOVER -> recoverReservation((DeleteControlRequest) body);
            case Commands.SERIES_INSPECT -> lifecycle.inspect(((SeriesCommandRequest) body).seriesKey());
            case Commands.RECONCILE -> lifecycle.reconcile((ReconcileRequest) body, rpc);
            default -> throw new IllegalArgumentException(command);
        };
    }

    /** Protect creation against unplaced surviving data anywhere in the registered cluster. */
    public SeriesStatus creationGate(String key) {
        for (var node : catalog.nodesLocal()) {
            if (!node.capabilities().contains(StorageCapabilities.SERIES_DELETE)) continue; // legacy placement during rolling upgrade
            var inspected = rpc.call(NodeId.of(node.nodeId()), Commands.SERIES_INSPECT,
                    new SeriesCommandRequest(key), SeriesInspectResponse.class);
            if (inspected.quarantined()) return SeriesStatus.QUARANTINED;
            if (inspected.deleting()) return SeriesStatus.MIGRATING;
        }
        return null;
    }

    public DeleteResult delete(DeleteRequest request) {
        String key = request.seriesKey();
        try (var drive = CoordinationLocks.acquire(driveLocks[Math.floorMod(key.hashCode(), driveLocks.length)])) {
            ensureLeader();
            SeriesPlacement placement;
            try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                ensureLeader();
                var current = catalog.placementStrong(key);
                if (current.isEmpty()) return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                placement = current.get();
                if (request.generationId() != null && !request.generationId().equals(placement.generationId()))
                    return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                if (placement.state() == PlacementState.MIGRATING) return refusal(DeleteStatus.REFUSED_MIGRATING, placement.ownerNodeId());
                if (placement.deletion() == null) {
                    List<String> participants = new ArrayList<>();
                    for (var node : catalog.nodesLocal()) {
                        var status = catalog.nodeStatusStrong(node.nodeId()).orElseThrow(() -> new IllegalStateException("missing status: " + node.nodeId()));
                        if (!status.capabilities().contains(StorageCapabilities.SERIES_DELETE)) {
                            lifecycle.record(DeleteStatus.ERROR);
                            return DeleteResult.error(ErrorCode.UNSUPPORTED_BY_NODE, node.nodeId() + " does not announce series.delete");
                        }
                        participants.add(node.nodeId());
                    }
                    if (!participants.contains(placement.ownerNodeId()))
                        return DeleteResult.error(ErrorCode.UNSUPPORTED_BY_NODE, "owner status unavailable: " + placement.ownerNodeId());
                    participants.sort(String::compareTo);
                    placement = placement.withDeletion(new SeriesDeletion(request.operationId(), request.precondition().lastWriteBefore(), participants, false), clock.millis());
                    catalog.putPlacement(key, placement);
                }
            }
            return drive(key, placement);
        } catch (RuntimeException e) {
            lifecycle.record(DeleteStatus.ERROR);
            return DeleteResult.error(e instanceof NgrrdClusterException n ? n.code() : ErrorCode.REMOTE_ERROR, e.getMessage());
        }
    }

    private DeleteResult refusal(DeleteStatus status, String owner) {
        lifecycle.record(status); return DeleteResult.of(status, owner);
    }
    private void ensureLeader() {
        if (closed || !leader.getAsBoolean() || !synced.getAsBoolean())
            throw new NgrrdClusterException(ErrorCode.NO_LEADER, "deletion requires the synchronized current leader");
    }
    private DeleteResult phase(String node, String command, String key, SeriesPlacement placement) {
        ensureLeader();
        return rpc.call(NodeId.of(node), command, new DeleteControlRequest(key, placement), DeleteResult.class);
    }

    private DeleteResult drive(String key, SeriesPlacement placement) {
        if (!placement.deletion().committed()) {
            try {
                for (String node : placement.deletion().participants()) {
                    DeleteResult response = phase(node, Commands.DELETE_PREPARE, key, placement);
                    if (response.status() != DeleteStatus.DELETED) {
                        cancel(key, placement);
                        lifecycle.record(response.status());
                        return response;
                    }
                }
                try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                    ensureLeader();
                    var current = catalog.placementStrong(key).orElseThrow();
                    requireSame(current, placement);
                    placement = current.withDeletion(current.deletion().commit(), clock.millis());
                    catalog.putPlacement(key, placement);
                }
            } catch (RuntimeException e) {
                // Cancellation checks the authoritative phase: a lost commit response is never rolled back.
                try { cancel(key, placement); } catch (RuntimeException ignored) { }
                throw e;
            }
        }
        for (String node : placement.deletion().participants()) phase(node, Commands.DELETE_COMMIT, key, placement);
        for (String node : placement.deletion().participants()) phase(node, Commands.DELETE_APPLY, key, placement);
        try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
            ensureLeader();
            var current = catalog.placementStrong(key);
            if (current.isPresent()) {
                requireSame(current.get(), placement);
                catalog.removePlacement(key);
            }
        }
        // Finish changes only the matching local tombstone. A fresh generation is never touched.
        for (String node : placement.deletion().participants()) {
            try { phase(node, Commands.DELETE_FINISH, key, placement); }
            catch (RuntimeException e) { LOG.log(Level.FINE, "tombstone finalization will retry: " + key, e); }
        }
        return DeleteResult.of(DeleteStatus.DELETED, placement.ownerNodeId());
    }

    private void requireSame(SeriesPlacement current, SeriesPlacement expected) {
        if (!current.generationId().equals(expected.generationId()) || current.deletion() == null
                || !current.deletion().operationId().equals(expected.deletion().operationId()))
            throw new IllegalStateException("stale deletion reservation");
    }
    private void cancel(String key, SeriesPlacement placement) {
        try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
            ensureLeader();
            var current = catalog.placementStrong(key);
            if (current.isPresent()) {
                requireSame(current.get(), placement);
                if (current.get().deletion().committed()) return;
                catalog.putPlacement(key, current.get().withDeletion(null, clock.millis()));
            }
        }
        for (String node : placement.deletion().participants()) {
            try { phase(node, Commands.DELETE_ABORT, key, placement); }
            catch (RuntimeException e) { LOG.log(Level.FINE, "preparation cancellation will retry: " + key, e); }
        }
    }

    private DeleteResult recoverReservation(DeleteControlRequest request) {
        String key = request.seriesKey();
        try (var drive = CoordinationLocks.acquire(driveLocks[Math.floorMod(key.hashCode(), driveLocks.length)])) {
            ensureLeader();
            SeriesPlacement placement;
            try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                ensureLeader();
                var current = catalog.placementStrong(key);
                if (current.isPresent() && !current.get().generationId().equals(request.placement().generationId()))
                    return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                placement = request.placement();
                if (current.isPresent() && current.get().deletion() != null) {
                    requireSame(current.get(), placement);
                    placement = current.get();
                } else {
                    if (placement.deletion() == null || !placement.deletion().committed())
                        return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                    catalog.putPlacement(key, placement);
                }
            }
            return drive(key, placement);
        }
    }

    private void recover() {
        if (closed || !synced.getAsBoolean()) return;
        try {
            for (var local : lifecycle.journal().snapshot().entrySet()) {
                var entry = local.getValue(); String key = local.getKey();
                if (entry.placement() == null || entry.placement().deletion() == null) continue;
                var current = catalog.placementStrong(key);
                if (entry.phase() == PREPARED && (current.isEmpty() || current.get().deletion() == null)) {
                    lifecycle.abort(new DeleteControlRequest(key, entry.placement()));
                } else if ((entry.phase() == COMMITTED || entry.phase() == DELETED || entry.phase() == FINISHED)
                        && (current.isPresent() || entry.phase() != FINISHED)) {
                    NodeId target = rpc.leaderId().orElse(null);
                    if (target != null) rpc.call(target, Commands.DELETE_RECOVER,
                            new DeleteControlRequest(key, entry.placement()), DeleteResult.class);
                }
            }
            if (leader.getAsBoolean()) {
                for (var item : catalog.placementsLocal().entrySet()) {
                    if (item.getValue().deletion() != null) {
                        delete(new DeleteRequest(item.getKey(), new DeletePrecondition(item.getValue().deletion().lastWriteBefore()),
                                item.getValue().deletion().operationId()));
                    }
                }
            }
        } catch (RuntimeException e) { LOG.log(Level.FINE, "series deletion recovery deferred", e); }
    }

    @Override public void close() {
        closed = true; recovery.shutdownNow();
        try { recovery.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
