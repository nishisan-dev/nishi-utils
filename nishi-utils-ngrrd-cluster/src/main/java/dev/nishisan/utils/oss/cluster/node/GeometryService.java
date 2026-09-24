package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.GeometryUpdateRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;

import java.time.Clock;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Leader geometry updates and bounded, resumable publication of existing local series. */
public final class GeometryService extends RequestHandlerSupport implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(GeometryService.class.getName());
    private final CatalogView catalog;
    private final BlobVolume volume;
    private final ClusterRpc rpc;
    private final PlacementRequestHandler.LeaderView leader;
    private final SeriesHandleRegistry registry;
    private final String prefix;
    private final Clock clock;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "ngrrd-geometry-backfill");
        thread.setDaemon(true);
        return thread;
    });
    private Iterator<String> pending = List.<String>of().iterator();
    private volatile boolean closed;

    public GeometryService(Transport transport, CatalogView catalog, BlobVolume volume, ClusterRpc rpc,
            PlacementRequestHandler.LeaderView leader, SeriesHandleRegistry registry, String prefix, Clock clock) {
        super(transport, Set.of(Commands.GEOMETRY_UPDATE));
        this.catalog = catalog;
        this.volume = volume;
        this.rpc = rpc;
        this.leader = leader;
        this.registry = registry;
        this.prefix = prefix;
        this.clock = clock;
    }

    @Override
    protected Object handle(String command, Object body, NodeId source) {
        GeometryUpdateRequest request = (GeometryUpdateRequest) body;
        synchronized (catalog.placementLock(request.seriesKey())) {
            if (!leader.isLeader()) {
                return new SeriesStatusResponse(SeriesStatus.NOT_LEADER, leader.leaderId().orElse(null), "not leader");
            }
            var placement = catalog.placementStrong(request.seriesKey()).orElse(null);
            if (!source.value().equals(request.ownerNodeId()) || placement == null
                    || !placement.isOwnedBy(request.ownerNodeId())) {
                return new SeriesStatusResponse(SeriesStatus.WRONG_OWNER,
                        placement == null ? null : placement.ownerNodeId(), "geometry owner changed");
            }
            if (placement.state() != PlacementState.ACTIVE) {
                return new SeriesStatusResponse(SeriesStatus.MIGRATING, placement.ownerNodeId(), "geometry update during migration");
            }
            GeometryDescriptor geometry = request.geometry();
            if (geometry != null) { catalog.putGeometry(geometry); }
            if (!leader.isLeader()) {
                return new SeriesStatusResponse(SeriesStatus.NOT_LEADER, null, "leadership changed");
            }
            catalog.putPlacement(request.seriesKey(), placement.withGeometry(
                    geometry == null ? placement.geometryId() : geometry.id(), geometry != null, clock.millis()));
            return new SeriesStatusResponse(SeriesStatus.OK, placement.ownerNodeId(), null);
        }
    }

    /** Invalidates the old confirmation before an OPEN can create or resize an object. */
    public void beforeOpen(String seriesKey) { publish(seriesKey, null); }

    /** Confirms the physical layout, including when OPEN retained a previous geometry. */
    public void afterOpen(String seriesKey) {
        var section = volume.storage().seriesStaticSection(SeriesObjectKeys.objectKey(prefix, seriesKey))
                .orElseThrow(() -> new IllegalStateException("OPEN did not create a series image"));
        publish(seriesKey, GeometryDescriptor.fromStaticSection(section));
    }

    /** A routing/leadership change that OPEN must expose as a retryable protocol status. */
    public static final class PublicationException extends RuntimeException {
        private final SeriesStatusResponse response;
        PublicationException(SeriesStatusResponse response) { super(response.message()); this.response = response; }
        public SeriesStatusResponse response() { return response; }
    }

    private void publish(String seriesKey, GeometryDescriptor geometry) {
        NodeId target = rpc.leaderId().orElseThrow(() -> new PublicationException(
                new SeriesStatusResponse(SeriesStatus.NOT_LEADER, null, "no leader for geometry publication")));
        SeriesStatusResponse result;
        try {
            result = rpc.call(target, Commands.GEOMETRY_UPDATE,
                    new GeometryUpdateRequest(seriesKey, rpc.localId().value(), geometry), SeriesStatusResponse.class);
        } catch (dev.nishisan.utils.oss.cluster.api.NgrrdClusterException e) {
            throw new PublicationException(new SeriesStatusResponse(SeriesStatus.NOT_LEADER, null, e.getMessage()));
        }
        if (result.status() != SeriesStatus.OK) { throw new PublicationException(result); }
    }

    /** Starts a single bounded worker; every pass revisits references still unconfirmed. */
    public void start() { worker.schedule(this::runBatch, 1, TimeUnit.SECONDS); }

    private void runBatch() {
        if (closed) { return; }
        long delayMillis = 100;
        try {
            if (!pending.hasNext()) {
                pending = catalog.placementsLocal().entrySet().stream()
                        .filter(e -> e.getValue().isOwnedBy(rpc.localId().value())
                                && e.getValue().state() == PlacementState.ACTIVE && !e.getValue().geometryConfirmed())
                        .map(java.util.Map.Entry::getKey).sorted().toList().iterator();
            }
            for (int count = 0; count < 256 && pending.hasNext() && !closed; count++) {
                String key = pending.next();
                synchronized (registry.operationLock(key)) {
                    if (!registry.isMigrating(key)) {
                        try { afterOpen(key); }
                        catch (RuntimeException e) { LOG.log(Level.FINE, "Geometry backfill deferred for " + key, e); }
                    }
                }
            }
            if (!pending.hasNext()) { delayMillis = 10_000; }
        } catch (RuntimeException e) {
            LOG.log(Level.FINE, "Geometry backfill will retry", e);
            delayMillis = 10_000;
        } finally {
            if (!closed) { worker.schedule(this::runBatch, delayMillis, TimeUnit.MILLISECONDS); }
        }
    }

    @Override
    public void close() {
        closed = true;
        worker.shutdownNow();
        try { worker.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
