package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.blob.*;
import dev.nishisan.utils.oss.cluster.api.*;
import dev.nishisan.utils.oss.cluster.catalog.*;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.protocol.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SeriesLifecycleTest {
    @TempDir Path base;
    static final long HOUR = 3_600_000;
    final Map<String, SeriesPlacement> placements = new HashMap<>();
    final Map<String, StorageNodeStatus> nodes = new HashMap<>();
    CatalogView catalog = (CatalogView) Proxy.newProxyInstance(CatalogView.class.getClassLoader(), new Class<?>[]{CatalogView.class}, (proxy, method, args) -> switch (method.getName()) {
        case "placementStrong", "placementLocal" -> Optional.ofNullable(placements.get(args[0]));
        case "placementsLocal" -> Map.copyOf(placements);
        case "nodesLocal" -> List.copyOf(nodes.values());
        case "nodeStatusStrong" -> Optional.ofNullable(nodes.get(args[0]));
        case "putPlacement" -> { placements.put((String) args[0], (SeriesPlacement) args[1]); yield null; }
        case "removePlacement" -> { placements.remove(args[0]); yield null; }
        case "placementLock" -> placements;
        default -> throw new UnsupportedOperationException(method.getName());
    });
    BlobVolumeRegistry volumes;
    BlobVolume volume;
    SeriesHandleRegistry handles;
    SeriesLifecycleJournal journal;
    SeriesLifecycleService service;
    String yaml;
    long now = Instant.parse("2026-10-04T10:05:00Z").toEpochMilli();
    Clock clock = new Clock() {
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return Instant.ofEpochMilli(now); }
    };
    @BeforeEach void setup() throws Exception {
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));
        volumes = NgrrdBlob.registry().basePath(base).volume("ngrrd").build();
        volume = volumes.require("ngrrd");
        handles = new SeriesHandleRegistry(volume, "ngrrd", Duration.ofMinutes(15), 1000, clock);
        journal = new SeriesLifecycleJournal(base.resolve("lifecycle"));
        service = new SeriesLifecycleService(journal, volume, catalog, handles, "owner", "series", clock, HOUR);
        handles.lifecycle(service);
    }
    @AfterEach void cleanup() { handles.close(); volumes.close(); }
    void legacy(String key, long timestamp) {
        placements.put(key, SeriesPlacement.active("owner", now - HOUR));
        var h = handles.open(key, yaml, Ngrrd.OpenOptions.defaults());
        if (timestamp > 0) { h.write("in_octets", new Sample(timestamp, 42)); h.checkpoint(); }
        handles.close(key);
    }
    @Test void legacyTimestampAllowsImmediatePurgeAndEmptyFallsBackToNow() {
        long old = now - Duration.ofDays(40).toMillis();
        legacy("old", old); legacy("empty", 0);
        service.initialize("old", placements.get("old")); service.initialize("empty", placements.get("empty"));
        assertEquals(SeriesLifecycleJournal.upperBound(old, HOUR), journal.get("old").receivedThrough());
        assertEquals(SeriesLifecycleJournal.upperBound(now, HOUR), journal.get("empty").receivedThrough());
        var p = placements.get("old").withDeletion(new SeriesDeletion("op", now - Duration.ofDays(30).toMillis(), List.of("owner"), false), now);
        placements.put("old", p);
        assertEquals(DeleteStatus.DELETED, service.prepare(new DeleteControlRequest("old", p)).status());
        p = p.withDeletion(p.deletion().commit(), now); placements.put("old", p);
        service.commit(new DeleteControlRequest("old", p)); service.apply(new DeleteControlRequest("old", p));
        assertFalse(volume.storage().exists("series/old.ngrr"));
    }
    @Test void receiptUpperBoundsUseOneFsyncPerBatchAndNoneWithinHour() {
        for (String key : List.of("a", "b", "c")) { legacy(key, now - HOUR); service.initialize(key, placements.get(key)); }
        long start = journal.fsyncCount();
        var keys = List.of("a", "b", "c");
        service.receipts(keys);
        assertEquals(start + 1, journal.fsyncCount());
        long upper = journal.get("a").receivedThrough();
        now = upper;
        for (int i = 0; i < 1000; i++) service.receipts(keys);
        assertEquals(start + 1, journal.fsyncCount());
        now++;
        service.receipts(keys);
        assertEquals(start + 2, journal.fsyncCount());
        assertEquals(upper + HOUR, journal.get("a").receivedThrough());
        var p = placements.get("a").withDeletion(new SeriesDeletion("recent", now, keys, false), now);
        placements.put("a", p);
        assertEquals(DeleteStatus.REFUSED_RECENT_WRITE, service.prepare(new DeleteControlRequest("a", p)).status());
        assertEquals(SeriesLifecycleJournal.Phase.ACTIVE, journal.get("a").phase());
    }
    @Test void corruptedLegacyMetadataBlocksDeletion() {
        legacy("broken", now - Duration.ofDays(40).toMillis());
        try (var channel = volume.storage().openSeries("series/broken.ngrr")) {
            channel.writeRegion(0, new byte[96]); channel.force();
        }
        var p = placements.get("broken").withDeletion(new SeriesDeletion("op", now, List.of("owner"), false), now);
        placements.put("broken", p);
        assertThrows(RuntimeException.class, () -> service.prepare(new DeleteControlRequest("broken", p)));
        assertTrue(volume.storage().exists("series/broken.ngrr"));
        assertNull(journal.get("broken"));
    }
    @Test void quarantinePersistsAndOpenAndWritesExposeItsCause() throws Exception {
        legacy("orphan", now - HOUR); placements.clear();
        assertTrue(service.inspect("orphan").quarantined());
        assertEquals(SeriesStatus.QUARANTINED, service.beforeOpen("orphan", null));
        assertEquals(SeriesStatus.QUARANTINED, service.gate("orphan", null));
        assertEquals(1L, service.metrics().get("quarantinedSeries"));
        journal.close();
        journal = new SeriesLifecycleJournal(base.resolve("lifecycle"));
        assertEquals(SeriesLifecycleJournal.Phase.QUARANTINED, journal.get("orphan").phase());
        journal.close();
    }
    @Test void journalReplaysDurableBatchTruncatesTornTailAndRejectsCorruption() throws Exception {
        var entry = new SeriesLifecycleJournal.Entry("g", now, SeriesLifecycleJournal.Phase.ACTIVE, null);
        journal.putAll(Map.of("a", entry, "b", entry));
        assertEquals(1, journal.fsyncCount());
        journal.putAll(Map.of("a", entry, "b", entry)); assertEquals(1, journal.fsyncCount());
        journal.close();
        Path path = base.resolve("lifecycle/series-lifecycle.wal");
        long durableLength = Files.size(path);
        Files.write(path, new byte[]{0,0,0}, StandardOpenOption.APPEND);
        journal = new SeriesLifecycleJournal(path.getParent());
        assertEquals(entry, journal.get("b")); assertEquals(durableLength, Files.size(path));
        journal.close();
        byte[] bytes = Files.readAllBytes(path); bytes[bytes.length - 1] ^= 1; Files.write(path, bytes);
        assertThrows(IOException.class, () -> new SeriesLifecycleJournal(path.getParent()));
    }
    @Test void failedPersistenceNeverAdmitsAnAdvancedReceipt() throws Exception {
        legacy("s", now - HOUR); service.initialize("s", placements.get("s"));
        long before = journal.get("s").receivedThrough(); journal.close();
        assertThrows(RuntimeException.class, () -> service.receipts(List.of("s")));
        assertEquals(before, journal.get("s").receivedThrough());
        assertThrows(IllegalStateException.class, () -> service.receipts(List.of("s")));
    }
    @Test void ceilingDoesNotOverflow() {
        assertEquals(Long.MAX_VALUE, SeriesLifecycleJournal.upperBound(Long.MAX_VALUE - 1, HOUR));
        assertEquals(HOUR, SeriesLifecycleJournal.upperBound(HOUR, HOUR));
    }
    class PhaseRpc implements ClusterRpc {
        String failCommand, failNode;
        boolean fail;
        public <R> R call(NodeId node, String command, Object body, Class<R> type) {
            if (fail && command.equals(failCommand) && node.value().equals(failNode))
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "injected phase failure");
            if (!node.value().equals("owner")) return type.cast(DeleteResult.of(DeleteStatus.DELETED, "owner"));
            var request = (DeleteControlRequest) body;
            return type.cast(switch (command) {
                case Commands.DELETE_PREPARE -> service.prepare(request);
                case Commands.DELETE_COMMIT -> service.commit(request);
                case Commands.DELETE_APPLY -> service.apply(request);
                case Commands.DELETE_ABORT -> service.abort(request);
                case Commands.DELETE_FINISH -> service.finish(request);
                default -> throw new IllegalArgumentException(command);
            });
        }
        public NodeId localId() { return NodeId.of("owner"); }
        public Optional<NodeId> leaderId() { return Optional.of(localId()); }
    }
    void announce(String id, Set<String> capabilities) {
        nodes.put(id, new StorageNodeStatus(id, NodeState.ACTIVE, 0, 0, 0, now, DistributionMode.COUNT, 1, 0, capabilities));
    }
    @Test void interruptedApplyResumesAfterJournalRestartAndCannotDeleteNewGeneration() throws Exception {
        legacy("s", now - Duration.ofDays(40).toMillis());
        announce("owner", StorageCapabilities.ALL); announce("z-copy", StorageCapabilities.ALL);
        var rpc = new PhaseRpc(); rpc.fail = true; rpc.failCommand = Commands.DELETE_APPLY; rpc.failNode = "z-copy";
        var original = placements.get("s");
        try (var grid = NGrid.local(1).start(); var handler = new SeriesDeleteHandler(grid.node(0).transport(), catalog, rpc, service, () -> true, () -> true, clock)) {
            var request = new DeleteRequest("s", new DeletePrecondition(now - Duration.ofDays(30).toMillis()), "operation", original.generationId());
            assertEquals(DeleteStatus.ERROR, handler.delete(request).status());
            assertTrue(placements.get("s").deletion().committed());
            assertFalse(volume.storage().exists("series/s.ngrr"));
            assertEquals(SeriesLifecycleJournal.Phase.DELETED, journal.get("s").phase());
            var committed = journal.get("s").placement();
            journal.close(); journal = new SeriesLifecycleJournal(base.resolve("lifecycle"));
            service = new SeriesLifecycleService(journal, volume, catalog, handles, "owner", "series", clock, HOUR);
            handles.lifecycle(service); rpc.fail = false;
            // Simulate an async catalogue WAL restored before its commit, on a newly elected leader.
            placements.put("s", original);
            try (var nextLeader = new SeriesDeleteHandler(grid.node(0).transport(), catalog, rpc, service, () -> true, () -> true, clock)) {
                var recovered = (DeleteResult) nextLeader.handle(Commands.DELETE_RECOVER, new DeleteControlRequest("s", committed), NodeId.of("owner"));
                assertEquals(DeleteStatus.DELETED, recovered.status()); assertFalse(placements.containsKey("s"));
                assertEquals(SeriesLifecycleJournal.Phase.FINISHED, journal.get("s").phase());
                var fresh = SeriesPlacement.active("owner", now); placements.put("s", fresh);
                assertNull(service.beforeOpen("s", fresh.generationId()));
                handles.open("s", yaml, Ngrrd.OpenOptions.defaults());
                assertEquals(DeleteStatus.NOT_FOUND, nextLeader.delete(request).status());
                assertThrows(RuntimeException.class, () -> service.apply(new DeleteControlRequest("s", committed)));
                assertTrue(volume.storage().exists("series/s.ngrr"));
                assertEquals(fresh, placements.get("s"));
            }
        }
    }
    @Test void failedPreparationIsCancelledAndLegacyStorageFailsWithoutRpcTimeout() throws Exception {
        legacy("s", now - Duration.ofDays(40).toMillis());
        announce("owner", StorageCapabilities.ALL); announce("z-old", Set.of());
        var rpc = new PhaseRpc();
        try (var grid = NGrid.local(1).start(); var handler = new SeriesDeleteHandler(grid.node(0).transport(), catalog, rpc, service, () -> true, () -> true, clock)) {
            var request = new DeleteRequest("s", new DeletePrecondition(now - Duration.ofDays(30).toMillis()), "op");
            var old = handler.delete(request);
            assertEquals(DeleteStatus.ERROR, old.status()); assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, old.errorCode());
            assertNull(placements.get("s").deletion());
            announce("z-old", StorageCapabilities.ALL);
            rpc.fail = true; rpc.failCommand = Commands.DELETE_PREPARE; rpc.failNode = "z-old";
            assertEquals(DeleteStatus.ERROR, handler.delete(request).status());
            assertNull(placements.get("s").deletion());
            assertEquals(SeriesLifecycleJournal.Phase.ACTIVE, journal.get("s").phase());
            assertNull(service.gate("s", placements.get("s").generationId()));
            assertTrue(volume.storage().exists("series/s.ngrr"));
            rpc.fail = false;
            assertEquals(DeleteStatus.DELETED, handler.delete(request).status());
        }
    }


    @Test void restoredOlderGenerationIsQuarantinedAndCannotDeleteNewerBytes() {
        legacy("s", now - Duration.ofDays(40).toMillis());
        var fresh = placements.get("s"); service.initialize("s", fresh);
        var older = SeriesPlacement.active("owner", now - HOUR).withDeletion(
                new SeriesDeletion("old-op", now, List.of("owner"), false), now);
        placements.put("s", older);
        var result = service.prepare(new DeleteControlRequest("s", older));
        assertEquals(ErrorCode.QUARANTINED, result.errorCode());
        assertEquals(fresh.generationId(), journal.get("s").generationId());
        assertTrue(volume.storage().exists("series/s.ngrr"));
    }
    @Test void explicitPurgePreservesSharedObjectsAndRevalidatesOtherOwner() {
        legacy("s", now - HOUR); placements.clear(); service.inspect("s");
        volume.storage().put("schema/shared", new byte[]{42});
        var rpc = new PhaseRpc();
        var purged = service.reconcile(new ReconcileRequest(ReconcileRequest.Action.PURGE, "s"), rpc);
        assertEquals("PURGED", purged.results().get("s"));
        assertFalse(volume.storage().exists("series/s.ngrr"));
        assertTrue(volume.storage().exists("schema/shared"));
        legacy("copy", now - HOUR); placements.remove("copy"); service.inspect("copy");
        placements.put("copy", SeriesPlacement.active("other-owner", now));
        var unavailable = service.reconcile(new ReconcileRequest(ReconcileRequest.Action.PURGE, "copy"), rpc);
        assertTrue(unavailable.results().get("copy").startsWith("ERROR"));
        assertTrue(volume.storage().exists("series/copy.ngrr"));
        var confirmed = new PhaseRpc() {
            public <R> R call(NodeId node, String command, Object body, Class<R> type) {
                if (command.equals(Commands.SERIES_EXISTS)) return type.cast(new SeriesExistsResponse(true, -1));
                return super.call(node, command, body, type);
            }
        };
        assertEquals("PURGED", service.reconcile(new ReconcileRequest(ReconcileRequest.Action.PURGE, "copy"), confirmed).results().get("copy"));
        assertFalse(volume.storage().exists("series/copy.ngrr"));
    }
    @Test
    @org.junit.jupiter.api.condition.EnabledIfSystemProperty(named="ngrrd.delete.benchmark", matches="true")
    void compareWriteBatchThroughputAtFiftyFiveThousandSamples() throws Exception {
        var lookup = new StorageRequestHandler.PlacementLookup() {
            public Optional<SeriesPlacement> placementLocal(String key) { return Optional.ofNullable(placements.get(key)); }
            public Optional<SeriesPlacement> placementStrong(String key) { return placementLocal(key); }
        };
        try (var grid = NGrid.local(1).start()) {
            var handler = new StorageRequestHandler(grid.node(0).transport(), lookup, handles, volume, "series",
                    NodeId.of("owner"), dev.nishisan.utils.oss.api.Durability.FSYNC,
                    dev.nishisan.utils.oss.api.OnGeometryChange.FAIL, clock);
            for (int k=0; k<50; k++) {
                String key = "bench-"+k;
                placements.put(key, SeriesPlacement.active("owner", now));
                service.initialize(key, placements.get(key));
                handles.open(key, yaml, Ngrrd.OpenOptions.defaults());
            }
            Path benchmarkOutput = Files.createTempFile("nishi-utils-series-receipt-", ".log");
            for (int mode=0; mode<4; mode++) {
                handles.lifecycle(mode == 1 ? null : service);
                long start = System.nanoTime(), forces = journal.fsyncCount();
                for (int batch=0; batch<55; batch++) {
                    if (mode == 3 && batch == 27) now += HOUR;
                    var writes = new ArrayList<SeriesWrite>();
                    for (int i=0; i<1000; i++) {
                        String key = "bench-"+(i%50);
                        long ts = now + (mode*1100L + batch*20L + i/50) * 300_000;
                        writes.add(new SeriesWrite(key, "in_octets", ts, i, placements.get(key).generationId()));
                    }
                    var result = (WriteBatchResponse) handler.handle(Commands.WRITE_BATCH, new WriteBatchRequest(writes), NodeId.of("bench-client"));
                    assertTrue(result.statusBySeries().values().stream().allMatch(status -> status == SeriesStatus.OK));
                }
                for (String key : placements.keySet()) handles.withHandle(key, h -> { h.checkpoint(); return true; });
                double seconds = (System.nanoTime()-start)/1e9;
                String measurement = String.format(Locale.ROOT, "SERIES_RECEIPT_BENCH mode=%s samples=55000 elapsed=%.3fs rate=%.0f/s fsyncs=%d%n",
                        new String[]{"warmup", "baseline", "steady", "hour-boundary"}[mode], seconds, 55000/seconds, journal.fsyncCount()-forces);
                System.err.print(measurement);
                Files.writeString(benchmarkOutput, measurement, StandardOpenOption.APPEND);
                if (mode == 2) assertEquals(forces, journal.fsyncCount());
                if (mode == 3) assertEquals(forces+1, journal.fsyncCount());
            }
        } finally { handles.lifecycle(service); }
    }

}
