package dev.nishisan.utils.oss.cluster.catalog;

import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

class GeometryDescriptorTest {
    @Test void deduplicatesValidatesAndReconstructsFromStaticSection() throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));
        var geometry = new SeriesGeometry(NgrrdYamlLoader.parse(yaml, ignored -> null));
        var descriptor = GeometryDescriptor.from(geometry);
        assertEquals(geometry.fileTotalBytes(), descriptor.objectBytes());
        assertEquals(BlobStorage.alignedRegionBytes(geometry.fileTotalBytes()), descriptor.regionBytes());
        byte[] section = SeriesFileCodec.encodeStaticSection(geometry, new byte[32], 1);
        assertEquals(descriptor, GeometryDescriptor.fromStaticSection(section));
        assertThrows(IllegalArgumentException.class, () -> new GeometryDescriptor(descriptor.id(), 1,
                descriptor.baseStepSec(), descriptor.columns(), descriptor.archives(), descriptor.objectBytes() + 1,
                descriptor.regionBytes()));
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(descriptor); }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertEquals(descriptor, in.readObject());
        }
    }

    @Test void readsRecordsPersistedByVersion831() throws Exception {
        try (var in = new ObjectInputStream(getClass().getResourceAsStream("/legacy-catalog/node-8.3.1.ser"))) {
            StorageNodeStatus status = (StorageNodeStatus) in.readObject();
            assertEquals("legacy", status.nodeId());
            assertEquals(42, status.seriesCount());
            assertEquals(DistributionMode.COUNT, status.distributionMode());
            assertEquals(1, status.weight());
            assertEquals(0, status.reservedBytes());
        }
        try (var in = new ObjectInputStream(getClass().getResourceAsStream("/legacy-catalog/placement-8.3.1.ser"))) {
            SeriesPlacement placement = (SeriesPlacement) in.readObject();
            assertEquals("legacy", placement.ownerNodeId());
            assertNull(placement.geometryId());
            assertFalse(placement.geometryConfirmed());
        }
    }

    @Test void migrationTransitionsPreserveGeometry() {
        var active = SeriesPlacement.active("a", 1).withGeometry("geometry", true, 2);
        var migrating = SeriesPlacement.migrating(active, "b", "move", 3);
        for (var placement : new SeriesPlacement[] {migrating, SeriesPlacement.completed(migrating, 4),
                SeriesPlacement.aborted(migrating, 4)}) {
            assertEquals("geometry", placement.geometryId());
            assertTrue(placement.geometryConfirmed());
        }
        var invalidated = active.withGeometry(active.geometryId(), false, 5);
        assertEquals("a", invalidated.ownerNodeId());
        assertFalse(invalidated.geometryConfirmed());
    }
}
