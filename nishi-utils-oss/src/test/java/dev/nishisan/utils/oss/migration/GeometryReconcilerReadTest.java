package dev.nishisan.utils.oss.migration;

import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

class GeometryReconcilerReadTest {
    @TempDir Path root;

    @Test
    void reopeningUnchangedGeometryDoesNotLoadOrRewriteArchiveHistory() throws Exception {
        String yaml;
        try (var in=getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            yaml=new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        var definition=NgrrdYamlLoader.parse(yaml,k->null);
        var geometry=new SeriesGeometry(definition);
        var storage=new MeasuredStorage(root);
        byte[] original=SeriesFileCodec.buildInitialImage(geometry,geometry.geometryHash(),1);
        storage.disk.put("test.ngrr",original);
        GeometryReconciler.reconcile(storage,"test.ngrr",geometry,geometry.geometryHash(),1,OnGeometryChange.FAIL);
        assertEquals(SeriesFileCodec.FIXED_HEADER_BYTES,storage.bytesRead);
        assertEquals(0,storage.fullReads);
        assertArrayEquals(original,storage.disk.get("test.ngrr").orElseThrow());
    }

    private static final class MeasuredStorage implements NgrrdStorage, SeriesChannelProvider {
        final LocalDiskStorage disk;
        long bytesRead;
        int fullReads;
        MeasuredStorage(Path path) {disk=new LocalDiskStorage(path);}
        public void put(String k,byte[] v) {disk.put(k,v);}
        public Optional<byte[]> get(String k) {fullReads++;return disk.get(k);}
        public boolean exists(String k) {return disk.exists(k);}
        public void delete(String k) {disk.delete(k);}
        public List<String> list(String p) {return disk.list(p);}
        public void atomicReplace(String k,byte[] v) {disk.atomicReplace(k,v);}
        public boolean seriesExists(String k) {return disk.seriesExists(k);}
        public SeriesChannel openSeries(String k) {
            var channel=disk.openSeries(k);
            return new SeriesChannel() {
                public long size() {return channel.size();}
                public void allocate(long n) {fail("Reconciliation must not allocate unchanged geometry");}
                public byte[] readRegion(long p,int n) {bytesRead+=n;return channel.readRegion(p,n);}
                public void writeRegion(long p,byte[] b) {fail("Reconciliation must not rewrite unchanged geometry");}
                public void force() {fail("Read-only reconciliation must not force");}
                public void close() {channel.close();}
            };
        }
    }
}
