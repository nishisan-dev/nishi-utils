package dev.nishisan.utils.oss.blob;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Locator ngrrd://&lt;volume&gt;/&lt;seriesPath&gt; (ver doc/oss/ngrrd-blob-volume.md). */
class NgrrdUriTest {

    @Test
    void parsesVolumeAndSeriesPath() {
        NgrrdUri uri = NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0/group:traffic-v1");
        assertEquals("ifaceStats", uri.volume());
        assertEquals("device:r1/iface:eth0/group:traffic-v1", uri.seriesPath());
    }

    @Test
    void stripsLeadingSlashFromPath() {
        NgrrdUri uri = NgrrdUri.parse("ngrrd://vol//a/b");
        assertEquals("vol", uri.volume());
        assertEquals("a/b", uri.seriesPath());
    }

    @Test
    void roundTripsThroughToString() {
        NgrrdUri uri = NgrrdUri.of("vol", "a/b/c");
        assertEquals("ngrrd://vol/a/b/c", uri.toString());
        assertEquals(uri, NgrrdUri.parse(uri.toString()));
    }

    @Test
    void parsesFromUri() {
        NgrrdUri uri = NgrrdUri.parse(URI.create("ngrrd://flows/dev:x/grp:y"));
        assertEquals("flows", uri.volume());
        assertEquals("dev:x/grp:y", uri.seriesPath());
    }

    @Test
    void rejectsWrongScheme() {
        assertThrows(IllegalArgumentException.class, () -> NgrrdUri.parse("file://vol/a"));
        assertThrows(IllegalArgumentException.class, () -> NgrrdUri.parse("ifaceStats/a/b"));
    }

    @Test
    void rejectsEmptyVolumeOrPath() {
        assertThrows(IllegalArgumentException.class, () -> NgrrdUri.parse("ngrrd:///a/b"));
        assertThrows(IllegalArgumentException.class, () -> NgrrdUri.parse("ngrrd://vol"));
        assertThrows(IllegalArgumentException.class, () -> NgrrdUri.parse("ngrrd://vol/"));
    }

    @Test
    void rejectsPathTraversal() {
        assertThrows(IllegalArgumentException.class, () -> NgrrdUri.parse("ngrrd://vol/a/../b"));
    }
}
