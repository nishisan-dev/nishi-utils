package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.NamingScheme;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StorageKeyTest {

    private static final ObjectNaming NAMING = new ObjectNaming(
            NamingScheme.DETERMINISTIC, "schema", "series");

    @Test
    void seriesUsaPrefixoEExtensaoNgrr() {
        String key = StorageKey.series(NAMING, "device:r1/iface:eth0");
        assertEquals("series/device:r1/iface:eth0.ngrr", key);
    }

    @Test
    void schemaSnapshotUsaNomeDaDefinicao() {
        String key = StorageKey.schemaSnapshot(NAMING, "iface-traffic-errors-v1");
        assertEquals("schema/iface-traffic-errors-v1.yaml", key);
    }

    @Test
    void prefixosOmitidosUsamDefaults() {
        ObjectNaming defaults = new ObjectNaming(NamingScheme.DETERMINISTIC, null, null);
        assertEquals("series/s.ngrr", StorageKey.series(defaults, "s"));
        assertEquals("schema/d.yaml", StorageKey.schemaSnapshot(defaults, "d"));
    }

    @Test
    void absorveBarrasExtrasNosPrefixos() {
        ObjectNaming sloppy = new ObjectNaming(NamingScheme.DETERMINISTIC, "/schema/", "/series/");
        assertEquals("series/x.ngrr", StorageKey.series(sloppy, "x"));
    }
}
