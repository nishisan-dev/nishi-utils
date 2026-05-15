package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.api.NamingScheme;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageKeyTest {

    private static final ObjectNaming NAMING = new ObjectNaming(
            NamingScheme.DETERMINISTIC, "raw", "agg", "manifest", "schema");

    @Test
    void rawBlockSegueTemplateDoYaml() {
        String key = StorageKey.rawBlock(NAMING, "device:r1/iface:eth0", "in_octets", 300, 1_747_339_200L);
        assertEquals("raw/device:r1/iface:eth0/in_octets/300/1747339200.ngrrd", key);
    }

    @Test
    void aggBlockUsaRraNameComoTerceiroSegmento() {
        String key = StorageKey.aggBlock(NAMING, "device:r1/iface:eth0", "rra_5m_30d", 300, 1_747_339_200L);
        assertEquals("agg/device:r1/iface:eth0/rra_5m_30d/300/1747339200.ngrrd", key);
    }

    @Test
    void manifestVersionUsaPrefixoVMaisVersao() {
        String key = StorageKey.manifestVersion(NAMING, "device:r1/iface:eth0", 42);
        assertEquals("manifest/device:r1/iface:eth0/v42.yaml", key);
    }

    @Test
    void manifestPrefixNaoIncluiVersao() {
        String key = StorageKey.manifestPrefix(NAMING, "device:r1/iface:eth0");
        assertEquals("manifest/device:r1/iface:eth0", key);
    }

    @Test
    void schemaSnapshotUsaNomeDaDefinicao() {
        String key = StorageKey.schemaSnapshot(NAMING, "iface-traffic-errors-v1");
        assertEquals("schema/iface-traffic-errors-v1.yaml", key);
    }

    @Test
    void rejeitaVersaoZero() {
        assertThrows(IllegalArgumentException.class,
                () -> StorageKey.manifestVersion(NAMING, "s", 0));
    }

    @Test
    void absorveBarrasExtrasNosPrefixos() {
        ObjectNaming sloppy = new ObjectNaming(
                NamingScheme.DETERMINISTIC, "/raw/", "/agg/", "/manifest/", "/schema/");
        assertEquals("raw/x/in/300/1.ngrrd",
                StorageKey.rawBlock(sloppy, "x", "in", 300, 1L));
    }
}
