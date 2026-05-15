package dev.nishisan.utils.oss.format;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManifestCodecTest {

    private static NgrrdManifest sample(int version) {
        return new NgrrdManifest(
                version,
                "device:r1/iface:eth0",
                "deadbeef",
                List.of(
                        new RraManifest("raw", 300, List.of(
                                new ManifestBlock(1_747_339_200L, 288, 0x12345678L,
                                        "raw/device:r1/iface:eth0/in_octets/300/1747339200.ngrrd"),
                                new ManifestBlock(1_747_425_600L, 288, 0xAABBCCDDL,
                                        "raw/device:r1/iface:eth0/in_octets/300/1747425600.ngrrd"))),
                        new RraManifest("rra_5m_30d", 300, List.of(
                                new ManifestBlock(1_747_339_200L, 8640, 0xFEEDFACEL,
                                        "agg/device:r1/iface:eth0/rra_5m_30d/300/1747339200.ngrrd")))));
    }

    @Test
    void roundTripPreservaCamposEOrdem() {
        NgrrdManifest original = sample(7);
        byte[] yaml = ManifestCodec.writeYaml(original);
        NgrrdManifest restored = ManifestCodec.readYaml(yaml);

        assertEquals(original, restored);
        assertEquals(7, restored.version());
        assertEquals("device:r1/iface:eth0", restored.seriesKey());
        assertEquals(2, restored.rras().size());
        assertEquals("raw", restored.rras().get(0).rraName());
        assertEquals("rra_5m_30d", restored.rras().get(1).rraName());
        assertEquals(2, restored.rras().get(0).blocks().size());
        assertEquals(1_747_339_200L, restored.rras().get(0).blocks().get(0).blockStartEpoch());
        assertEquals(0x12345678L, restored.rras().get(0).blocks().get(0).crc32());
    }

    @Test
    void roundTripMantemListaImutavelVazia() {
        NgrrdManifest empty = new NgrrdManifest(1, "s1", "0", List.of());
        NgrrdManifest restored = ManifestCodec.readYaml(ManifestCodec.writeYaml(empty));

        assertEquals(0, restored.rras().size());
        assertThrows(UnsupportedOperationException.class,
                () -> restored.rras().add(new RraManifest("x", 1, List.of())));
    }

    @Test
    void varianteEntreVersoesGeraConteudoDiferente() {
        byte[] v1 = ManifestCodec.writeYaml(sample(1));
        byte[] v2 = ManifestCodec.writeYaml(sample(2));
        assertNotEquals(new String(v1), new String(v2));

        NgrrdManifest r1 = ManifestCodec.readYaml(v1);
        NgrrdManifest r2 = ManifestCodec.readYaml(v2);
        assertEquals(1, r1.version());
        assertEquals(2, r2.version());
    }

    @Test
    void rejeitaVersaoZeroNaConstrucao() {
        assertThrows(IllegalArgumentException.class,
                () -> new NgrrdManifest(0, "s", "h", List.of()));
    }

    @Test
    void rejeitaYamlInvalido() {
        assertThrows(NgrrdFormatException.class,
                () -> ManifestCodec.readYaml("isto não é YAML válido: : :".getBytes()));
    }

    @Test
    void computeDefinitionHashEhDeterministico() {
        String yaml = "apiVersion: ngrrd/v1\nkind: MetricSeriesDefinition\n";
        String h1 = ManifestCodec.computeDefinitionHash(yaml);
        String h2 = ManifestCodec.computeDefinitionHash(yaml);
        assertEquals(h1, h2);
        assertEquals(64, h1.length()); // SHA-256 hex
        assertTrue(h1.matches("[0-9a-f]{64}"));
    }

    @Test
    void computeDefinitionHashDifereParaConteudoDiferente() {
        String a = ManifestCodec.computeDefinitionHash("a");
        String b = ManifestCodec.computeDefinitionHash("b");
        assertNotEquals(a, b);
    }
}
