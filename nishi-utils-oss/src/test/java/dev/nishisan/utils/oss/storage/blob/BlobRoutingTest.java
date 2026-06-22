package dev.nishisan.utils.oss.storage.blob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Vetores-ouro de roteamento série→shard (algoritmo SHA256_PREFIX64, v1).
 * Os mesmos vetores são verificados pela ferramenta Python
 * ({@code python/ngrrd-python/tests/test_blob_routing.py}) para garantir
 * paridade cross-language. Ver {@code doc/oss/ngrrd-blob-volume.md} §8.
 */
class BlobRoutingTest {

    @Test
    void goldenVectorsMatchPythonReference() {
        int n = 64;
        assertEquals(37, BlobRouting.shardFor("series/device:r1/iface:eth0/group:traffic-v1.ngrr", n));
        assertEquals(57, BlobRouting.shardFor(
                "series/device:i-br-sc-bnu-bsh-hl5d-01/iface:int-GigabitEthernet0_1_3/group:traffic-v1.ngrr", n));
        assertEquals(35, BlobRouting.shardFor("series/a.ngrr", n));
        assertEquals(15, BlobRouting.shardFor("series/host=x;if=eth0.ngrr", n));
        assertEquals(10, BlobRouting.shardFor("series/0.ngrr", n));
    }

    @Test
    void handlesUnsignedHighBitOfDigest() {
        // h64 dos 8 primeiros bytes de SHA-256 desta chave = 16162343004957874405,
        // que excede Long.MAX_VALUE: um módulo COM sinal daria resultado errado.
        // O resultado correto (unsigned) é 37.
        assertEquals(37, BlobRouting.shardFor("series/device:r1/iface:eth0/group:traffic-v1.ngrr", 64));
    }

    @Test
    void resultIsAlwaysWithinBounds() {
        for (int i = 0; i < 1000; i++) {
            int shard = BlobRouting.shardFor("series/k-" + i + ".ngrr", 64);
            assertTrue(shard >= 0 && shard < 64, "shard fora de [0,64): " + shard);
        }
    }

    @Test
    void rejectsNonPositiveShardCount() {
        assertThrows(IllegalArgumentException.class, () -> BlobRouting.shardFor("series/a.ngrr", 0));
        assertThrows(IllegalArgumentException.class, () -> BlobRouting.shardFor("series/a.ngrr", -1));
    }
}
