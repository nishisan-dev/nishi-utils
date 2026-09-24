package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributionConfigTest {
    private static final String YAML = """
            node:
              id: storage
              host: 127.0.0.1
              port: 9100
              dataDir: /tmp/storage-data
            ngrrd:
              volume:
                dir: /tmp/storage-volume
                name: ngrrd
              distribution:
                mode: WEIGHT
              weight: 3.5
            """;

    @Test void parsesExplicitModeAndWeight() {
        var config = StorageNodeConfig.fromYaml(YAML, ignored -> null);
        assertEquals(DistributionMode.WEIGHT, config.distributionMode());
        assertEquals(3.5, config.weight());
    }

    @Test void invalidWeightsAndModeAreRejected() {
        for (String weight : new String[]{"0", "-1", ".NaN", ".inf"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> StorageNodeConfig.fromYaml(YAML.replace("3.5", weight), ignored -> null));
        }
        assertThrows(IllegalArgumentException.class,
                () -> StorageNodeConfig.fromYaml(YAML.replace("WEIGHT", "UNKNOWN"), ignored -> null));
    }
}
