package dev.nishisan.utils.ngrid.config;

import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DeploymentProfile;
import dev.nishisan.utils.ngrid.structures.MapConfig;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for production guardrails in {@link NGridConfig}.
 * <p>
 * When {@link DeploymentProfile#PRODUCTION} is set, the builder must
 * reject configurations that violate safety invariants.
 */
class NGridConfigValidationTest {

    @TempDir
    Path tempDir;

    private static final NodeInfo LOCAL =
            new NodeInfo(NodeId.of("test-node"), "127.0.0.1", 9000);

    // ── PRODUCTION guardrails ──

    @Test
    void productionRejectsRelaxedConsistency() {
        var builder = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.PRODUCTION)
                .strictConsistency(false)
                .replicationFactor(2);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);
        assertTrue(ex.getMessage().contains("[PRODUCTION]"));
        assertTrue(ex.getMessage().contains("strictConsistency"));
    }

    @Test
    void productionRejectsSingleReplicaFactor() {
        var builder = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.PRODUCTION)
                .strictConsistency(true)
                .replicationFactor(1);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);
        assertTrue(ex.getMessage().contains("[PRODUCTION]"));
        assertTrue(ex.getMessage().contains("replicationFactor"));
    }

    @Test
    void productionRejectsMapWithDisabledPersistence() {
        MapConfig badMap = MapConfig.builder("ephemeral-map")
                .persistenceMode(NMapPersistenceMode.DISABLED)
                .build();

        var builder = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.PRODUCTION)
                .strictConsistency(true)
                .replicationFactor(2)
                .addMap(badMap);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);
        assertTrue(ex.getMessage().contains("[PRODUCTION]"));
        assertTrue(ex.getMessage().contains("ephemeral-map"));
        assertTrue(ex.getMessage().contains("DISABLED"));
    }

    @Test
    void productionAcceptsValidConfig() {
        MapConfig validMap = MapConfig.builder("durable-map")
                .persistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .build();

        NGridConfig config = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.PRODUCTION)
                .strictConsistency(true)
                .replicationFactor(3)
                .addMap(validMap)
                .build();

        assertNotNull(config);
        assertEquals(DeploymentProfile.PRODUCTION, config.deploymentProfile());
        assertTrue(config.strictConsistency());
        assertEquals(3, config.replicationFactor());
    }

    @Test
    void productionAcceptsConfigWithoutMaps() {
        // Sem maps configurados, a validação de persistência não se aplica
        NGridConfig config = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.PRODUCTION)
                .strictConsistency(true)
                .replicationFactor(2)
                .build();

        assertNotNull(config);
        assertEquals(DeploymentProfile.PRODUCTION, config.deploymentProfile());
    }

    // ── DEV profile: sem guardrails ──

    @Test
    void devAcceptsAnyConfig() {
        NGridConfig config = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.DEV)
                .strictConsistency(false)
                .replicationFactor(1)
                .addMap(MapConfig.builder("temp")
                        .persistenceMode(NMapPersistenceMode.DISABLED)
                        .build())
                .build();

        assertNotNull(config);
        assertEquals(DeploymentProfile.DEV, config.deploymentProfile());
        assertFalse(config.strictConsistency());
    }

    @Test
    void stagingAcceptsAnyConfig() {
        NGridConfig config = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .deploymentProfile(DeploymentProfile.STAGING)
                .strictConsistency(false)
                .replicationFactor(1)
                .build();

        assertNotNull(config);
        assertEquals(DeploymentProfile.STAGING, config.deploymentProfile());
    }

    @Test
    void defaultProfileIsDev() {
        NGridConfig config = NGridConfig.builder(LOCAL)
                .dataDirectory(tempDir)
                .build();

        assertEquals(DeploymentProfile.DEV, config.deploymentProfile());
    }
}
