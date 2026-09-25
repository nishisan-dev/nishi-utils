/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre {@link StorageNodeConfig#fromYaml(String, Function)} (seção 3 da spec do M4). */
class StorageNodeConfigYamlTest {

    private static final Function<String, String> NO_ENV = name -> null;

    @Test
    void yamlCompletoPreencheTodosOsCampos() {
        String yaml = """
                node:
                  id: storage-0
                  host: 127.0.0.1
                  port: 9100
                  priority: 200
                  dataDir: /var/ngrrd/storage-0/data
                  seed: 127.0.0.1:9000
                  peers:
                    - 127.0.0.1:9101
                    - 127.0.0.1:9102
                ngrrd:
                  volume:
                    dir: /var/ngrrd/storage-0/volume
                    name: ngrrd
                    shardCount: 32
                    segmentBytes: 134217728
                    initialShardCapacityBytes: 134217728
                    capacityBytes: 1073741824
                  statusReportInterval: 5s
                  nodeStatusStaleAfter: 25s
                  handleIdleTtl: 20m
                  maxOpenHandles: 5000
                  requestTimeout: 15s
                  defaultDurability: OS_CACHE
                  defaultOnGeometryChange: RECREATE
                  rebalance:
                    enabled: false
                    interval: 90s
                    minDelta: 25
                    tolerance: 0.20
                    maxConcurrentMigrations: 4
                    maxMovesPerCycle: 100
                    migrationTimeout: 5m
                    chunkBytes: 131072
                    maxBytesPerSecond: 8388608
                    maxSeriesBytes: 33554432
                    maxDestinationCatalogLag: 250
                  reconcile:
                    interval: 15m
                    orphanGrace: 10m
                """;

        StorageNodeConfig config = StorageNodeConfig.fromYaml(yaml, NO_ENV);

        assertEquals("storage-0", config.nodeId());
        assertEquals("127.0.0.1", config.host());
        assertEquals(9100, config.port());
        assertEquals(200, config.priority());
        assertEquals(Path.of("/var/ngrrd/storage-0/data"), config.dataDir());
        assertEquals("127.0.0.1:9000", config.seed());
        assertEquals(List.of("127.0.0.1:9101", "127.0.0.1:9102"), config.peers());
        assertEquals(Path.of("/var/ngrrd/storage-0/volume"), config.volumeDir());
        assertEquals("ngrrd", config.volumeName());
        assertEquals(32, config.shardCount());
        assertEquals(134_217_728L, config.segmentBytes());
        assertEquals(134_217_728L, config.initialShardCapacityBytes());
        assertEquals(1_073_741_824L, config.capacityBytes());
        assertEquals(Duration.ofSeconds(5), config.statusReportInterval());
        assertEquals(Duration.ofSeconds(25), config.nodeStatusStaleAfter());
        assertEquals(Duration.ofMinutes(20), config.handleIdleTtl());
        assertEquals(5000, config.maxOpenHandles());
        assertEquals(Duration.ofSeconds(15), config.requestTimeout());
        assertEquals(Durability.OS_CACHE, config.defaultDurability());
        assertEquals(OnGeometryChange.RECREATE, config.defaultOnGeometryChange());
        assertEquals(false, config.rebalanceEnabled());
        assertEquals(Duration.ofSeconds(90), config.rebalanceInterval());
        assertEquals(25L, config.rebalanceMinDelta());
        assertEquals(0.20, config.rebalanceTolerance());
        assertEquals(4, config.maxConcurrentMigrations());
        assertEquals(100, config.maxMovesPerCycle());
        assertEquals(Duration.ofMinutes(5), config.migrationTimeout());
        assertEquals(131_072L, config.migrationChunkBytes());
        assertEquals(8_388_608L, config.migrationBytesPerSecond());
        assertEquals(33_554_432L, config.maxSeriesBytes());
        assertEquals(250L, config.maxDestinationCatalogLag());
        assertEquals(Duration.ofMinutes(15), config.reconcileInterval());
        assertEquals(Duration.ofMinutes(10), config.orphanGrace());
    }

    @Test
    void seriesObjectPrefixCustomizadoNoYamlEAplicado() {
        String yaml = """
                node:
                  id: storage-0
                  host: 127.0.0.1
                  port: 9100
                  dataDir: /var/ngrrd/storage-0/data
                ngrrd:
                  volume:
                    dir: /var/ngrrd/storage-0/volume
                    name: ngrrd
                  seriesObjectPrefix: legacy-series
                """;

        StorageNodeConfig config = StorageNodeConfig.fromYaml(yaml, NO_ENV);

        assertEquals("legacy-series", config.seriesObjectPrefix());
    }

    @Test
    void yamlMinimoUsaOsDefaultsDoBuilder() {
        String yaml = """
                node:
                  id: storage-0
                  host: 127.0.0.1
                  port: 9100
                  dataDir: /var/ngrrd/storage-0/data
                ngrrd:
                  volume:
                    dir: /var/ngrrd/storage-0/volume
                    name: ngrrd
                """;

        StorageNodeConfig config = StorageNodeConfig.fromYaml(yaml, NO_ENV);
        StorageNodeConfig defaults = StorageNodeConfig.builder()
                .nodeId("storage-0")
                .port(9100)
                .dataDir(Path.of("/var/ngrrd/storage-0/data"))
                .volumeDir(Path.of("/var/ngrrd/storage-0/volume"))
                .build();

        assertEquals(defaults.handleIdleTtl(), config.handleIdleTtl());
        assertEquals(defaults.rebalanceEnabled(), config.rebalanceEnabled());
        assertEquals(defaults.reconcileInterval(), config.reconcileInterval());
        assertEquals(defaults.orphanGrace(), config.orphanGrace());
        assertEquals(1_000L, config.maxDestinationCatalogLag());
        assertEquals(List.of(), config.peers());
    }

    @Test
    void variaveisDeAmbienteComDefaultSaoInterpoladas() {
        String yaml = """
                node:
                  id: ${NODE_ID:storage-fallback}
                  host: 127.0.0.1
                  port: ${NODE_PORT:9200}
                  dataDir: /var/ngrrd/${NODE_ID:storage-fallback}/data
                ngrrd:
                  volume:
                    dir: /var/ngrrd/${NODE_ID:storage-fallback}/volume
                    name: ngrrd
                  reconcile:
                    interval: ${RECONCILE_INTERVAL:10m}
                """;

        // Sem a variável definida: usa o default embutido no placeholder.
        StorageNodeConfig withoutEnv = StorageNodeConfig.fromYaml(yaml, NO_ENV);
        assertEquals("storage-fallback", withoutEnv.nodeId());
        assertEquals(9200, withoutEnv.port());
        assertEquals(Path.of("/var/ngrrd/storage-fallback/data"), withoutEnv.dataDir());
        assertEquals(Duration.ofMinutes(10), withoutEnv.reconcileInterval());

        // Com a variável definida: o valor do ambiente prevalece sobre o default.
        Map<String, String> env = Map.of("NODE_ID", "storage-9", "NODE_PORT", "9333",
                "RECONCILE_INTERVAL", "20m");
        StorageNodeConfig withEnv = StorageNodeConfig.fromYaml(yaml, env::get);
        assertEquals("storage-9", withEnv.nodeId());
        assertEquals(9333, withEnv.port());
        assertEquals(Path.of("/var/ngrrd/storage-9/data"), withEnv.dataDir());
        assertEquals(Duration.ofMinutes(20), withEnv.reconcileInterval());
    }

    @Test
    void secaoNodeAusenteFalhaComMensagemClara() {
        String yaml = """
                ngrrd:
                  volume:
                    dir: /var/ngrrd/x/volume
                    name: ngrrd
                """;

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StorageNodeConfig.fromYaml(yaml, NO_ENV));
        assertTrue(e.getMessage().contains("node"), "mensagem deveria indicar a seção ausente: " + e.getMessage());
    }

    @Test
    void volumeDirAusenteFalhaComMensagemClara() {
        String yaml = """
                node:
                  id: storage-0
                  host: 127.0.0.1
                  port: 9100
                  dataDir: /var/ngrrd/storage-0/data
                ngrrd:
                  volume:
                    name: ngrrd
                """;

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StorageNodeConfig.fromYaml(yaml, NO_ENV));
        assertTrue(e.getMessage().contains("volume.dir"), "mensagem: " + e.getMessage());
    }

    @Test
    void maxDestinationCatalogLagMenosUmDesligaEAbaixoDissoFalha() {
        String yaml = """
                node:
                  id: storage-0
                  host: 127.0.0.1
                  port: 9100
                  dataDir: /var/ngrrd/storage-0/data
                ngrrd:
                  volume:
                    dir: /var/ngrrd/storage-0/volume
                    name: ngrrd
                  rebalance:
                    maxDestinationCatalogLag: %d
                """;

        assertEquals(-1L, StorageNodeConfig.fromYaml(yaml.formatted(-1), NO_ENV).maxDestinationCatalogLag());
        assertEquals(0L, StorageNodeConfig.fromYaml(yaml.formatted(0), NO_ENV).maxDestinationCatalogLag());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StorageNodeConfig.fromYaml(yaml.formatted(-2), NO_ENV));
        assertTrue(e.getMessage().contains("maxDestinationCatalogLag"), "mensagem: " + e.getMessage());
    }

    @Test
    void yamlInvalidoFalhaComMensagemClara() {
        String yaml = "node: [isso não é um objeto";

        assertThrows(IllegalArgumentException.class, () -> StorageNodeConfig.fromYaml(yaml, NO_ENV));
    }
}
