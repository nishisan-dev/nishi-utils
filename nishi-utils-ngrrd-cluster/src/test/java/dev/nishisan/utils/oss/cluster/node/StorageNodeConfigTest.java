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

import dev.nishisan.utils.oss.blob.VolumeWriteMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parâmetros de coordenação do {@link StorageNodeConfig} repassados ao NGrid: a janela de
 * boot-discovery e o handback orquestrado de liderança. Os defaults são os que fecham os furos
 * observados nos testes de cluster (autoeleição de um nó que volta atrasado; handoff de afinidade
 * sob carga descartando a cauda do incumbente), então precisam ficar cravados aqui.
 */
class StorageNodeConfigTest {

    private static StorageNodeConfig.Builder minimal(Path base) {
        return StorageNodeConfig.builder()
                .nodeId("storage-0")
                .port(0)
                .dataDir(base.resolve("data"))
                .volumeDir(base.resolve("volume"));
    }

    @Test
    void migrationBudgetIsPositiveAndConfigurable(@TempDir Path base) {
        assertEquals(16L * 1024 * 1024, minimal(base).build().migrationBytesPerSecond());
        assertEquals(12345, minimal(base).migrationBytesPerSecond(12345).build().migrationBytesPerSecond());
        assertThrows(IllegalArgumentException.class, () -> minimal(base).migrationBytesPerSecond(0).build());
        assertThrows(IllegalArgumentException.class, () -> minimal(base).migrationBytesPerSecond(-1).build());
    }

    @Test
    void placementInspectTimeoutTemDefaultDeDoisSegundosEExigeValorPositivo(@TempDir Path base) {
        assertEquals(Duration.ofSeconds(2), minimal(base).build().placementInspectTimeout());
        assertEquals(StorageNodeConfig.DEFAULT_PLACEMENT_INSPECT_TIMEOUT, minimal(base).build().placementInspectTimeout());
        assertEquals(Duration.ofMillis(750),
                minimal(base).placementInspectTimeout(Duration.ofMillis(750)).build().placementInspectTimeout());
        assertThrows(IllegalArgumentException.class,
                () -> minimal(base).placementInspectTimeout(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
                () -> minimal(base).placementInspectTimeout(Duration.ofMillis(-1)).build());
        assertThrows(NullPointerException.class, () -> minimal(base).placementInspectTimeout(null).build());
    }

    @Test
    void volumeWriteModeTemDefaultMmapENaoAceitaNulo(@TempDir Path base) {
        assertEquals(VolumeWriteMode.MMAP, minimal(base).build().volumeWriteMode());
        assertEquals(VolumeWriteMode.PWRITE,
                minimal(base).volumeWriteMode(VolumeWriteMode.PWRITE).build().volumeWriteMode());
        assertThrows(NullPointerException.class, () -> minimal(base).volumeWriteMode(null).build());
    }

    @Test
    void avisoDeSubidaQuandoPlacementInspectTimeoutNaoFicaAbaixoDoRequestTimeout() {
        assertTrue(NgrrdStorageNode.placementInspectTimeoutWarning(Duration.ofSeconds(2), Duration.ofSeconds(20))
                .isEmpty());
        String equal = NgrrdStorageNode.placementInspectTimeoutWarning(Duration.ofSeconds(20), Duration.ofSeconds(20))
                .orElseThrow();
        assertTrue(equal.contains("ngrrd.placement.inspectTimeout=PT20S"), equal);
        assertTrue(NgrrdStorageNode.placementInspectTimeoutWarning(Duration.ofSeconds(30), Duration.ofSeconds(20))
                .isPresent());
    }

    @Test
    void defaultsDeCoordenacaoSaoJanelaDeTresSegundosEHandbackLigado(@TempDir Path base) {
        StorageNodeConfig config = minimal(base).build();

        assertEquals(Duration.ofSeconds(3), config.bootDiscoveryWindow());
        assertTrue(config.affinityHandbackMode(), "handback orquestrado deve vir ligado por default");
    }

    @Test
    void janelaDeBootEHandbackSaoConfiguraveis(@TempDir Path base) {
        StorageNodeConfig config = minimal(base)
                .bootDiscoveryWindow(Duration.ofMillis(500))
                .affinityHandbackMode(false)
                .build();

        assertEquals(Duration.ofMillis(500), config.bootDiscoveryWindow());
        assertFalse(config.affinityHandbackMode());
    }

    @Test
    void janelaDeBootZeroEAceitaENegativaERejeitada(@TempDir Path base) {
        assertEquals(Duration.ZERO, minimal(base).bootDiscoveryWindow(Duration.ZERO).build().bootDiscoveryWindow());
        assertThrows(IllegalArgumentException.class,
                () -> minimal(base).bootDiscoveryWindow(Duration.ofMillis(-1)).build());
    }

    @Test
    void seriesObjectPrefixDefaultVemDoOssNaoDeUmLiteral(@TempDir Path base) {
        // MÉDIO-6 do Refuter: o default não pode ser um literal duplicado aqui — precisa vir do próprio
        // ObjectNaming do oss (que hoje resolve para "series").
        StorageNodeConfig config = minimal(base).build();

        assertEquals(new dev.nishisan.utils.oss.definition.ObjectNaming(null, null, null).seriesPrefixOrDefault(),
                config.seriesObjectPrefix());
    }

    @Test
    void seriesObjectPrefixConfiguravelERejeitaVazio(@TempDir Path base) {
        StorageNodeConfig config = minimal(base).seriesObjectPrefix("legacy-series").build();
        assertEquals("legacy-series", config.seriesObjectPrefix());

        assertThrows(IllegalArgumentException.class, () -> minimal(base).seriesObjectPrefix("").build());
        assertThrows(NullPointerException.class, () -> minimal(base).seriesObjectPrefix(null).build());
    }

    @Test
    void seriesObjectPrefixNormalizaBarrasIniciaisEFinais(@TempDir Path base) {
        // BAIXO-D do Refuter: "series/" e "/series/" e "series" são o mesmo prefixo — normalizado já na
        // validação do record, para casar com SeriesObjectKeys/StorageRequestHandler sem surpresas.
        assertEquals("series", minimal(base).seriesObjectPrefix("series/").build().seriesObjectPrefix());
        assertEquals("series", minimal(base).seriesObjectPrefix("/series/").build().seriesObjectPrefix());
        assertEquals("series", minimal(base).seriesObjectPrefix("series").build().seriesObjectPrefix());

        // Só barras (nada sobra após normalizar) continua sendo rejeitado como vazio.
        assertThrows(IllegalArgumentException.class, () -> minimal(base).seriesObjectPrefix("///").build());
    }

    @Test
    void checkpointLocalTemDefaultDesligadoENaoAceitaNulo(@TempDir Path base) {
        assertEquals(LocalCheckpointSettings.disabled(), minimal(base).build().localCheckpoint());
        LocalCheckpointSettings enabled = new LocalCheckpointSettings(true, Duration.ofSeconds(30), 4);
        assertEquals(enabled, minimal(base).localCheckpoint(enabled).build().localCheckpoint());
        assertThrows(NullPointerException.class, () -> minimal(base).localCheckpoint(null).build());
    }

    @Test
    void localCheckpointSettingsValidaIntervaloEMaxInFlight() {
        int processors = Runtime.getRuntime().availableProcessors();
        assertEquals(Math.min(8, Math.max(2, processors)), LocalCheckpointSettings.defaultMaxInFlight());
        assertFalse(LocalCheckpointSettings.disabled().enabled());
        assertEquals(Duration.ofSeconds(300), LocalCheckpointSettings.disabled().interval());
        assertThrows(IllegalArgumentException.class, () -> new LocalCheckpointSettings(true, Duration.ZERO, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LocalCheckpointSettings(true, Duration.ofSeconds(-1), 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LocalCheckpointSettings(true, Duration.ofSeconds(1), 0));
        assertThrows(NullPointerException.class, () -> new LocalCheckpointSettings(true, null, 1));
    }

    @Test
    void construtorDeCompatibilidadeDa8130DeixaOCheckpointLocalDesligado(@TempDir Path base) {
        StorageNodeConfig c = minimal(base).localCheckpoint(new LocalCheckpointSettings(true, Duration.ofSeconds(9), 1))
                .build();
        StorageNodeConfig legacy = new StorageNodeConfig(c.nodeId(), c.host(), c.port(), c.seed(), c.peers(),
                c.dataDir(), c.priority(), c.volumeDir(), c.volumeName(), c.shardCount(), c.segmentBytes(),
                c.initialShardCapacityBytes(), c.capacityBytes(), c.statusReportInterval(), c.nodeStatusStaleAfter(),
                c.handleIdleTtl(), c.maxOpenHandles(), c.requestTimeout(), c.defaultDurability(),
                c.defaultOnGeometryChange(), c.metricsListener(), c.bootDiscoveryWindow(), c.affinityHandbackMode(),
                c.placementGraceAfterLeadership(), c.rebalanceEnabled(), c.rebalanceInterval(), c.rebalanceMinDelta(),
                c.rebalanceTolerance(), c.maxConcurrentMigrations(), c.maxMovesPerCycle(), c.migrationTimeout(),
                c.migrationChunkBytes(), c.maxSeriesBytes(), c.migrationStatusPollInterval(), c.reconcileInterval(),
                c.orphanGrace(), c.seriesObjectPrefix(), c.distributionMode(), c.weight(), c.migrationBytesPerSecond(),
                c.maxDestinationCatalogLag(), c.quotaMaxSeries(), c.quotaMaxBytes(), c.placementRules(),
                c.placementInspectTimeout(), c.volumeWriteMode());

        assertEquals(LocalCheckpointSettings.disabled(), legacy.localCheckpoint());
        assertEquals(c.volumeWriteMode(), legacy.volumeWriteMode());
        assertEquals(c.nodeId(), legacy.nodeId());
    }
}
