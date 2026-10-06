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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * O {@code ngrrd.volume.writeMode} do {@link StorageNodeConfig} chega ao blob volume local do nó.
 * Um único nó sem peers sobe rápido, então roda na suíte padrão do módulo (não é {@code *ClusterTest}).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class NgrrdStorageNodeWriteModeTest {

    @Test
    void modoPwriteDaConfiguracaoChegaAoVolumeDoNo(@TempDir Path tempDir) throws Exception {
        try (NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, VolumeWriteMode.PWRITE))) {
            assertEquals(VolumeWriteMode.PWRITE, node.volume().storage().writeMode());
            assertEquals(VolumeWriteMode.PWRITE, node.volume().stats().writeMode());
        }
    }

    @Test
    void semModoExplicitoOVolumeDoNoUsaMmap(@TempDir Path tempDir) throws Exception {
        try (NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, null))) {
            assertEquals(VolumeWriteMode.MMAP, node.volume().storage().writeMode());
        }
    }

    private static StorageNodeConfig config(Path tempDir, VolumeWriteMode mode) throws IOException {
        StorageNodeConfig.Builder builder = StorageNodeConfig.builder()
                .nodeId("storage-solo")
                .port(allocateFreeLocalPort())
                .dataDir(tempDir.resolve("data"))
                .volumeDir(tempDir.resolve("volume"))
                .shardCount(2)
                .segmentBytes(1L << 20)
                .initialShardCapacityBytes(1L << 20)
                .bootDiscoveryWindow(Duration.ZERO);
        if (mode != null) {
            builder.volumeWriteMode(mode);
        }
        return builder.build();
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
