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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ligação do {@code ngrrd.checkpoint} no {@link NgrrdStorageNode}: desligado por padrão (nenhum
 * componente nem thread), ligado quando configurado, métricas em {@code lifecycleMetrics} e parada no
 * {@code close()}. Um único nó sem peers, na suíte padrão do módulo.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class NgrrdStorageNodeLocalCheckpointTest {

    private static final String THREAD_NAME = "ngrrd-local-checkpoint";

    @Test
    void checkpointLocalVemDesligadoPorPadraoSemComponenteNemThread(@TempDir Path tempDir) throws Exception {
        try (NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, null))) {
            assertTrue(node.localCheckpointer().isEmpty());
            assertFalse(threadAlive(), "nenhum executor de checkpoint local com a opção desligada");
            Map<String, Long> metrics = node.metricsSnapshot().lifecycleMetrics();
            assertEquals(0L, metrics.get("localCheckpoint.enabled"));
            assertEquals(0L, metrics.get("localCheckpoint.cycles"));
        }
    }

    @Test
    void checkpointLocalLigadoSobeComONoEParaNoClose(@TempDir Path tempDir) throws Exception {
        LocalCheckpointSettings settings = new LocalCheckpointSettings(true, Duration.ofMillis(100), 2);
        List<String> statusLines = new CopyOnWriteArrayList<>();
        Logger reporterLogger = Logger.getLogger(NodeStatusReporter.class.getName());
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                String message = String.valueOf(record.getMessage());
                if (message.startsWith("NGRRD_NODE_STATUS")) {
                    statusLines.add(message);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        reporterLogger.addHandler(capture);
        NgrrdStorageNode node = NgrrdStorageNode.start(config(tempDir, settings));
        try {
            awaitTrue(() -> !statusLines.isEmpty());
            String line = statusLines.get(0);
            assertTrue(line.contains(" redirectCacheHits=") && line.contains(" lcEnabled=1 lcDirty="), line);
            assertTrue(line.indexOf("redirectCacheHits=") < line.indexOf("lcEnabled="),
                    "os campos novos entram só no fim da linha: " + line);
            assertTrue(node.localCheckpointer().isPresent());
            awaitTrue(() -> node.metricsSnapshot().lifecycleMetrics().get("localCheckpoint.cycles") >= 1);
            Map<String, Long> metrics = node.metricsSnapshot().lifecycleMetrics();
            assertEquals(1L, metrics.get("localCheckpoint.enabled"));
            assertTrue(metrics.containsKey("localCheckpoint.overruns"));
            assertTrue(threadAlive());
        } finally {
            node.close();
            reporterLogger.removeHandler(capture);
        }
        awaitTrue(() -> !threadAlive());
    }

    private static StorageNodeConfig config(Path tempDir, LocalCheckpointSettings settings) throws IOException {
        StorageNodeConfig.Builder builder = StorageNodeConfig.builder()
                .nodeId("storage-solo")
                .port(allocateFreeLocalPort())
                .dataDir(tempDir.resolve("data"))
                .volumeDir(tempDir.resolve("volume"))
                .shardCount(2)
                .segmentBytes(1L << 20)
                .initialShardCapacityBytes(1L << 20)
                .bootDiscoveryWindow(Duration.ZERO);
        if (settings != null) {
            builder.localCheckpoint(settings);
        }
        return builder.build();
    }

    private static boolean threadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().equals(THREAD_NAME) && thread.isAlive());
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condição não atingida em 10 s");
            }
            Thread.sleep(10);
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
