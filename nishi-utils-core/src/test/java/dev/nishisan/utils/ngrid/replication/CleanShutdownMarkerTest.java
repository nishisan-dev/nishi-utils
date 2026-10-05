/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — marcador de shutdown limpo adiado para depois da drenagem dos handlers.
 *
 * <p>O {@code NGridNode} fecha os {@code MapClusterService} (e drena o writer assíncrono do NMap) depois
 * de parar o {@link ReplicationManager}. Com o marcador gravado no {@code stop()}, um kill durante essa
 * drenagem deixava marcador limpo sobre um WAL incompleto, e o próximo start não fazia o bootstrap que
 * restauraria as escritas perdidas.
 */
class CleanShutdownMarkerTest {

    private static final String TOPIC = "map:catalog";

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("clean-shutdown-marker");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void marcadorAdiadoSoEGravadoComDrenagemConcluida() throws Exception {
        ReplicationManager manager = startManager();
        manager.deferCleanShutdownMarker();
        assertFalse(manager.markCleanShutdownIfEligible(true), "antes do stop não há o que marcar");
        manager.close();

        Path relayDir = tempDir.resolve("relay");
        assertTrue(RelayStore.isUncleanRestart(relayDir), "adiado, o stop não grava o marcador");
        assertFalse(manager.markCleanShutdownIfEligible(false), "drenagem falha: sem marcador");
        assertTrue(RelayStore.isUncleanRestart(relayDir),
                "sem marcador, o próximo start trata o shutdown como sujo e faz bootstrap");
        assertTrue(manager.markCleanShutdownIfEligible(true), "drenagem concluída: grava o marcador");
        assertFalse(RelayStore.isUncleanRestart(relayDir), "com marcador, o próximo start retoma sem bootstrap");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void semAdiamentoOStopContinuaGravandoOMarcador() throws Exception {
        ReplicationManager manager = startManager();
        manager.close();
        assertFalse(RelayStore.isUncleanRestart(tempDir.resolve("relay")),
                "quem usa o ReplicationManager sem adiar o marcador mantém o comportamento anterior");
    }

    private ReplicationManager startManager() throws InterruptedException {
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(NodeId.of("solo"), "127.0.0.1", 1),
                List.of());
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 1, null).withPairMode(true),
                scheduler);
        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(false)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .build());
        manager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        manager.start();
        coordinator.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (!coordinator.isLeader()) {
            if (System.currentTimeMillis() > deadline) {
                fail("o nó sozinho deve assumir a liderança");
            }
            Thread.sleep(25);
        }
        return manager;
    }
}
