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

package dev.nishisan.utils.ngrid.debug;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterDebugLoggerTest {

    private NGridNode node;
    private Path tempDir;
    private Path statusFile;
    private ClusterDebugLogger logger;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("debug-logger-test");
        statusFile = tempDir.resolve("status.yaml");

        int port = allocateFreeLocalPort();
        NodeInfo info = new NodeInfo(NodeId.of("debug-node"), "127.0.0.1", port);

        node = new NGridNode(NGridConfig.builder(info)
                .queueDirectory(tempDir)
                .queueName("main-queue")
                .replicationQuorum(1) // Single node cluster
                .build());
        node.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logger != null) logger.close();
        if (node != null) node.close();
    }

    @Test
    void shouldUpdateReactiveOnResourceCreation() throws Exception {
        // Set a long interval so periodic updates don't interfere
        logger = new ClusterDebugLogger(node, statusFile, Duration.ofMinutes(1));
        logger.start();

        // Initial check
        waitForContent("main-queue");

        // Create a NEW map and verify it appears quickly (reactive update)
        node.getMap("reactive-map", String.class, String.class);
        waitForContent("reactive-map");

        // Create a NEW queue and verify
        node.getQueue("reactive-queue", String.class);
        waitForContent("reactive-queue");
    }

    private void waitForContent(String snippet) throws InterruptedException, IOException {
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(statusFile)) {
                String content = Files.readString(statusFile);
                if (content.contains(snippet)) {
                    return;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("File did not contain '" + snippet + "' within timeout");
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }
}