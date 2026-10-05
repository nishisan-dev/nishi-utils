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
package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 (M1) — {@link NGridNode#close()} closes and drains the persistence of every map.
 *
 * <p>Up to 8.10.0 each {@link DistributedMap}'s {@code close()} fired the callback that removed the
 * {@code MapClusterService} from {@code mapServices}, so the following loop, which iterated that same map,
 * closed no service at all: the NMap async writer (daemon thread {@code nmap-persistence}) was never
 * stopped nor drained, and writes still queued could be lost at process exit.
 */
class NGridNodeCloseDrainsMapPersistenceTest {

    private static final String MAP_NAME = "drain-map";
    private static final String WRITER_THREAD = "nmap-persistence";
    private static final int KEYS = 2_000;

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void closeDoNoDrenaOWriterDoMapaEORecarregamentoTemTodasAsEscritas() throws Exception {
        Path dir = Files.createTempDirectory("ngrid-close-drain");
        NodeInfo info = new NodeInfo(NodeId.of("drain-node"), "127.0.0.1", allocateFreeLocalPort());

        Set<Thread> writersBefore = writerThreads();
        NGridNode node = newNode(info, dir);
        node.start();
        try {
            awaitLeader(node, 20_000);
            DistributedMap<String, String> map = node.getMap(MAP_NAME, String.class, String.class);
            for (int i = 0; i < KEYS; i++) {
                map.put("k-" + i, "v-" + i);
            }
        } catch (RuntimeException | Error e) {
            node.close();
            throw e;
        }
        Set<Thread> writersOfNode = writerThreads();
        writersOfNode.removeAll(writersBefore);
        assertFalse(writersOfNode.isEmpty(), "the persistent map must have started an async writer");

        node.close();

        // close drains and stops the writer: no persistence thread of this node stays alive.
        for (Thread writer : writersOfNode) {
            writer.join(15_000);
            assertFalse(writer.isAlive(), "writer " + writer + " must terminate on node close");
        }

        // The maps drained, so close wrote the clean-shutdown marker (after the drain, 8.10.1).
        assertTrue(Files.exists(cleanMarker(dir)), "a fully drained close writes the clean-shutdown marker");

        // Reloading the directory, every write is there.
        NGridNode reloaded = newNode(new NodeInfo(info.nodeId(), "127.0.0.1", allocateFreeLocalPort()), dir);
        reloaded.start();
        try {
            awaitLeader(reloaded, 20_000);
            DistributedMap<String, String> map = reloaded.getMap(MAP_NAME, String.class, String.class);
            List<String> missing = new ArrayList<>();
            for (int i = 0; i < KEYS; i++) {
                if (!Optional.of("v-" + i).equals(map.getOptional("k-" + i))) {
                    missing.add("k-" + i);
                }
            }
            assertTrue(missing.isEmpty(), missing.size() + " writes did not survive close + reload: "
                    + missing.subList(0, Math.min(10, missing.size())));
        } finally {
            reloaded.close();
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void closeSemDrenagemDoWriterNaoGravaOMarcadorEOProximoStartFazBootstrap() throws Exception {
        Path dir = Files.createTempDirectory("ngrid-close-no-drain");
        NodeInfo info = new NodeInfo(NodeId.of("no-drain-node"), "127.0.0.1", allocateFreeLocalPort());
        NGridNode node = newNode(info, dir);
        node.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            awaitLeader(node, 20_000);
            DistributedMap<String, BlockingValue> map = node.getMap(MAP_NAME, String.class, BlockingValue.class);
            BlockingValue.arm(release);
            map.put("blocked", new BlockingValue("v"));
            assertTrue(BlockingValue.entered.await(10, TimeUnit.SECONDS),
                    "the NMap writer must be serializing the entry");

            // The writer cannot drain within the join bound: close reports it and writes no marker.
            assertThrows(IOException.class, node::close, "an undrained map persistence fails the close");
            assertFalse(Files.exists(cleanMarker(dir)), "no clean-shutdown marker over an undrained WAL");
        } finally {
            release.countDown();
            BlockingValue.disarm();
        }

        // The next start treats the shutdown as unclean (and bootstraps the topics with relay data).
        LogCapture capture = LogCapture.attach();
        NGridNode restarted = newNode(new NodeInfo(info.nodeId(), "127.0.0.1", allocateFreeLocalPort()), dir);
        try {
            restarted.start();
            assertTrue(capture.contains("Unclean relay restart detected"),
                    "without the marker the next start must detect an unclean restart");
        } finally {
            capture.detach();
            restarted.close();
        }
    }

    private static Path cleanMarker(Path dataDir) {
        return dataDir.resolve("replication").resolve("relay").resolve(".clean-shutdown");
    }

    private static NGridNode newNode(NodeInfo info, Path dir) {
        return new NGridNode(NGridConfig.builder(info)
                .dataDirectory(dir)
                .mapDirectory(dir.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.ASYNC_NO_FSYNC)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .build());
    }

    private static Set<Thread> writerThreads() {
        Set<Thread> out = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (WRITER_THREAD.equals(thread.getName()) && thread.isAlive()) {
                out.add(thread);
            }
        }
        return out;
    }

    private static void awaitLeader(NGridNode node, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()) {
                return;
            }
            Thread.sleep(50);
        }
        fail("the node did not take leadership in time");
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }

    /**
     * Map value whose Java serialization blocks on the NMap writer thread while armed (the wire codec is
     * Jackson and never calls {@code writeObject}).
     */
    public static final class BlockingValue implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        static volatile CountDownLatch gate;
        static volatile CountDownLatch entered = new CountDownLatch(1);

        public String name;

        public BlockingValue() {
        }

        BlockingValue(String name) {
            this.name = name;
        }

        static void arm(CountDownLatch release) {
            entered = new CountDownLatch(1);
            gate = release;
        }

        static void disarm() {
            gate = null;
        }

        @Serial
        private void writeObject(ObjectOutputStream out) throws IOException {
            CountDownLatch current = gate;
            if (current != null && WRITER_THREAD.equals(Thread.currentThread().getName())) {
                entered.countDown();
                try {
                    current.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            out.defaultWriteObject();
        }
    }

    /** Captures the {@link ReplicationManager} log records. */
    private static final class LogCapture extends Handler {
        private final Logger logger = Logger.getLogger(ReplicationManager.class.getName());
        private final List<LogRecord> records = new CopyOnWriteArrayList<>();

        static LogCapture attach() {
            LogCapture capture = new LogCapture();
            capture.logger.addHandler(capture);
            return capture;
        }

        void detach() {
            logger.removeHandler(this);
        }

        boolean contains(String text) {
            for (LogRecord record : records) {
                if (record.getMessage() != null && record.getMessage().contains(text)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
