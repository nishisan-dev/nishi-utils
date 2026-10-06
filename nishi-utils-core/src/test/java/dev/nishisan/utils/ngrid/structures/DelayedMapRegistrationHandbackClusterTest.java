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
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cluster de três nós (8.11.2) reproduzindo o incidente do CTP: o nó de maior afinidade reinicia e um
 * dos seus mapas configurados demora a registrar (o seam {@code mapRegistrationBarrier} segura o
 * {@code createMapService}); enquanto isso o coordenador está vivo e o incumbente serve. Com escrita
 * contínua no mapa:
 * <ul>
 *   <li>o nó não fica elegível nem pede handback antes de registrar o último mapa (gate de prontidão);</li>
 *   <li>nenhuma fronteira do incumbente diminui;</li>
 *   <li>o terceiro nó não termina à frente do novo líder;</li>
 *   <li>todo put confirmado está presente nos três nós;</li>
 *   <li>há um único HANDBACK_REQUEST na tentativa.</li>
 * </ul>
 * O caso de nó único cobre o gate isoladamente: com o mapa preso, o nó não é elegível e não lidera.
 */
class DelayedMapRegistrationHandbackClusterTest {

    private static final String CATALOG_MAP = "catalog";
    private static final String NODES_MAP = "nodes";
    private static final String CATALOG_TOPIC = MapClusterService.TOPIC_PREFIX + CATALOG_MAP;
    private static final String NODES_TOPIC = MapClusterService.TOPIC_PREFIX + NODES_MAP;
    private static final int WARMUP_OPS = 20;

    private static final String LOCAL_ONLY_MAP = "local-only";
    private static final String LOCAL_ONLY_TOPIC = MapClusterService.TOPIC_PREFIX + LOCAL_ONLY_MAP;
    private static final List<String> SHARED_MAPS = List.of(CATALOG_MAP, NODES_MAP);
    private static final int LOCAL_ONLY_OPS = 7;

    private final List<NGridNode> running = new ArrayList<>();
    /** HANDBACK_REQUESTs que chegam ao incumbente B (contados no transporte de B). */
    private final AtomicInteger handbackRequests = new AtomicInteger();
    /** Marcadores de divergência do vetor emitidos por qualquer nó do processo. */
    private final List<String> mismatchMarkers = new CopyOnWriteArrayList<>();
    // Referência forte: o LogManager guarda os loggers por referência fraca.
    private final Logger managerLogger = Logger.getLogger(ReplicationManager.class.getName());
    private Handler markerCapture;

    @BeforeEach
    void setUp() {
        markerCapture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                String message = record.getMessage();
                if (message != null && message.contains("NGRID_HANDBACK_VECTOR_MISMATCH")) {
                    mismatchMarkers.add(message);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        managerLogger.addHandler(markerCapture);
    }

    @AfterEach
    void tearDown() {
        managerLogger.removeHandler(markerCapture);
        for (NGridNode node : running) {
            closeQuietly(node);
        }
        running.clear();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void handbackComMapaRegistradoTardeNaoRebaixaOIncumbenteNemDeixaOTerceiroNoAFrente() throws Exception {
        Set<Integer> ports = new HashSet<>();
        NodeInfo infoA = new NodeInfo(NodeId.of("delay-a"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 100);
        NodeInfo infoB = new NodeInfo(NodeId.of("delay-b"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 50);
        NodeInfo infoC = new NodeInfo(NodeId.of("delay-c"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 10);
        Path base = Files.createTempDirectory("ngrid-delayed-map");
        Path dirA = Files.createDirectories(base.resolve("a"));
        Path dirB = Files.createDirectories(base.resolve("b"));
        Path dirC = Files.createDirectories(base.resolve("c"));

        // B e C sobem sem A; B lidera e aquece os dois mapas.
        NGridNode b = start(newNode(infoB, dirB, name -> { }, infoA, infoC));
        NGridNode c = start(newNode(infoC, dirC, name -> { }, infoA, infoB));
        awaitReadyLeader(b, 60_000);
        b.transport().addListener(new TransportListener() {
            @Override
            public void onPeerConnected(NodeInfo peer) {
            }

            @Override
            public void onPeerDisconnected(NodeId peerId) {
            }

            @Override
            public void onMessage(ClusterMessage message) {
                if (message.type() == MessageType.HANDBACK_REQUEST) {
                    handbackRequests.incrementAndGet();
                }
            }
        });
        DistributedMap<String, String> catalogB = catalog(b);
        DistributedMap<String, String> nodesB = nodes(b);
        for (int i = 0; i < WARMUP_OPS; i++) {
            putWithRetry(catalogB, "warm-" + i, "v");
            putWithRetry(nodesB, "warm-" + i, "v");
        }
        awaitFrontier(c, CATALOG_TOPIC, frontier(b, CATALOG_TOPIC), 30_000);
        awaitFrontier(c, NODES_TOPIC, frontier(b, NODES_TOPIC), 30_000);

        // A (maior afinidade) sobe com o registro do catálogo preso no seam.
        CountDownLatch catalogBlocked = new CountDownLatch(1);
        CountDownLatch releaseCatalog = new CountDownLatch(1);
        NGridNode a = newNode(infoA, dirA, name -> {
            if (CATALOG_MAP.equals(name)) {
                catalogBlocked.countDown();
                awaitLatch(releaseCatalog);
            }
        }, infoB, infoC);
        running.add(a);
        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        Thread starter = new Thread(() -> {
            try {
                a.start();
            } catch (Throwable t) {
                startFailure.set(t);
            }
        }, "delayed-start-a");
        starter.start();
        assertTrue(catalogBlocked.await(60, TimeUnit.SECONDS), "o seam deve prender o registro do catálogo");

        // Escrita contínua no catálogo via B (confirmações registradas) e monitor das fronteiras de B.
        Set<String> confirmed = ConcurrentHashMap.newKeySet();
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            int i = 0;
            try {
                while (writing.get()) {
                    String key = "live-" + i++;
                    putWithRetry(catalogB, key, "v");
                    confirmed.add(key);
                    Thread.sleep(5);
                }
            } catch (Throwable t) {
                writerFailure.set(t);
            }
        }, "catalog-writer");
        writer.start();
        FrontierMonitor monitorB = new FrontierMonitor(b);
        monitorB.start();

        // Enquanto o catálogo não registra: A não é elegível, não pede handback e B segue líder.
        long gateDeadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < gateDeadline) {
            assertFalse(a.replicationManager().isLeadershipEligible(),
                    "A não pode ser elegível com um mapa configurado por registrar");
            assertFalse(a.coordinator().isLeader(), "A não pode liderar com handler set parcial");
            assertEquals(0, handbackRequests.get(), "A não pode pedir handback com handler set parcial");
            assertTrue(b.coordinator().isLeader(), "B segue líder enquanto A não está pronto");
            Thread.sleep(50);
        }

        // O catálogo registra: A fica pronto, pede o handback e assume.
        releaseCatalog.countDown();
        starter.join(TimeUnit.SECONDS.toMillis(60));
        assertFalse(starter.isAlive(), "o start de A deve concluir após liberar o seam");
        if (startFailure.get() != null) {
            throw new AssertionError("start de A falhou", startFailure.get());
        }
        awaitCondition(() -> a.coordinator().isLeader(), 120_000, 20L,
                () -> "A não assumiu via handback (líder visto por A: "
                        + a.coordinator().leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
        awaitReadyLeader(a, 60_000);
        Thread.sleep(1_000); // mais escrita já com A líder
        writing.set(false);
        writer.join(TimeUnit.SECONDS.toMillis(60));
        if (writerFailure.get() != null) {
            throw new AssertionError("o writer falhou", writerFailure.get());
        }
        awaitFollowerOf(b, a, 30_000);
        awaitFollowerOf(c, a, 30_000);

        // Todo put confirmado está nos três nós.
        for (NGridNode node : List.of(a, b, c)) {
            DistributedMap<String, String> map = catalog(node);
            awaitCondition(() -> missing(map, confirmed).isEmpty(), 60_000, 100L,
                    () -> "nó " + node.transport().local().nodeId() + " sem as chaves confirmadas: "
                            + missing(map, confirmed));
        }
        monitorB.stop();
        assertTrue(monitorB.decreases.isEmpty(), "fronteiras de B diminuíram: " + monitorB.decreases);

        // O terceiro nó não fica à frente do novo líder.
        for (String topic : List.of(CATALOG_TOPIC, NODES_TOPIC)) {
            awaitFrontier(c, topic, frontier(a, topic), 30_000);
            assertTrue(frontier(c, topic) <= frontier(a, topic),
                    "C à frente de A em " + topic + ": " + frontier(c, topic) + " > " + frontier(a, topic));
            assertTrue(c.replicationManager().getRelayStreamCursor(topic) <= frontier(a, topic),
                    "cursor de C acima da fronteira de A em " + topic);
            assertTrue(frontier(b, topic) <= frontier(a, topic),
                    "B à frente de A em " + topic + ": " + frontier(b, topic) + " > " + frontier(a, topic));
        }
        assertEquals(1, handbackRequests.get(), "um único HANDBACK_REQUEST na tentativa");
    }

    /**
     * Forma do incidente: o nó de maior afinidade reinicia com o diretório de dados ANTIGO (fronteira de
     * disco abaixo da do líder, que seguiu escrevendo) e com um mapa configurado que o incumbente não
     * serve. O GRANT só nomeia os tópicos do incumbente; o candidato instala exatamente esses, não arma o
     * mapa local, envia no vetor só o que instalou e o incumbente não reancora nada fora do vetor
     * congelado — nenhuma fronteira diminui, nenhum marcador de divergência, um único REQUEST.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void reinicioComDiscoAntigoEMapaLocalExtraInstalaOsTopicosDoGrantSemRebaixarNinguem() throws Exception {
        Set<Integer> ports = new HashSet<>();
        NodeInfo infoA = new NodeInfo(NodeId.of("stale-a"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 100);
        NodeInfo infoB = new NodeInfo(NodeId.of("stale-b"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 50);
        NodeInfo infoC = new NodeInfo(NodeId.of("stale-c"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 10);
        Path base = Files.createTempDirectory("ngrid-stale-disk");
        Path dirA = Files.createDirectories(base.resolve("a"));
        Path dirB = Files.createDirectories(base.resolve("b"));
        Path dirC = Files.createDirectories(base.resolve("c"));
        List<String> mapsOfA = List.of(CATALOG_MAP, NODES_MAP, LOCAL_ONLY_MAP);

        // (1) Os três sobem; A lidera e aquece catalog, nodes e o seu mapa local.
        NGridNode a = start(newNode(infoA, dirA, name -> { }, mapsOfA, infoB, infoC));
        NGridNode b = start(newNode(infoB, dirB, name -> { }, SHARED_MAPS, infoA, infoC));
        NGridNode c = start(newNode(infoC, dirC, name -> { }, SHARED_MAPS, infoA, infoB));
        awaitReadyLeader(a, 60_000);
        b.transport().addListener(new TransportListener() {
            @Override
            public void onPeerConnected(NodeInfo peer) {
            }

            @Override
            public void onPeerDisconnected(NodeId peerId) {
            }

            @Override
            public void onMessage(ClusterMessage message) {
                if (message.type() == MessageType.HANDBACK_REQUEST) {
                    handbackRequests.incrementAndGet();
                }
            }
        });
        DistributedMap<String, String> localOnlyA = a.getMap(LOCAL_ONLY_MAP, String.class, String.class);
        for (int i = 0; i < WARMUP_OPS; i++) {
            putWithRetry(catalog(a), "warm-" + i, "v");
            putWithRetry(nodes(a), "warm-" + i, "v");
        }
        for (int i = 0; i < LOCAL_ONLY_OPS; i++) {
            putWithRetry(localOnlyA, "local-" + i, "v");
        }
        awaitFrontier(b, CATALOG_TOPIC, frontier(a, CATALOG_TOPIC), 30_000);
        awaitFrontier(c, CATALOG_TOPIC, frontier(a, CATALOG_TOPIC), 30_000);
        awaitFrontier(b, NODES_TOPIC, frontier(a, NODES_TOPIC), 30_000);
        awaitFrontier(c, NODES_TOPIC, frontier(a, NODES_TOPIC), 30_000);

        // (2) A sai de forma graciosa; B assume e segue escrevendo: a fronteira de disco de A fica para trás.
        closeQuietly(a);
        running.remove(a);
        awaitReadyLeader(b, 60_000);
        awaitFollowerOf(c, b, 30_000);
        DistributedMap<String, String> catalogB = catalog(b);
        Set<String> confirmed = ConcurrentHashMap.newKeySet();
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            int i = 0;
            try {
                while (writing.get()) {
                    String key = "live-" + i++;
                    putWithRetry(catalogB, key, "v");
                    confirmed.add(key);
                    Thread.sleep(5);
                }
            } catch (Throwable t) {
                writerFailure.set(t);
            }
        }, "catalog-writer-stale");
        writer.start();
        FrontierMonitor monitorB = new FrontierMonitor(b);
        monitorB.start();
        Thread.sleep(1_000);
        assertTrue(confirmed.size() > 10, "B deve ter avançado o catálogo enquanto A estava fora");

        // (3) A volta com o disco antigo; o handback instala os tópicos do GRANT e A assume.
        NGridNode aBack = start(newNode(infoA, dirA, name -> { }, mapsOfA, infoB, infoC));
        awaitCondition(() -> aBack.coordinator().isLeader(), 120_000, 20L,
                () -> "A não assumiu via handback (líder visto por A: "
                        + aBack.coordinator().leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
        awaitReadyLeader(aBack, 60_000);
        Thread.sleep(1_000); // mais escrita já com A líder
        writing.set(false);
        writer.join(TimeUnit.SECONDS.toMillis(60));
        if (writerFailure.get() != null) {
            throw new AssertionError("o writer falhou", writerFailure.get());
        }
        awaitFollowerOf(b, aBack, 30_000);
        awaitFollowerOf(c, aBack, 30_000);

        for (NGridNode node : List.of(aBack, b, c)) {
            DistributedMap<String, String> map = catalog(node);
            awaitCondition(() -> missing(map, confirmed).isEmpty(), 60_000, 100L,
                    () -> "nó " + node.transport().local().nodeId() + " sem as chaves confirmadas: "
                            + missing(map, confirmed));
        }
        monitorB.stop();
        assertTrue(monitorB.decreases.isEmpty(), "fronteiras de B diminuíram: " + monitorB.decreases);
        for (String topic : List.of(CATALOG_TOPIC, NODES_TOPIC)) {
            awaitFrontier(c, topic, frontier(aBack, topic), 30_000);
            assertTrue(frontier(c, topic) <= frontier(aBack, topic), "C à frente de A em " + topic);
            assertTrue(c.replicationManager().getRelayStreamCursor(topic) <= frontier(aBack, topic),
                    "cursor de C acima da fronteira de A em " + topic);
            assertTrue(frontier(b, topic) <= frontier(aBack, topic), "B à frente de A em " + topic);
        }
        // O mapa que só A serve não entrou no handback: nem armado, nem reancorado, nem perdido.
        DistributedMap<String, String> localOnlyBack = aBack.getMap(LOCAL_ONLY_MAP, String.class, String.class);
        for (int i = 0; i < LOCAL_ONLY_OPS; i++) {
            assertTrue(localOnlyBack.getOptional("local-" + i, Consistency.EVENTUAL).isPresent(),
                    "o mapa local de A perdeu local-" + i);
        }
        assertEquals(LOCAL_ONLY_OPS, frontier(aBack, LOCAL_ONLY_TOPIC), "fronteira do mapa local intocada");
        assertFalse(b.replicationManager().appliedFrontiers().byTopic().containsKey(LOCAL_ONLY_TOPIC),
                "B não ganha um tópico que não serve");
        assertEquals(1, handbackRequests.get(), "um único HANDBACK_REQUEST na tentativa");
        assertTrue(mismatchMarkers.isEmpty(), "nenhum marcador de divergência: " + mismatchMarkers);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void noSozinhoComMapaPresoNaoFicaElegivelNemLideraAteOUltimoMapaRegistrar() throws Exception {
        Set<Integer> ports = new HashSet<>();
        NodeInfo info = new NodeInfo(NodeId.of("gate-a"), "127.0.0.1", allocateFreeLocalPort(ports), Set.of(), 100);
        Path dir = Files.createTempDirectory("ngrid-gate-single");
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        NGridNode node = newNode(info, dir, name -> {
            if (CATALOG_MAP.equals(name)) {
                blocked.countDown();
                awaitLatch(release);
            }
        });
        running.add(node);
        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        Thread starter = new Thread(() -> {
            try {
                node.start();
            } catch (Throwable t) {
                startFailure.set(t);
            }
        }, "gate-start");
        starter.start();
        assertTrue(blocked.await(60, TimeUnit.SECONDS), "o seam deve prender o registro do catálogo");

        long deadline = System.currentTimeMillis() + 1_500;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(node.replicationManager().isLeadershipEligible(), "mapa preso: não elegível");
            assertFalse(node.coordinator().isLeader(), "mapa preso: não lidera");
            Thread.sleep(50);
        }
        release.countDown();
        starter.join(TimeUnit.SECONDS.toMillis(60));
        if (startFailure.get() != null) {
            throw new AssertionError("start falhou", startFailure.get());
        }
        assertTrue(node.replicationManager().isLeadershipEligible(), "último mapa registrado: elegível");
        awaitCondition(() -> node.coordinator().isLeader(), 60_000, 50L, () -> "o nó sozinho deve liderar depois de pronto");
    }

    // ---- apoio ----

    /** Registra qualquer diminuição da fronteira aplicada por tópico de um nó. */
    private static final class FrontierMonitor {
        private final NGridNode node;
        private final Map<String, Long> highest = new ConcurrentHashMap<>();
        final List<String> decreases = new ArrayList<>();
        private final AtomicBoolean active = new AtomicBoolean(true);
        private Thread thread;

        FrontierMonitor(NGridNode node) {
            this.node = node;
        }

        void start() {
            thread = new Thread(() -> {
                while (active.get()) {
                    Map<String, Long> current = node.replicationManager().appliedFrontiers().byTopic();
                    current.forEach((topic, frontier) -> {
                        Long previous = highest.get(topic);
                        if (previous != null && frontier < previous) {
                            synchronized (decreases) {
                                decreases.add(topic + ": " + previous + " -> " + frontier);
                            }
                        }
                        highest.merge(topic, frontier, Math::max);
                    });
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "frontier-monitor");
            thread.setDaemon(true);
            thread.start();
        }

        void stop() throws InterruptedException {
            active.set(false);
            thread.join(5_000);
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(120, TimeUnit.SECONDS)) {
                throw new IllegalStateException("seam latch not released in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted on the seam latch", e);
        }
    }

    private static DistributedMap<String, String> catalog(NGridNode node) {
        return node.getMap(CATALOG_MAP, String.class, String.class);
    }

    private static DistributedMap<String, String> nodes(NGridNode node) {
        return node.getMap(NODES_MAP, String.class, String.class);
    }

    private static List<String> missing(DistributedMap<String, String> map, Set<String> keys) {
        List<String> missing = new ArrayList<>();
        for (String key : keys) {
            if (map.getOptional(key, Consistency.EVENTUAL).isEmpty()) {
                missing.add(key);
            }
        }
        return missing;
    }

    private NGridNode start(NGridNode node) {
        node.start();
        running.add(node);
        return node;
    }

    private static NGridNode newNode(NodeInfo self, Path dir, java.util.function.Consumer<String> barrier,
            NodeInfo... peers) {
        return newNode(self, dir, barrier, SHARED_MAPS, peers);
    }

    private static NGridNode newNode(NodeInfo self, Path dir, java.util.function.Consumer<String> barrier,
            List<String> maps, NodeInfo... peers) {
        NGridConfig.Builder builder = NGridConfig.builder(self)
                .dataDirectory(dir)
                .replicationFactor(1)
                .replicationOperationTimeout(Duration.ofSeconds(10))
                .heartbeatInterval(Duration.ofMillis(200))
                .minClusterSize(1)
                .bootDiscoveryWindow(Duration.ofSeconds(2))
                .affinityHandbackMode(true)
                .handoverMaxDuration(Duration.ofSeconds(30))
                .handoverSnapshotTimeout(Duration.ofSeconds(30))
                .handoverRequestTimeout(Duration.ofSeconds(5))
                .handoverCooldown(Duration.ofSeconds(3));
        for (String mapName : maps) {
            // Persistentes: um reinício com o mesmo diretório recupera conteúdo e fronteira de disco.
            builder.addMap(MapConfig.builder(mapName).persistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC).build());
        }
        for (NodeInfo peer : peers) {
            builder.addPeer(peer);
        }
        return new NGridNode(builder.build(), barrier);
    }

    private static void putWithRetry(DistributedMap<String, String> map, String key, String value)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                map.put(key, value);
                return;
            } catch (RuntimeException e) {
                last = e; // líder em transição, drenando ou congelado pelo handback: tenta de novo
                Thread.sleep(5);
            }
        }
        throw new AssertionError("put de " + key + " não concluiu em 30 s", last);
    }

    private static long frontier(NGridNode node, String topic) {
        ReplicationManager.TopicReplicationStatus status = node.replicationManager().getTopicReplicationStatuses()
                .get(topic);
        return status == null ? -1L : status.nextExpectedSequence() - 1L;
    }

    private static void awaitFrontier(NGridNode node, String topic, long target, long timeoutMs)
            throws InterruptedException {
        awaitCondition(() -> frontier(node, topic) >= target, timeoutMs, 100L,
                () -> "nó " + node.transport().local().nodeId() + " não aplicou " + topic + " até " + target
                        + " (fronteira=" + frontier(node, topic) + ")");
    }

    private static void awaitReadyLeader(NGridNode node, long timeoutMs) throws InterruptedException {
        awaitCondition(() -> node.coordinator().isLeader() && !node.replicationManager().isLeaderSyncing()
                        && !node.replicationManager().isHandoverFreezing(), timeoutMs, 100L,
                () -> "nó " + node.transport().local().nodeId() + " não virou líder pronto (líder visto: "
                        + node.coordinator().leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
    }

    private static void awaitFollowerOf(NGridNode follower, NGridNode leader, long timeoutMs)
            throws InterruptedException {
        NodeId leaderId = leader.transport().local().nodeId();
        awaitCondition(() -> follower.coordinator().leaderInfo().map(NodeInfo::nodeId)
                        .filter(leaderId::equals).isPresent()
                        && !follower.coordinator().isLeader()
                        && follower.replicationManager().isStreaming(CATALOG_TOPIC)
                        && follower.replicationManager().isStreaming(NODES_TOPIC), timeoutMs, 100L,
                () -> "nó " + follower.transport().local().nodeId() + " não passou a seguir " + leaderId);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs, long pollMs,
            Supplier<String> message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(pollMs);
        }
        fail(message.get());
    }

    private static void closeQuietly(NGridNode node) {
        try {
            node.close();
        } catch (IOException | RuntimeException ignored) {
            // teardown best-effort
        }
    }

    private static int allocateFreeLocalPort(Set<Integer> used) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && used.add(port)) {
                    return port;
                }
            }
        }
        throw new IOException("no free local port");
    }
}
