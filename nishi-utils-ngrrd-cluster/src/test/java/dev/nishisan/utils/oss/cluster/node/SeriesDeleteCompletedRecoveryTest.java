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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.api.DeletePrecondition;
import dev.nishisan.utils.oss.cluster.api.DeleteResult;
import dev.nishisan.utils.oss.cluster.api.DeleteStatus;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.DeleteControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.DeleteRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesInspectResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.COMMITTED;
import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.DELETED;
import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.FINISHED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code DELETE_RECOVER} que chega depois de o condutor remover o placement e antes de o {@code DELETE_FINISH}
 * ser entregue: a exclusão já concluída não tem o placement recriado nem as fases reconduzidas, com o mesmo líder
 * ou com um líder novo. O participante {@link #OWNER} é real (journal, volume e {@link SeriesLifecycleService});
 * {@link #COPY} é um participante em memória com as mesmas fases. O catálogo é um mapa compartilhado pelos
 * handlers, no papel do catálogo replicado.
 */
class SeriesDeleteCompletedRecoveryTest {

    private static final String OWNER = "owner";
    private static final String COPY = "copy";
    private static final String KEY = "s";
    private static final long HOUR = 3_600_000L;
    private static final long NOW = Instant.parse("2026-11-26T10:05:00Z").toEpochMilli();

    @TempDir
    Path base;

    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);
    private final MemoryCatalog catalog = new MemoryCatalog();
    private final CopyParticipant copy = new CopyParticipant();
    private final PhaseRpc rpc = new PhaseRpc();
    private final List<SeriesDeleteHandler> handlers = new ArrayList<>();
    /** Métricas de cada líder: o {@link SeriesLifecycleService} do nó dele. */
    private final Map<SeriesDeleteHandler, SeriesLifecycleService> lifecycles = new IdentityHashMap<>();
    private final List<SeriesLifecycleJournal> journals = new ArrayList<>();
    private BlobVolumeRegistry volumes;
    private BlobVolume volume;
    private SeriesHandleRegistry registry;
    private SeriesLifecycleJournal ownerJournal;
    private SeriesLifecycleService owner;
    private SeriesPlacement original;

    @BeforeEach
    void setUp() throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"));
        volumes = NgrrdBlob.registry().basePath(base.resolve("blob")).volume("ngrrd").build();
        volume = volumes.require("ngrrd");
        registry = new SeriesHandleRegistry(volume, "ngrrd", Duration.ofMinutes(15), 1000, clock);
        ownerJournal = journal("owner-lifecycle");
        owner = new SeriesLifecycleService(ownerJournal, volume, catalog, registry, OWNER, "series", clock, HOUR);
        registry.lifecycle(owner);
        catalog.nodes.put(OWNER, status(OWNER));
        catalog.nodes.put(COPY, status(COPY));
        original = SeriesPlacement.active(OWNER, NOW - HOUR);
        catalog.placements.put(KEY, original);
        var handle = registry.open(KEY, yaml, Ngrrd.OpenOptions.defaults());
        handle.write("in_octets", new Sample(NOW - Duration.ofDays(40).toMillis(), 42));
        handle.checkpoint();
        registry.close(KEY);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (var handler : handlers) handler.close();
        registry.close();
        volumes.close();
        for (var journal : journals) journal.close();
    }

    private SeriesLifecycleJournal journal(String directory) throws Exception {
        var journal = new SeriesLifecycleJournal(base.resolve(directory));
        journals.add(journal);
        return journal;
    }

    private static StorageNodeStatus status(String nodeId) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, 0, 0, 0, NOW, DistributionMode.COUNT, 1, 0,
                StorageCapabilities.ALL);
    }

    /** Handler de um líder com o próprio {@link SeriesLifecycleService} (métricas do nó dele). */
    private SeriesDeleteHandler leader(String nodeDirectory, AtomicBoolean isLeader) throws Exception {
        var lifecycle = new SeriesLifecycleService(journal(nodeDirectory), null, catalog, null, nodeDirectory,
                "series", clock, HOUR);
        var handler = new SeriesDeleteHandler(fakeTransport(), catalog, rpc, lifecycle, isLeader::get, () -> true,
                clock, () -> Set.of(OWNER, COPY), Duration.ofSeconds(2));
        handlers.add(handler);
        lifecycles.put(handler, lifecycle);
        return handler;
    }

    private long completed(SeriesDeleteHandler handler) {
        return metric(handler, "seriesDelete.completed");
    }

    private long metric(SeriesDeleteHandler handler, String name) {
        return lifecycles.get(handler).deleteMetrics().get(name);
    }

    private DeleteRequest request() {
        return new DeleteRequest(KEY, new DeletePrecondition(NOW - Duration.ofDays(30).toMillis()), "op-1",
                original.generationId());
    }

    private static DeleteResult recover(SeriesDeleteHandler handler, String participant, SeriesPlacement placement) {
        return (DeleteResult) handler.handle(Commands.DELETE_RECOVER, new DeleteControlRequest(KEY, placement),
                NodeId.of(participant));
    }

    @Test
    void recuperacaoNaJanelaEntreRemocaoEFinishNaoRecriaOPlacement() throws Exception {
        var handler = leader("leader", new AtomicBoolean(true));
        CountDownLatch finishReached = new CountDownLatch(1);
        CountDownLatch releaseFinish = new CountDownLatch(1);
        rpc.beforeCall = (node, command) -> {
            if (Commands.DELETE_FINISH.equals(command) && OWNER.equals(node) && finishReached.getCount() > 0) {
                finishReached.countDown();
                await(releaseFinish);
            }
        };
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<DeleteResult> client = pool.submit(() -> handler.delete(request()));
            assertTrue(finishReached.await(10, TimeUnit.SECONDS));
            // Janela: placement removido, FINISH ainda não entregue ao dono, journal dele em DELETED.
            assertTrue(catalog.placements.isEmpty());
            assertEquals(DELETED, ownerJournal.get(KEY).phase());
            SeriesPlacement journaled = ownerJournal.get(KEY).placement();
            int putsBefore = catalog.puts.size();
            AtomicReference<DeleteResult> recovered = new AtomicReference<>();
            Thread recovery = Thread.ofPlatform().start(() -> recovered.set(recover(handler, OWNER, journaled)));
            // O DELETE_RECOVER espera o driveLock da série, que o condutor segura até o fim dos FINISH.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (recovery.getState() != Thread.State.WAITING && System.nanoTime() < deadline) Thread.onSpinWait();
            assertEquals(Thread.State.WAITING, recovery.getState());
            releaseFinish.countDown();

            assertEquals(DeleteStatus.DELETED, client.get(10, TimeUnit.SECONDS).status());
            recovery.join(10_000);
            assertEquals(DeleteStatus.DELETED, recovered.get().status());
            assertEquals(putsBefore, catalog.puts.size(), "nenhum putPlacement depois da remoção");
        } finally {
            releaseFinish.countDown();
            pool.shutdownNow();
        }
        assertTrue(catalog.placements.isEmpty());
        assertEquals(1, catalog.removals.get());
        assertEquals(1, completed(handler));
        assertEquals(1, metric(handler, "seriesDelete.recoveryAttempts"));
        assertEquals(FINISHED, ownerJournal.get(KEY).phase());
        assertEquals(FINISHED, copy.phase);
        assertEquals(1, rpc.count(Commands.DELETE_APPLY, OWNER), "fases não reconduzidas");
        assertEquals(1, rpc.count(Commands.DELETE_COMMIT, OWNER), "fases não reconduzidas");
    }

    @Test
    void liderNovoNaJanelaNaoRecriaOPlacement() throws Exception {
        AtomicBoolean oldIsLeader = new AtomicBoolean(true);
        var oldLeader = leader("old-leader", oldIsLeader);
        // O líder antigo perde a liderança logo depois de remover o placement: nenhum FINISH sai dele.
        catalog.afterRemove = () -> oldIsLeader.set(false);

        assertEquals(DeleteStatus.DELETED, oldLeader.delete(request()).status());
        assertTrue(catalog.placements.isEmpty());
        assertEquals(DELETED, ownerJournal.get(KEY).phase());
        assertEquals(DELETED, copy.phase);
        assertEquals(0, rpc.count(Commands.DELETE_FINISH, OWNER) + rpc.count(Commands.DELETE_FINISH, COPY));
        int putsBefore = catalog.puts.size();
        rpc.calls.clear();

        // Líder novo, sem memória do anterior: só o catálogo replicado e os journals dos participantes.
        var newLeader = leader("new-leader", new AtomicBoolean(true));
        assertEquals(DeleteStatus.DELETED, recover(newLeader, OWNER, ownerJournal.get(KEY).placement()).status());
        assertEquals(DeleteStatus.DELETED, recover(newLeader, COPY, copy.placement).status());

        assertEquals(putsBefore, catalog.puts.size(), "o líder novo não recria o placement");
        assertTrue(catalog.placements.isEmpty());
        assertEquals(1, catalog.removals.get());
        assertEquals(1, completed(oldLeader));
        assertEquals(0, completed(newLeader), "completed conta uma vez por operação");
        assertEquals(FINISHED, ownerJournal.get(KEY).phase());
        assertEquals(FINISHED, copy.phase);
        for (String command : List.of(Commands.DELETE_PREPARE, Commands.DELETE_COMMIT, Commands.DELETE_APPLY)) {
            assertEquals(0, rpc.count(command, OWNER) + rpc.count(command, COPY), command + " não é reconduzido");
        }
    }

    @Test
    void catalogoQuePerdeuAReservaDeParticipanteNaoAplicadoAindaEConduzido() throws Exception {
        var handler = leader("leader", new AtomicBoolean(true));
        rpc.beforeCall = (node, command) -> {
            if (Commands.DELETE_APPLY.equals(command) && OWNER.equals(node))
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "apply perdido");
        };
        assertEquals(DeleteStatus.ERROR, handler.delete(request()).status());
        assertEquals(COMMITTED, ownerJournal.get(KEY).phase());
        assertEquals(DELETED, copy.phase);
        // Catálogo restaurado sem a série (perda, não remoção pelo fluxo): o dono ainda não aplicou.
        catalog.placements.clear();
        rpc.beforeCall = (node, command) -> { };
        int putsBefore = catalog.puts.size();

        // A cópia já aplicou: recebe só o FINISH, sem regravar o placement.
        assertEquals(DeleteStatus.DELETED, recover(handler, COPY, copy.placement).status());
        assertEquals(putsBefore, catalog.puts.size());
        assertEquals(FINISHED, copy.phase);

        // O dono ainda está COMMITTED: a ausência não veio do fluxo e a exclusão é conduzida até o fim.
        assertEquals(DeleteStatus.DELETED, recover(handler, OWNER, ownerJournal.get(KEY).placement()).status());
        assertEquals(putsBefore + 1, catalog.puts.size());
        assertTrue(catalog.placements.isEmpty());
        assertFalse(volume.storage().exists("series/" + KEY + ".ngrr"));
        assertEquals(FINISHED, ownerJournal.get(KEY).phase());
        assertEquals(1, completed(handler));
    }

    @Test
    void pedidoDeQuemNaoEParticipanteNaoTemEfeito() throws Exception {
        var handler = leader("leader", new AtomicBoolean(true));
        assertEquals(DeleteStatus.DELETED, handler.delete(request()).status());
        SeriesPlacement journaled = ownerJournal.get(KEY).placement();
        int putsBefore = catalog.puts.size();
        rpc.calls.clear();

        assertEquals(DeleteStatus.NOT_FOUND, recover(handler, "stranger", journaled).status());

        assertEquals(putsBefore, catalog.puts.size());
        assertTrue(rpc.calls.isEmpty());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("latch timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static Transport fakeTransport() {
        return (Transport) Proxy.newProxyInstance(Transport.class.getClassLoader(), new Class<?>[]{Transport.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Participante em memória: as mesmas transições de fase do {@link SeriesLifecycleService}, para uma série. */
    private static final class CopyParticipant {
        volatile SeriesLifecycleJournal.Phase phase;
        volatile SeriesPlacement placement;

        synchronized Object handle(String command, Object body) {
            if (Commands.SERIES_INSPECT.equals(command)) {
                boolean deleting = phase == SeriesLifecycleJournal.Phase.PREPARED || phase == COMMITTED;
                return new SeriesInspectResponse(false, false,
                        placement == null ? null : placement.generationId(), deleting);
            }
            var request = (DeleteControlRequest) body;
            switch (command) {
                case Commands.DELETE_PREPARE -> {
                    if (phase == null) { phase = SeriesLifecycleJournal.Phase.PREPARED; placement = request.placement(); }
                }
                case Commands.DELETE_COMMIT -> {
                    if (phase != DELETED && phase != FINISHED) { phase = COMMITTED; placement = request.placement(); }
                }
                case Commands.DELETE_APPLY -> {
                    if (phase != DELETED && phase != FINISHED) { phase = DELETED; placement = request.placement(); }
                }
                case Commands.DELETE_FINISH -> {
                    if (phase == DELETED && placement.deletion().operationId()
                            .equals(request.placement().deletion().operationId())) phase = FINISHED;
                }
                default -> throw new IllegalArgumentException(command);
            }
            return DeleteResult.of(DeleteStatus.DELETED, request.placement().ownerNodeId());
        }
    }

    /** Fases e sonda roteadas ao participante certo; registra cada chamada e aceita um gancho antes dela. */
    private final class PhaseRpc implements ClusterRpc {
        final List<String> calls = new CopyOnWriteArrayList<>();
        volatile BiConsumer<String, String> beforeCall = (node, command) -> { };

        long count(String command, String node) {
            return calls.stream().filter((node + " " + command)::equals).count();
        }

        @Override
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            String node = target.value();
            calls.add(node + " " + command);
            beforeCall.accept(node, command);
            if (COPY.equals(node)) return responseType.cast(copy.handle(command, body));
            if (!OWNER.equals(node)) throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, "nó desconhecido: " + node);
            if (Commands.SERIES_INSPECT.equals(command))
                return responseType.cast(owner.inspect(((SeriesCommandRequest) body).seriesKey()));
            var request = (DeleteControlRequest) body;
            return responseType.cast(switch (command) {
                case Commands.DELETE_PREPARE -> owner.prepare(request);
                case Commands.DELETE_COMMIT -> owner.commit(request);
                case Commands.DELETE_APPLY -> owner.apply(request);
                case Commands.DELETE_ABORT -> owner.abort(request);
                case Commands.DELETE_FINISH -> owner.finish(request);
                default -> throw new IllegalArgumentException(command);
            });
        }

        @Override
        public NodeId localId() {
            return NodeId.of("leader");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.of(localId());
        }
    }

    /** Catálogo em memória compartilhado pelos handlers, com o registro de cada gravação de placement. */
    private static final class MemoryCatalog implements CatalogView {
        final Map<String, SeriesPlacement> placements = new ConcurrentHashMap<>();
        final Map<String, StorageNodeStatus> nodes = new ConcurrentHashMap<>();
        final List<SeriesPlacement> puts = new CopyOnWriteArrayList<>();
        final AtomicInteger removals = new AtomicInteger();
        volatile Runnable afterRemove = () -> { };

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.ofNullable(placements.get(seriesKey));
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.ofNullable(nodes.get(nodeId));
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.copyOf(nodes.values());
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            return Map.copyOf(placements);
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            puts.add(placement);
            placements.put(seriesKey, placement);
        }

        @Override
        public void removePlacement(String seriesKey) {
            placements.remove(seriesKey);
            removals.incrementAndGet();
            afterRemove.run();
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            nodes.put(status.nodeId(), status);
        }
    }
}
