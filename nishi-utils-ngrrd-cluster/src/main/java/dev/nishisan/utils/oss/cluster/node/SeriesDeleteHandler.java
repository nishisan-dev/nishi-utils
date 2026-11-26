package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.*;
import dev.nishisan.utils.oss.cluster.catalog.*;
import dev.nishisan.utils.oss.cluster.protocol.*;
import dev.nishisan.utils.oss.cluster.rpc.*;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.*;
import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.*;

/** Leader coordinates prepare/commit/apply; local phases are durable and generation fenced. */
public final class SeriesDeleteHandler extends RequestHandlerSupport implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(SeriesDeleteHandler.class.getName());
    private final CatalogView catalog;
    private final ClusterRpc rpc;
    private final SeriesLifecycleService lifecycle;
    private final BooleanSupplier leader, synced;
    private final Clock clock;
    /** Storages alcançáveis na visão do líder; um participante fora dela fecha o gate sem RPC. */
    private final Supplier<Set<String>> reachableNodeIds;
    /** Prazo de cada inspeção do gate de criação e, com {@link #INSPECT_WAIT_SLACK}, da espera total. */
    private final Duration placementInspectTimeout;
    /** Inspeções do gate em paralelo, uma thread virtual por participante; nenhuma segura lock. */
    private final ExecutorService inspections = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("ngrrd-creation-gate-", 0).factory());
    private final Object[] driveLocks = java.util.stream.IntStream.range(0, 256).mapToObj(i -> new Object()).toArray();
    private final ScheduledExecutorService recovery = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "ngrrd-series-deletion-recovery"); thread.setDaemon(true); return thread;
    });
    private volatile boolean closed;
    /** Folga da espera total do gate sobre {@code placementInspectTimeout}, para a entrega da resposta. */
    static final Duration INSPECT_WAIT_SLACK = Duration.ofMillis(250);
    /** Janela do log com taxa limitada de {@code NGRRD_PLACEMENT_UNAVAILABLE}. */
    private static final Duration UNAVAILABLE_LOG_WINDOW = Duration.ofSeconds(10);
    private static final long NEVER_LOGGED = Long.MIN_VALUE;
    /** Instante ({@link #clock}) do último WARNING {@code NGRRD_PLACEMENT_UNAVAILABLE}. */
    private final AtomicLong unavailableLoggedAtMs = new AtomicLong(NEVER_LOGGED);
    /** Ocorrências suprimidas desde o último WARNING {@code NGRRD_PLACEMENT_UNAVAILABLE}. */
    private final AtomicLong unavailableSuppressed = new AtomicLong();
    /** Ciclos seguidos de {@code recover()} com exclusão pendente antes do primeiro WARNING. */
    static final int PENDING_CYCLES_BEFORE_WARNING = 5;
    /** Janela do log com taxa limitada de {@code NGRRD_DELETE_PENDING}. */
    static final Duration PENDING_LOG_WINDOW = Duration.ofMinutes(1);
    /** Só a thread de recuperação (ou o teste, no lugar dela) toca estes dois campos. */
    private volatile int pendingCycles;
    private volatile long pendingLoggedAtMs = NEVER_LOGGED;

    /**
     * @param reachableNodeIds        storages alcançáveis na visão do líder (o próprio nó é sempre
     *                                considerado alcançável: a inspeção local não passa pela rede)
     * @param placementInspectTimeout prazo de cada {@code ngrrd.series.inspect} do gate de criação; positivo
     */
    public SeriesDeleteHandler(Transport transport, CatalogView catalog, ClusterRpc rpc,
            SeriesLifecycleService lifecycle, BooleanSupplier leader, BooleanSupplier synced, Clock clock,
            Supplier<Set<String>> reachableNodeIds, Duration placementInspectTimeout) {
        super(transport, Set.of(Commands.SERIES_DELETE, Commands.SERIES_DELETE_BATCH, Commands.DELETE_PREPARE,
                Commands.DELETE_COMMIT, Commands.DELETE_APPLY, Commands.DELETE_ABORT, Commands.DELETE_FINISH,
                Commands.DELETE_RECOVER, Commands.SERIES_INSPECT, Commands.RECONCILE));
        this.catalog = catalog; this.rpc = rpc; this.lifecycle = lifecycle;
        this.leader = leader; this.synced = synced; this.clock = clock;
        this.reachableNodeIds = Objects.requireNonNull(reachableNodeIds, "reachableNodeIds");
        this.placementInspectTimeout = Objects.requireNonNull(placementInspectTimeout, "placementInspectTimeout");
        if (placementInspectTimeout.isNegative() || placementInspectTimeout.isZero())
            throw new IllegalArgumentException("placementInspectTimeout deve ser > 0");
    }
    public void start() { recovery.scheduleWithFixedDelay(this::recover, 1, 1, TimeUnit.SECONDS); }

    @Override protected Object handle(String command, Object body, NodeId source) {
        return switch (command) {
            case Commands.SERIES_DELETE -> delete((DeleteRequest) body);
            case Commands.SERIES_DELETE_BATCH -> {
                Map<String, DeleteResult> results = new LinkedHashMap<>();
                for (var request : ((DeleteBatchRequest) body).requests()) results.put(request.seriesKey(), delete(request));
                yield new DeleteBatchResponse(results);
            }
            case Commands.DELETE_PREPARE -> lifecycle.prepare((DeleteControlRequest) body);
            case Commands.DELETE_COMMIT -> lifecycle.commit((DeleteControlRequest) body);
            case Commands.DELETE_APPLY -> lifecycle.apply((DeleteControlRequest) body);
            case Commands.DELETE_ABORT -> lifecycle.abort((DeleteControlRequest) body);
            case Commands.DELETE_FINISH -> lifecycle.finish((DeleteControlRequest) body);
            case Commands.DELETE_RECOVER -> recoverReservation((DeleteControlRequest) body);
            case Commands.SERIES_INSPECT -> lifecycle.inspect(((SeriesCommandRequest) body).seriesKey());
            case Commands.RECONCILE -> lifecycle.reconcile((ReconcileRequest) body, rpc);
            default -> throw new IllegalArgumentException(command);
        };
    }

    /**
     * Gate de criação de série nova: protege contra dados sobreviventes não posicionados em qualquer
     * storage registrado. Participantes são os nós do catálogo que anunciam
     * {@link StorageCapabilities#SERIES_DELETE} (nós legados ficam de fora durante a atualização).
     *
     * <p>Um participante fora de {@code reachableNodeIds} fica indisponível sem RPC. Os alcançáveis são
     * inspecionados em paralelo, cada um com {@code placementInspectTimeout}, e a espera total é limitada a
     * esse prazo mais uma folga curta; timeout ou falha conta como indisponível. Precedência:
     * {@code QUARANTINED} &gt; {@code MIGRATING} (remoção em curso) &gt; {@code PLACEMENT_UNAVAILABLE}.</p>
     *
     * <p>Chamado pelo {@link PlacementRequestHandler} fora de qualquer lock de placement.</p>
     */
    public PlacementRequestHandler.CreationGateResult creationGate(String key) {
        List<String> participants = new ArrayList<>();
        for (var node : catalog.nodesLocal()) {
            if (node.capabilities().contains(StorageCapabilities.SERIES_DELETE)) participants.add(node.nodeId());
        }
        participants.sort(String::compareTo);
        if (participants.isEmpty()) return PlacementRequestHandler.CreationGateResult.OPEN;
        Set<String> reachable = reachableNodeIds.get();
        String self = rpc.localId().value();
        SortedSet<String> unavailable = new TreeSet<>();
        Map<String, Future<SeriesInspectResponse>> pending = new LinkedHashMap<>();
        for (String nodeId : participants) {
            if (!nodeId.equals(self) && !reachable.contains(nodeId)) { unavailable.add(nodeId); continue; }
            try {
                pending.put(nodeId, inspections.submit(() -> rpc.call(NodeId.of(nodeId), Commands.SERIES_INSPECT,
                        new SeriesCommandRequest(key), SeriesInspectResponse.class, placementInspectTimeout)));
            } catch (RejectedExecutionException e) {
                unavailable.add(nodeId); // handler fechado: o nó está parando
            }
        }
        boolean quarantined = false, deleting = false;
        long deadline = System.nanoTime() + placementInspectTimeout.plus(INSPECT_WAIT_SLACK).toNanos();
        try {
            for (var entry : pending.entrySet()) {
                try {
                    SeriesInspectResponse inspected = entry.getValue()
                            .get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    if (inspected == null) { unavailable.add(entry.getKey()); continue; }
                    quarantined |= inspected.quarantined();
                    deleting |= inspected.deleting();
                } catch (TimeoutException e) {
                    unavailable.add(entry.getKey());
                } catch (ExecutionException e) {
                    unavailable.add(entry.getKey());
                    LOG.log(Level.FINE, "creation gate inspection failed: series=" + key + " node=" + entry.getKey(),
                            e.getCause());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    "interrompido aguardando " + Commands.SERIES_INSPECT + " de " + key, e);
        } finally {
            // A inspeção que não respondeu a tempo é abandonada sem interrupção: a inspeção local pode estar
            // em I/O de canal de arquivo (interromper fecharia o canal), e a remota já tem o próprio prazo.
            for (var future : pending.values()) future.cancel(false);
        }
        if (quarantined) return PlacementRequestHandler.CreationGateResult.blocked(SeriesStatus.QUARANTINED);
        if (deleting) return PlacementRequestHandler.CreationGateResult.blocked(SeriesStatus.MIGRATING);
        if (!unavailable.isEmpty()) {
            logPlacementUnavailable(key, unavailable);
            return PlacementRequestHandler.CreationGateResult.unavailable(List.copyOf(unavailable));
        }
        return PlacementRequestHandler.CreationGateResult.OPEN;
    }

    /**
     * {@code NGRRD_PLACEMENT_UNAVAILABLE} com taxa limitada: no máximo um WARNING por
     * {@link #UNAVAILABLE_LOG_WINDOW}, com os nós da ocorrência atual e quantas ocorrências foram suprimidas
     * desde o último log — com um storage fora e séries novas chegando, um WARNING por PLACE inundaria o log.
     * Só contadores atômicos; o log em si sai fora de qualquer lock.
     */
    private void logPlacementUnavailable(String key, Collection<String> unavailable) {
        long now = clock.millis();
        long last = unavailableLoggedAtMs.get();
        boolean due = last == NEVER_LOGGED || now - last >= UNAVAILABLE_LOG_WINDOW.toMillis();
        if (!due || !unavailableLoggedAtMs.compareAndSet(last, now)) {
            unavailableSuppressed.incrementAndGet();
            return;
        }
        long suppressed = unavailableSuppressed.getAndSet(0);
        LOG.log(Level.WARNING, "NGRRD_PLACEMENT_UNAVAILABLE series=" + key + " nodes=" + unavailable
                + " suppressed=" + suppressed);
    }

    /** Origem de uma tentativa de exclusão: pedido de cliente ou laço de recuperação do líder. */
    enum Origin { CLIENT, RECOVERY }

    /**
     * Ponto único de contagem de {@code seriesDelete.recoveryAttempts}: toda chamada de {@code delete} ou
     * {@code recoverReservation} com origem {@link Origin#RECOVERY} passa por aqui, seja qual for o laço que a
     * dispara (hoje {@link #recover()}).
     */
    private void beginAttempt(Origin origin) {
        if (origin == Origin.RECOVERY) lifecycle.recordRecoveryAttempt();
    }

    /** Pedido de cliente: o status devolvido conta em {@code seriesDelete.requests.<STATUS>}. */
    public DeleteResult delete(DeleteRequest request) {
        DeleteResult result = delete(request, Origin.CLIENT);
        lifecycle.recordRequest(result.status());
        return result;
    }

    private DeleteResult delete(DeleteRequest request, Origin origin) {
        beginAttempt(origin);
        String key = request.seriesKey();
        try (var drive = CoordinationLocks.acquire(driveLocks[Math.floorMod(key.hashCode(), driveLocks.length)])) {
            ensureLeader();
            SeriesPlacement placement;
            try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                ensureLeader();
                var current = catalog.placementStrong(key);
                if (current.isEmpty()) return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                placement = current.get();
                if (request.generationId() != null && !request.generationId().equals(placement.generationId()))
                    return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                if (placement.state() == PlacementState.MIGRATING) return refusal(DeleteStatus.REFUSED_MIGRATING, placement.ownerNodeId());
                if (placement.deletion() == null) {
                    List<String> participants = new ArrayList<>();
                    for (var node : catalog.nodesLocal()) {
                        var status = catalog.nodeStatusStrong(node.nodeId()).orElseThrow(() -> new DeleteFailureCause.MissingNodeStatusException(node.nodeId()));
                        if (!status.capabilities().contains(StorageCapabilities.SERIES_DELETE)) {
                            lifecycle.record(DeleteStatus.ERROR);
                            lifecycle.recordAttemptFailed(DeleteFailureCause.PARTICIPANT);
                            return DeleteResult.error(ErrorCode.UNSUPPORTED_BY_NODE, node.nodeId() + " does not announce series.delete");
                        }
                        participants.add(node.nodeId());
                    }
                    if (!participants.contains(placement.ownerNodeId())) {
                        lifecycle.recordAttemptFailed(DeleteFailureCause.PARTICIPANT);
                        return DeleteResult.error(ErrorCode.UNSUPPORTED_BY_NODE, "owner status unavailable: " + placement.ownerNodeId());
                    }
                    participants.sort(String::compareTo);
                    placement = placement.withDeletion(new SeriesDeletion(request.operationId(), request.precondition().lastWriteBefore(), participants, false), clock.millis());
                    catalog.putPlacement(key, placement);
                }
            }
            return drive(key, placement);
        } catch (RuntimeException e) {
            lifecycle.record(DeleteStatus.ERROR);
            lifecycle.recordAttemptFailed(DeleteFailureCause.of(e));
            return DeleteResult.error(e instanceof NgrrdClusterException n ? n.code() : ErrorCode.REMOTE_ERROR, e.getMessage());
        }
    }

    private DeleteResult refusal(DeleteStatus status, String owner) {
        lifecycle.record(status); return DeleteResult.of(status, owner);
    }
    private void ensureLeader() {
        if (closed || !leader.getAsBoolean() || !synced.getAsBoolean())
            throw new NgrrdClusterException(ErrorCode.NO_LEADER, "deletion requires the synchronized current leader");
    }
    private DeleteResult phase(String node, String command, String key, SeriesPlacement placement) {
        ensureLeader();
        return rpc.call(NodeId.of(node), command, new DeleteControlRequest(key, placement), DeleteResult.class);
    }

    private DeleteResult drive(String key, SeriesPlacement placement) {
        if (!placement.deletion().committed()) {
            try {
                for (String node : placement.deletion().participants()) {
                    DeleteResult response = phase(node, Commands.DELETE_PREPARE, key, placement);
                    if (response.status() != DeleteStatus.DELETED) {
                        cancel(key, placement);
                        lifecycle.recordCancelled();
                        lifecycle.recordAttemptFailed(DeleteFailureCause.PARTICIPANT);
                        lifecycle.record(response.status());
                        return response;
                    }
                }
                try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                    ensureLeader();
                    var current = catalog.placementStrong(key).orElseThrow();
                    requireSame(current, placement);
                    placement = current.withDeletion(current.deletion().commit(), clock.millis());
                    catalog.putPlacement(key, placement);
                }
            } catch (RuntimeException e) {
                // Cancellation checks the authoritative phase: a lost commit response is never rolled back.
                try { cancel(key, placement); } catch (RuntimeException ignored) { }
                throw e;
            }
        }
        for (String node : placement.deletion().participants()) phase(node, Commands.DELETE_COMMIT, key, placement);
        for (String node : placement.deletion().participants()) phase(node, Commands.DELETE_APPLY, key, placement);
        try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
            ensureLeader();
            var current = catalog.placementStrong(key);
            if (current.isPresent()) {
                requireSame(current.get(), placement);
                catalog.removePlacement(key);
                lifecycle.recordCompleted();
            }
        }
        // Finish changes only the matching local tombstone. A fresh generation is never touched.
        for (String node : placement.deletion().participants()) {
            try { phase(node, Commands.DELETE_FINISH, key, placement); }
            catch (RuntimeException e) { LOG.log(Level.FINE, "tombstone finalization will retry: " + key, e); }
        }
        return DeleteResult.of(DeleteStatus.DELETED, placement.ownerNodeId());
    }

    private void requireSame(SeriesPlacement current, SeriesPlacement expected) {
        if (!current.generationId().equals(expected.generationId()) || current.deletion() == null
                || !current.deletion().operationId().equals(expected.deletion().operationId()))
            throw new DeleteFailureCause.StaleReservationException();
    }
    private void cancel(String key, SeriesPlacement placement) {
        try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
            ensureLeader();
            var current = catalog.placementStrong(key);
            if (current.isPresent()) {
                requireSame(current.get(), placement);
                if (current.get().deletion().committed()) return;
                catalog.putPlacement(key, current.get().withDeletion(null, clock.millis()));
            }
        }
        for (String node : placement.deletion().participants()) {
            try { phase(node, Commands.DELETE_ABORT, key, placement); }
            catch (RuntimeException e) { LOG.log(Level.FINE, "preparation cancellation will retry: " + key, e); }
        }
    }

    private DeleteResult recoverReservation(DeleteControlRequest request) {
        beginAttempt(Origin.RECOVERY);
        try {
            return doRecoverReservation(request);
        } catch (RuntimeException e) {
            lifecycle.recordAttemptFailed(DeleteFailureCause.of(e));
            throw e;
        }
    }

    private DeleteResult doRecoverReservation(DeleteControlRequest request) {
        String key = request.seriesKey();
        try (var drive = CoordinationLocks.acquire(driveLocks[Math.floorMod(key.hashCode(), driveLocks.length)])) {
            ensureLeader();
            SeriesPlacement placement;
            try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                ensureLeader();
                var current = catalog.placementStrong(key);
                if (current.isPresent() && !current.get().generationId().equals(request.placement().generationId()))
                    return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                placement = request.placement();
                if (current.isPresent() && current.get().deletion() != null) {
                    requireSame(current.get(), placement);
                    placement = current.get();
                } else {
                    if (placement.deletion() == null || !placement.deletion().committed())
                        return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                    catalog.putPlacement(key, placement);
                }
            }
            return drive(key, placement);
        }
    }

    /**
     * Ciclo de recuperação (a cada 1 s). Contagem de métricas: {@code seriesDelete.recoveryAttempts} sai de
     * {@link #beginAttempt}, chamado por {@code delete(request, Origin.RECOVERY)} e por
     * {@code recoverReservation}; {@code seriesDelete.pending} e o alarme {@code NGRRD_DELETE_PENDING} saem de
     * {@link #trackPending}. Quem reescrever este laço só precisa manter essas chamadas.
     */
    void recover() {
        if (closed || !synced.getAsBoolean()) return;
        try {
            for (var local : lifecycle.journal().snapshot().entrySet()) {
                var entry = local.getValue(); String key = local.getKey();
                if (entry.placement() == null || entry.placement().deletion() == null) continue;
                var current = catalog.placementStrong(key);
                if (entry.phase() == PREPARED && (current.isEmpty() || current.get().deletion() == null)) {
                    lifecycle.abort(new DeleteControlRequest(key, entry.placement()));
                } else if ((entry.phase() == COMMITTED || entry.phase() == DELETED || entry.phase() == FINISHED)
                        && (current.isPresent() || entry.phase() != FINISHED)) {
                    NodeId target = rpc.leaderId().orElse(null);
                    if (target != null) rpc.call(target, Commands.DELETE_RECOVER,
                            new DeleteControlRequest(key, entry.placement()), DeleteResult.class);
                }
            }
            if (leader.getAsBoolean()) {
                List<Map.Entry<String, SeriesPlacement>> pending = new ArrayList<>();
                for (var item : catalog.placementsLocal().entrySet()) {
                    if (item.getValue().deletion() != null) pending.add(item);
                }
                trackPending(pending);
                for (var item : pending) {
                    delete(new DeleteRequest(item.getKey(), new DeletePrecondition(item.getValue().deletion().lastWriteBefore()),
                            item.getValue().deletion().operationId()), Origin.RECOVERY);
                }
            } else {
                trackPending(List.of());
            }
        } catch (RuntimeException e) { LOG.log(Level.FINE, "series deletion recovery deferred", e); }
    }

    /**
     * Atualiza o gauge {@code seriesDelete.pending} e emite, no máximo uma vez por
     * {@link #PENDING_LOG_WINDOW}, o WARNING {@code NGRRD_DELETE_PENDING} quando há exclusão pendente por mais
     * de {@link #PENDING_CYCLES_BEFORE_WARNING} ciclos seguidos. A "mais antiga" é a de menor
     * {@code updatedAtEpochMs} do placement (instante da reserva).
     */
    private void trackPending(List<Map.Entry<String, SeriesPlacement>> pending) {
        lifecycle.pendingDeletes(pending.size());
        if (pending.isEmpty()) { pendingCycles = 0; return; }
        if (++pendingCycles <= PENDING_CYCLES_BEFORE_WARNING) return;
        long now = clock.millis();
        long last = pendingLoggedAtMs;
        if (last != NEVER_LOGGED && now - last < PENDING_LOG_WINDOW.toMillis()) return;
        pendingLoggedAtMs = now;
        String oldest = pending.stream()
                .min(Comparator.comparingLong((Map.Entry<String, SeriesPlacement> e) -> e.getValue().updatedAtEpochMs()))
                .map(e -> e.getValue().deletion().operationId()).orElse("?");
        LOG.log(Level.WARNING, "NGRRD_DELETE_PENDING count=" + pending.size() + " oldestOperationId=" + oldest);
    }

    @Override public void close() {
        closed = true; recovery.shutdownNow();
        // shutdown (não shutdownNow): as inspeções já têm prazo, e interromper a auto-inspeção durante o
        // journal.put/force fecharia o FileChannel do SeriesLifecycleJournal.
        inspections.shutdown();
        try { recovery.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
