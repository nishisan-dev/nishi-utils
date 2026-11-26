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
    /** Primeiro intervalo do backoff da recuperação, depois da primeira tentativa falha de uma operação. */
    static final Duration RECOVERY_BACKOFF_INITIAL = Duration.ofSeconds(1);
    /** Teto do backoff da recuperação: toda exclusão pendente é tentada de novo pelo menos a esta cadência. */
    static final Duration RECOVERY_BACKOFF_MAX = Duration.ofSeconds(60);
    /**
     * Período da reconstrução do índice de pendentes do catálogo enquanto este nó segue líder. Cobre a
     * escrita que chega à réplica do líder sem passar pelo {@code CatalogService} dele (ex.: a de um líder
     * antigo, encaminhada depois da reconstrução feita ao assumir).
     */
    static final Duration PENDING_INDEX_RESYNC = Duration.ofSeconds(60);
    private static final long NEVER_REBUILT = Long.MIN_VALUE;
    /**
     * Instante ({@link #clock}) da última reconstrução do índice de pendentes; {@link #NEVER_REBUILT} força
     * a próxima. Volta a {@link #NEVER_REBUILT} sempre que o nó deixa de ser líder sincronizado, para que
     * o novo mandato comece de uma varredura completa. Só a thread de recuperação (ou o teste) toca.
     */
    private volatile long pendingIndexRebuiltAtMs = NEVER_REBUILT;
    /**
     * Backoff da recuperação por série e {@code operationId}. Uma entrada só existe enquanto a operação está
     * pendente: sai quando a tentativa termina ({@code DELETED}/{@code NOT_FOUND}), quando a reserva some
     * do catálogo e quando o nó deixa de ser líder; o tamanho é limitado ao número de pendentes.
     */
    private final Map<RecoveryKey, RecoveryBackoff> recoveryBackoff = new ConcurrentHashMap<>();
    /**
     * Backoff do reenvio de {@code DELETE_RECOVER} por quem tem a entrada no journal, por série e
     * {@code operationId},
     * com a mesma progressão de {@link #recoveryBackoff}. Todo envio avança o intervalo, qualquer que seja a
     * resposta: a entrada que continua candidata depois de uma resposta não tem nada novo a pedir. Sai quando
     * a entrada do journal deixa de ser candidata ao reenvio.
     */
    private final Map<RecoveryKey, RecoveryBackoff> journalRecoveryBackoff = new ConcurrentHashMap<>();
    /**
     * Entradas {@code FINISHED} do journal já resolvidas, por série: geração e operação da entrada que, numa
     * leitura do catálogo, não tinha mais o que pedir ao líder. {@link #recoverJournal()} não lê o catálogo de
     * novo para elas; o histórico de exclusões concluídas custa uma leitura por entrada por processo, e não uma
     * por ciclo. Só em memória: depois de um restart, cada uma é conferida uma vez. Sai quando a entrada do
     * journal muda.
     */
    private final Map<String, ResolvedFinish> resolvedFinished = new ConcurrentHashMap<>();
    /** Janela do log com taxa limitada de falha do ciclo de recuperação. */
    static final Duration RECOVERY_LOG_WINDOW = Duration.ofMinutes(1);
    /** Instante ({@link #clock}) do último log de falha do ciclo de recuperação. */
    private final AtomicLong recoveryLoggedAtMs = new AtomicLong(NEVER_LOGGED);
    /** Falhas do ciclo de recuperação suprimidas desde o último log. */
    private final AtomicLong recoverySuppressed = new AtomicLong();

    /** Falhas seguidas da recuperação de uma operação e o instante a partir do qual ela é tentada de novo. */
    private record RecoveryBackoff(int failures, long nextAttemptAtMs) { }

    /**
     * Chave do backoff: série e operação. O {@code operationId} vem do cliente e não precisa ser único entre
     * séries (um lote pode reusar o mesmo), então só ele não identifica a reserva.
     */
    private record RecoveryKey(String seriesKey, String operationId) { }

    /** Geração e operação de uma entrada {@code FINISHED} já resolvida ({@link #resolvedFinished}). */
    private record ResolvedFinish(String generationId, String operationId) { }

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
            case Commands.DELETE_RECOVER -> recoverReservation((DeleteControlRequest) body, source);
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
                // Reserva vista pelo líder: entra já no índice de recuperação, sem esperar a reconstrução.
                if (placement.deletion() != null) catalog.notePendingDeletion(key);
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

    private DeleteResult recoverReservation(DeleteControlRequest request, NodeId source) {
        beginAttempt(Origin.RECOVERY);
        try {
            return doRecoverReservation(request, source);
        } catch (RuntimeException e) {
            lifecycle.recordAttemptFailed(DeleteFailureCause.of(e));
            throw e;
        }
    }

    /**
     * {@code DELETE_RECOVER} de um participante ({@code source}) com a reserva confirmada do journal dele.
     *
     * <ul>
     *   <li>Placement com a reserva: conduz o que falta.</li>
     *   <li>Placement de outra geração: a do pedido já foi substituída, e o participante recebe só o
     *       {@code DELETE_FINISH} idempotente ({@link #finishSuperseded}).</li>
     *   <li>Placement da mesma geração sem reserva (catálogo que regrediu): regrava a reserva confirmada e conduz.</li>
     *   <li>Placement ausente: nunca é regravado só por causa do pedido. A remoção do placement é o marcador
     *       replicado de conclusão ({@link #drive} só remove depois do {@code DELETE_APPLY} de todos), então o
     *       participante é sondado: se já aplicou, recebe só o {@code DELETE_FINISH} idempotente
     *       ({@link #recoverWithoutPlacement}).</li>
     * </ul>
     */
    private DeleteResult doRecoverReservation(DeleteControlRequest request, NodeId source) {
        String key = request.seriesKey();
        try (var drive = CoordinationLocks.acquire(driveLocks[Math.floorMod(key.hashCode(), driveLocks.length)])) {
            ensureLeader();
            SeriesPlacement placement;
            boolean superseded = false;
            try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
                ensureLeader();
                var current = catalog.placementStrong(key);
                placement = request.placement();
                if (current.isPresent() && !current.get().generationId().equals(placement.generationId())) {
                    superseded = true;
                } else if (current.isPresent() && current.get().deletion() != null) {
                    catalog.notePendingDeletion(key);
                    requireSame(current.get(), placement);
                    placement = current.get();
                } else {
                    if (placement.deletion() == null || !placement.deletion().committed())
                        return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
                    if (current.isEmpty()) placement = null;
                    else catalog.putPlacement(key, placement);
                }
            }
            if (superseded) return finishSuperseded(key, request.placement(), source);
            if (placement == null) return recoverWithoutPlacement(key, request.placement(), source);
            return drive(key, placement);
        }
    }

    /**
     * {@code DELETE_RECOVER} de uma geração que o catálogo já substituiu por outra. Chamado sob o
     * {@code driveLock} da série, depois da leitura forte que viu a geração nova.
     *
     * <p>Só {@link #drive} remove um placement, e só depois do {@code DELETE_APPLY} de todos; uma geração nova
     * só nasce com a anterior fora do catálogo. Então a operação do pedido já concluiu: o participante
     * ({@code source}) recebe o {@code DELETE_FINISH} idempotente, que só muda o tombstone daquela geração e
     * operação, e a resposta é {@code NOT_FOUND}, sem placement gravado e sem contar {@code completed}. Sem o
     * FINISH, o participante que ficou em {@code DELETED} (FINISH perdido na janela entre a remoção e a
     * criação da geração nova) pediria a recuperação para sempre.</p>
     */
    private DeleteResult finishSuperseded(String key, SeriesPlacement placement, NodeId source) {
        if (source != null && placement.deletion() != null
                && placement.deletion().participants().contains(source.value()))
            phase(source.value(), Commands.DELETE_FINISH, key, placement);
        return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
    }

    /**
     * Reserva confirmada cujo placement não está no catálogo. Chamado sob o {@code driveLock} da série, depois da
     * leitura forte que viu o placement ausente.
     *
     * <p>{@link #drive} só remove o placement depois de todo participante responder ao {@code DELETE_APPLY}, e o
     * participante grava {@code DELETED} no journal antes de responder; o journal de uma geração não volta de
     * {@code DELETED}/{@code FINISHED} para {@code COMMITTED}. Então o participante que, sondado depois da leitura,
     * já não está em exclusão naquela geração não tem fase a reconduzir: recebe só o {@code DELETE_FINISH} e a
     * resposta é {@code DELETED}, sem placement regravado e sem contar {@code completed}. A decisão depende só do
     * catálogo replicado e do journal durável do participante, e vale igual para um líder novo.</p>
     *
     * <p>Se o participante ainda está {@code PREPARED}/{@code COMMITTED} naquela geração, a ausência não veio do
     * fluxo (o catálogo perdeu a reserva): a reserva confirmada é regravada e conduzida, como antes.</p>
     */
    private DeleteResult recoverWithoutPlacement(String key, SeriesPlacement placement, NodeId source) {
        if (source == null || !placement.deletion().participants().contains(source.value()))
            return DeleteResult.of(DeleteStatus.NOT_FOUND, null);
        ensureLeader();
        SeriesInspectResponse inspected = rpc.call(source, Commands.SERIES_INSPECT, new SeriesCommandRequest(key),
                SeriesInspectResponse.class);
        if (inspected == null)
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, Commands.SERIES_INSPECT + " sem resposta: " + source);
        if (!inspected.deleting() || !placement.generationId().equals(inspected.generationId())) {
            phase(source.value(), Commands.DELETE_FINISH, key, placement);
            return DeleteResult.of(DeleteStatus.DELETED, placement.ownerNodeId());
        }
        try (var guard = CoordinationLocks.acquire(catalog.placementLock(key))) {
            ensureLeader();
            // Algo gravou a série entre a leitura e a sonda: o próximo pedido decide com o estado novo.
            if (catalog.placementStrong(key).isPresent()) throw new DeleteFailureCause.StaleReservationException();
            catalog.putPlacement(key, placement);
        }
        return drive(key, placement);
    }

    /**
     * Ciclo de recuperação (a cada 1 s). Contagem de métricas: {@code seriesDelete.recoveryAttempts} sai de
     * {@link #beginAttempt}, chamado por {@code delete(request, Origin.RECOVERY)} e por
     * {@code recoverReservation}; {@code seriesDelete.pending} e o alarme {@code NGRRD_DELETE_PENDING} saem de
     * {@link #trackPending}. Quem reescrever este laço só precisa manter essas chamadas.
     *
     * <p>No líder, as pendentes vêm de {@link CatalogView#pendingDeletionsLocal()} (um índice, custo
     * proporcional aos pendentes) e não de uma cópia do catálogo. O índice é reconstruído por varredura
     * completa quando o nó passa a ser líder sincronizado e a cada {@link #PENDING_INDEX_RESYNC}. Cada
     * operação tem backoff exponencial próprio, de {@link #RECOVERY_BACKOFF_INITIAL} a
     * {@link #RECOVERY_BACKOFF_MAX}; o gauge conta todas as pendentes, inclusive as em espera.</p>
     *
     * <p>Antes, {@link #recoverJournal()} trata o journal local entrada a entrada: a falha de uma não pula a
     * parte do líder, e o reenvio de {@code DELETE_RECOVER} tem backoff próprio por operação.</p>
     */
    void recover() {
        if (closed || !synced.getAsBoolean()) {
            pendingIndexRebuiltAtMs = NEVER_REBUILT;
            return;
        }
        try {
            recoverJournal();
            if (leader.getAsBoolean()) {
                long now = clock.millis();
                long rebuiltAt = pendingIndexRebuiltAtMs;
                // Relógio que recuou (now < rebuiltAt) também força a reconstrução: senão ela ficaria parada.
                if (rebuiltAt == NEVER_REBUILT || now < rebuiltAt || now - rebuiltAt >= PENDING_INDEX_RESYNC.toMillis()) {
                    int found = catalog.rebuildPendingDeletions();
                    pendingIndexRebuiltAtMs = now;
                    LOG.log(Level.FINE, "series deletion pending index rebuilt: found=" + found);
                }
                List<Map.Entry<String, SeriesPlacement>> pending = new ArrayList<>(catalog.pendingDeletionsLocal().entrySet());
                trackPending(pending);
                Set<RecoveryKey> pendingOperations = new HashSet<>();
                for (var item : pending) {
                    pendingOperations.add(new RecoveryKey(item.getKey(), item.getValue().deletion().operationId()));
                }
                recoveryBackoff.keySet().retainAll(pendingOperations);
                for (var item : pending) {
                    SeriesDeletion deletion = item.getValue().deletion();
                    RecoveryKey operation = new RecoveryKey(item.getKey(), deletion.operationId());
                    RecoveryBackoff backoff = recoveryBackoff.get(operation);
                    if (!due(backoff, clock.millis())) continue;
                    DeleteResult result = delete(new DeleteRequest(item.getKey(),
                            new DeletePrecondition(deletion.lastWriteBefore()), deletion.operationId()), Origin.RECOVERY);
                    recordRecoveryOutcome(operation, backoff, result);
                }
            } else {
                pendingIndexRebuiltAtMs = NEVER_REBUILT;
                recoveryBackoff.clear();
                trackPending(List.of());
            }
        } catch (RuntimeException e) { logRecoveryDeferred(e); }
    }

    /**
     * Lado de quem tem a entrada no journal: aborta o {@code PREPARED} cuja reserva sumiu e pede ao líder
     * ({@code DELETE_RECOVER}) que conclua o que já foi confirmado. Cada entrada tem o próprio
     * {@code try}: a falha de uma (RPC, líder indisponível) não pula as demais nem a parte do líder em
     * {@link #recover()}. O reenvio de cada operação segue {@link #journalRecoveryBackoff}.
     *
     * <p>Só lê o catálogo para as entradas que ainda podem ter o que fazer: as fases não terminais
     * ({@code PREPARED}, {@code COMMITTED}, {@code DELETED}) e a {@code FINISHED} ainda não resolvida
     * ({@link #resolvedFinished}). {@code ACTIVE} e {@code QUARANTINED} não são da recuperação de exclusão.
     * Leituras por ciclo: O(exclusões pendentes neste nó), não O(histórico do journal).</p>
     */
    private void recoverJournal() {
        Set<RecoveryKey> candidates = new HashSet<>();
        Set<String> resolved = new HashSet<>();
        for (var local : lifecycle.journal().snapshot().entrySet()) {
            var entry = local.getValue(); String key = local.getKey();
            if (entry.placement() == null || entry.placement().deletion() == null) continue;
            var phase = entry.phase();
            if (phase != PREPARED && phase != COMMITTED && phase != DELETED && phase != FINISHED) continue;
            String operationId = entry.placement().deletion().operationId();
            ResolvedFinish finish = phase == FINISHED
                    ? new ResolvedFinish(entry.placement().generationId(), operationId) : null;
            if (finish != null && finish.equals(resolvedFinished.get(key))) {
                resolved.add(key);
                continue;
            }
            RecoveryKey operation = new RecoveryKey(key, operationId);
            boolean confirmed = phase != PREPARED;
            // Candidata antes da leitura do catálogo: uma falha transitória ali não zera o backoff da operação.
            if (confirmed) candidates.add(operation);
            try {
                var current = catalog.placementStrong(key);
                if (phase == PREPARED) {
                    if (current.isEmpty() || current.get().deletion() == null)
                        lifecycle.abort(new DeleteControlRequest(key, entry.placement()));
                } else if (needsLeader(entry, current)) {
                    NodeId target = rpc.leaderId().orElse(null);
                    if (target == null) continue;
                    RecoveryBackoff backoff = journalRecoveryBackoff.get(operation);
                    if (!due(backoff, clock.millis())) continue;
                    journalRecoveryBackoff.put(operation, nextBackoff(backoff));
                    rpc.call(target, Commands.DELETE_RECOVER, new DeleteControlRequest(key, entry.placement()),
                            DeleteResult.class);
                } else {
                    candidates.remove(operation);
                    if (finish != null) {
                        resolvedFinished.put(key, finish);
                        resolved.add(key);
                    }
                }
            } catch (RuntimeException e) {
                logRecoveryDeferred(e);
            }
        }
        journalRecoveryBackoff.keySet().retainAll(candidates);
        resolvedFinished.keySet().retainAll(resolved);
    }

    /**
     * Se a entrada confirmada do journal ainda tem o que pedir ao líder com {@code DELETE_RECOVER}.
     * {@code DELETED} sempre: falta o {@code DELETE_FINISH}, e o líder o manda mesmo com a geração já
     * substituída ({@link #finishSuperseded}). {@code COMMITTED} e {@code FINISHED} só se o placement atual for
     * da mesma geração da entrada, e o {@code COMMITTED} também sem placement (o líder sonda e decide,
     * {@link #recoverWithoutPlacement}). Com outra geração no catálogo, a da entrada já saiu dele: o líder não
     * teria fase a conduzir e só responderia {@code NOT_FOUND}.
     */
    private static boolean needsLeader(SeriesLifecycleJournal.Entry entry, Optional<SeriesPlacement> current) {
        if (entry.phase() == DELETED) return true;
        boolean sameGeneration = current.isPresent()
                && current.get().generationId().equals(entry.placement().generationId());
        return sameGeneration || (entry.phase() == COMMITTED && current.isEmpty());
    }

    /**
     * {@code series deletion recovery deferred} com taxa limitada: no máximo um log por
     * {@link #RECOVERY_LOG_WINDOW}, com quantas ocorrências foram suprimidas desde o anterior. Com o
     * {@code try} por entrada do journal, um líder fora do ar geraria um log por entrada a cada ciclo.
     */
    private void logRecoveryDeferred(RuntimeException e) {
        long now = clock.millis();
        long last = recoveryLoggedAtMs.get();
        boolean due = last == NEVER_LOGGED || now - last >= RECOVERY_LOG_WINDOW.toMillis() || now < last;
        if (!due || !recoveryLoggedAtMs.compareAndSet(last, now)) {
            recoverySuppressed.incrementAndGet();
            return;
        }
        LOG.log(Level.FINE, "series deletion recovery deferred: suppressed=" + recoverySuppressed.getAndSet(0), e);
    }

    /**
     * Se a operação pode ser tentada em {@code now}. Uma espera maior que {@link #RECOVERY_BACKOFF_MAX} só
     * acontece com relógio que recuou; ela é tratada como vencida, para não esconder a operação.
     */
    private static boolean due(RecoveryBackoff backoff, long now) {
        return backoff == null || now >= backoff.nextAttemptAtMs()
                || backoff.nextAttemptAtMs() - now > RECOVERY_BACKOFF_MAX.toMillis();
    }

    /** Próximo estado do backoff depois de mais uma tentativa falha (ou envio, no lado do journal). */
    private RecoveryBackoff nextBackoff(RecoveryBackoff previous) {
        int failures = previous == null ? 1 : previous.failures() + 1;
        return new RecoveryBackoff(failures, clock.millis() + recoveryBackoffDelay(failures).toMillis());
    }

    /**
     * Encerra o backoff da operação quando a tentativa terminou ({@code DELETED} ou {@code NOT_FOUND}); em
     * qualquer outro desfecho, agenda a próxima tentativa com o intervalo dobrado
     * ({@link #recoveryBackoffDelay}).
     */
    private void recordRecoveryOutcome(RecoveryKey operation, RecoveryBackoff previous, DeleteResult result) {
        DeleteStatus status = result == null ? null : result.status();
        if (status == DeleteStatus.DELETED || status == DeleteStatus.NOT_FOUND) {
            recoveryBackoff.remove(operation);
            return;
        }
        recoveryBackoff.put(operation, nextBackoff(previous));
    }

    /**
     * Intervalo depois de {@code failures} tentativas falhas seguidas: {@link #RECOVERY_BACKOFF_INITIAL}
     * dobrado a cada falha, limitado a {@link #RECOVERY_BACKOFF_MAX} (1, 2, 4, …, 32, 60, 60 s).
     */
    static Duration recoveryBackoffDelay(int failures) {
        if (failures < 1) throw new IllegalArgumentException("failures deve ser >= 1");
        int exponent = Math.min(failures - 1, 30);
        Duration delay = RECOVERY_BACKOFF_INITIAL.multipliedBy(1L << exponent);
        return delay.compareTo(RECOVERY_BACKOFF_MAX) > 0 ? RECOVERY_BACKOFF_MAX : delay;
    }

    /** Operações com backoff de recuperação registrado (diagnóstico e testes). */
    int recoveryBackoffSize() {
        return recoveryBackoff.size();
    }

    /** Operações com backoff de reenvio de {@code DELETE_RECOVER} registrado (diagnóstico e testes). */
    int journalRecoveryBackoffSize() {
        return journalRecoveryBackoff.size();
    }

    /** Entradas {@code FINISHED} do journal já resolvidas, sem leitura de catálogo por ciclo (diagnóstico e testes). */
    int resolvedFinishedSize() {
        return resolvedFinished.size();
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
