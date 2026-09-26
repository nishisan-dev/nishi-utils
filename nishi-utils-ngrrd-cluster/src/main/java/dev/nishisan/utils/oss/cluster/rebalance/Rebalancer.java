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

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.node.PlacementRequestHandler;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationResult;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Dispara ciclos de rebalanceamento no líder: por agendamento
 * ({@code rebalanceInterval}, só enquanto líder), por mudança de membership
 * (com debounce) e por {@code ngrrd.admin.rebalance}. Cada ciclo monta o plano com
 * {@link RebalancePlanner} sobre a visão local do catálogo e submete os
 * movimentos ao {@link MigrationCoordinator}, que já respeita
 * {@code maxConcurrentMigrations} internamente.
 */
public final class Rebalancer implements LeadershipListener, ClusterCoordinator.MembershipListener, Closeable {

    private static final Logger LOGGER = Logger.getLogger(Rebalancer.class.getName());
    private static final Duration MEMBERSHIP_DEBOUNCE = Duration.ofSeconds(5);

    private final CatalogService catalog;
    private final PlacementRequestHandler.LeaderView leaderView;
    private final MigrationCoordinator coordinator;
    private final RebalanceSettings settings;
    private final boolean rebalanceEnabled;
    private final Duration rebalanceInterval;
    private final Duration migrationTimeout;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    /**
     * Exclusões de destino do ciclo anterior (nó → motivo): {@code NGRRD_REBALANCE_DEST_EXCLUDED} sai em
     * INFO só quando o conjunto muda — um nó que segue atrasado repetiria a mesma linha a cada ciclo.
     */
    private volatile Map<String, String> lastExcludedDestinations = Map.of();
    private volatile ScheduledFuture<?> intervalTask;
    private volatile ScheduledFuture<?> debounceTask;

    public Rebalancer(CatalogService catalog, PlacementRequestHandler.LeaderView leaderView,
            MigrationCoordinator coordinator, RebalanceSettings settings, boolean rebalanceEnabled,
            Duration rebalanceInterval, Duration migrationTimeout, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.leaderView = Objects.requireNonNull(leaderView, "leaderView");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.rebalanceEnabled = rebalanceEnabled;
        this.rebalanceInterval = Objects.requireNonNull(rebalanceInterval, "rebalanceInterval");
        this.migrationTimeout = Objects.requireNonNull(migrationTimeout, "migrationTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ngrrd-rebalancer");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        if (leaderView.isLeader()) {
            if (rebalanceEnabled) {
                startSchedule();
            }
        } else {
            cancelSchedule();
        }
    }

    @Override
    public void onMembershipChanged() {
        if (!rebalanceEnabled || !leaderView.isLeader()) {
            return;
        }
        ScheduledFuture<?> previous = debounceTask;
        if (previous != null) {
            previous.cancel(false);
        }
        try {
            debounceTask = scheduler.schedule(this::runCycleIfNotRunning, MEMBERSHIP_DEBOUNCE.toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "Não foi possível agendar o rebalanceamento por mudança de membership "
                    + "(scheduler provavelmente encerrado)", e);
        }
    }

    private void startSchedule() {
        if (intervalTask != null) {
            return;
        }
        intervalTask = scheduler.scheduleWithFixedDelay(this::runCycleIfNotRunning, rebalanceInterval.toMillis(),
                rebalanceInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void cancelSchedule() {
        ScheduledFuture<?> current = intervalTask;
        if (current != null) {
            current.cancel(false);
            intervalTask = null;
        }
    }

    /**
     * Resultado síncrono de um disparo imediato ({@code ngrrd.admin.rebalance}).
     *
     * @param planned              movimentos planejados
     * @param started              movimentos submetidos ao {@link MigrationCoordinator}
     * @param excludedDestinations nós excluídos como destino neste ciclo por causa da réplica do catálogo
     *                             (issue #177), com o motivo ({@link CatalogLagGate}); nunca {@code null}
     */
    public record TriggerResult(int planned, int started, Map<String, String> excludedDestinations) {

        public TriggerResult {
            excludedDestinations = Map.copyOf(Objects.requireNonNullElse(excludedDestinations, Map.of()));
        }

        /** Resultado sem exclusões de destino. */
        public TriggerResult(int planned, int started) {
            this(planned, started, Map.of());
        }
    }

    /**
     * Plano de um ciclo e os destinos que ficaram de fora dele.
     *
     * @param moves                movimentos, na ordem de submissão
     * @param excludedDestinations nó → motivo da exclusão como destino
     */
    private record CyclePlan(List<Move> moves, Map<String, String> excludedDestinations) {
    }

    /**
     * Dispara um ciclo imediatamente, independente de {@code rebalanceEnabled} — usado por
     * {@code ngrrd.admin.rebalance}. Planeja e submete de forma síncrona, mas NÃO bloqueia esperando as
     * migrações completarem — {@code ngrrd.admin.rebalance} responde "planned/started" na hora; se um
     * ciclo já está em andamento, não planeja um novo (devolve 0/0).
     *
     * <p>Achado dos MÉDIOS do Refuter: {@code running} marcava "em andamento" só até o laço de {@code
     * submit} terminar — no instante seguinte já liberava, enquanto as migrações submetidas ainda
     * corriam em segundo plano. Um ciclo agendado (ou outro {@code ngrrd.admin.rebalance}) podia então
     * montar um plano NOVO por cima de migrações ainda em curso, no mesmo ou em nós adjacentes.
     * {@code running} agora só volta a {@code false} quando o ciclo termina de verdade — todas as
     * migrações resolvidas, ou {@code migrationTimeout} esgotado — sem bloquear esta chamada: a espera é
     * feita via {@link CompletableFuture#allOf} com {@link CompletableFuture#orTimeout}, cujo callback
     * roda na thread que completar o último future (do pool {@code ngrrd-migration-coord}), não numa
     * thread dedicada parada à toa.</p>
     */
    public TriggerResult triggerNow() {
        if (!running.compareAndSet(false, true)) {
            return new TriggerResult(0, 0);
        }
        // (Refuter r2, BAIXO) try/finally cobrindo TUDO que pode lançar antes de running virar
        // responsabilidade do allOf/orTimeout assíncrono: buildPlan() (leitura do catálogo) e
        // coordinator.migrate() (pode lançar RejectedExecutionException se o pool de migração já foi
        // fechado, ex.: node.close() correndo ao mesmo tempo). Sem isto, uma exceção aqui deixava
        // running=true para sempre, travando todo ciclo futuro (agendado ou outro
        // ngrrd.admin.rebalance) — pior que o defeito original.
        try {
            CyclePlan cyclePlan = buildPlan();
            List<Move> plan = cyclePlan.moves();
            if (plan.isEmpty()) {
                // Nada para mover agora — ainda assim pode haver um nó DRAINING já vazio (ex.: um
                // segundo drain() sobre um nó que só tinha séries MIGRATING da vez anterior) esperando
                // a promoção a DRAINED; ver Javadoc de promoteDrainedNodes().
                promoteDrainedNodes();
                running.set(false);
                return new TriggerResult(0, 0, cyclePlan.excludedDestinations());
            }
            List<CompletableFuture<MigrationResult>> futures = plan.stream()
                    // Ao contrário de runCycle() (ciclo agendado, que loga um resumo agregado ao esperar
                    // os futures), aqui cada movimento loga o próprio desfecho assim que resolve — sem
                    // isso, um SKIPPED/FAILED silencioso (ex.: perda de liderança bem no início de
                    // runMigration, que não loga nada por si só) ficava invisível até o cliente perceber
                    // o sintoma bem mais tarde (ex.: imagem nunca apagada na origem).
                    .map(move -> coordinator.migrate(move.seriesKey(), move.src(), move.dst())
                            .whenComplete((result, error) -> logMoveOutcome(move, result, error)))
                    .toList();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .orTimeout(migrationTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .whenComplete((ignoredValue, ignoredError) -> {
                        promoteDrainedNodes();
                        running.set(false);
                    });
            return new TriggerResult(plan.size(), plan.size(), cyclePlan.excludedDestinations());
        } catch (RuntimeException e) {
            running.set(false);
            throw e;
        }
    }

    private void logMoveOutcome(Move move, MigrationResult result, Throwable error) {
        if (error != null) {
            LOGGER.log(Level.WARNING, "NGRRD_REBALANCE_MOVE série=" + move.seriesKey() + " " + move.src() + "->"
                    + move.dst() + " falhou com exceção não tratada", error);
            return;
        }
        switch (result.outcome()) {
            case COMPLETED -> LOGGER.log(Level.FINE, "NGRRD_REBALANCE_MOVE série=" + move.seriesKey() + " "
                    + move.src() + "->" + move.dst() + " COMPLETED em " + result.durationMs() + "ms");
            case SKIPPED, FAILED -> LOGGER.log(Level.WARNING, "NGRRD_REBALANCE_MOVE série=" + move.seriesKey() + " "
                    + move.src() + "->" + move.dst() + " " + result.outcome() + ": " + result.reason());
        }
    }

    private void runCycleIfNotRunning() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            runCycle();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Falha inesperada num ciclo de rebalanceamento", e);
        } finally {
            running.set(false);
        }
    }

    private void runCycle() {
        if (!leaderView.isLeader()) {
            return;
        }
        List<Move> plan = buildPlan().moves();
        if (plan.isEmpty()) {
            promoteDrainedNodes();
            return;
        }
        List<CompletableFuture<MigrationResult>> futures = plan.stream()
                .map(move -> coordinator.migrate(move.seriesKey(), move.src(), move.dst()))
                .toList();

        long deadline = clock.millis() + migrationTimeout.toMillis();
        int completed = 0;
        int failed = 0;
        int skipped = 0;
        for (CompletableFuture<MigrationResult> future : futures) {
            long remainingMs = Math.max(0L, deadline - clock.millis());
            try {
                MigrationResult result = future.get(remainingMs, TimeUnit.MILLISECONDS);
                switch (result.outcome()) {
                    case COMPLETED -> completed++;
                    case SKIPPED -> skipped++;
                    case FAILED -> failed++;
                }
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                failed++;
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        LOGGER.info("NGRRD_REBALANCE moves=" + plan.size() + " completed=" + completed + " failed=" + failed
                + " skipped=" + skipped);
        promoteDrainedNodes();
    }

    /**
     * Promove {@link NodeState#DRAINING} → {@link NodeState#DRAINED} todo nó que não possui mais
     * nenhuma série {@link PlacementState#ACTIVE} local ({@code seriesByOwnerLocal()} vazio para ele) e
     * não é origem de nenhuma migração ativamente conduzida pelo {@link MigrationCoordinator}
     * ({@link MigrationCoordinator#activeSourceNodeIds()} — não mais uma varredura direta do catálogo,
     * ver achado MÉDIO-4 do Refuter: um placement {@code MIGRATING} solto, já resolvido por outro líder
     * mas ainda não convergido no catálogo local, não deveria bloquear a promoção) — chamado ao final de
     * todo ciclo (agendado ou {@link #triggerNow()}), inclusive quando o plano do ciclo veio vazio (seção
     * 1 da spec do M4: um nó já sem séries só precisa desta verificação, sem nenhum movimento a fazer).
     * Best-effort: uma falha ao gravar (ex.: liderança perdida entre a leitura e a escrita) só é logada —
     * o próximo ciclo tenta de novo.
     */
    private void promoteDrainedNodes() {
        if (!leaderView.isLeader()) {
            return;
        }
        Map<String, List<String>> seriesByOwner = catalog.seriesByOwnerLocal();
        // MÉDIO-4 (achado do Refuter): fonte de verdade sobre "migração em curso" passa a ser o
        // MigrationCoordinator (activeMigrationIds, o que ele está de fato conduzindo agora), não uma
        // varredura do catálogo — um placement MIGRATING solto (ex.: de uma migração já resolvida por
        // outro líder, catálogo ainda não convergiu) não deveria mais bloquear a promoção a DRAINED.
        Set<String> migratingSources = coordinator.activeSourceNodeIds();
        long now = clock.millis();
        for (StorageNodeStatus status : catalog.nodesLocal()) {
            if (status.state() != NodeState.DRAINING) {
                continue;
            }
            boolean hasOwnedSeries = !seriesByOwner.getOrDefault(status.nodeId(), List.of()).isEmpty();
            if (hasOwnedSeries || migratingSources.contains(status.nodeId())) {
                continue;
            }
            try {
                catalog.putNodeStatus(status.withState(NodeState.DRAINED, now));
                LOGGER.info("NGRRD_NODE_DRAINED nodeId=" + status.nodeId());
            } catch (RuntimeException e) {
                LOGGER.log(Level.FINE, "Falha ao promover " + status.nodeId() + " para DRAINED "
                        + "(o próximo ciclo tenta de novo)", e);
            }
        }
    }

    /**
     * Monta o plano do ciclo sobre a visão local do líder — usado pelo ciclo agendado, pelo disparo por
     * membership e por {@link #triggerNow()}. Destinos com a réplica do catálogo atrasada
     * ({@link CatalogLagGate}, limite {@link RebalanceSettings#maxDestinationCatalogLag()}) ficam de fora e
     * são logados em {@code NGRRD_REBALANCE_DEST_EXCLUDED} (INFO quando o conjunto muda, FINE quando se
     * repete).
     */
    private CyclePlan buildPlan() {
        Collection<StorageNodeStatus> nodes = catalog.nodesLocal();
        Map<String, List<String>> seriesByOwner = catalog.seriesByOwnerLocal();
        Set<String> reachable = leaderView.reachableNodeIds();
        Set<String> migratingKeys = catalog.placementsLocal().entrySet().stream()
                .filter(entry -> entry.getValue().state() == PlacementState.MIGRATING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
        Map<String, String> excluded = excludedDestinations(nodes, reachable);
        return new CyclePlan(planMoves(nodes, seriesByOwner, reachable, migratingKeys, excluded.keySet()), excluded);
    }

    /**
     * Destinos candidatos ({@code ACTIVE} e alcançáveis) excluídos pela réplica do catálogo, em ordem de
     * {@code nodeId}. O próprio líder nunca é excluído: a réplica dele é a fonte, mesmo que o último status
     * que publicou seja de antes de assumir.
     */
    private Map<String, String> excludedDestinations(Collection<StorageNodeStatus> nodes, Set<String> reachable) {
        Optional<String> leaderId = leaderView.leaderId();
        Map<String, String> excluded = new TreeMap<>();
        for (StorageNodeStatus node : nodes) {
            if (node.state() != NodeState.ACTIVE || !reachable.contains(node.nodeId())
                    || leaderId.map(node.nodeId()::equals).orElse(false)) {
                continue;
            }
            CatalogLagGate.exclusionReason(node, settings.maxDestinationCatalogLag())
                    .ifPresent(reason -> excluded.put(node.nodeId(), reason));
        }
        if (!excluded.isEmpty()) {
            Level level = excluded.equals(lastExcludedDestinations) ? Level.FINE : Level.INFO;
            LOGGER.log(level, "NGRRD_REBALANCE_DEST_EXCLUDED nodes=" + excluded.entrySet().stream()
                    .map(entry -> entry.getKey() + "(" + entry.getValue() + ")")
                    .collect(Collectors.joining(",")));
        }
        lastExcludedDestinations = Map.copyOf(excluded);
        return excluded;
    }

    private List<Move> planMoves(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, Set<String> migratingKeys, Set<String> excludedDestinations) {
        if (!catalog.geometryTrackingEnabled()) {
            return RebalancePlanner.plan(nodes, seriesByOwner, reachable, migratingKeys, settings,
                    excludedDestinations);
        }
        Map<String, Long> sizes = new java.util.HashMap<>();
        Map<String, Long> pendingBytes = new java.util.HashMap<>();
        Map<String, Long> pendingSeries = new java.util.HashMap<>();
        Map<String, Long> reported = new java.util.HashMap<>();
        nodes.forEach(n -> reported.put(n.nodeId(), n.reportedAtEpochMs()));
        catalog.placementsLocal().forEach((key, placement) -> {
            var geometry = catalog.geometryLocal(placement.geometryId());
            if (placement.geometryConfirmed()) { geometry.ifPresent(g -> sizes.put(key, g.regionBytes())); }
            String target = placement.targetNodeId() != null ? placement.targetNodeId() : placement.ownerNodeId();
            if (placement.targetNodeId() != null) { pendingSeries.merge(target, 1L, Long::sum); }
            if (placement.targetNodeId() != null || !placement.geometryConfirmed()
                    || placement.updatedAtEpochMs() >= reported.getOrDefault(target, 0L)) {
                geometry.ifPresent(g -> pendingBytes.merge(target, g.regionBytes(), Math::addExact));
            }
        });
        var plan = RebalancePlanner.plan(nodes, seriesByOwner, reachable, migratingKeys, settings,
                sizes, pendingBytes, pendingSeries, excludedDestinations);
        if (plan.isEmpty() && nodes.stream().anyMatch(n -> n.state() == NodeState.DRAINING
                && !seriesByOwner.getOrDefault(n.nodeId(), List.of()).isEmpty())) {
            LOGGER.info("NGRRD_DRAIN_PENDING reason=no_admissible_destination_or_confirmed_geometry");
        }
        return plan;
    }

    @Override
    public void close() {
        cancelSchedule();
        ScheduledFuture<?> pendingDebounce = debounceTask;
        if (pendingDebounce != null) {
            pendingDebounce.cancel(false);
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(Level.WARNING, "Scheduler do Rebalancer não parou em 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
