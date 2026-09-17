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
import java.util.Set;
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

    /** Resultado síncrono de um disparo imediato ({@code ngrrd.admin.rebalance}). */
    public record TriggerResult(int planned, int started) {
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
            List<Move> plan = buildPlan();
            if (plan.isEmpty()) {
                running.set(false);
                return new TriggerResult(0, 0);
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
                    .whenComplete((ignoredValue, ignoredError) -> running.set(false));
            return new TriggerResult(plan.size(), plan.size());
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
        List<Move> plan = buildPlan();
        if (plan.isEmpty()) {
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
    }

    private List<Move> buildPlan() {
        Collection<StorageNodeStatus> nodes = catalog.nodesLocal();
        Map<String, List<String>> seriesByOwner = catalog.seriesByOwnerLocal();
        Set<String> reachable = leaderView.reachableNodeIds();
        Set<String> migratingKeys = catalog.placementsLocal().entrySet().stream()
                .filter(entry -> entry.getValue().state() == PlacementState.MIGRATING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
        return RebalancePlanner.plan(nodes, seriesByOwner, reachable, migratingKeys, settings);
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
