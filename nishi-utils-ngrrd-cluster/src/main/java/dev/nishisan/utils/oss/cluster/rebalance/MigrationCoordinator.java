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

import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;

import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.metrics.LatencyHistogram;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.node.PlacementRequestHandler;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStartRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Orquestra, no líder, cada movimento de uma série entre dois storage nodes —
 * máquina de estados idempotente por {@code migrationId} (ver seção 8 de
 * {@code planning/ngrrd-cluster.md}). Nunca toca o volume/registry local: só
 * fala com {@link MigrationExecutor} da origem/destino via {@link ClusterRpc} e
 * com o catálogo via {@link CatalogView}.
 */
public final class MigrationCoordinator implements LeadershipListener {

    private static final Logger LOGGER = Logger.getLogger(MigrationCoordinator.class.getName());

    /** Resultado de {@link #migrate}. */
    public enum MigrationOutcome {
        /** A migração terminou com a série ativa no destino. */
        COMPLETED,
        /** Nada foi feito — o placement já não estava {@code ACTIVE} no {@code src} esperado. */
        SKIPPED,
        /** A migração foi abortada; a série continua {@code ACTIVE} no dono original. */
        FAILED
    }

    /**
     * @param bytes      tamanho da imagem migrada, quando conhecido (0 se {@code outcome != COMPLETED}
     *                   ou se o destino não o reportou)
     * @param durationMs duração total, do início ao desfecho
     */
    public record MigrationResult(MigrationOutcome outcome, String reason, long bytes, long durationMs) {
    }

    /**
     * Pontos de intercepção usados exclusivamente pelos testes de queda do líder durante uma migração
     * (ver {@code LeaderFailoverDuringMigrationClusterTest}). Não é API estável do cliente — pública
     * apenas porque {@code NgrrdStorageNode} (pacote {@code node}) precisa repassá-la ao construir o
     * coordenador; visibilidade de pacote não seria suficiente para essa composição entre pacotes.
     */
    public interface MigrationHooks {
        /** Chamado logo antes de gravar o placement {@code MIGRATING} e enviar {@code MIGRATE_START}. */
        default void beforeStart(String seriesKey, String migrationId) {
        }

        /** Chamado logo antes de flipar o catálogo para {@code ACTIVE(dst)} e enviar {@code MIGRATE_FINISH}. */
        default void beforeComplete(String migrationId) {
        }
    }

    private static final MigrationHooks NO_OP_HOOKS = new MigrationHooks() {
    };
    /**
     * Tentativas (e intervalo entre elas) de {@link Commands#MIGRATE_FINISH} à origem após o flip do
     * catálogo para {@code ACTIVE(dst)}.
     *
     * <p>Achado do Refuter do M3 em {@code RebalanceClusterTest} (reproduzido de forma determinística: a
     * imagem apagada no dono antigo nunca chegava dentro do prazo do teste): 3 tentativas com backoff de
     * {@code 200 ms * tentativa} somam menos de 1,2 s de orçamento — nada perto dos 10-15 s que uma
     * resolução de {@code dual-leader} do NGrid (issue tems#9/D9/D10c, ruído padrão de bootstrap/entrada
     * de nó já registrado no checkpoint do M1b) leva para assentar, período em que a origem pode estar
     * temporariamente desconectando/reconectando. Esgotadas as tentativas antigas, a cópia ficava órfã
     * para sempre (o reconciliador do M4 ainda não existe). Mesmo orçamento de
     * {@link #PLACEMENT_WRITE_ATTEMPTS}/{@link #PLACEMENT_WRITE_BACKOFF} (10 s) — a origem já confirmou
     * o COMMIT antes disso, então insistir aqui não arrisca nada além de tempo.</p>
     */
    private static final int FINISH_RETRY_ATTEMPTS = 20;
    private static final Duration FINISH_RETRY_BACKOFF = Duration.ofMillis(500L);
    private static final int RESUME_STATUS_ATTEMPTS = 3;
    /**
     * Tentativas (e intervalo entre elas) de {@link #resumeInFlight()} varrer a cópia LOCAL do catálogo
     * em busca de placements {@code MIGRATING} — mesmo orçamento de {@link #PLACEMENT_WRITE_ATTEMPTS}/
     * {@link #PLACEMENT_WRITE_BACKOFF} (10 s), pela mesma razão: a réplica local deste nó recém-eleito
     * pode não ter convergido ainda o {@code MIGRATING} que o líder anterior gravou (ou mesmo uma
     * eleição dupla/instável que descarta temporariamente a cauda local — ver achado do Refuter do M3
     * em {@code RebalanceClusterTest}). Uma varredura única, disparada só no instante da transição, corre
     * essa janela: acha o catálogo vazio, não encontra nada para resumir, e nunca mais tenta de novo —
     * a migração em curso fica travada para sempre (ninguém mais dispara {@link #resumeInFlight()} até a
     * PRÓXIMA troca de liderança). {@code dispatchedMigrationIds} evita resumir a MESMA migração duas
     * vezes caso ela apareça em mais de uma varredura antes de {@link #resumeOne} terminar de resolvê-la.
     */
    private static final int RESUME_SCAN_ATTEMPTS = 20;
    private static final Duration RESUME_SCAN_BACKOFF = Duration.ofMillis(500L);
    /**
     * Tentativas (e intervalo entre elas) para gravar um placement que <strong>decide</strong> o
     * desfecho de uma migração — o flip para {@code ACTIVE(dst)} em {@link #complete} e a reversão em
     * {@link #abort}.
     *
     * <p>Defeito que isto corrige (M3): essas duas gravações eram tentadas UMA vez e, em falha,
     * apenas logadas — a migração era contabilizada como resolvida enquanto o catálogo ficava preso
     * em {@code MIGRATING} para sempre. Na prática é o caso comum, não o raro: {@link #resumeInFlight}
     * roda exatamente quando este nó acaba de assumir a liderança, que é justamente a janela em que o
     * {@code ReplicationManager} do core recusa escritas com {@code LeaderSyncingException}
     * ("Leader is syncing (catch-up in progress)") — reproduzido por
     * {@code LeaderFailoverDuringMigrationClusterTest}. A recusa é transitória por construção (dura o
     * catch-up), então retentar enquanto este nó continuar líder resolve; 20 × 500 ms dá 10 s de
     * orçamento, gastos só no caminho de falha.</p>
     */
    private static final int PLACEMENT_WRITE_ATTEMPTS = 20;
    private static final Duration PLACEMENT_WRITE_BACKOFF = Duration.ofMillis(500L);

    /**
     * Carência aplicada em {@link #pollUntilResolved} quando a origem já reportou erro, mas o poll de
     * {@code MIGRATE_STATUS} ao destino continua falhando por transporte (destino segurando o lock da
     * série no commit/fsync, o mesmo lock que atende {@code MIGRATE_STATUS} — ver Javadoc de {@link
     * #pollUntilResolved}).
     *
     * <p>Sem esta carência, a série ficaria presa em {@code MIGRATING} (clientes recebendo {@code
     * MIGRATING}) até o {@code migrationTimeout} inteiro (10 min por padrão) sempre que o destino
     * realmente caísse durante o cutover — mesmo sem nenhuma chance real de um {@code COMMITTED} chegar
     * depois. Trade-off aceito: até {@link #SOURCE_FAILURE_DESTINATION_GRACE} de congelamento extra da
     * série nesse cenário específico (destino cai bem no cutover), documentado no CHANGELOG 8.5.1 e no
     * guia operacional. A carência é contada a partir da PRIMEIRA iteração em que a origem reportou erro
     * (não é renovada a cada iteração) e é sempre limitada pelo {@code migrationTimeout} — nunca estende
     * o prazo total da migração, só evita esperar o prazo inteiro quando já não há dúvida de que a
     * origem falhou.</p>
     */
    private static final Duration SOURCE_FAILURE_DESTINATION_GRACE = Duration.ofSeconds(10);

    private final CatalogView catalog;
    private final ClusterRpc rpc;
    private final PlacementRequestHandler.LeaderView leaderView;
    private final Duration migrationStatusPollInterval;
    private final Duration migrationTimeout;
    private final Semaphore concurrencyLimiter;
    private final ExecutorService pool;
    private final Clock clock;
    private final MigrationHooks hooks;

    private final LongAdder migrationsStarted = new LongAdder();
    private final LongAdder migrationsCompleted = new LongAdder();
    private final LongAdder migrationsFailed = new LongAdder();
    private final LongAdder migrationBytes = new LongAdder();
    private final LatencyHistogram migrationLatency = new LatencyHistogram();

    /**
     * Última leitura conhecida de {@code leaderView.isLeader()}, usada SÓ para detectar a transição
     * false→true dentro de {@link #onLeaderChanged} (disparar {@link #resumeInFlight()} uma vez ao
     * assumir). Nenhum outro método lê este campo — todo gate de "ainda sou líder?" chama
     * {@code leaderView.isLeader()} diretamente (ver Javadoc de {@link #onLeaderChanged}).
     */
    private volatile boolean leaderSeenAtLastChange;

    /**
     * {@code migrationId} atualmente sendo conduzido por ALGUMA thread deste coordenador — tanto pelo
     * {@link #runMigration} original (do início ao {@link #complete}/{@link #abort}) quanto por um
     * {@link #resumeOne} disparado por {@link #resumeInFlight()}. Usado como trava (via {@link
     * Set#add}, que devolve {@code false} se o id já está presente) para que as duas fontes — e
     * varreduras repetidas de {@link #resumeInFlight()} — nunca conduzam a MESMA migração ao mesmo
     * tempo.
     *
     * <p>Defeito que isto evita: sem essa trava, um {@code onLeaderChanged} espúrio (líder que nunca
     * chegou a cair de verdade, só "re-carimbou" a época — ver logs de churn do NGrid) disparava outro
     * {@link #resumeInFlight()} enquanto o {@link #runMigration} original ainda estava no meio do
     * {@link #pollUntilResolved}; as duas execuções concorrentes podiam mandar {@code MIGRATE_FINISH}
     * ao mesmo tempo para a mesma origem — {@code MigrationExecutor#handleFinish} é idempotente para
     * uma chamada isolada, mas não há garantia de comportamento sob duas em paralelo (a leitura do
     * {@code storageKey}, o {@code registry.forget} e o {@code storage().delete} não são atômicos entre
     * si), o que reproduziu, de forma intermitente, uma imagem nunca apagada no dono antigo em
     * {@code RebalanceClusterTest}.</p>
     */
    private final Set<String> activeMigrationIds = ConcurrentHashMap.newKeySet();
    /** Ligado por {@link #close()}: nenhuma condução prossegue depois disto (ver {@link #driving()}). */
    private volatile boolean closed;

    public MigrationCoordinator(CatalogView catalog, ClusterRpc rpc, PlacementRequestHandler.LeaderView leaderView,
            int maxConcurrentMigrations, Duration migrationStatusPollInterval, Duration migrationTimeout,
            Clock clock) {
        this(catalog, rpc, leaderView, maxConcurrentMigrations, migrationStatusPollInterval, migrationTimeout, clock,
                NO_OP_HOOKS);
    }

    public MigrationCoordinator(CatalogView catalog, ClusterRpc rpc, PlacementRequestHandler.LeaderView leaderView,
            int maxConcurrentMigrations, Duration migrationStatusPollInterval, Duration migrationTimeout,
            Clock clock, MigrationHooks hooks) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.leaderView = Objects.requireNonNull(leaderView, "leaderView");
        if (maxConcurrentMigrations <= 0) {
            throw new IllegalArgumentException("maxConcurrentMigrations deve ser > 0: " + maxConcurrentMigrations);
        }
        this.concurrencyLimiter = new Semaphore(maxConcurrentMigrations);
        this.migrationStatusPollInterval =
                Objects.requireNonNull(migrationStatusPollInterval, "migrationStatusPollInterval");
        this.migrationTimeout = Objects.requireNonNull(migrationTimeout, "migrationTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.hooks = Objects.requireNonNull(hooks, "hooks");
        this.pool = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "ngrrd-migration-coord");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Move {@code seriesKey} de {@code src} para {@code dst}, respeitando o limite de concorrência. */
    public CompletableFuture<MigrationResult> migrate(String seriesKey, String src, String dst) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(src, "src");
        Objects.requireNonNull(dst, "dst");
        return CompletableFuture.supplyAsync(() -> runMigration(seriesKey, src, dst), pool);
    }

    private MigrationResult runMigration(String seriesKey, String src, String dst) {
        long startedAt = clock.millis();
        try {
            concurrencyLimiter.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new MigrationResult(MigrationOutcome.FAILED, "interrompido aguardando semáforo de concorrência",
                    0L, 0L);
        }
        try {
            if (!leaderView.isLeader()) {
                return new MigrationResult(MigrationOutcome.SKIPPED, "este nó não é mais líder", 0L, 0L);
            }
            Optional<SeriesPlacement> current = catalog.placementStrong(seriesKey);
            if (current.isEmpty() || current.get().state() != PlacementState.ACTIVE
                    || !current.get().isOwnedBy(src)) {
                return new MigrationResult(MigrationOutcome.SKIPPED,
                        "placement de " + seriesKey + " não está ACTIVE em " + src, 0L, 0L);
            }
            SeriesPlacement activePlacement = current.get();
            if (catalog.geometryTrackingEnabled() && !activePlacement.geometryConfirmed()) {
                return new MigrationResult(MigrationOutcome.SKIPPED, "geometry is not confirmed", 0L, 0L);
            }
            String migrationId = UUID.randomUUID().toString();
            // Reivindica o id ANTES de qualquer escrita — se por acaso já estiver reivindicado (não
            // deveria, é recém-gerado por UUID, mas o Javadoc de activeMigrationIds explica por que a
            // reivindicação existe: protege contra um resumeInFlight() concorrente pegando o MESMO id
            // mais tarde, quando ele aparecer MIGRATING no catálogo).
            activeMigrationIds.add(migrationId);
            try {
                SeriesPlacement migratingPlacement =
                        SeriesPlacement.migrating(activePlacement, dst, migrationId, clock.millis());
                // Mesma retentativa do desfecho: um líder recém-eleito recusa escritas durante o catch-up
                // (LeaderSyncingException) e, sem retentar, a migração era descartada como SKIPPED por uma
                // condição puramente transitória. Nada foi gravado se todas falharem, então SKIPPED
                // continua sendo o resultado seguro.
                if (!putPlacementWithRetries(seriesKey, migratingPlacement, "MIGRATING de " + seriesKey,
                        () -> catalog.placementStrong(seriesKey).filter(activePlacement::equals).isPresent())) {
                    return new MigrationResult(MigrationOutcome.SKIPPED,
                            "falha ao gravar placement MIGRATING de " + seriesKey, 0L, clock.millis() - startedAt);
                }
                migrationsStarted.increment();
                // Depois do catálogo já refletir MIGRATING: um teste que bloqueia aqui (beforeStart) e mata
                // o líder em seguida exercita o mesmo caminho de resumeInFlight()/abort() de uma queda real
                // entre o flip do catálogo e o envio de MIGRATE_START — o próximo líder acha a entrada
                // MIGRATING, consulta o destino (que nunca ouviu falar desta migração: UNKNOWN) e aborta,
                // devolvendo o dono original.
                hooks.beforeStart(seriesKey, migrationId);
                if (!driving()) {
                    return stoppedDriving(seriesKey, startedAt, clock);
                }

                MigrateResponse startResponse;
                try {
                    startResponse = rpc.call(NodeId.of(src), Commands.MIGRATE_START,
                            new MigrateStartRequest(seriesKey, migrationId, dst), MigrateResponse.class);
                } catch (NgrrdClusterException e) {
                    return abort(seriesKey, migratingPlacement, src, dst,
                            "falha ao iniciar migração na origem: " + e.getMessage(), startedAt);
                }
                if (startResponse.status() != MigrateStatus.OK) {
                    return abort(seriesKey, migratingPlacement, src, dst,
                            "origem recusou MIGRATE_START: " + startResponse.status()
                                    + (startResponse.message() != null ? " (" + startResponse.message() + ")" : ""),
                            startedAt);
                }

                return pollUntilResolved(seriesKey, migratingPlacement, src, dst, migrationId, startedAt);
            } finally {
                // Libera o id só DEPOIS de complete()/abort() já terem rodado (estão todos dentro deste
                // bloco, via pollUntilResolved) — é isso que impede um resumeInFlight() concorrente de
                // disparar um SEGUNDO MIGRATE_FINISH/abort para a mesma migração enquanto esta ainda
                // está em andamento.
                activeMigrationIds.remove(migrationId);
            }
        } finally {
            concurrencyLimiter.release();
        }
    }

    /**
     * Poll periódico de {@code MIGRATE_STATUS} ao destino (e, quando necessário, à origem) até o
     * cutover se resolver, respeitando a seguinte ordem de prioridade a cada iteração:
     * <ol>
     *   <li>Destino responde {@code COMMITTED} → completa. Sempre vence, mesmo com a origem em erro.</li>
     *   <li>Destino responde (não-nulo) {@code ERROR}/{@code HASH_MISMATCH} → aborta na hora.</li>
     *   <li>Destino responde {@code PARTIAL}/{@code UNKNOWN} e a origem reporta erro → aborta na hora
     *       (não espera o {@code migrationTimeout}).</li>
     *   <li>Destino não responde (falha de transporte) e a origem reporta erro → dá a carência de
     *       {@link #SOURCE_FAILURE_DESTINATION_GRACE} antes de reconsultar o destino uma última vez e
     *       decidir (completa se {@code COMMITTED}, aborta caso contrário).</li>
     *   <li>Nenhuma das condições acima e o {@code migrationTimeout} estoura → reconsulta o destino uma
     *       última vez antes de abortar (mesmo racional do caso anterior: um {@code COMMITTED} tardio
     *       ainda vence).</li>
     * </ol>
     */
    private MigrationResult pollUntilResolved(String seriesKey, SeriesPlacement migratingPlacement, String src,
            String dst, String migrationId, long startedAt) {
        long deadline = clock.millis() + migrationTimeout.toMillis();
        // Marca a PRIMEIRA iteração em que a origem reportou erro enquanto o poll do destino falhava
        // por transporte — dispara a carência de SOURCE_FAILURE_DESTINATION_GRACE (ver Javadoc da
        // constante). -1 enquanto isso nunca aconteceu.
        long sourceFailureObservedAtMillis = -1L;
        for (;;) {
            if (!driving()) {
                // Não desfaz nada: a migração pode continuar nos dois nós envolvidos, e o próximo
                // líder a resolve via resumeInFlight() ao assumir. Abortar aqui poderia contradizer um
                // COMMIT que já aconteceu (ou vai acontecer) no destino.
                return new MigrationResult(MigrationOutcome.FAILED,
                        "liderança perdida durante o poll de " + seriesKey + "; o próximo líder resolve via "
                                + "resumeInFlight", 0L, clock.millis() - startedAt);
            }
            MigrateResponse pollResponse = pollStatusQuietly(dst, seriesKey, migrationId);
            if (pollResponse != null) {
                if (pollResponse.status() == MigrateStatus.COMMITTED) {
                    return complete(seriesKey, migratingPlacement, src, dst, migrationId, pollResponse.bytes(),
                            startedAt);
                }
                if (pollResponse.status() == MigrateStatus.ERROR || pollResponse.status() == MigrateStatus.HASH_MISMATCH) {
                    return abort(seriesKey, migratingPlacement, src, dst,
                            "destino reportou " + pollResponse.status()
                                    + (pollResponse.message() != null ? " (" + pollResponse.message() + ")" : ""),
                            startedAt);
                }
                // PARTIAL/UNKNOWN: o destino respondeu (não-nulo) e não é COMMITTED — só agora vale a
                // pena checar se a origem já falhou, para abortar sem esperar o migrationTimeout inteiro
                // (destino COMMITTED sempre venceria antes deste ponto, inclusive um COMMIT perdido).
                MigrateResponse sourceResponse = pollStatusQuietly(src, seriesKey, migrationId);
                if (sourceResponse != null && sourceResponse.status() == MigrateStatus.ERROR) {
                    return abort(seriesKey, migratingPlacement, src, dst,
                            "origem reportou falha: " + sourceResponse.message(), startedAt);
                }
            } else {
                // O poll do destino FALHOU (transporte): o destino segura o lock da série durante o
                // commit (fsync) e o mesmo lock atende MIGRATE_STATUS, então esse timeout bem na
                // iteração em que a origem já expirou é o caso comum, não o raro — abortar direto aqui
                // podia contradizer um COMMITTED que o destino já tem, só ainda não confirmado a este
                // líder. Mas sem NENHUM limite, isso deixaria a série MIGRATING até o migrationTimeout
                // inteiro sempre que o destino realmente tivesse caído no cutover — daí a carência:
                // consulta a origem só para saber SE ela já falhou (não para abortar por conta disso
                // agora); ao primeiro erro observado da origem, arma o relógio da carência, e só quando
                // ela se esgota é que reconsulta o destino uma ÚLTIMA vez antes de decidir.
                MigrateResponse sourceResponse = pollStatusQuietly(src, seriesKey, migrationId);
                if (sourceResponse != null && sourceResponse.status() == MigrateStatus.ERROR) {
                    long now = clock.millis();
                    if (sourceFailureObservedAtMillis < 0) {
                        sourceFailureObservedAtMillis = now;
                    }
                    long graceDeadline = Math.min(deadline,
                            sourceFailureObservedAtMillis + SOURCE_FAILURE_DESTINATION_GRACE.toMillis());
                    if (now >= graceDeadline) {
                        MigrateResponse finalPoll = pollStatusQuietly(dst, seriesKey, migrationId);
                        if (finalPoll != null && finalPoll.status() == MigrateStatus.COMMITTED) {
                            return complete(seriesKey, migratingPlacement, src, dst, migrationId,
                                    finalPoll.bytes(), startedAt);
                        }
                        // Com um migrationTimeout curto (< SOURCE_FAILURE_DESTINATION_GRACE), o
                        // Math.min acima já corta a carência no próprio deadline — quem realmente
                        // esgotou foi o migrationTimeout, não a carência, então o motivo cita o timeout.
                        String reason = graceDeadline < deadline
                                ? "origem em erro (" + sourceResponse.message() + ") e destino não "
                                        + "respondeu dentro da carência de " + SOURCE_FAILURE_DESTINATION_GRACE
                                        + " após a falha da origem"
                                : "timeout (" + migrationTimeout + ") aguardando COMMITTED no destino " + dst
                                        + " (origem em erro: " + sourceResponse.message() + ")";
                        return abort(seriesKey, migratingPlacement, src, dst, reason, startedAt);
                    }
                }
            }
            // O laço apenas continua; o próximo poll do destino tenta de novo, respeitando o
            // migrationTimeout (e a carência acima, quando aplicável).
            if (clock.millis() >= deadline) {
                // Antes de desistir, reconsulta o destino uma última vez: um COMMITTED aqui ainda vence
                // sobre o timeout, pelo mesmo motivo dos comentários acima.
                MigrateResponse finalPoll = pollStatusQuietly(dst, seriesKey, migrationId);
                if (finalPoll != null && finalPoll.status() == MigrateStatus.COMMITTED) {
                    return complete(seriesKey, migratingPlacement, src, dst, migrationId, finalPoll.bytes(),
                            startedAt);
                }
                return abort(seriesKey, migratingPlacement, src, dst,
                        "timeout (" + migrationTimeout + ") aguardando COMMITTED no destino " + dst, startedAt);
            }
            sleepQuietly(migrationStatusPollInterval);
        }
    }

    private MigrateResponse pollStatusQuietly(String dst, String seriesKey, String migrationId) {
        try {
            return rpc.call(NodeId.of(dst), Commands.MIGRATE_STATUS,
                    new MigrateControlRequest(seriesKey, migrationId), MigrateResponse.class);
        } catch (NgrrdClusterException e) {
            LOGGER.log(Level.FINE, "Falha de transporte ao consultar MIGRATE_STATUS de " + seriesKey + " em " + dst,
                    e);
            return null;
        }
    }

    private MigrationResult complete(String seriesKey, SeriesPlacement migratingPlacement, String src, String dst,
            String migrationId, long bytes, long startedAt) {
        hooks.beforeComplete(migrationId);
        if (!driving()) {
            return stoppedDriving(seriesKey, startedAt, clock);
        }
        SeriesPlacement completedPlacement = SeriesPlacement.completed(migratingPlacement, clock.millis());
        // (Refuter r2, item 1) mesma pré-condição de abort(): revalida a CADA tentativa que o placement
        // forte ainda é MIGRATING com este migrationId antes de flipar — sem isto, um complete() atrasado
        // (ex.: líder que demorou a processar o poll) podia flipar por cima de um estado que outra
        // migração (outro migrationId) já tinha sobrescrito nesse meio-tempo.
        if (!putPlacementWithRetries(seriesKey, completedPlacement,
                "ACTIVE(" + dst + ") de " + seriesKey + " após COMMITTED confirmado",
                () -> isStillMigratingWithId(seriesKey, migrationId))) {
            // Sem o flip do catálogo, a imagem no destino não é alcançável pelo cliente e a da origem
            // ainda é a boa: NÃO envia MIGRATE_FINISH (que apagaria a origem) e reporta FAILED, para
            // o ciclo/líder seguinte reconverter a partir do placement MIGRATING que ficou.
            migrationsFailed.increment();
            return new MigrationResult(MigrationOutcome.FAILED,
                    "COMMITTED no destino " + dst + ", mas o catálogo não pôde ser flipado para ACTIVE(" + dst + ")",
                    0L, clock.millis() - startedAt);
        }
        if (!driving()) {
            // Catálogo já em ACTIVE(dst), mas o coordenador foi fechado/interrompido antes do FINISH: NÃO
            // apaga a origem daqui. Nada fica MIGRATING para o resumeInFlight (que só olha MIGRATING);
            // a cópia da origem vira órfã de migração e é apagada pelo LocalReconciler (placement forte
            // ACTIVE noutro dono há mais de orphanGrace + SERIES_EXISTS confirmado no dono).
            LOGGER.log(Level.WARNING, "Coordenador fechado após flipar " + seriesKey + " para ACTIVE(" + dst
                    + ") e antes do MIGRATE_FINISH — a cópia na origem " + src + " fica para o reconciliador");
            return stoppedDriving(seriesKey, startedAt, clock);
        }
        boolean finished = callWithRetries(NodeId.of(src), Commands.MIGRATE_FINISH,
                new MigrateControlRequest(seriesKey, migrationId), FINISH_RETRY_ATTEMPTS, FINISH_RETRY_BACKOFF);
        if (!finished) {
            LOGGER.log(Level.WARNING, "MIGRATE_FINISH falhou após " + FINISH_RETRY_ATTEMPTS + " tentativas para "
                    + seriesKey + " em " + src + " — a cópia órfã fica para o reconciliador (M4)");
        }
        migrationsCompleted.increment();
        migrationBytes.add(bytes);
        long durationMs = clock.millis() - startedAt;
        migrationLatency.record(durationMs * 1_000_000L);
        return new MigrationResult(MigrationOutcome.COMPLETED, null, bytes, durationMs);
    }

    /**
     * Reverte {@code seriesKey} para {@code ACTIVE(src)} e SÓ DEPOIS manda {@code MIGRATE_ABORT} aos
     * dois lados — revalidando contra o líder, a CADA tentativa de escrita, que a migração ainda está
     * mesmo em curso.
     *
     * <p>Defeito bloqueante achado pelo Refuter (perda de dados): sem essa revalidação, um {@code abort}
     * que perde a corrida — ex.: este líder (L1) não alcança o destino dentro de {@code migrationTimeout}
     * e decide abortar, enquanto outro líder (L2), numa partição/dual-leader, já viu {@code COMMITTED} via
     * {@link #resumeInFlight()} e completou de verdade (catálogo em {@code ACTIVE(dst)}, {@code
     * MIGRATE_FINISH} já apagou a origem) — mandava {@code MIGRATE_ABORT} ao destino, que apagava a ÚNICA
     * cópia real (já {@code COMMITTED}/ativa), e revertia o catálogo para {@code ACTIVE(src)} sem imagem
     * em lugar nenhum: o próximo {@code OPEN} recriava a série VAZIA. {@link #isStillMigratingWithId}
     * decide, a cada tentativa de {@link #putPlacementWithRetries}: só MIGRATING com este id grava a
     * reversão; ACTIVE(dst) (já concluída por outra via), ACTIVE(src) (já revertido), MIGRATING de outro
     * id ou ausente → nada é gravado.</p>
     *
     * <p>Achado dos ALTOS residuais do Refuter (r2): a reversão do catálogo é gravada ANTES de enviar
     * {@code MIGRATE_ABORT} às duas pontas (mesma ordem de {@link #complete}: fonte da verdade primeiro,
     * aviso aos nós depois) — se o catálogo não pôde ser revertido (precondição falhou, ou perdeu
     * liderança), NENHUM {@code MIGRATE_ABORT} é enviado: notificar os nós de um abort que não está mais
     * refletido no catálogo os deixaria potencialmente desalinhados com a verdade compartilhada.</p>
     */
    private MigrationResult abort(String seriesKey, SeriesPlacement migratingPlacement, String src, String dst,
            String reason, long startedAt) {
        if (!driving()) {
            // Fechado/interrompido: nem reverte o catálogo nem manda MIGRATE_ABORT — o próximo líder
            // resolve pelo resumeInFlight (o MIGRATING fica).
            return stoppedDriving(seriesKey, startedAt, clock);
        }
        String migrationId = migratingPlacement.migrationId();
        boolean reverted = putPlacementWithRetries(seriesKey, SeriesPlacement.aborted(migratingPlacement, clock.millis()),
                "ACTIVE(" + src + ") de " + seriesKey + " após abort",
                () -> isStillMigratingWithId(seriesKey, migrationId));
        migrationsFailed.increment();
        if (!reverted) {
            LOGGER.log(Level.INFO, "Abort de " + seriesKey + " (migrationId=" + migrationId + ", " + src + " -> "
                    + dst + ") não notificado a " + src + "/" + dst + ": a reversão do catálogo não foi gravada "
                    + "(placement forte já não era MIGRATING com este id, ou liderança/escrita falhou) — motivo "
                    + "original do abort: " + reason);
            return new MigrationResult(MigrationOutcome.FAILED, reason, 0L, clock.millis() - startedAt);
        }
        safeAbort(NodeId.of(src), seriesKey, migrationId);
        safeAbort(NodeId.of(dst), seriesKey, migrationId);
        LOGGER.log(Level.WARNING, "Migração de " + seriesKey + " (" + src + " -> " + dst + ") abortada: " + reason);
        return new MigrationResult(MigrationOutcome.FAILED, reason, 0L, clock.millis() - startedAt);
    }

    /** {@code true} se a leitura FORTE do placement confirma {@code MIGRATING} com este {@code migrationId}. */
    private boolean isStillMigratingWithId(String seriesKey, String migrationId) {
        Optional<SeriesPlacement> current = catalog.placementStrong(seriesKey);
        return current.isPresent() && current.get().state() == PlacementState.MIGRATING
                && migrationId.equals(current.get().migrationId());
    }

    /** {@link #putPlacementWithRetries(String, SeriesPlacement, String, BooleanSupplier)} sem pré-condição. */
    private boolean putPlacementWithRetries(String seriesKey, SeriesPlacement placement, String what) {
        return putPlacementWithRetries(seriesKey, placement, what, () -> true);
    }

    /**
     * Grava um placement que decide o desfecho de uma migração, retentando enquanto este nó continuar
     * líder (ver {@link #PLACEMENT_WRITE_ATTEMPTS}) e {@code precondition} continuar satisfeita (checada
     * a CADA tentativa, não só na primeira — ver Javadoc de {@link #abort}). Devolve {@code false} se
     * desistiu — por perda de liderança (o próximo líder resolve pelo {@link #resumeInFlight()}),
     * pré-condição não satisfeita mais, ou por esgotar as tentativas. Nunca lança: o chamador decide o
     * que fazer com o desfecho não gravado.
     */
    private boolean putPlacementWithRetries(String seriesKey, SeriesPlacement placement, String what,
            BooleanSupplier precondition) {
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= PLACEMENT_WRITE_ATTEMPTS; attempt++) {
            if (!driving()) {
                LOGGER.log(Level.WARNING, "Liderança perdida (ou coordenador fechado) antes de gravar o placement " + what
                        + "; o próximo líder resolve via resumeInFlight");
                return false;
            }
            try {
                try (var guard = CoordinationLocks.acquire(catalog.placementLock(seriesKey))) {
                    if (!driving() || !precondition.getAsBoolean()) {
                        LOGGER.log(Level.INFO, "Pré-condição não satisfeita mais — desistindo de gravar " + what);
                        return false;
                    }
                    catalog.putPlacement(seriesKey, placement);
                    return true;
                }
            } catch (RuntimeException e) {
                lastFailure = e;
                if (attempt < PLACEMENT_WRITE_ATTEMPTS) {
                    sleepQuietly(PLACEMENT_WRITE_BACKOFF);
                }
            }
        }
        LOGGER.log(Level.WARNING, "Falha ao gravar o placement " + what + " após " + PLACEMENT_WRITE_ATTEMPTS
                + " tentativas — o próximo ciclo/líder deve reconverter", lastFailure);
        return false;
    }

    private void safeAbort(NodeId target, String seriesKey, String migrationId) {
        try {
            rpc.call(target, Commands.MIGRATE_ABORT, new MigrateControlRequest(seriesKey, migrationId),
                    MigrateResponse.class);
        } catch (NgrrdClusterException e) {
            LOGGER.log(Level.FINE, "Falha (ignorada) ao abortar migração de " + seriesKey + " em " + target, e);
        }
    }

    private boolean callWithRetries(NodeId target, String command, Object body, int attempts, Duration backoff) {
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                rpc.call(target, command, body, MigrateResponse.class);
                return true;
            } catch (NgrrdClusterException e) {
                if (attempt >= attempts) {
                    return false;
                }
                sleepQuietly(backoff);
            }
        }
        return false;
    }

    // ---------------------------------------------------------------- resumeInFlight

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        // Detecção de transição SÓ para decidir se dispara resumeInFlight (uma vez, ao ASSUMIR a
        // liderança) — os gates de "ainda sou líder?" usados no resto da classe consultam
        // leaderView.isLeader() diretamente (nunca este campo), porque addLeadershipListener não
        // dispara um callback sintético para quem já registra o listener com o nó JÁ líder (o primeiro
        // líder eleito de um cluster recém-formado, por exemplo) — cachear a resposta aqui e usá-la como
        // fonte de verdade faria esse nó nunca migrar nada, mesmo sendo líder de verdade.
        boolean wasLeaderLastSeen = leaderSeenAtLastChange;
        leaderSeenAtLastChange = leaderView.isLeader();
        if (leaderSeenAtLastChange && !wasLeaderLastSeen) {
            pool.execute(() -> {
                // (Refuter r3) uma exceção não capturada aqui mataria silenciosamente esta execução de
                // resumeInFlight() sem log nenhum — mesmo padrão de proteção do tick() do
                // NodeStatusReporter: nunca deixar uma tarefa de fundo periódica/disparada por evento
                // escapar com Throwable não tratado.
                try {
                    resumeInFlight();
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "Falha inesperada em resumeInFlight()", t);
                }
            });
        }
    }

    /**
     * Ao assumir a liderança, resolve todo placement {@code MIGRATING} encontrado na cópia local do
     * catálogo — a migração pode ter avançado (ou não) enquanto o líder anterior caiu, e não há como
     * saber sem perguntar diretamente ao destino. Varre em até {@link #RESUME_SCAN_ATTEMPTS} tentativas
     * (ver Javadoc da constante). A reivindicação em {@link #activeMigrationIds} (não uma marca local
     * desta chamada) é o que evita despachar duas vezes: cobre tanto repetir a varredura quanto um
     * {@link #runMigration} original que ainda esteja com a MESMA migração em andamento.
     */
    private void resumeInFlight() {
        for (int attempt = 1; attempt <= RESUME_SCAN_ATTEMPTS; attempt++) {
            if (!driving()) {
                return;
            }
            try {
                // (Refuter r3) catalog.placementsLocal() é leitura eventual (DistributedMap), mas pode
                // lançar em condições transitórias (ex.: réplica ainda inicializando) — sem este catch,
                // UMA falha aqui abortava a varredura inteira (as próximas RESUME_SCAN_ATTEMPTS
                // tentativas nunca rodavam), não só a rodada atual.
                for (Map.Entry<String, SeriesPlacement> entry : catalog.placementsLocal().entrySet()) {
                    SeriesPlacement placement = entry.getValue();
                    if (placement.state() != PlacementState.MIGRATING
                            || !activeMigrationIds.add(placement.migrationId())) {
                        continue;
                    }
                    String seriesKey = entry.getKey();
                    LOGGER.info(() -> "resumeInFlight: retomando a migração de " + seriesKey + " (migrationId="
                            + placement.migrationId() + ", " + placement.ownerNodeId() + " -> "
                            + placement.targetNodeId() + ") encontrada MIGRATING na cópia local");
                    pool.execute(() -> {
                        try {
                            resumeOne(seriesKey, placement);
                        } catch (Throwable t) {
                            LOGGER.log(Level.WARNING, "Falha inesperada ao resumir a migração de " + seriesKey
                                    + " (migrationId=" + placement.migrationId() + ")", t);
                        } finally {
                            activeMigrationIds.remove(placement.migrationId());
                        }
                    });
                }
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Falha ao varrer o catálogo local em busca de migrações MIGRATING "
                        + "(tentativa " + attempt + "/" + RESUME_SCAN_ATTEMPTS + ")", t);
            }
            if (attempt < RESUME_SCAN_ATTEMPTS) {
                sleepQuietly(RESUME_SCAN_BACKOFF);
            }
        }
    }

    private void resumeOne(String seriesKey, SeriesPlacement placement) {
        if (!driving()) {
            return;
        }
        long startedAt = clock.millis();
        String dst = placement.targetNodeId();
        String src = placement.ownerNodeId();
        String migrationId = placement.migrationId();
        MigrateResponse response = null;
        for (int attempt = 1; attempt <= RESUME_STATUS_ATTEMPTS && driving(); attempt++) {
            response = pollStatusQuietly(dst, seriesKey, migrationId);
            if (response != null) {
                break;
            }
            sleepQuietly(Duration.ofMillis(200L * attempt));
        }
        if (!driving()) {
            return;
        }
        if (response != null && response.status() == MigrateStatus.COMMITTED) {
            complete(seriesKey, placement, src, dst, migrationId, response.bytes(), startedAt);
        } else {
            abort(seriesKey, placement, src, dst,
                    "resumeInFlight: destino " + dst + " não confirma COMMITTED (status="
                            + (response != null ? response.status() : "INALCANÇÁVEL") + ")",
                    startedAt);
        }
    }

    // ---------------------------------------------------------------- consulta de migrações ativas

    /**
     * Nós que são origem de alguma migração que este coordenador está ativamente conduzindo agora
     * ({@code migrationId} presente em {@link #activeMigrationIds}, cruzado com o placement
     * {@code MIGRATING} correspondente no catálogo) — usado por {@code Rebalancer#promoteDrainedNodes}
     * para nunca promover a {@code DRAINED} um nó que ainda está no meio de uma migração de saída.
     * Devolve um snapshot; nunca lança (uma falha de leitura do catálogo é tratada como "nenhuma",
     * já que {@code activeMigrationIds} vazio também devolve vazio sem tocar o catálogo).
     */
    public Set<String> activeSourceNodeIds() {
        Set<String> ids = Set.copyOf(activeMigrationIds);
        if (ids.isEmpty()) {
            return Set.of();
        }
        return catalog.placementsLocal().values().stream()
                .filter(placement -> placement.state() == PlacementState.MIGRATING
                        && ids.contains(placement.migrationId()))
                .map(SeriesPlacement::ownerNodeId)
                .collect(Collectors.toUnmodifiableSet());
    }

    /** Quantidade de migrações que este coordenador está ativamente conduzindo agora. */
    public int activeMigrationCount() {
        return activeMigrationIds.size();
    }

    // ---------------------------------------------------------------- observabilidade

    /** Snapshot das métricas deste coordenador. */
    public record CoordinatorMetrics(long migrationsStarted, long migrationsCompleted, long migrationsFailed,
            long migrationBytes, LatencySnapshot migrationLatency) {
    }

    public CoordinatorMetrics metricsSnapshot() {
        return new CoordinatorMetrics(migrationsStarted.sum(), migrationsCompleted.sum(), migrationsFailed.sum(),
                migrationBytes.sum(), migrationLatency.snapshot());
    }

    /** Fecha o pool de coordenação ({@code ngrrd-migration-coord}). */
    public void close() {
        closed = true;
        pool.shutdownNow();
    }

    /**
     * Se este coordenador ainda deve conduzir migrações: não fechado, thread não interrompida (o
     * {@code shutdownNow} do {@link #close()} interrompe a condução em curso) e nó ainda líder. Depois
     * de um {@code false} a condução PARA sem efeitos colaterais — nada de RPC nem de escrita no
     * catálogo: um líder em fechamento que continuasse (START, chunks, COMMIT, flip para ACTIVE(dst))
     * concluía a migração por cima do próprio encerramento, e o próximo líder encontrava o catálogo
     * num estado que ele nunca conduziu. O placement MIGRATING que ficou é resolvido pelo próximo
     * líder via {@link #resumeInFlight()}.
     */
    private boolean driving() {
        return !closed && !Thread.currentThread().isInterrupted() && leaderView.isLeader();
    }

    private static MigrationResult stoppedDriving(String seriesKey, long startedAt, Clock clock) {
        return new MigrationResult(MigrationOutcome.FAILED, "condução de " + seriesKey
                + " interrompida (coordenador fechado ou liderança perdida); o próximo líder resolve via resumeInFlight",
                0L, clock.millis() - startedAt);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
