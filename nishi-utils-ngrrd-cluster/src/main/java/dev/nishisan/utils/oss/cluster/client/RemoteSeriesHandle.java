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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadPresetRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadPresetResponse;
import dev.nishisan.utils.oss.cluster.protocol.ReadRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link NgrrdHandle} remoto: cada operação é roteada ao storage node dono da
 * série ({@link #owner}), com retentativa transparente em {@code WRONG_OWNER},
 * {@code NOT_OPEN} e {@code MIGRATING}. Escritas passam pelo {@link WriteBuffer}
 * do cliente; as demais operações são RPC direto ao dono.
 */
public final class RemoteSeriesHandle implements NgrrdHandle {

    private static final Logger LOGGER = Logger.getLogger(RemoteSeriesHandle.class.getName());

    private final String seriesKey;
    private final String yaml;
    private final String definitionHashHex;
    private final dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry;
    private final Map<String, String> tags;
    private final Ngrrd.OpenOptions options;
    private final PlacementLookup resolver;
    private final ClusterRpc rpc;
    private final WriteBuffer dispatcher;
    private final RetryPolicy retryPolicy;
    /** Teto de CADA tentativa individual de RPC (ver {@link #callWithTransportRetry}) — nunca o orçamento total. */
    private final Duration requestTimeout;
    /** Orçamento TOTAL do {@link #close()} público (achado do Refuter, B1: antes usava {@code retryPolicy.timeout()}). */
    private final Duration closeTimeout;
    private final Clock clock;
    private final Consumer<String> onClose;

    private volatile String owner;
    private volatile boolean closed;
    /** {@code true} depois que o dono confirmou {@code NOT_FOUND} para esta série ({@code createIfMissing=false}). */
    private volatile boolean notFound;

    public RemoteSeriesHandle(String seriesKey, String yaml, String definitionHashHex, Map<String, String> tags,
            Ngrrd.OpenOptions options, PlacementLookup resolver, ClusterRpc rpc, WriteBuffer dispatcher,
            RetryPolicy retryPolicy, Duration requestTimeout, Duration closeTimeout, Clock clock,
            Consumer<String> onClose) {
        this(seriesKey, yaml, definitionHashHex, tags, options, resolver, rpc, dispatcher, retryPolicy,
                requestTimeout, closeTimeout, clock, onClose, null);
    }

    public RemoteSeriesHandle(String seriesKey, String yaml, String definitionHashHex, Map<String, String> tags,
            Ngrrd.OpenOptions options, PlacementLookup resolver, ClusterRpc rpc, WriteBuffer dispatcher,
            RetryPolicy retryPolicy, Duration requestTimeout, Duration closeTimeout, Clock clock,
            Consumer<String> onClose, dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor geometry) {
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey");
        this.yaml = Objects.requireNonNull(yaml, "yaml");
        this.geometry = geometry;
        this.definitionHashHex = Objects.requireNonNull(definitionHashHex, "definitionHashHex");
        this.tags = Map.copyOf(Objects.requireNonNullElse(tags, Map.of()));
        this.options = options != null ? options : Ngrrd.OpenOptions.defaults();
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.closeTimeout = Objects.requireNonNull(closeTimeout, "closeTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.onClose = Objects.requireNonNull(onClose, "onClose");
    }

    @Override
    public String seriesKey() {
        return seriesKey;
    }

    /**
     * Resolve o dono e abre a série nele. Chamado uma vez por
     * {@code DefaultNgrrdClusterClient.open}; {@link #reopen()} o reexecuta
     * quando um dono sinaliza {@code NOT_OPEN}.
     *
     * <p>item 12 (achado do Refuter): package-private de propósito — só {@code DefaultNgrrdClusterClient}
     * e o próprio {@code client} chamam isto; não faz parte do contrato público de {@link NgrrdHandle}.</p>
     */
    void open() {
        open(new OperationRetry(Commands.OPEN));
    }

    private void open(OperationRetry retry) {
        for (;;) {
            SeriesPlacement placement = resolvePlacement(retry.remaining());
            String candidateOwner = placement.ownerNodeId();
            OpenRequest request = new OpenRequest(seriesKey, yaml, tags, options.durability(),
                    options.onGeometryChange(), placement, options.createIfMissing() ? null : Boolean.FALSE);
            retry.remaining();
            SeriesStatusResponse response = callWithTransportRetry(NodeId.of(candidateOwner), Commands.OPEN, request,
                    SeriesStatusResponse.class, retry.deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                owner = candidateOwner;
                return;
            }
            switch (response.status()) {
                case WRONG_OWNER, MIGRATING, NOT_LEADER -> {
                    retry.pause(Commands.OPEN, candidateOwner, response.status(), response.ownerNodeId());
                    if (response.status() == SeriesStatus.WRONG_OWNER) {
                        noteWrongOwner(response.ownerNodeId(), retry);
                    }
                }
                case NOT_FOUND -> throw new SeriesNotFoundException(seriesKey);
                default -> throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        response.message() != null ? response.message() : ("OPEN respondeu " + response.status()));
            }
        }
    }

    /**
     * Placement usado para posicionar o {@code OPEN}: com {@code createIfMissing=true} (default),
     * cria a série se preciso ({@code ngrrd.place}); com {@code createIfMissing=false}, nunca cria —
     * exige que a série já exista ({@link PlacementLookup#resolveExisting}), lançando
     * {@link SeriesNotFoundException} se o líder confirmar que não há placement.
     */
    private SeriesPlacement resolvePlacement(Duration maxWait) {
        return options.createIfMissing() ? resolver.resolve(seriesKey, definitionHashHex, geometry, maxWait)
                : resolver.resolveExisting(seriesKey, maxWait);
    }

    /**
     * Reexecuta {@link #open()}, absorvendo qualquer falha genérica — usado como callback pelo
     * {@code WriteDispatcher} quando um dono responde {@code NOT_OPEN} a um lote. {@link SeriesNotFoundException}
     * NÃO é absorvida: marca o handle como definitivamente inexistente ({@link #markSeriesNotFound()})
     * e relança, para que o {@code WriteDispatcher} falhe as escritas pendentes em vez de adiá-las para
     * sempre (uma série apagada nunca vai reabrir sozinha).
     */
    boolean reopen() {
        try {
            open();
            return true;
        } catch (SeriesNotFoundException e) {
            markSeriesNotFound();
            throw e;
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao reabrir a série " + seriesKey, e);
            return false;
        }
    }

    /**
     * Marca a série como definitivamente inexistente: {@code write}/{@code flush}/{@code checkpoint}/
     * {@code read} passam a lançar {@link SeriesNotFoundException} e o handle se remove do mapa do
     * cliente ({@link #onClose}), de modo que um {@code open} posterior refaz o fluxo do zero. Idempotente.
     */
    private void markSeriesNotFound() {
        if (notFound) {
            return;
        }
        notFound = true;
        closed = true;
        onClose.accept(seriesKey);
    }

    @Override
    public void write(String dsName, Sample sample) {
        ensureOpen();
        dispatcher.enqueue(owner, new SeriesWrite(seriesKey, dsName, sample.tsEpochMs(), sample.value()));
    }

    /**
     * M3 (nota do Refuter do M1c): callback do {@code WriteDispatcher} quando um {@code WRONG_OWNER}
     * já traz o dono novo — sem isto, {@link #owner} só mudava via {@link #open()}/{@link #noteWrongOwner},
     * então {@link #write} continuava enfileirando no dono antigo até a próxima falha explícita numa
     * operação síncrona (checkpoint/flush/read), invertendo a ordem dos lotes reroteados pelo
     * dispatcher (o backlog "pula na frente" do lote que acabou de ser reenfileirado). Idempotente e
     * seguro contra corrida com {@link #noteWrongOwner}: ambos só fazem uma atribuição simples a um
     * campo {@code volatile}, a pior coisa que pode acontecer é uma delas "vencer" por um instante —
     * a próxima chamada de qualquer uma delas sempre converge para o dono mais recente conhecido.
     */
    void ownerChanged(String newOwnerNodeId) {
        this.owner = Objects.requireNonNull(newOwnerNodeId, "newOwnerNodeId");
    }

    @Override
    public void flush() {
        ensureOpen();
        OperationRetry retry = new OperationRetry(Commands.FLUSH);
        dispatcher.flushSeriesSync(seriesKey, owner, retry.remaining());
        executeSeriesCommand(Commands.FLUSH, retry);
    }

    @Override
    public void checkpoint() {
        ensureOpen();
        OperationRetry retry = new OperationRetry(Commands.CHECKPOINT);
        dispatcher.flushSeriesSync(seriesKey, owner, retry.remaining());
        executeSeriesCommand(Commands.CHECKPOINT, retry);
    }

    @Override
    public SeriesResult read(String dsName, ViewQuery query) {
        return read(dsName, query, null);
    }

    @Override
    public SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs) {
        return read(dsName, query, (Long) endExclusiveEpochMs);
    }

    private SeriesResult read(String dsName, ViewQuery query, Long endExclusiveEpochMs) {
        ensureOpen();
        ReadRequest request = ReadRequest.of(seriesKey, dsName, query, endExclusiveEpochMs);
        OperationRetry retry = new OperationRetry(Commands.READ);
        for (;;) {
            retry.remaining();
            String target = owner;
            ReadResponse response = callWithTransportRetry(NodeId.of(target), Commands.READ, request,
                    ReadResponse.class, retry.deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                return response.result();
            }
            handleRetryableStatus(Commands.READ, target, response.status(), response.ownerNodeId(),
                    response.message(), retry);
        }
    }

    @Override
    public Map<String, SeriesResult> read(String presetName) {
        return readPreset(presetName, null);
    }

    @Override
    public Map<String, SeriesResult> read(String presetName, long endExclusiveEpochMs) {
        return readPreset(presetName, endExclusiveEpochMs);
    }

    private Map<String, SeriesResult> readPreset(String presetName, Long endExclusiveEpochMs) {
        ensureOpen();
        ReadPresetRequest request = new ReadPresetRequest(seriesKey, presetName, endExclusiveEpochMs);
        OperationRetry retry = new OperationRetry(Commands.READ_PRESET);
        for (;;) {
            retry.remaining();
            String target = owner;
            ReadPresetResponse response = callWithTransportRetry(NodeId.of(target), Commands.READ_PRESET, request,
                    ReadPresetResponse.class, retry.deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                return response.results();
            }
            handleRetryableStatus(Commands.READ_PRESET, target, response.status(), response.ownerNodeId(),
                    response.message(), retry);
        }
    }

    @Override
    public void close() {
        // B1 (achado do Refuter): usa closeTimeout da config, não retryTimeout — um close() avulso
        // (fora de DefaultNgrrdClusterClient.close(), que sempre chama close(Duration) com o orçamento
        // restante do fechamento compartilhado) segue o mesmo orçamento que o cliente anuncia para
        // "quanto tempo um close pode levar", não o prazo de retentativa de operações normais.
        close(closeTimeout);
    }

    /**
     * Fecha a série com um ÚNICO orçamento total para flush + CLOSE remoto — usado por
     * {@code DefaultNgrrdClusterClient.close()} para respeitar um orçamento compartilhado entre vários
     * handles (O1).
     *
     * <p>B1 (achado do Refuter): antes, o {@code flushBudget} só limitava o flush — o {@code CLOSE}
     * remoto em seguida usava {@code retryPolicy.timeout()} (minutos, por padrão) como teto próprio,
     * então fechar várias séries cujo dono está morto podia levar muito mais que {@code flushBudget}
     * no total. Agora {@code flushBudget} é o prazo de TUDO: se sobrar orçamento depois do flush, o
     * {@code CLOSE} remoto o usa; se não sobrar (ou se {@link ClusterRpc#isConnected} já disser que o
     * dono está inalcançável), o {@code CLOSE} remoto é pulado (WARN) — {@link #onClose} sempre roda,
     * então a referência local do cliente é liberada de qualquer forma.</p>
     *
     * <p>item 12 (achado do Refuter): package-private — só {@code DefaultNgrrdClusterClient.close()}
     * chama esta sobrecarga; o contrato público de {@link NgrrdHandle} continua sendo só {@link #close()}.</p>
     */
    void close(Duration budget) {
        if (closed) {
            return;
        }
        closed = true;
        long deadlineMs = clock.millis() + budget.toMillis();
        try {
            dispatcher.flushSeriesSync(seriesKey, owner, budget);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao drenar buffer de escrita ao fechar a série " + seriesKey, e);
        }
        long remainingMs = deadlineMs - clock.millis();
        NodeId ownerNodeId = NodeId.of(owner);
        if (remainingMs <= 0 || !rpc.isConnected(ownerNodeId)) {
            LOGGER.log(Level.WARNING, "Pulando o CLOSE remoto da série " + seriesKey + " em " + owner
                    + " (orçamento esgotado ou dono inalcançável) — a referência local é liberada mesmo assim");
        } else {
            try {
                callWithTransportRetry(ownerNodeId, Commands.CLOSE, new SeriesCommandRequest(seriesKey),
                        SeriesStatusResponse.class, deadlineMs);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Falha ao fechar remotamente a série " + seriesKey, e);
            }
        }
        onClose.accept(seriesKey);
    }

    private void executeSeriesCommand(String command, OperationRetry retry) {
        for (;;) {
            retry.remaining();
            String target = owner;
            SeriesStatusResponse response = callWithTransportRetry(NodeId.of(target), command,
                    new SeriesCommandRequest(seriesKey), SeriesStatusResponse.class, retry.deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                return;
            }
            handleRetryableStatus(command, target, response.status(), response.ownerNodeId(), response.message(), retry);
        }
    }

    /** Redirects and reopens can alternate during migration; all share the original deadline. */
    private void handleRetryableStatus(String command, String target, SeriesStatus status, String ownerNodeId,
            String message, OperationRetry retry) {
        switch (status) {
            case WRONG_OWNER, NOT_OPEN, MIGRATING -> {
                retry.pause(command, target, status, ownerNodeId);
                if (status == SeriesStatus.WRONG_OWNER) {
                    noteWrongOwner(ownerNodeId, retry);
                } else if (status == SeriesStatus.NOT_OPEN) {
                    // Unlike the dispatcher's boolean callback, preserve failures and the caller's budget.
                    open(retry);
                }
            }
            default -> throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    message != null ? message : (command + " respondeu " + status));
        }
    }

    /** Per-invocation state, also used by nested OPENs. Never shared between concurrent callers. */
    private final class OperationRetry {
        private final String operation;
        private final long deadlineMs = clock.millis() + retryPolicy.timeout().toMillis();
        private int attempts;
        private SeriesStatus lastStatus;

        private OperationRetry(String operation) {
            this.operation = operation;
        }

        private Duration remaining() {
            long remainingMs = deadlineMs - clock.millis();
            if (remainingMs <= 0) {
                ErrorCode code = lastStatus == SeriesStatus.MIGRATING ? ErrorCode.MIGRATING
                        : lastStatus == SeriesStatus.WRONG_OWNER ? ErrorCode.WRONG_OWNER
                        : lastStatus == SeriesStatus.NOT_LEADER ? ErrorCode.NO_LEADER : ErrorCode.TIMEOUT;
                throw new NgrrdClusterException(code, "prazo de retentativa esgotado em " + operation
                        + " de " + seriesKey + " (último status: " + lastStatus + ")");
            }
            return Duration.ofMillis(remainingMs);
        }

        private void pause(String command, String target, SeriesStatus status, String ownerHint) {
            lastStatus = status;
            attempts++;
            LOGGER.log(Level.FINE, () -> "Retry " + operation + " series=" + seriesKey + " command=" + command
                    + " target=" + target + " status=" + status + " ownerHint=" + ownerHint
                    + " attempt=" + attempts + " remainingMs=" + Math.max(0L, deadlineMs - clock.millis()));
            Duration remaining = remaining();
            Duration backoff = retryPolicy.backoffFor(attempts);
            sleepQuietly(backoff.compareTo(remaining) > 0 ? remaining : backoff);
            remaining();
        }
    }

    /** Retries transport failures without letting an RPC or backoff exceed the caller's deadline. */
    private <R> R callWithTransportRetry(NodeId target, String command, Object body, Class<R> responseType,
            long deadlineMs) {
        long startedAt = clock.millis();
        int attempt = 0;
        for (;;) {
            long remainingMs = deadlineMs - clock.millis();
            if (remainingMs <= 0) {
                throw new NgrrdClusterException(ErrorCode.TIMEOUT, "prazo esgotado em " + command + " de " + seriesKey);
            }
            Duration attemptTimeout = Duration.ofMillis(Math.max(1L, Math.min(requestTimeout.toMillis(), remainingMs)));
            try {
                return rpc.call(target, command, body, responseType, attemptTimeout);
            } catch (NgrrdClusterException e) {
                attempt++;
                long now = clock.millis();
                if (!TransportRetry.isTransportFailure(e) || retryPolicy.exhausted(startedAt, now)
                        || now >= deadlineMs) {
                    throw e;
                }
                long backoffCeilingMs = deadlineMs - now;
                Duration backoff = retryPolicy.backoffFor(attempt);
                Duration clampedBackoff = backoff.toMillis() > backoffCeilingMs
                        ? Duration.ofMillis(backoffCeilingMs) : backoff;
                TransportRetry.awaitConnectionOrBackoff(rpc, target, clampedBackoff);
            }
        }
    }

    private void noteWrongOwner(String newOwnerNodeId, OperationRetry retry) {
        if (newOwnerNodeId != null) {
            resolver.noteOwner(seriesKey, newOwnerNodeId);
            owner = newOwnerNodeId;
        } else {
            resolver.invalidate(seriesKey);
            owner = resolvePlacement(retry.remaining()).ownerNodeId();
        }
    }

    private void ensureOpen() {
        if (notFound) {
            throw new SeriesNotFoundException(seriesKey);
        }
        if (closed) {
            throw new NgrrdClusterException(ErrorCode.CLOSED, "handle fechado: " + seriesKey);
        }
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando retentativa", e);
        }
    }
}
