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
import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link NgrrdHandle} remoto: cada operação é roteada ao storage node dono da
 * série ({@link #owner}), com retentativa transparente em {@code WRONG_OWNER},
 * {@code NOT_OPEN} e {@code MIGRATING}. Escritas passam pelo {@link WriteBuffer}
 * do cliente; as demais operações são RPC direto ao dono.
 *
 * <p><strong>Somente leitura.</strong> Aberto com {@code createIfMissing=false}, o handle só lê:
 * {@link #write}, {@link #flush} e {@link #checkpoint} lançam {@link IllegalStateException} na hora, e o
 * handle nunca toca o {@link WriteBuffer}. O dono é sempre resolvido por
 * {@link PlacementLookup#resolveExisting} (nunca posiciona) e toda (re)abertura vai com
 * {@code createIfMissing=false}; se a série não existir mais, a leitura lança
 * {@link SeriesNotFoundException} e o handle se fecha localmente, saindo do mapa do cliente. O
 * {@link #close()} desse handle é local (ver {@link #close(Duration)}). O modo (somente leitura ou
 * gravável) é fixo: um {@code open} com criação da mesma chave abre um handle gravável NOVO, que
 * substitui este no mapa do cliente; este fica destacado — continua lendo para quem o tem, e seu
 * {@code close()} continua local e não afeta o gravável (a remoção condicional do mapa vira no-op).</p>
 */
public final class RemoteSeriesHandle implements NgrrdHandle {

    private static final Logger LOGGER = Logger.getLogger(RemoteSeriesHandle.class.getName());

    private final String seriesKey;
    private final String yaml;
    private final String definitionHashHex;
    private final GeometryDescriptor geometry;
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
    /**
     * Recebe {@code (seriesKey, this)} — a instância, não só a chave — para que quem remove do mapa do
     * cliente use remoção condicional ({@code Map#remove(key, value)}), nunca um {@code remove(key)}
     * incondicional: sem isso, o {@link #close(Duration)}/{@link #markSeriesNotFound} de um handle
     * ANTIGO (ex.: um close lento em andamento) poderia remover um handle NOVO já registrado para a
     * mesma chave.
     */
    private final BiConsumer<String, RemoteSeriesHandle> onClose;
    /** Confere {@code open.createIfMissing} no dono antes de todo {@code OPEN} de handle somente leitura. */
    private final NodeCapabilities capabilities;

    private volatile String owner;
    /**
     * Estado do handle, num único valor imutável: toda transição é um {@code compareAndSet} de um
     * {@link State} inteiro para outro, de modo que nenhuma thread observa uma combinação intermediária
     * (ex.: fechado sem a causa da ausência). Quem leva o handle a fechado ({@link #close(Duration)} ou
     * {@link #markSeriesNotFound}) é o único a executar o encerramento.
     */
    private final AtomicReference<State> state;

    /**
     * Foto do estado do handle.
     *
     * @param writable {@code true} se o handle aceita escrita (aberto com criação): decide se a
     *                 (re)abertura posiciona com criação ({@code ngrrd.place}) ou exige a série existente
     * @param closed   {@code true} depois do fechamento — nunca mais reaproveitável
     * @param notFound causa da ausência quando o fechamento veio da descoberta de que a série não existe;
     *                 {@code null} num handle aberto ou fechado pelo chamador
     */
    private record State(boolean writable, boolean closed, SeriesNotFoundException notFound) {

        static State opened(boolean writable) {
            return new State(writable, false, null);
        }

        State closedByCaller() {
            return new State(writable, true, null);
        }

        State closedBySeriesNotFound(SeriesNotFoundException cause) {
            return new State(writable, true, cause);
        }
    }

    public RemoteSeriesHandle(String seriesKey, String yaml, String definitionHashHex, Map<String, String> tags,
            Ngrrd.OpenOptions options, PlacementLookup resolver, ClusterRpc rpc, WriteBuffer dispatcher,
            RetryPolicy retryPolicy, Duration requestTimeout, Duration closeTimeout, Clock clock,
            BiConsumer<String, RemoteSeriesHandle> onClose, NodeCapabilities capabilities) {
        this(seriesKey, yaml, definitionHashHex, tags, options, resolver, rpc, dispatcher, retryPolicy,
                requestTimeout, closeTimeout, clock, onClose, capabilities, null);
    }

    public RemoteSeriesHandle(String seriesKey, String yaml, String definitionHashHex, Map<String, String> tags,
            Ngrrd.OpenOptions options, PlacementLookup resolver, ClusterRpc rpc, WriteBuffer dispatcher,
            RetryPolicy retryPolicy, Duration requestTimeout, Duration closeTimeout, Clock clock,
            BiConsumer<String, RemoteSeriesHandle> onClose, NodeCapabilities capabilities,
            GeometryDescriptor geometry) {
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
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.state = new AtomicReference<>(State.opened(this.options.createIfMissing()));
    }

    @Override
    public String seriesKey() {
        return seriesKey;
    }

    /**
     * Resolve o dono e abre a série nele. Chamado uma vez por
     * {@code DefaultNgrrdClusterClient.open}, para um handle novo; {@link #reopen()} reexecuta
     * a abertura quando um dono sinaliza {@code NOT_OPEN}.
     *
     * <p>item 12 (achado do Refuter): package-private de propósito — só {@code DefaultNgrrdClusterClient}
     * e o próprio {@code client} chamam isto; não faz parte do contrato público de {@link NgrrdHandle}.</p>
     */
    void open() {
        open(new OperationRetry(Commands.OPEN));
    }

    private void open(OperationRetry retry) {
        for (;;) {
            boolean writable = state.get().writable();
            SeriesPlacement placement = resolvePlacement(writable, retry.remaining());
            String candidateOwner = placement.ownerNodeId();
            if (!writable) {
                // Dono de versão anterior ignoraria createIfMissing=false e criaria a série.
                capabilities.require(candidateOwner, StorageCapabilities.OPEN_CREATE_IF_MISSING);
            }
            OpenRequest request = new OpenRequest(seriesKey, yaml, tags, options.durability(),
                    options.onGeometryChange(), placement, writable ? null : Boolean.FALSE);
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
                // NOT_FOUND: o dono confirmou com o líder que a série é dele e o arquivo não existe.
                case NOT_FOUND -> throw new SeriesNotFoundException(seriesKey,
                        SeriesNotFoundException.Reason.MISSING_ON_OWNER);
                default -> throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        response.message() != null ? response.message() : ("OPEN respondeu " + response.status()));
            }
        }
    }

    /**
     * Placement usado para posicionar o {@code OPEN}: num handle gravável (aberto com criação), cria a
     * série se preciso ({@code ngrrd.place}); num handle somente leitura, nunca cria —
     * exige que a série já exista ({@link PlacementLookup#resolveExisting}), lançando
     * {@link SeriesNotFoundException} se o líder confirmar que não há placement.
     */
    private SeriesPlacement resolvePlacement(boolean writable, Duration maxWait) {
        return writable ? resolver.resolve(seriesKey, definitionHashHex, geometry, maxWait)
                : resolver.resolveExisting(seriesKey, maxWait);
    }

    /**
     * Reexecuta a abertura remota, absorvendo qualquer falha — usado como callback pelo
     * {@code WriteDispatcher} quando um dono responde {@code NOT_OPEN} a um lote. Nunca lança: o
     * chamador só precisa saber se deu certo.
     *
     * <p>Um handle somente leitura não participa de escritas: devolve {@code false} sem RPC, como se não
     * houvesse handle para a chave. Isso só acontece se escritas de um handle gravável anterior da mesma
     * chave ainda estiverem pendentes quando este ocupou o mapa do cliente.</p>
     */
    boolean reopen() {
        if (!state.get().writable()) {
            return false;
        }
        try {
            open(new OperationRetry(Commands.OPEN));
            return true;
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao reabrir a série " + seriesKey, e);
            return false;
        }
    }

    /**
     * Executa {@code action}; se ela descobrir {@link SeriesNotFoundException}, fecha o handle somente
     * leitura ({@link #markSeriesNotFound}) antes de relançar. Usado pelos dois caminhos em que uma leitura
     * pode descobrir isso ao reabrir/reposicionar a série sem criar: {@link #handleRetryableStatus} em
     * {@code NOT_OPEN} e {@link #noteWrongOwner} em {@code WRONG_OWNER} sem dono informado.
     */
    private <T> T markingSeriesNotFound(Supplier<T> action) {
        try {
            return action.get();
        } catch (SeriesNotFoundException e) {
            markSeriesNotFound(e);
            throw e;
        }
    }

    /**
     * Fecha localmente o handle somente leitura cuja série se confirmou ausente: as operações passam a
     * lançar {@link SeriesNotFoundException} e o handle sai do mapa do cliente ({@link #onClose}, remoção
     * condicional à instância), de modo que um {@code open} posterior refaz o fluxo do zero. Sem
     * {@code CLOSE} remoto: a série não está aberta no dono. Não faz nada num handle já fechado (a leitura
     * em curso recebe a exceção, mas o estado fechado pelo chamador é preservado) nem num gravável.
     */
    private void markSeriesNotFound(SeriesNotFoundException cause) {
        for (;;) {
            State current = state.get();
            if (current.closed() || current.writable()) {
                return;
            }
            if (state.compareAndSet(current, current.closedBySeriesNotFound(cause))) {
                onClose.accept(seriesKey, this);
                return;
            }
        }
    }

    /** Se o handle ainda pode ser reaproveitado por um {@code open} futuro da mesma chave. */
    boolean isOpen() {
        return !state.get().closed();
    }

    /** Se o handle aceita escrita — aberto com criação; fixo desde a construção. */
    boolean isWritable() {
        return state.get().writable();
    }

    /**
     * Fecha localmente um handle que nunca foi publicado no mapa do cliente — o perdedor de uma abertura
     * concorrente da mesma chave. Sem flush, sem {@code CLOSE} remoto e sem {@link #onClose}: nenhuma
     * escrita passou por ele, e um {@code CLOSE} fecharia no storage a série que o handle vencedor usa.
     */
    void discard() {
        closeLocally();
    }

    /** Recusa de escrita em handle (ou vista) somente leitura; mensagem única para os dois. */
    static IllegalStateException readOnlyViolation(String seriesKey) {
        return new IllegalStateException("série " + seriesKey + " aberta somente leitura (createIfMissing=false);"
                + " abra com criação para escrever");
    }

    @Override
    public void write(String dsName, Sample sample) {
        ensureWritable();
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
        ensureWritable();
        OperationRetry retry = new OperationRetry(Commands.FLUSH);
        dispatcher.flushSeriesSync(seriesKey, owner, retry.remaining());
        executeSeriesCommand(Commands.FLUSH, retry);
    }

    @Override
    public void checkpoint() {
        ensureWritable();
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

    /**
     * Fecha o handle ({@link #close(Duration)} com o {@code closeTimeout} do cliente). Aberto somente
     * leitura, o fechamento é local e síncrono — sem {@code CLOSE} remoto; a série no storage fecha
     * pela ociosidade do próprio storage.
     */
    @Override
    public void close() {
        // B1 (achado do Refuter): usa closeTimeout da config, não retryTimeout — um close() avulso
        // (fora de DefaultNgrrdClusterClient.close(), que sempre chama close(Duration) com o orçamento
        // restante do fechamento compartilhado) segue o mesmo orçamento que o cliente anuncia para
        // "quanto tempo um close pode levar", não o prazo de retentativa de operações normais.
        close(closeTimeout);
    }

    /**
     * Fecha o handle. Num handle somente leitura, o fechamento é LOCAL e síncrono: só sai do mapa do
     * cliente ({@link #onClose}, remoção condicional) e passa a recusar operações — sem flush, sem
     * {@code CLOSE} remoto, sem checkpoint, sem tocar o {@link WriteBuffer}. A série continua aberta no
     * storage até ser fechada pela ociosidade do próprio storage.
     *
     * <p>Num handle gravável (aberto com criação), fecha a série com um ÚNICO orçamento total
     * para flush + CLOSE remoto — usado por {@code DefaultNgrrdClusterClient.close()} para respeitar um
     * orçamento compartilhado entre vários handles (O1).</p>
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
        State closedFrom = closeLocally();
        if (closedFrom == null) {
            return;
        }
        if (!closedFrom.writable()) {
            onClose.accept(seriesKey, this);
            return;
        }
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
        onClose.accept(seriesKey, this);
    }

    /**
     * Leva o handle a fechado e devolve o estado de onde saiu, ou {@code null} se outro encerramento
     * (inclusive a descoberta de série ausente) já tinha vencido — só um encerramento executa o
     * fechamento.
     */
    private State closeLocally() {
        for (;;) {
            State current = state.get();
            if (current.closed()) {
                return null;
            }
            if (state.compareAndSet(current, current.closedByCaller())) {
                return current;
            }
        }
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
                    markingSeriesNotFound(() -> {
                        open(retry);
                        return null;
                    });
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

    /**
     * Redireciona depois de um {@code WRONG_OWNER}. Com o dono informado, segue com ele. Sem dono, um
     * handle gravável re-resolve como sempre ({@link PlacementLookup#resolve}); um handle somente leitura
     * confirma direto com o líder ({@link PlacementLookup#resolveExistingAtLeader}) — a réplica local pode
     * estar atrasada e devolveria o mesmo dono até o prazo se esgotar. Ausente no líder, a leitura termina
     * em {@link SeriesNotFoundException} com {@code NOT_PLACED} e o handle se fecha.
     */
    private void noteWrongOwner(String newOwnerNodeId, OperationRetry retry) {
        if (newOwnerNodeId != null) {
            resolver.noteOwner(seriesKey, newOwnerNodeId);
            owner = newOwnerNodeId;
            return;
        }
        resolver.invalidate(seriesKey);
        if (state.get().writable()) {
            owner = resolvePlacement(true, retry.remaining()).ownerNodeId();
        } else {
            owner = markingSeriesNotFound(() -> resolver.resolveExistingAtLeader(seriesKey, retry.remaining()))
                    .ownerNodeId();
        }
    }

    private void ensureOpen() {
        ensureOpen(state.get());
    }

    /** Recusa operações num handle fechado, a partir de uma única leitura do estado. */
    private void ensureOpen(State current) {
        if (current.notFound() != null) {
            throw new SeriesNotFoundException(seriesKey, current.notFound().reason());
        }
        if (current.closed()) {
            throw new NgrrdClusterException(ErrorCode.CLOSED, "handle fechado: " + seriesKey);
        }
    }

    /** Recusa, antes de qualquer outra checagem, operações de escrita em handle somente leitura. */
    private void ensureWritable() {
        State current = state.get();
        if (!current.writable()) {
            throw readOnlyViolation(seriesKey);
        }
        ensureOpen(current);
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
