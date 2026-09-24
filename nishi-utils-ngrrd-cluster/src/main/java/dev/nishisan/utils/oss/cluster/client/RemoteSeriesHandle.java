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

    /** Máximo de retentativas consecutivas de {@code WRONG_OWNER} ao abrir, antes de desistir. */
    private static final int MAX_WRONG_OWNER_ATTEMPTS_ON_OPEN = 5;

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
        long startedAt = clock.millis();
        // Chamadores atuais passam startedAt + retryTimeout — comportamento inalterado em relação a
        // antes do B1 (achado do Refuter): callWithTransportRetry só ganhou um SEGUNDO critério de
        // saída (o deadline explícito), não um prazo mais curto para este caminho.
        long deadlineMs = startedAt + retryPolicy.timeout().toMillis();
        int wrongOwnerAttempts = 0;
        int migratingAttempts = 0;
        for (;;) {
            SeriesPlacement placement = resolver.resolve(seriesKey, definitionHashHex, geometry);
            String candidateOwner = placement.ownerNodeId();
            OpenRequest request = new OpenRequest(seriesKey, yaml, tags, options.durability(),
                    options.onGeometryChange(), placement);
            SeriesStatusResponse response = callWithTransportRetry(NodeId.of(candidateOwner), Commands.OPEN, request,
                    SeriesStatusResponse.class, deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                owner = candidateOwner;
                return;
            }
            if (response.status() == SeriesStatus.NOT_LEADER) {
                if (retryPolicy.exhausted(startedAt, clock.millis())) {
                    throw new NgrrdClusterException(ErrorCode.NO_LEADER, "geometry publication has no stable leader");
                }
                sleepQuietly(retryPolicy.backoffFor(++migratingAttempts));
                continue;
            }
            if (response.status() == SeriesStatus.WRONG_OWNER) {
                wrongOwnerAttempts++;
                noteWrongOwner(response.ownerNodeId());
                if (wrongOwnerAttempts >= MAX_WRONG_OWNER_ATTEMPTS_ON_OPEN) {
                    throw new NgrrdClusterException(ErrorCode.WRONG_OWNER,
                            "WRONG_OWNER persistente ao abrir a série " + seriesKey);
                }
                continue;
            }
            if (response.status() == SeriesStatus.MIGRATING) {
                if (retryPolicy.exhausted(startedAt, clock.millis())) {
                    throw new NgrrdClusterException(ErrorCode.MIGRATING,
                            "série em migração além do prazo ao abrir: " + seriesKey);
                }
                sleepQuietly(retryPolicy.backoffFor(++migratingAttempts));
                continue;
            }
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    response.message() != null ? response.message() : ("OPEN respondeu " + response.status()));
        }
    }

    /**
     * Reexecuta {@link #open()}, absorvendo qualquer falha — usado como
     * callback pelo {@code WriteDispatcher} quando um dono responde
     * {@code NOT_OPEN} a um lote. Nunca lança: o chamador só precisa saber se
     * deu certo.
     */
    boolean reopen() {
        try {
            open();
            return true;
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao reabrir a série " + seriesKey, e);
            return false;
        }
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
        dispatcher.flushSeriesSync(seriesKey, owner);
        executeSeriesCommand(Commands.FLUSH);
    }

    @Override
    public void checkpoint() {
        ensureOpen();
        dispatcher.flushSeriesSync(seriesKey, owner);
        executeSeriesCommand(Commands.CHECKPOINT);
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
        long startedAt = clock.millis();
        long deadlineMs = startedAt + retryPolicy.timeout().toMillis();
        boolean retriedOnce = false;
        int[] migratingAttempts = {0};
        for (;;) {
            ReadResponse response = callWithTransportRetry(NodeId.of(owner), Commands.READ, request,
                    ReadResponse.class, deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                return response.result();
            }
            retriedOnce = handleRetryableStatus(Commands.READ, response.status(), response.ownerNodeId(),
                    response.message(), startedAt, retriedOnce, migratingAttempts);
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
        long startedAt = clock.millis();
        long deadlineMs = startedAt + retryPolicy.timeout().toMillis();
        boolean retriedOnce = false;
        int[] migratingAttempts = {0};
        for (;;) {
            ReadPresetResponse response = callWithTransportRetry(NodeId.of(owner), Commands.READ_PRESET, request,
                    ReadPresetResponse.class, deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                return response.results();
            }
            retriedOnce = handleRetryableStatus(Commands.READ_PRESET, response.status(), response.ownerNodeId(),
                    response.message(), startedAt, retriedOnce, migratingAttempts);
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

    private void executeSeriesCommand(String command) {
        long startedAt = clock.millis();
        long deadlineMs = startedAt + retryPolicy.timeout().toMillis();
        boolean retriedOnce = false;
        int[] migratingAttempts = {0};
        for (;;) {
            SeriesStatusResponse response = callWithTransportRetry(NodeId.of(owner), command,
                    new SeriesCommandRequest(seriesKey), SeriesStatusResponse.class, deadlineMs);
            if (response.status() == SeriesStatus.OK) {
                return;
            }
            retriedOnce = handleRetryableStatus(command, response.status(), response.ownerNodeId(),
                    response.message(), startedAt, retriedOnce, migratingAttempts);
        }
    }

    /**
     * Trata os status não-OK comuns a {@code checkpoint}/{@code flush}/
     * {@code read}/{@code readPreset}: {@code WRONG_OWNER} e {@code NOT_OPEN}
     * repetem uma única vez; {@code MIGRATING} faz backoff até
     * {@code retryTimeout}; qualquer outro status vira {@link NgrrdClusterException}.
     *
     * @param migratingAttempts contador de tentativas de MIGRATING do chamador (posição 0), mutado
     *                          aqui — item 6 (achado do Refuter): sem isso, cada MIGRATING dormia
     *                          sempre {@code backoffFor(1)} em vez de crescer exponencialmente
     * @return o novo valor de {@code retriedOnce} para a próxima iteração do chamador
     */
    private boolean handleRetryableStatus(String command, SeriesStatus status, String ownerNodeId, String message,
            long startedAt, boolean retriedOnce, int[] migratingAttempts) {
        if (status == SeriesStatus.WRONG_OWNER) {
            if (retriedOnce) {
                throw new NgrrdClusterException(ErrorCode.WRONG_OWNER,
                        "WRONG_OWNER persistente em " + command + " de " + seriesKey);
            }
            noteWrongOwner(ownerNodeId);
            return true;
        }
        if (status == SeriesStatus.NOT_OPEN) {
            if (retriedOnce) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "NOT_OPEN persistente em " + command + " de " + seriesKey);
            }
            if (!reopen()) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "falha ao reabrir a série " + seriesKey + " após NOT_OPEN em " + command);
            }
            return true;
        }
        if (status == SeriesStatus.MIGRATING) {
            if (retryPolicy.exhausted(startedAt, clock.millis())) {
                throw new NgrrdClusterException(ErrorCode.MIGRATING,
                        "série em migração além do prazo em " + command + ": " + seriesKey);
            }
            migratingAttempts[0]++;
            sleepQuietly(retryPolicy.backoffFor(migratingAttempts[0]));
            return retriedOnce;
        }
        throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                message != null ? message : (command + " respondeu " + status));
    }

    /**
     * B3(ii) (achado do Refuter): envolve {@code rpc.call} com retentativa de falha de TRANSPORTE
     * (não de aplicação) com backoff exponencial — usado por {@code open}, {@code executeSeriesCommand}
     * (CHECKPOINT/FLUSH), {@code read}/{@code readPreset} e {@code close}. Falhas de aplicação (status
     * não-OK do protocolo) continuam subindo normalmente na resposta, sem passar por aqui.
     *
     * <p>B1 (achado do Refuter): cada tentativa usa {@code min(requestTimeout, restante-até-o-deadline)}
     * como teto da PRÓPRIA chamada — não o {@code requestTimeout} cheio incondicionalmente — e a
     * DECISÃO DE RETENTAR (não a de tentar a primeira vez) sai quando {@code retryPolicy.exhausted(...)}
     * OU {@code now >= deadlineMs}, o que vier primeiro. Antes desta correção, uma única tentativa
     * contra um nó morto podia consumir o {@code requestTimeout} inteiro mesmo com um
     * {@code deadlineMs}/{@code retryTimeout} bem mais curto configurado (ex.: {@code close()} com um
     * orçamento apertado) — o teto "efetivo" real acabava sendo o maior dos dois, não o menor. O
     * backoff entre tentativas também é clampado ao que resta até {@code deadlineMs}, nunca dorme além
     * dele.</p>
     *
     * <p><strong>De propósito, a PRIMEIRA tentativa nunca é recusada de antemão por
     * {@code now >= deadlineMs}</strong> (só o teto da própria chamada encolhe, com piso de 1 ms): para
     * os chamadores existentes (tudo exceto {@code close}), {@code deadlineMs} é sempre
     * {@code startedAt + retryPolicy.timeout()} — matematicamente o MESMO instante que
     * {@code retryPolicy.exhausted(startedAt, now)} já testava antes do B1 — então recusar de antemão
     * mudaria o tipo da exceção (TIMEOUT em vez de MIGRATING/WRONG_OWNER persistente) num laço externo
     * de retentativa (ex.: MIGRATING em {@link #handleRetryableStatus}) sempre que o backoff entre
     * chamadas empurrasse {@code now} ligeiramente além do deadline entre uma chamada e outra —
     * "comportamento inalterado" para esses chamadores, como pedido.</p>
     */
    private <R> R callWithTransportRetry(NodeId target, String command, Object body, Class<R> responseType,
            long deadlineMs) {
        long startedAt = clock.millis();
        int attempt = 0;
        for (;;) {
            long remainingMs = deadlineMs - clock.millis();
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

    private void noteWrongOwner(String newOwnerNodeId) {
        if (newOwnerNodeId != null) {
            resolver.noteOwner(seriesKey, newOwnerNodeId);
            owner = newOwnerNodeId;
        } else {
            resolver.invalidate(seriesKey);
            owner = resolver.resolve(seriesKey, definitionHashHex, geometry).ownerNodeId();
        }
    }

    private void ensureOpen() {
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
