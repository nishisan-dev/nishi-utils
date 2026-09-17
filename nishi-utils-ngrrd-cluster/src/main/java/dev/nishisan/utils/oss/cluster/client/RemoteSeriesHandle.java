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
    private final Map<String, String> tags;
    private final Ngrrd.OpenOptions options;
    private final PlacementLookup resolver;
    private final ClusterRpc rpc;
    private final WriteBuffer dispatcher;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final Consumer<String> onClose;

    private volatile String owner;
    private volatile boolean closed;

    public RemoteSeriesHandle(String seriesKey, String yaml, String definitionHashHex, Map<String, String> tags,
            Ngrrd.OpenOptions options, PlacementLookup resolver, ClusterRpc rpc, WriteBuffer dispatcher,
            RetryPolicy retryPolicy, Clock clock, Consumer<String> onClose) {
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey");
        this.yaml = Objects.requireNonNull(yaml, "yaml");
        this.definitionHashHex = Objects.requireNonNull(definitionHashHex, "definitionHashHex");
        this.tags = Map.copyOf(Objects.requireNonNullElse(tags, Map.of()));
        this.options = options != null ? options : Ngrrd.OpenOptions.defaults();
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
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
        int wrongOwnerAttempts = 0;
        int migratingAttempts = 0;
        for (;;) {
            SeriesPlacement placement = resolver.resolve(seriesKey, definitionHashHex);
            String candidateOwner = placement.ownerNodeId();
            OpenRequest request = new OpenRequest(seriesKey, yaml, tags, options.durability(),
                    options.onGeometryChange(), placement);
            SeriesStatusResponse response = callWithTransportRetry(NodeId.of(candidateOwner), Commands.OPEN, request,
                    SeriesStatusResponse.class);
            if (response.status() == SeriesStatus.OK) {
                owner = candidateOwner;
                return;
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

    @Override
    public void flush() {
        ensureOpen();
        dispatcher.flushNodeSync(owner);
        executeSeriesCommand(Commands.FLUSH);
    }

    @Override
    public void checkpoint() {
        ensureOpen();
        dispatcher.flushNodeSync(owner);
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
        boolean retriedOnce = false;
        int[] migratingAttempts = {0};
        for (;;) {
            ReadResponse response = callWithTransportRetry(NodeId.of(owner), Commands.READ, request,
                    ReadResponse.class);
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
        boolean retriedOnce = false;
        int[] migratingAttempts = {0};
        for (;;) {
            ReadPresetResponse response = callWithTransportRetry(NodeId.of(owner), Commands.READ_PRESET, request,
                    ReadPresetResponse.class);
            if (response.status() == SeriesStatus.OK) {
                return response.results();
            }
            retriedOnce = handleRetryableStatus(Commands.READ_PRESET, response.status(), response.ownerNodeId(),
                    response.message(), startedAt, retriedOnce, migratingAttempts);
        }
    }

    @Override
    public void close() {
        // Uso direto fora de DefaultNgrrdClusterClient.close() (que sempre chama close(Duration) com o
        // orçamento restante do fechamento compartilhado — O1): usa o próprio retryTimeout como teto,
        // generoso o bastante para não truncar um flush legítimo num close() avulso.
        close(retryPolicy.timeout());
    }

    /**
     * Fecha a série com um teto explícito para o flush do buffer de escrita — usado por
     * {@code DefaultNgrrdClusterClient.close()} para respeitar um orçamento TOTAL compartilhado entre
     * vários handles (O1): se {@code flushBudget} estourar, o flush é abandonado (as amostras
     * pendentes ficam a cargo do {@code WriteDispatcher.close()} final, que loga quantas foram
     * descartadas), mas o {@code CLOSE} remoto e a remoção do registro do cliente sempre acontecem.
     *
     * <p>item 12 (achado do Refuter): package-private — só {@code DefaultNgrrdClusterClient.close()}
     * chama esta sobrecarga; o contrato público de {@link NgrrdHandle} continua sendo só {@link #close()}.</p>
     */
    void close(Duration flushBudget) {
        if (closed) {
            return;
        }
        closed = true;
        try {
            dispatcher.flushNodeSync(owner, flushBudget);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao drenar buffer de escrita ao fechar a série " + seriesKey, e);
        }
        try {
            callWithTransportRetry(NodeId.of(owner), Commands.CLOSE, new SeriesCommandRequest(seriesKey),
                    SeriesStatusResponse.class);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao fechar remotamente a série " + seriesKey, e);
        }
        onClose.accept(seriesKey);
    }

    private void executeSeriesCommand(String command) {
        long startedAt = clock.millis();
        boolean retriedOnce = false;
        int[] migratingAttempts = {0};
        for (;;) {
            SeriesStatusResponse response = callWithTransportRetry(NodeId.of(owner), command,
                    new SeriesCommandRequest(seriesKey), SeriesStatusResponse.class);
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
     * (não de aplicação) com backoff exponencial até {@code retryPolicy.timeout()} — usado por
     * {@code open}, {@code executeSeriesCommand} (CHECKPOINT/FLUSH), {@code read}/{@code readPreset} e
     * {@code close}. Falhas de aplicação (status não-OK do protocolo) continuam subindo normalmente
     * na resposta, sem passar por aqui.
     */
    private <R> R callWithTransportRetry(NodeId target, String command, Object body, Class<R> responseType) {
        long startedAt = clock.millis();
        int attempt = 0;
        for (;;) {
            try {
                return rpc.call(target, command, body, responseType);
            } catch (NgrrdClusterException e) {
                attempt++;
                if (!TransportRetry.isTransportFailure(e) || retryPolicy.exhausted(startedAt, clock.millis())) {
                    throw e;
                }
                TransportRetry.awaitConnectionOrBackoff(rpc, target, retryPolicy.backoffFor(attempt));
            }
        }
    }

    private void noteWrongOwner(String newOwnerNodeId) {
        if (newOwnerNodeId != null) {
            resolver.noteOwner(seriesKey, newOwnerNodeId);
            owner = newOwnerNodeId;
        } else {
            resolver.invalidate(seriesKey);
            owner = resolver.resolve(seriesKey, definitionHashHex).ownerNodeId();
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
