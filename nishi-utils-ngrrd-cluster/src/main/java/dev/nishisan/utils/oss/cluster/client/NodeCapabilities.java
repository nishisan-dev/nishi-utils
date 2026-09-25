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

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Confere se um storage node anuncia uma capacidade de protocolo ({@link StorageCapabilities}) antes de
 * o cliente lhe enviar uma operação que depende dela.
 *
 * <p>Lê primeiro o status do nó na réplica local do catálogo {@code ngrrd.nodes}; se ele anunciar a
 * capacidade, segue sem nenhum RPC. Em qualquer outro caso (status local ausente, ou presente sem a
 * capacidade — a réplica pode estar atrasada logo depois de uma atualização do nó) confirma com uma
 * leitura forte no líder, custo que só existe no caminho de falha:</p>
 * <ul>
 *   <li>status no líder com a capacidade: segue;</li>
 *   <li>status no líder sem a capacidade (inclusive o de um nó de versão anterior, que publica o status
 *       sem capacidades): {@link ErrorCode#UNSUPPORTED_BY_NODE} ("&lt;nó&gt; não anuncia &lt;capacidade&gt;")
 *       na hora;</li>
 *   <li>nenhum status nem no líder (ex.: cluster subindo, nó que ainda não publicou) ou falha da leitura
 *       forte (sem líder, falha de transporte — o {@code DistributedMap} não distingue as duas): relê com
 *       backoff curto dentro do prazo restante da operação. No fim do prazo, status ainda ausente vira
 *       {@link ErrorCode#UNSUPPORTED_BY_NODE} ("status de &lt;nó&gt; indisponível"); uma leitura forte que
 *       só falhou vira {@link ErrorCode#TIMEOUT} com a última causa — nunca confundida com falta de
 *       capacidade.</li>
 * </ul>
 *
 * <p>Usado por {@link CatalogLookupClient} ({@code catalog.lookup} no líder, antes de
 * {@code exists}/{@code find}/resolução de séries existentes) e por {@link RemoteSeriesHandle}
 * ({@code open.createIfMissing} no dono, antes de todo {@code OPEN} de handle somente leitura), e
 * reutilizável por qualquer outra operação que exija uma capacidade.</p>
 */
public final class NodeCapabilities {

    /** Primeira espera entre releituras do status ausente; dobra até {@link #MAX_BACKOFF}. */
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(10);
    private static final Duration MAX_BACKOFF = Duration.ofMillis(200);

    private final Function<String, Optional<StorageNodeStatus>> localStatus;
    private final Function<String, Optional<StorageNodeStatus>> strongStatus;
    private final Clock clock;
    private final Consumer<Duration> sleeper;

    /**
     * @param localStatus  status do nó na réplica local (leitura eventual, sem RPC)
     * @param strongStatus status do nó confirmado no líder — só consultado quando o local não basta
     */
    public NodeCapabilities(Function<String, Optional<StorageNodeStatus>> localStatus,
            Function<String, Optional<StorageNodeStatus>> strongStatus) {
        this(localStatus, strongStatus, Clock.systemUTC(), LeaderCalls::sleepQuietly);
    }

    /** Variante com relógio e espera injetáveis, para testar o prazo sem dormir de verdade. */
    NodeCapabilities(Function<String, Optional<StorageNodeStatus>> localStatus,
            Function<String, Optional<StorageNodeStatus>> strongStatus, Clock clock, Consumer<Duration> sleeper) {
        this.localStatus = Objects.requireNonNull(localStatus, "localStatus");
        this.strongStatus = Objects.requireNonNull(strongStatus, "strongStatus");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /** Capacidades lidas do catálogo {@code ngrrd.nodes} de {@code catalog}. */
    public static NodeCapabilities from(CatalogService catalog) {
        Objects.requireNonNull(catalog, "catalog");
        return new NodeCapabilities(catalog::nodeStatusLocal, catalog::nodeStatusStrong);
    }

    /**
     * Exige que {@code nodeId} anuncie {@code capability}, esperando no máximo {@code maxWait} por um
     * status que ainda não foi publicado.
     *
     * @throws NgrrdClusterException com {@link ErrorCode#UNSUPPORTED_BY_NODE} se o líder confirmar que o
     *         nó não anuncia a capacidade, ou se não houver status do nó até o fim do prazo; com
     *         {@link ErrorCode#TIMEOUT} se a leitura forte só falhou até o fim do prazo
     */
    public void require(String nodeId, String capability, Duration maxWait) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(maxWait, "maxWait");
        if (localStatus.apply(nodeId).map(status -> status.advertises(capability)).orElse(false)) {
            return;
        }
        long deadline = clock.millis() + Math.max(0L, maxWait.toMillis());
        Duration backoff = INITIAL_BACKOFF;
        RuntimeException lastFailure;
        for (;;) {
            lastFailure = null;
            StorageNodeStatus strong = null;
            try {
                strong = strongStatus.apply(nodeId).orElse(null);
            } catch (RuntimeException e) {
                if (e instanceof NgrrdClusterException clusterFailure
                        && !TransportRetry.isTransportFailure(clusterFailure)) {
                    throw clusterFailure;
                }
                lastFailure = e;
            }
            if (strong != null) {
                if (strong.advertises(capability)) {
                    return;
                }
                throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE,
                        nodeId + " não anuncia " + capability);
            }
            long remainingMs = deadline - clock.millis();
            if (remainingMs <= 0) {
                break;
            }
            sleeper.accept(backoff.toMillis() > remainingMs ? Duration.ofMillis(remainingMs) : backoff);
            backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2);
        }
        if (lastFailure != null) {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "não foi possível ler no líder o status de " + nodeId
                    + " para conferir " + capability + " dentro do prazo", lastFailure);
        }
        throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE, "status de " + nodeId
                + " indisponível (nenhum status publicado dentro do prazo); não foi possível confirmar " + capability);
    }
}
