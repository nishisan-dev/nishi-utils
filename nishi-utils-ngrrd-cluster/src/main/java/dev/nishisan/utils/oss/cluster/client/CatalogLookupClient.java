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
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupRequest;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Consulta o catálogo no líder em páginas sequenciais; nunca cria placement.
 *
 * <p>Usada por {@link PlacementResolver#resolveExisting} para confirmar com o líder, com autoridade,
 * séries ausentes (ou em {@code MIGRATING}) no cache local antes de reportar {@code SeriesNotFoundException}
 * — um consumidor (TEMS) que receba um "não existe" errado apaga a série do catálogo externo, então
 * qualquer falha ao consultar vira {@link NgrrdClusterException}, nunca resposta parcial nem ausência
 * silenciosa.</p>
 */
public final class CatalogLookupClient {

    private final ClusterRpc rpc;
    private final RetryPolicy retry;
    private final Clock clock;
    private final int batchSize;

    public CatalogLookupClient(ClusterRpc rpc, RetryPolicy retry, Clock clock, int batchSize) {
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.retry = Objects.requireNonNull(retry, "retry");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize deve ser > 0: " + batchSize);
        }
        this.batchSize = batchSize;
    }

    /**
     * Placements presentes no líder; chaves ausentes ficam fora do mapa.
     *
     * @throws NgrrdClusterException em qualquer falha (NO_LEADER, TIMEOUT, transporte, REMOTE_ERROR) —
     *         nunca resposta parcial
     */
    public Map<String, SeriesPlacement> lookup(Collection<String> seriesKeys, Duration maxWait) {
        Objects.requireNonNull(seriesKeys, "seriesKeys");
        Objects.requireNonNull(maxWait, "maxWait");
        List<String> distinctKeys = List.copyOf(new LinkedHashSet<>(seriesKeys));
        if (distinctKeys.isEmpty()) {
            return Map.of();
        }
        // Prazo único para TODAS as páginas — não reinicia a cada página, senão um lote grande com
        // páginas lentas poderia consumir várias vezes o orçamento do chamador.
        long deadline = clock.millis() + Math.min(retry.timeout().toMillis(), maxWait.toMillis());
        Map<String, SeriesPlacement> found = new LinkedHashMap<>();
        for (int start = 0; start < distinctKeys.size(); start += batchSize) {
            List<String> page = distinctKeys.subList(start, Math.min(start + batchSize, distinctKeys.size()));
            found.putAll(lookupPage(page, deadline));
        }
        return Map.copyOf(found);
    }

    private Map<String, SeriesPlacement> lookupPage(List<String> page, long deadline) {
        String description = "consultar o catálogo (" + page.size() + " chave(s))";
        int attempt = 0;
        // Mesma técnica de PlacementResolver#placeAtLeader: um hint de líder vindo da resposta
        // NOT_LEADER é usado direto na próxima tentativa, sem reconsultar rpc.leaderId() (que pode
        // estar vazio/desatualizado bem no meio de um handoff).
        NodeId leaderHint = null;
        for (;;) {
            attempt++;
            NodeId leader = leaderHint != null ? leaderHint
                    : LeaderCalls.awaitLeaderOrThrow(rpc, clock, deadline, description);
            leaderHint = null;
            CatalogLookupResponse response;
            try {
                response = rpc.call(leader, Commands.CATALOG_LOOKUP, new CatalogLookupRequest(page),
                        CatalogLookupResponse.class, LeaderCalls.remainingUntil(clock, deadline, description));
            } catch (NgrrdClusterException e) {
                // Falha de TRANSPORTE (não de aplicação) ao chamar o líder — retenta com backoff até o
                // prazo, esperando a conexão voltar em vez de tentar de novo às cegas. Um líder antigo
                // sem handler para o comando cai neste mesmo ramo: ninguém responde, a chamada expira
                // com TIMEOUT (que TransportRetry.isTransportFailure trata como falha de transporte),
                // é retentada até o prazo se esgotar e termina em NgrrdClusterException(TIMEOUT).
                // Qualquer outra falha de aplicação (REMOTE_ERROR sem causa de transporte) propaga
                // imediatamente, sem retentativa.
                if (!TransportRetry.isTransportFailure(e) || clock.millis() >= deadline) {
                    throw e;
                }
                TransportRetry.awaitConnectionOrBackoff(rpc, leader,
                        LeaderCalls.cappedBackoff(clock, retry.backoffFor(attempt), deadline, description));
                continue;
            }
            if (response == null || response.status() == null) {
                // Resposta nula ou status desconhecido (o codec lê enum desconhecido como null, ex.:
                // líder de versão anterior) é falha, nunca ausência.
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "resposta inválida do líder para CATALOG_LOOKUP");
            }
            switch (response.status()) {
                case OK -> {
                    return response.found();
                }
                case NOT_LEADER -> {
                    if (response.leaderNodeId() != null) {
                        leaderHint = NodeId.of(response.leaderNodeId());
                    }
                    LeaderCalls.sleepQuietly(
                            LeaderCalls.cappedBackoff(clock, retry.backoffFor(attempt), deadline, description));
                }
                default -> throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "CATALOG_LOOKUP respondeu " + response.status()
                                + (response.message() != null ? ": " + response.message() : ""));
            }
        }
    }
}
