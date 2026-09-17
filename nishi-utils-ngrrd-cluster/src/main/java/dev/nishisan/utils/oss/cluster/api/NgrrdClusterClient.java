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

package dev.nishisan.utils.oss.cluster.api;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;

/**
 * Cliente transparente do cluster ngrrd: entra na malha NGrid como membro sem
 * volume (papel {@code client}+{@code leader-ineligible}) e devolve
 * {@link NgrrdHandle} que roteiam cada operação ao storage node dono da série,
 * com placement, retentativa e batching de escrita transparentes ao chamador.
 *
 * <p>Um único handle é mantido por {@code seriesKey}: chamar {@link #open}
 * duas vezes com as mesmas tags devolve o mesmo {@link NgrrdHandle}
 * (referência compartilhada, sem contagem de referências) — {@link #close()}
 * do cliente fecha esse handle para todos os chamadores que o obtiveram.</p>
 */
public interface NgrrdClusterClient extends Closeable {

    /** Abre (ou devolve o já aberto) o handle da série identificada por {@code tags} na definição {@code yaml}. */
    NgrrdHandle open(String yaml, Map<String, String> tags);

    /** Variante de {@link #open(String, Map)} com {@link Ngrrd.OpenOptions} explícitas. */
    NgrrdHandle open(String yaml, Map<String, String> tags, Ngrrd.OpenOptions options);

    /** Variante de {@link #open(String, Map)} que lê a definição YAML de um arquivo. */
    NgrrdHandle open(Path yamlFile, Map<String, String> tags);

    /** Drena os buffers de escrita de todos os nós de destino conhecidos, de forma síncrona. */
    void flushAll();

    /** Snapshot atual das métricas do cliente. */
    ClientMetricsSnapshot metrics();

    /**
     * Status geral do cluster segundo o líder atual — vai ao líder ({@code ngrrd.admin.status}),
     * com re-resolução automática se a resposta indicar {@code NOT_LEADER} (mesmo tratamento de
     * {@code NOT_LEADER} usado ao posicionar uma série nova).
     */
    AdminStatusResponse clusterStatus();

    /** Métricas operacionais do storage node {@code nodeId} ({@code ngrrd.admin.metrics}, um RPC direto). */
    NodeMetricsSnapshot nodeMetrics(String nodeId);

    /**
     * Dispara um ciclo imediato de rebalanceamento no líder ({@code ngrrd.admin.rebalance}), com a
     * mesma re-resolução automática de {@code NOT_LEADER} de {@link #clusterStatus()}. Não espera as
     * migrações planejadas completarem — só confirma que o líder aceitou o pedido e planejou um ciclo.
     */
    void rebalanceNow();

    /** Identificador deste cliente no cluster NGrid. */
    NodeId clientNodeId();

    /** Fecha todos os handles abertos, drena os buffers e sai do cluster. */
    @Override
    void close();
}
