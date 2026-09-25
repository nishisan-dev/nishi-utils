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
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Cliente transparente do cluster ngrrd: entra na malha NGrid como membro sem
 * volume (papel {@code client}+{@code leader-ineligible}) e devolve
 * {@link NgrrdHandle} que roteiam cada operação ao storage node dono da série,
 * com placement, retentativa e batching de escrita transparentes ao chamador.
 *
 * <p>Um único handle principal é mantido por {@code seriesKey}: chamar {@link #open}
 * duas vezes com as mesmas tags e o mesmo modo devolve o mesmo {@link NgrrdHandle}
 * (referência compartilhada, sem contagem de referências) — {@link #close()}
 * do cliente fecha esse handle para todos os chamadores que o obtiveram. A
 * exceção é abrir sem criar uma chave cujo principal é gravável: cada chamada
 * recebe uma vista somente leitura NOVA sobre ele. As combinações entre abertura
 * com e sem criação estão em {@link #open(String, Map, Ngrrd.OpenOptions)}.</p>
 */
public interface NgrrdClusterClient extends Closeable {

    /** Abre (ou devolve o já aberto) o handle da série identificada por {@code tags} na definição {@code yaml}. */
    NgrrdHandle open(String yaml, Map<String, String> tags);

    /**
     * Variante de {@link #open(String, Map)} com {@link Ngrrd.OpenOptions} explícitas.
     *
     * <p><b>{@code createIfMissing=false} abre um handle SOMENTE LEITURA.</b> O cliente nunca posiciona
     * a série ({@code ngrrd.place}): o dono vem do catálogo e o storage recusa abrir série inexistente.
     * Série ausente faz o {@code open} (ou uma leitura posterior, se ela deixar de existir) lançar
     * {@link SeriesNotFoundException}; nesse caso o handle se fecha e sai do cache do cliente.
     * {@link SeriesNotFoundException#reason()} distingue {@code NOT_PLACED} (o líder não tem placement:
     * a série não existe no cluster) de {@code MISSING_ON_OWNER} (há placement, mas o dono confirmou
     * que o arquivo não existe — inconsistência do cluster, não ausência no catálogo). Se o dono
     * responder {@code WRONG_OWNER} sem indicar o dono novo, o handle confirma direto com o líder (a réplica
     * local pode estar atrasada) e, sem placement no líder, termina em {@code NOT_PLACED} na hora.
     * {@code write}, {@code flush} e {@code checkpoint} lançam {@link IllegalStateException}. O
     * {@code close()} desse handle é local: não drena buffers nem envia {@code CLOSE} ao storage, que fecha
     * a série por ociosidade.</p>
     *
     * <p><b>Compatibilidade de versões.</b> Antes de todo {@code OPEN} sem criar (inclusive reaberturas e
     * redirecionamentos), o cliente exige que o dono anuncie a capacidade {@code open.createIfMissing} no
     * status publicado em {@code ngrrd.nodes}; um storage de versão anterior ignoraria o campo e criaria a
     * série, então o {@code open} falha com {@link NgrrdClusterException} de código
     * {@link ErrorCode#UNSUPPORTED_BY_NODE} sem enviar nada. Como defesa extra, um {@code OK} a um
     * {@code OPEN} sem criar que não traga a confirmação do storage também vira
     * {@code UNSUPPORTED_BY_NODE} — isso detecta um storage antigo, mas não desfaz uma criação que ele já
     * tenha feito. Atualize os storages antes dos clientes. Handles abertos com criação não conferem
     * capacidade (comportamento da 8.5.0).</p>
     *
     * <p><b>Cache de handles</b> — no máximo um handle principal por chave de série:</p>
     * <ul>
     *   <li>com criação, com um gravável aberto em cache: devolve o existente (compartilhado);</li>
     *   <li>com criação, com um somente leitura aberto em cache: abre um gravável NOVO, com as opções
     *       ({@code durability}, {@code onGeometryChange}, YAML e tags) de quem pediu criar, e o coloca no
     *       lugar do somente leitura. O somente leitura antigo fica destacado do cache: continua lendo
     *       para quem já o tem, e o {@code close()} dele segue local, sem afetar o gravável;</li>
     *   <li>sem criar, com um gravável aberto em cache: devolve uma VISTA somente leitura sobre ele — as
     *       leituras delegam ao gravável, escrita lança {@link IllegalStateException}, e o
     *       {@code close()} fecha só a vista (nunca o gravável, que continua escrevendo); cada chamada
     *       recebe a sua vista;</li>
     *   <li>sem criar, com um somente leitura aberto em cache: devolve o existente (compartilhado);</li>
     *   <li>handle em cache fechado, ou nenhum: um novo é aberto no lugar.</li>
     * </ul>
     * <p>Handles devolvidos a mais de um chamador (o gravável para quem abre com criação, o somente
     * leitura para quem abre sem criar) são compartilhados, sem contagem de referências: o
     * {@code close()} de um gravável por um chamador o fecha para todos (contrato da 8.5.0); o de um
     * somente leitura é local, mas também vale para todos que o compartilham.</p>
     */
    NgrrdHandle open(String yaml, Map<String, String> tags, Ngrrd.OpenOptions options);

    /** Variante de {@link #open(String, Map)} que lê a definição YAML de um arquivo. */
    NgrrdHandle open(Path yamlFile, Map<String, String> tags);

    /**
     * Indica se a série existe no cluster: existência é presença de placement no catálogo
     * ({@code ngrrd.catalog}) — {@code MIGRATING} conta como existente. Um hit local — na réplica do
     * catálogo replicado OU num override recente (ex.: de um {@code WRONG_OWNER}) — responde sem RPC;
     * só um miss nos dois é confirmado em lote no líder antes de responder {@code false}.
     *
     * <p><b>Contrato de consistência:</b></p>
     * <ul>
     *   <li>Depois de um {@code open}/{@code PLACE} feito por outro cliente: se a réplica local ainda
     *       não o viu, o miss é confirmado no líder, que já tem o placement — devolve {@code true}.</li>
     *   <li>Durante migração ({@code MIGRATING}): devolve {@code true}; {@code open} sem criar (somente
     *       leitura) segue o fluxo normal de espera/redirect.</li>
     *   <li>{@code false} significa "o líder atual não tem placement para a chave no momento da
     *       consulta" — não é atômico com um {@code open} concorrente de outro cliente que crie a
     *       série logo em seguida.</li>
     *   <li>Não lê o storage: um placement sem arquivo (cliente caiu entre {@code PLACE} e
     *       {@code OPEN}, ou disco perdido) aparece como {@code true} — e o {@code open} sem criar dessa
     *       série lança {@link SeriesNotFoundException} com
     *       {@link SeriesNotFoundException.Reason#MISSING_ON_OWNER}.</li>
     * </ul>
     *
     * <p>Confirmar um miss exige que o líder anuncie a capacidade {@code catalog.lookup} no status
     * publicado em {@code ngrrd.nodes} (réplica local; ausente, uma leitura forte). Um líder de versão
     * anterior não responde a consulta: o cliente falha na hora com
     * {@link ErrorCode#UNSUPPORTED_BY_NODE}, sem RPC — nunca {@code false}, nunca espera o prazo até um
     * {@code TIMEOUT}. Atualize os storages antes dos clientes.</p>
     *
     * @throws NgrrdClusterException se não foi possível confirmar com o líder (sem líder, timeout,
     *         falha de transporte, resposta inválida) — uma falha ao consultar nunca vira {@code false};
     *         com {@link ErrorCode#UNSUPPORTED_BY_NODE} se o líder não anuncia {@code catalog.lookup}
     */
    boolean exists(String seriesKey);

    /**
     * Variante em lote de {@link #exists(String)}: devolve um mapa com TODAS as chaves pedidas — nunca
     * uma resposta parcial; qualquer falha ao consultar propaga {@link NgrrdClusterException} em vez de
     * um mapa incompleto. Consultas de misses ao líder são paginadas em
     * {@link NgrrdClusterConfig#catalogLookupBatchSize()} chaves por chamada, sequenciais.
     *
     * @see #exists(String)
     */
    Map<String, Boolean> exists(Collection<String> seriesKeys);

    /**
     * Informações de placement da série, se ela existir — mesma semântica de existência, o mesmo
     * contrato de consistência e a mesma exigência de {@code catalog.lookup} no líder (só para um miss)
     * de {@link #exists(String)}; um hit no cache local não faz RPC.
     *
     * @throws NgrrdClusterException nas mesmas condições de {@link #exists(String)}, inclusive
     *         {@link ErrorCode#UNSUPPORTED_BY_NODE}
     * @see #exists(String)
     */
    Optional<SeriesInfo> find(String seriesKey);

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

    /**
     * Marca {@code nodeId} como {@code DRAINING} no líder ({@code ngrrd.admin.drain}), com a mesma
     * re-resolução automática de {@code NOT_LEADER} de {@link #clusterStatus()}. Idempotente — chamar de
     * novo sobre um nó já {@code DRAINING}/{@code DRAINED} apenas redispara o ciclo de rebalanceamento.
     *
     * @throws NgrrdClusterException se {@code nodeId} não é conhecido pelo catálogo do líder
     */
    StorageNodeStatus drainNode(String nodeId);

    /**
     * Marca {@code nodeId} como {@code ACTIVE} novamente ({@code ngrrd.admin.activate}) — volta a ser
     * candidato a novos placements. Idempotente.
     *
     * @throws NgrrdClusterException se {@code nodeId} não é conhecido pelo catálogo do líder
     */
    StorageNodeStatus activateNode(String nodeId);

    /** Identificador deste cliente no cluster NGrid. */
    NodeId clientNodeId();

    /** Fecha todos os handles abertos, drena os buffers e sai do cluster. */
    @Override
    void close();
}
