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
import dev.nishisan.utils.oss.cluster.protocol.AdminForgetResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
     * <p>Confirmar um miss exige que o líder anuncie a capacidade {@code catalog.lookup}: a réplica
     * local do status ({@code ngrrd.nodes}) resolve sem RPC quando já traz a capacidade; em qualquer
     * outro caso — status local ausente ou sem a capacidade (a réplica pode estar atrasada logo após
     * uma atualização do nó) — uma leitura forte confirma no líder, relida com backoff curto dentro do
     * prazo restante da consulta enquanto não houver status (ex.: o líder mudou antes de publicar o
     * seu). Confirmado sem a capacidade — inclusive a de um líder de versão anterior, que publica o
     * status sem capacidades — o cliente falha na hora com {@link ErrorCode#UNSUPPORTED_BY_NODE}, sem
     * enviar o {@code CATALOG_LOOKUP}; sem status até o fim do prazo, também
     * {@link ErrorCode#UNSUPPORTED_BY_NODE}; uma leitura forte que só falhou no transporte vira
     * {@link ErrorCode#TIMEOUT} com a causa. Nunca {@code false} nesses casos. Atualize os storages
     * antes dos clientes.</p>
     *
     * @throws NgrrdClusterException se não foi possível confirmar com o líder (sem líder, timeout,
     *         falha de transporte, resposta inválida) — uma falha ao consultar nunca vira {@code false};
     *         com {@link ErrorCode#UNSUPPORTED_BY_NODE} se o líder não anuncia {@code catalog.lookup}
     */
    boolean exists(String seriesKey);

    /** Conditional, retry-safe deletion. An ERROR never certifies absence. */
    default DeleteResult deleteSeries(String seriesKey, DeletePrecondition precondition) {
        throw new UnsupportedOperationException("series.delete");
    }

    /** Independent outcomes for every key; the batch is not atomic. */
    default Map<String, DeleteResult> deleteSeriesBatch(Map<String, DeletePrecondition> requests) {
        throw new UnsupportedOperationException("series.delete.batch");
    }

    /** Explicit inventory/adoption/purge of unplaced objects on one storage. */
    default dev.nishisan.utils.oss.cluster.protocol.ReconcileResponse reconcile(String nodeId,
            dev.nishisan.utils.oss.cluster.protocol.ReconcileRequest request) {
        throw new UnsupportedOperationException("series.reconcile");
    }

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

    /**
     * Verificação FÍSICA em lote: confirma no dono de cada série se o objeto existe de fato no volume,
     * diferente de {@link #exists(Collection)}, que só olha o catálogo. Devolve TODAS as chaves pedidas,
     * nunca uma resposta parcial — cada uma com um {@link SeriesVerification}:
     * <ul>
     *   <li>{@link SeriesVerification#PRESENT}: o dono confirmou o objeto;</li>
     *   <li>{@link SeriesVerification#MISSING_ON_OWNER}: há placement no catálogo, mas o dono confirmou
     *       que o objeto não existe (inconsistência do cluster);</li>
     *   <li>{@link SeriesVerification#NOT_PLACED}: não há placement no catálogo — a série não existe;</li>
     *   <li>{@link SeriesVerification#UNVERIFIED}: não foi possível confirmar com o dono (capacidade
     *       ausente, RPC ou status de erro) — nunca interpretado como ausência.</li>
     * </ul>
     *
     * <p>Usado num relatório de conciliação sob demanda: uma falha ao consultar o LÍDER (catálogo)
     * propaga {@link NgrrdClusterException}, como em {@link #exists(Collection)}; já uma falha ao
     * consultar um DONO específico marca só as chaves daquele dono como {@code UNVERIFIED}, sem afetar
     * os demais. Consultas ao líder e ao(s) dono(s) são paginadas em
     * {@link NgrrdClusterConfig#catalogLookupBatchSize()} chaves por chamada, sequenciais.</p>
     *
     * @throws NgrrdClusterException se não foi possível confirmar o placement com o líder (sem líder,
     *         timeout, falha de transporte, resposta inválida); com {@link ErrorCode#UNSUPPORTED_BY_NODE}
     *         se o líder não anuncia {@code catalog.lookup}
     */
    Map<String, SeriesVerification> verify(Collection<String> seriesKeys);

    /**
     * Drena os buffers de escrita de todos os nós de destino conhecidos, de forma síncrona: espera a resposta
     * final de toda escrita admitida antes da chamada, até {@link NgrrdClusterConfig#closeTimeout()}.
     *
     * <p>Em {@link NgrrdClusterConfig.WriteFailureReporting#BARRIER} lança a primeira falha de escrita coberta
     * (e continua lançando até o restart). Em {@link NgrrdClusterConfig.WriteFailureReporting#MARKS} só espera:
     * falhas de escrita chegam pelas marcas ({@link #mark()}).</p>
     *
     * @throws NgrrdClusterException {@link ErrorCode#TIMEOUT} se o prazo esgotar
     */
    void flushAll();

    /**
     * Captura, sem esperar ACK, a fronteira de todas as escritas admitidas até agora (ver {@link WriteMark}).
     * Custo proporcional às séries com escrita pendente. Exige
     * {@link NgrrdClusterConfig.WriteFailureReporting#MARKS}.
     *
     * <p>Contrato de ciclo de vida, nesse modo: falha de escrita (amostra perdida) chega só pelo
     * {@link WriteMarkResult}; série removida ou em quarentena continua sinalizada em {@code write},
     * {@code flush} e {@code checkpoint} do handle sempre como {@link NgrrdClusterException} com
     * {@link ErrorCode#SERIES_DELETED} ou {@link ErrorCode#QUARANTINED} ({@link NgrrdClusterException#code()};
     * a causa pode vir encadeada). {@code QUARANTINED} não se resolve sozinho: exige recuperação operacional.</p>
     *
     * @throws IllegalStateException  se o cliente não estiver em {@code MARKS}
     * @throws NgrrdClusterException {@link ErrorCode#CLOSED} se o cliente já foi fechado
     * @throws UnsupportedOperationException numa implementação que não suporta marcas (default desta interface)
     * @since 8.12.0
     */
    default WriteMark mark() {
        throw new UnsupportedOperationException("mark() não suportado por " + getClass().getName());
    }

    /**
     * {@link #release(Collection, ReleaseOptions)} com {@link ReleaseOptions#defaults()}: sem checkpoint e sem
     * {@code CLOSE} no nó, com prazo {@link NgrrdClusterConfig#closeTimeout()}.
     *
     * @since 8.15.0
     */
    default CompletableFuture<ReleaseResult> release(Collection<String> seriesKeys) {
        return release(seriesKeys, ReleaseOptions.defaults());
    }

    /**
     * Entrega as séries {@code seriesKeys}: encerra a admissão de escritas nelas, espera a resposta final do dono para
     * toda escrita já admitida e solta o estado local do cliente — para confirmar com segurança o progresso que cobre
     * essas séries (por exemplo, o offset de uma partição) antes de outro cliente assumi-las. Não bloqueia: devolve
     * uma future, concluída num executor próprio de virtual threads.
     *
     * <p><b>Semântica, por série:</b></p>
     * <ol>
     *   <li><b>Admissão encerrada.</b> Sob o lock de abertura da chave (o mesmo de {@link #open}), o handle principal
     *       — gravável ou somente leitura — sai do cache e é fechado localmente, e a rota de escrita é aposentada com a
     *       fronteira fotografada (tudo o que foi admitido até ali). Um {@code write} concorrente no handle liberado
     *       lança {@link NgrrdClusterException} com {@link ErrorCode#CLOSED}, nunca {@code SERIES_DELETED}: pare de
     *       escrever nas séries antes de chamar o {@code release}. Um {@link #open} concorrente da mesma chave espera
     *       esse lock, então nenhuma escrita dele entra na fronteira; um {@code open} posterior cria um handle novo,
     *       como sempre. Uma vista somente leitura obtida antes passa a falhar com {@code CLOSED}.</li>
     *   <li><b>Dreno.</b> Espera a resposta final de toda escrita até a fronteira, sem lançar. Em
     *       {@link NgrrdClusterConfig.WriteFailureReporting#MARKS}, as falhas ainda não reportadas a uma marca até a
     *       fronteira são entregues no resultado e não vão mais para nenhuma marca (cada falha é reportada uma única
     *       vez, a uma marca ou a um release). Em {@code BARRIER}, a primeira falha da série, se estiver até a
     *       fronteira, vira {@link ReleaseStatus#WRITE_FAILURES}. Série removida ou em quarentena vira
     *       {@link ReleaseStatus#SERIES_DELETED}.</li>
     *   <li><b>Checkpoint opcional</b> ({@link ReleaseOptions#checkpoint()}): o {@code CHECKPOINT} de sempre, só nas
     *       séries sem falha de escrita; falha ou prazo vira {@link ReleaseStatus#CHECKPOINT_FAILED}.</li>
     *   <li><b>{@code CLOSE} no nó opcional</b> ({@link ReleaseOptions#closeOnNode()}), com o orçamento e a regra de
     *       conexão do {@code close()} do handle; uma falha só vai para o log. <b>Padrão: não manda</b> — na entrega, o
     *       próximo dono reabre a série no mesmo nó, e o {@code CLOSE} faria o nó fazer checkpoint, fechar e reabrir à
     *       toa (além de descartar a definição no nó, o que custa um {@code NOT_OPEN} ao cliente que já a abriu). Série
     *       abandonada de vez fecha pela ociosidade do nó.</li>
     *   <li><b>Limpeza local.</b> A rota de escrita sai do cliente quando não houver mais escrita em voo — com a falha
     *       entregue por este release, sai mesmo com falha —, e o dono em cache da série é esquecido. Se outro handle
     *       gravável deste cliente ainda segura a rota (aberto por fora do release), o release só drena: a rota
     *       continua, e as escritas desse handle seguem normalmente.</li>
     *   <li><b>{@link ReleaseStatus#TIMED_OUT}.</b> O handle já está fechado e a rota fica sem dono; as escritas
     *       pendentes continuam sendo entregues em segundo plano e a rota sai quando ficar ociosa. Falhas que surgirem
     *       depois vão para a próxima marca ({@code MARKS}) ou ficam na rota ({@code BARRIER}). <b>Não confirme</b> o
     *       progresso dessas séries.</li>
     * </ol>
     *
     * <p><b>Durabilidade.</b> Sem {@code checkpoint}, a garantia de {@link ReleaseStatus#RELEASED} é a mesma de uma
     * marca de escrita: ACK do dono, sem fsync — uma queda abrupta do nó logo depois pode perder as últimas amostras.
     * Quando o checkpoint local periódico ({@code ngrrd.checkpoint}) está ligado no nó, ele limita essa janela; vem
     * desligado por padrão, e aí a janela não tem esse limite. Com {@code checkpoint}, é fsync confirmado no dono, ao
     * custo de uma RPC por série.</p>
     *
     * <p><b>Prazo e execução.</b> O prazo TOTAL ({@link ReleaseOptions#timeout()}, padrão
     * {@link NgrrdClusterConfig#closeTimeout()}) vale para dreno, checkpoint e {@code CLOSE}. As RPCs de
     * {@code CHECKPOINT}/{@code CLOSE} são limitadas a {@link ReleaseOptions#maxConcurrentPerNode()} por nó dono, em cada
     * chamada de {@code release} (o limite não é global: releases simultâneos somam). Sem {@code checkpoint} nem
     * {@code closeOnNode}, o custo é só local (dreno e limpeza), sem RPC por série. A espera pelo lock de abertura de uma
     * chave fica fora do prazo: ela só acontece se um {@link #open} da mesma chave estiver em curso neste cliente (o
     * {@code OPEN} remoto roda sob esse lock, limitado pelo {@link NgrrdClusterConfig#retryTimeout()}, de 5 min por
     * padrão), o que o protocolo de entrega abaixo não prevê — quem libera parou de usar as séries, e quem as assume é
     * outro cliente. Se acontecer, essa espera pode estourar o prazo do grupo de consumo para o callback de revogação:
     * não abra no mesmo cliente as séries que estão sendo liberadas.</p>
     *
     * <p><b>Protocolo de entrega entre clientes</b> (consumidores particionados, com séries particionadas de forma
     * disjunta entre clientes): o cliente que perde a partição P para de chamar {@code write} nas séries de P, chama
     * {@code release(séries de P).join()} no callback de revogação e só confirma o offset de P se
     * {@link ReleaseResult#allReleased()} (senão, não confirma, ou confirma até a última marca concluída sem falha, e
     * registra {@link ReleaseResult#notReleased()}); o cliente que ganha P abre as séries sob demanda e retoma do offset
     * confirmado. O {@code timeout} tem de caber, com folga, no prazo do grupo para o callback. Se o cliente antigo
     * estourar esse prazo e o grupo reatribuir P, os dois podem escrever ao mesmo tempo, e vale o último escritor
     * (esta versão não tem cerca contra isso). Amostras gravadas depois do offset confirmado são reescritas por quem
     * assume; com o mesmo timestamp, vale a {@code lateSamplePolicy} da série.</p>
     *
     * @param seriesKeys chaves das séries; repetidas contam uma vez. Uma chave que este cliente não usa termina
     *                   {@link ReleaseStatus#NOT_HELD}
     * @param options    opções; {@code null} vale {@link ReleaseOptions#defaults()}
     * @return future com um desfecho por chave. Ela nunca falha por causa de uma série: só falha com
     *         {@link NgrrdClusterException} {@link ErrorCode#CLOSED} (cliente fechado antes ou durante o release) ou
     *         com {@link NullPointerException} se {@code seriesKeys} for nulo ou contiver {@code null}
     * @since 8.15.0
     */
    default CompletableFuture<ReleaseResult> release(Collection<String> seriesKeys, ReleaseOptions options) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("release"));
    }

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
     * Como {@link #rebalanceNow()}, devolvendo o que o líder planejou — inclusive os nós excluídos como
     * destino por causa da réplica do catálogo (issue #177). O default chama {@link #rebalanceNow()} e
     * devolve {@link RebalanceTrigger#unknown()}; o cliente do cluster devolve as contagens reais.
     */
    default RebalanceTrigger triggerRebalance() {
        rebalanceNow();
        return RebalanceTrigger.unknown();
    }

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

    /**
     * Esquece um storage node substituído ou desativado ({@code ngrrd.admin.forget}, revisão #178 B9;
     * desde a 8.8.0): o líder remove {@code nodeId} do catálogo e ordena a todos os storages alcançáveis
     * que o esqueçam no transporte do NGrid — ele deixa de contar na maioria de votantes da eleição, que
     * de outro modo ficaria inflada para sempre por um storage substituído sob outro id. Mesma
     * re-resolução automática de {@code NOT_LEADER} de {@link #clusterStatus()}. Pré-condições, verificadas
     * pelo líder: o nó já não está alcançável e não tem séries nem migrações de entrada no catálogo
     * (drene-o e pare o processo antes). Storages que não confirmaram voltam em
     * {@link AdminForgetResponse#failedOn()}: repita o comando quando voltarem.
     *
     * @throws NgrrdClusterException {@code REMOTE_ERROR} se o líder recusou (pré-condições) ou o comando
     *                               não é suportado pelo cluster
     */
    default AdminForgetResponse forgetNode(String nodeId) {
        throw new UnsupportedOperationException("forgetNode não suportado por " + getClass().getName());
    }

    /** Identificador deste cliente no cluster NGrid. */
    NodeId clientNodeId();

    /** Fecha todos os handles abertos, drena os buffers e sai do cluster. */
    @Override
    void close();
}
