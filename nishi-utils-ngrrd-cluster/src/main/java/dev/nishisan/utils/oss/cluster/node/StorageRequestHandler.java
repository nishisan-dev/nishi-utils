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

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.metrics.LatencyHistogram;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadPresetRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadPresetResponse;
import dev.nishisan.utils.oss.cluster.protocol.ReadRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.ObjectNaming;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Atende, no dono da série, os comandos {@link Commands#OWNER_COMMANDS}:
 * {@code open}, {@code writeBatch}, {@code checkpoint}, {@code flush},
 * {@code read}, {@code readPreset} e {@code close}.
 *
 * <p>Toda operação passa antes por {@link #ownership}, que decide se este nó é
 * mesmo o dono segundo sua cópia local (eventual) do catálogo — cobrindo o caso
 * de corrida líder→dono (via {@code placementHint}) e o de catálogo local
 * atrasado (via {@link SeriesHandleRegistry#isOpen}).</p>
 *
 * <p>Nenhum método aqui manuseia um {@link NgrrdHandle} bruto: toda operação
 * passa por {@link SeriesHandleRegistry#withHandle}, que serializa contra
 * fechamentos concorrentes (ver Javadoc de {@link SeriesHandleRegistry}). Todos
 * os comandos que usam o handle — {@code writeBatch}, {@code checkpoint},
 * {@code flush}, {@code read} e {@code readPreset} — passam por
 * {@link #withHandleSelfHealing}, que se auto-cura de um fechamento por
 * ociosidade/LRU tentando {@link SeriesHandleRegistry#reopenIfKnown} antes de
 * desistir. Isso não reabre uma série fechada por {@code CLOSE} explícito: essa
 * semântica ({@code NOT_OPEN} até um novo {@code open}) já é garantida pelo
 * próprio registro — {@link SeriesHandleRegistry#close(String)} descarta a
 * definição em cache e {@link SeriesHandleRegistry#reopenIfKnown} não reabre
 * sem ela, então a auto-cura aqui só se aplica ao caso de ociosidade/LRU.</p>
 */
public final class StorageRequestHandler extends RequestHandlerSupport {

    /**
     * Consulta de placement consumida por este handler — isola a dependência
     * de {@code CatalogService} para permitir testes com fake, sem subir um
     * {@code NGrid} real.
     */
    public interface PlacementLookup {
        /** Leitura eventual (replicada localmente); pode estar vazia/atrasada logo após um restart. */
        Optional<SeriesPlacement> placementLocal(String seriesKey);

        /**
         * Leitura forte (round-trip ao líder). Usada como último recurso em
         * {@link #ownership} quando a réplica local não tem a entrada — nunca
         * responder {@code WRONG_OWNER} com dono desconhecido só porque a
         * cópia local ainda não convergiu.
         */
        Optional<SeriesPlacement> placementStrong(String seriesKey);
    }

    /**
     * Snapshot das métricas deste handler, incluindo latência (M2) de
     * {@code writeBatch}/{@code checkpoint}/leitura ({@code read}+{@code readPreset}
     * somados no mesmo histograma).
     *
     * @param samplesFailed amostras descartadas por {@link SeriesStatus#ERROR} — não inclui
     *                      pendências rejeitadas por {@code WRONG_OWNER}/{@code NOT_OPEN}/
     *                      {@code MIGRATING} (essas são retentadas pelo cliente, não perdidas)
     * @param flushes       total de requisições {@code flush} atendidas com sucesso
     * @param leaderConfirmations       leituras fortes de placement feitas no líder para confirmar o dono
     *                                  de uma série sem objeto no volume, antes de criá-la ou de responder
     *                                  {@code NOT_FOUND} a um {@code OPEN} (issue #174) — inclui as que
     *                                  falharam no transporte
     * @param leaderConfirmationLatency latência dessas leituras fortes
     */
    public record StorageHandlerMetrics(
            long writeBatches,
            long samplesWritten,
            long samplesFailed,
            long reads,
            long checkpoints,
            long flushes,
            Map<SeriesStatus, Long> errorsByStatus,
            LatencySnapshot writeBatchLatency,
            LatencySnapshot checkpointLatency,
            LatencySnapshot readLatency,
            long leaderConfirmations,
            LatencySnapshot leaderConfirmationLatency) {

        public StorageHandlerMetrics {
            errorsByStatus = Map.copyOf(Objects.requireNonNullElse(errorsByStatus, Map.of()));
            writeBatchLatency = Objects.requireNonNullElse(writeBatchLatency, LatencySnapshot.EMPTY);
            checkpointLatency = Objects.requireNonNullElse(checkpointLatency, LatencySnapshot.EMPTY);
            readLatency = Objects.requireNonNullElse(readLatency, LatencySnapshot.EMPTY);
            leaderConfirmationLatency = Objects.requireNonNullElse(leaderConfirmationLatency, LatencySnapshot.EMPTY);
        }

        /** Assinatura anterior à 8.6.0, sem as métricas de confirmação no líder (zeradas). */
        public StorageHandlerMetrics(long writeBatches, long samplesWritten, long samplesFailed, long reads,
                long checkpoints, long flushes, Map<SeriesStatus, Long> errorsByStatus,
                LatencySnapshot writeBatchLatency, LatencySnapshot checkpointLatency, LatencySnapshot readLatency) {
            this(writeBatches, samplesWritten, samplesFailed, reads, checkpoints, flushes, errorsByStatus,
                    writeBatchLatency, checkpointLatency, readLatency, 0L, LatencySnapshot.EMPTY);
        }
    }

    /** Prazo do cache negativo de {@code placementStrong}: evita martelar o líder por chave. */
    private static final Duration NEGATIVE_LOOKUP_CACHE_TTL = Duration.ofSeconds(5);

    /**
     * {@link Commands#SERIES_EXISTS} e {@link Commands#SERIES_EXISTS_BATCH} não passam pela checagem de
     * {@link #ownership} — não têm dono.
     */
    private static final Set<String> HANDLED_COMMANDS = Stream
            .concat(Commands.OWNER_COMMANDS.stream(), Stream.of(Commands.SERIES_EXISTS, Commands.SERIES_EXISTS_BATCH))
            .collect(Collectors.toUnmodifiableSet());

    private GeometryService geometryService;
    /** Enables replicated geometry tracking in a fully wired storage node. */
    public void geometryService(GeometryService service) { this.geometryService = service; }
    private final PlacementLookup placementLookup;
    private final SeriesHandleRegistry registry;
    private final BlobVolume volume;
    private final String seriesObjectPrefix;
    private final NodeId self;
    private final Durability defaultDurability;
    private final OnGeometryChange defaultOnGeometryChange;
    private final Clock clock;

    private final LongAdder writeBatchesCount = new LongAdder();
    private final LongAdder samplesWrittenCount = new LongAdder();
    private final LongAdder samplesFailedCount = new LongAdder();
    private final LongAdder readsCount = new LongAdder();
    private final LongAdder checkpointsCount = new LongAdder();
    private final LongAdder flushesCount = new LongAdder();
    private final ConcurrentMap<SeriesStatus, LongAdder> errorsByStatus = new ConcurrentHashMap<>();
    private final LatencyHistogram writeBatchLatency = new LatencyHistogram();
    private final LatencyHistogram checkpointLatency = new LatencyHistogram();
    private final LatencyHistogram readLatency = new LatencyHistogram();
    private final LongAdder leaderConfirmationsCount = new LongAdder();
    private final LatencyHistogram leaderConfirmationLatency = new LatencyHistogram();
    private final ConcurrentMap<String, Long> negativeLookupCacheExpiryMs = new ConcurrentHashMap<>();

    public StorageRequestHandler(Transport transport, PlacementLookup placementLookup,
            SeriesHandleRegistry registry, BlobVolume volume, String seriesObjectPrefix, NodeId self,
            Durability defaultDurability, OnGeometryChange defaultOnGeometryChange, Clock clock) {
        super(transport, HANDLED_COMMANDS);
        this.placementLookup = Objects.requireNonNull(placementLookup, "placementLookup");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.volume = Objects.requireNonNull(volume, "volume");
        this.seriesObjectPrefix = Objects.requireNonNull(seriesObjectPrefix, "seriesObjectPrefix");
        this.self = Objects.requireNonNull(self, "self");
        this.defaultDurability = Objects.requireNonNull(defaultDurability, "defaultDurability");
        this.defaultOnGeometryChange = Objects.requireNonNull(defaultOnGeometryChange, "defaultOnGeometryChange");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    protected Object handle(String command, Object body, NodeId source) {
        return switch (command) {
            case Commands.OPEN -> handleOpen((OpenRequest) body);
            case Commands.WRITE_BATCH -> handleWriteBatch((WriteBatchRequest) body);
            case Commands.CHECKPOINT -> handleCheckpoint((SeriesCommandRequest) body);
            case Commands.FLUSH -> handleFlush((SeriesCommandRequest) body);
            case Commands.READ -> handleRead((ReadRequest) body);
            case Commands.READ_PRESET -> handleReadPreset((ReadPresetRequest) body);
            case Commands.CLOSE -> handleClose((SeriesCommandRequest) body);
            case Commands.SERIES_EXISTS -> handleSeriesExists((SeriesExistsRequest) body);
            case Commands.SERIES_EXISTS_BATCH -> handleSeriesExistsBatch((SeriesExistsBatchRequest) body);
            default -> throw new IllegalArgumentException("Comando não suportado por StorageRequestHandler: " + command);
        };
    }

    /**
     * ALTO-1 do M4: responde se este nó possui fisicamente o objeto da série no seu volume local, sem
     * abrir handle nenhum e sem checagem de dono — quem chama já decidiu, via catálogo, que este é o nó
     * a perguntar (tipicamente o dono forte).
     *
     * <p>BAIXO-E do Refuter: só {@code volume.storage().exists(key)} — nunca {@code get(key)}, que
     * carregaria o objeto inteiro (potencialmente dezenas de MB) na memória só para medir o tamanho.
     * {@code BlobStorage}/{@code NgrrdStorage} não expõe uma API barata de tamanho (só {@code exists}
     * booleano ou {@code get} que lê tudo), então {@code bytes} é sempre {@code -1} quando
     * {@code exists=true} — documentado em {@link SeriesExistsResponse#bytes()}.</p>
     */
    private SeriesExistsResponse handleSeriesExists(SeriesExistsRequest request) {
        String objectKey = SeriesObjectKeys.objectKey(seriesObjectPrefix, request.seriesKey());
        boolean exists = volume.storage().exists(objectKey);
        return new SeriesExistsResponse(exists, exists ? -1L : 0L);
    }

    /**
     * Variante em lote de {@link #handleSeriesExists}: mesma checagem barata ({@code exists}, sem
     * {@code get}), sem abrir handle e sem checagem de dono, usada pela verificação física em lote do
     * cliente. Pedidos acima de {@link SeriesExistsBatchRequest#MAX_KEYS} são recusados com
     * {@link SeriesStatus#ERROR} em vez de processados parcialmente.
     */
    private SeriesExistsBatchResponse handleSeriesExistsBatch(SeriesExistsBatchRequest request) {
        if (request.seriesKeys().size() > SeriesExistsBatchRequest.MAX_KEYS) {
            return SeriesExistsBatchResponse.error("lote com " + request.seriesKeys().size()
                    + " chaves excede o máximo de " + SeriesExistsBatchRequest.MAX_KEYS);
        }
        Set<String> present = request.seriesKeys().stream()
                .filter(seriesKey -> volume.storage().exists(SeriesObjectKeys.objectKey(seriesObjectPrefix, seriesKey)))
                .collect(Collectors.toUnmodifiableSet());
        return SeriesExistsBatchResponse.ok(present);
    }

    /** Snapshot atual das métricas do handler. */
    public StorageHandlerMetrics metricsSnapshot() {
        Map<SeriesStatus, Long> errors = errorsByStatus.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sum()));
        return new StorageHandlerMetrics(writeBatchesCount.sum(), samplesWrittenCount.sum(), samplesFailedCount.sum(),
                readsCount.sum(), checkpointsCount.sum(), flushesCount.sum(), errors, writeBatchLatency.snapshot(),
                checkpointLatency.snapshot(), readLatency.snapshot(), leaderConfirmationsCount.sum(),
                leaderConfirmationLatency.snapshot());
    }

    private SeriesStatusResponse handleOpen(OpenRequest request) {
        try (var guard = CoordinationLocks.acquire(registry.operationLock(request.seriesKey()))) {
            if (registry.isMigrating(request.seriesKey())) {
                return new SeriesStatusResponse(SeriesStatus.MIGRATING, self.value(), null);
            }
            return openWithMetadata(request);
        }
    }

    private SeriesStatusResponse openWithMetadata(OpenRequest request) {
        Ownership ownership = ownership(request.seriesKey(), request.placementHint());
        if (ownership.status() != SeriesStatus.OK) {
            recordError(ownership.status());
            return new SeriesStatusResponse(ownership.status(), ownership.owner(), null);
        }
        try {
            String definitionPrefix = seriesPrefixOf(request.yaml());
            if (!seriesObjectPrefix.equals(definitionPrefix)) {
                recordError(SeriesStatus.ERROR);
                return new SeriesStatusResponse(SeriesStatus.ERROR, self.value(),
                        "definição da série usa storage.objectNaming.seriesPrefix='" + definitionPrefix
                                + "', mas este nó está configurado com seriesObjectPrefix='" + seriesObjectPrefix
                                + "' — todas as definições servidas por um cluster ngrrd devem usar o mesmo prefixo "
                                + "(ver Javadoc de StorageNodeConfig.seriesObjectPrefix)");
            }
            if (!registry.isOpen(request.seriesKey())
                    && !volume.storage().exists(SeriesObjectKeys.objectKey(seriesObjectPrefix, request.seriesKey()))) {
                if (!request.createIfMissingOrDefault()) {
                    return confirmSeriesNotFound(request.seriesKey());
                }
                if (!ownership.confirmedByLeader()) {
                    // Issue #174: réplica local ACTIVE(self) com o objeto ausente tem a mesma assinatura de
                    // "série recém-colocada" e de "origem de migração reiniciada com a réplica atrasada" —
                    // só o líder distingue. Sem esta confirmação, o OPEN recriaria a série vazia no dono
                    // antigo e as escritas da janela iriam para uma órfã.
                    Optional<SeriesStatusResponse> redirect = confirmOwnerWithLeader(request.seriesKey());
                    if (redirect.isPresent()) {
                        return redirect.get();
                    }
                }
            }
            Durability durability = request.durability() != null ? request.durability() : defaultDurability;
            // Handle somente leitura (sem criar) nunca migra nem recria a série: com geometria divergente
            // o leitor recebe erro e o arquivo fica como está, qualquer que seja a política pedida. Se a
            // série já estiver aberta, o registry devolve o handle existente e a política nem é usada.
            OnGeometryChange onGeometryChange = !request.createIfMissingOrDefault()
                    ? OnGeometryChange.FAIL
                    : request.onGeometryChange() != null ? request.onGeometryChange() : defaultOnGeometryChange;
            // Só um OPEN que pode criar (ou reescrever) o objeto invalida a confirmação de geometria antes
            // de abrir; sem criar, a geometria gravada nunca muda (FAIL acima), então um leitor não
            // derruba a confirmação nem gera uma escrita replicada extra — só confirma depois do sucesso.
            if (geometryService != null && request.createIfMissingOrDefault()) {
                geometryService.beforeOpen(request.seriesKey());
            }
            registry.open(request.seriesKey(), request.yaml(), Ngrrd.OpenOptions.of(durability, onGeometryChange)
                    .withCreateIfMissing(request.createIfMissingOrDefault()));
            if (geometryService != null) { geometryService.afterOpen(request.seriesKey()); }
            // Confirma ao cliente que um OPEN sem criar foi honrado — um storage anterior responderia OK
            // sem este campo, e o cliente saberia que o createIfMissing=false pode ter sido ignorado.
            return new SeriesStatusResponse(SeriesStatus.OK, self.value(), null,
                    request.createIfMissingOrDefault() ? null : Boolean.TRUE);
        } catch (SeriesNotFoundException e) {
            // Defesa em profundidade: o objeto sumiu entre a checagem acima e o open() propriamente dito
            // (corrida com uma limpeza externa, por exemplo) — o writer recusa criar e sinaliza aqui.
            return confirmSeriesNotFound(e.seriesKey());
        } catch (GeometryService.PublicationException e) {
            recordError(e.response().status());
            return e.response();
        } catch (RuntimeException e) {
            recordError(SeriesStatus.ERROR);
            return new SeriesStatusResponse(SeriesStatus.ERROR, self.value(), describe(e));
        }
    }

    /**
     * Responde {@code NOT_FOUND} a um {@code OPEN} sem criar só depois de {@link #confirmOwnerWithLeader}
     * confirmar {@code ACTIVE(self)}: a série é deste nó e o arquivo não existe. {@code NOT_FOUND} (que o
     * cliente reporta como {@code MISSING_ON_OWNER}) nunca sai com base só na réplica local, que pode
     * estar atrasada logo após um restart.
     */
    private SeriesStatusResponse confirmSeriesNotFound(String seriesKey) {
        Optional<SeriesStatusResponse> redirect = confirmOwnerWithLeader(seriesKey);
        if (redirect.isPresent()) {
            return redirect.get();
        }
        recordError(SeriesStatus.NOT_FOUND);
        return new SeriesStatusResponse(SeriesStatus.NOT_FOUND, self.value(), "série inexistente: " + seriesKey);
    }

    /**
     * Confirma com o líder ({@link PlacementLookup#placementStrong}) que a série é deste nó antes de
     * agir sobre um objeto ausente no volume — responder {@code NOT_FOUND} a um {@code OPEN} sem criar
     * ou criar a série num {@code OPEN} com criação. A réplica local pode estar atrasada logo após um
     * restart: a marca {@link SeriesHandleRegistry#isForgotten} é só em memória, então se a origem de
     * uma migração reiniciar pouco depois do {@code FINISH}, a réplica local ainda pode dizer
     * {@code ACTIVE(self)} com o objeto já apagado (issue #174).
     *
     * @return vazio quando o líder confirma {@code ACTIVE(self)}; senão a resposta de redirecionamento:
     *         outro dono → {@code WRONG_OWNER} com o dono; {@code MIGRATING} → {@code MIGRATING};
     *         ausente no líder → {@code WRONG_OWNER} SEM dono (não cabe a este nó responder por uma
     *         série sem placement: o cliente re-resolve pelo catálogo); falha da consulta →
     *         {@code ERROR} (nunca vira {@code NOT_FOUND} nem criação às cegas)
     */
    private Optional<SeriesStatusResponse> confirmOwnerWithLeader(String seriesKey) {
        Optional<SeriesPlacement> strong;
        leaderConfirmationsCount.increment();
        long startNanos = System.nanoTime();
        try {
            strong = placementLookup.placementStrong(seriesKey);
        } catch (RuntimeException e) {
            recordError(SeriesStatus.ERROR);
            return Optional.of(new SeriesStatusResponse(SeriesStatus.ERROR, self.value(), describe(e)));
        } finally {
            leaderConfirmationLatency.record(System.nanoTime() - startNanos);
        }
        if (strong.isEmpty()) {
            recordError(SeriesStatus.WRONG_OWNER);
            return Optional.of(new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, null, null));
        }
        SeriesPlacement current = strong.get();
        if (current.state() == PlacementState.MIGRATING) {
            recordError(SeriesStatus.MIGRATING);
            return Optional.of(new SeriesStatusResponse(SeriesStatus.MIGRATING, current.ownerNodeId(), null));
        }
        if (!current.isOwnedBy(self.value())) {
            recordError(SeriesStatus.WRONG_OWNER);
            return Optional.of(new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, current.ownerNodeId(), null));
        }
        return Optional.empty();
    }

    /** {@code storage.objectNaming.seriesPrefix} efetivo (com o default do oss aplicado) da definição YAML. */
    private static String seriesPrefixOf(String yaml) {
        NgrrdDefinition definition = NgrrdYamlLoader.parse(yaml, System::getenv);
        ObjectNaming naming = definition.spec().storage().objectNaming();
        return naming != null ? naming.seriesPrefixOrDefault() : new ObjectNaming(null, null, null).seriesPrefixOrDefault();
    }

    private WriteBatchResponse handleWriteBatch(WriteBatchRequest request) {
        writeBatchesCount.increment();
        Map<String, SeriesStatus> statusBySeries = new LinkedHashMap<>();
        Map<String, String> ownerBySeries = new LinkedHashMap<>();
        Map<String, String> errorBySeries = new LinkedHashMap<>();

        Map<String, List<SeriesWrite>> writesBySeries = request.writes().stream()
                .collect(Collectors.groupingBy(SeriesWrite::seriesKey, LinkedHashMap::new, Collectors.toList()));

        for (Map.Entry<String, List<SeriesWrite>> entry : writesBySeries.entrySet()) {
            String seriesKey = entry.getKey();
            Ownership ownership = ownership(seriesKey, null);
            if (ownership.status() != SeriesStatus.OK) {
                statusBySeries.put(seriesKey, ownership.status());
                if (ownership.owner() != null) {
                    ownerBySeries.put(seriesKey, ownership.owner());
                }
                recordError(ownership.status());
                continue;
            }
            // Contador mutável capturado pela lambda: se `handle.write` lançar no meio do lote, as
            // amostras já gravadas antes da falha continuam contando em `samplesWritten` (o lote não
            // é atômico por série — ver Javadoc de WriteBatchResponse).
            long[] writtenSoFar = {0L};
            long startNanos = System.nanoTime();
            try {
                Optional<Long> written = withHandleSelfHealing(seriesKey, handle -> {
                    for (SeriesWrite write : entry.getValue()) {
                        handle.write(write.dsName(), new Sample(write.tsEpochMs(), write.value()));
                        writtenSoFar[0]++;
                    }
                    return writtenSoFar[0];
                });
                writeBatchLatency.record(System.nanoTime() - startNanos);
                if (written.isEmpty()) {
                    statusBySeries.put(seriesKey, SeriesStatus.NOT_OPEN);
                    recordError(SeriesStatus.NOT_OPEN);
                } else {
                    samplesWrittenCount.add(written.get());
                    statusBySeries.put(seriesKey, SeriesStatus.OK);
                }
            } catch (RuntimeException e) {
                writeBatchLatency.record(System.nanoTime() - startNanos);
                samplesWrittenCount.add(writtenSoFar[0]);
                samplesFailedCount.add(entry.getValue().size() - writtenSoFar[0]);
                statusBySeries.put(seriesKey, SeriesStatus.ERROR);
                errorBySeries.put(seriesKey, describe(e));
                recordError(SeriesStatus.ERROR);
            }
        }
        return new WriteBatchResponse(statusBySeries, ownerBySeries, errorBySeries);
    }

    private SeriesStatusResponse handleCheckpoint(SeriesCommandRequest request) {
        SeriesStatusResponse response = handleSeriesOp(request.seriesKey(), NgrrdHandle::checkpoint, checkpointLatency);
        if (response.status() == SeriesStatus.OK) {
            checkpointsCount.increment();
        }
        return response;
    }

    private SeriesStatusResponse handleFlush(SeriesCommandRequest request) {
        SeriesStatusResponse response = handleSeriesOp(request.seriesKey(), NgrrdHandle::flush, null);
        if (response.status() == SeriesStatus.OK) {
            flushesCount.increment();
        }
        return response;
    }

    /**
     * @param latency histograma a alimentar com a duração da chamada ao handle (dentro do lock de
     *                {@link SeriesHandleRegistry#withHandle}); {@code null} = não medir (ex.: {@code flush},
     *                sem campo dedicado em {@link StorageHandlerMetrics})
     */
    private SeriesStatusResponse handleSeriesOp(String seriesKey, Consumer<NgrrdHandle> operation,
            LatencyHistogram latency) {
        Ownership ownership = ownership(seriesKey, null);
        if (ownership.status() != SeriesStatus.OK) {
            recordError(ownership.status());
            return new SeriesStatusResponse(ownership.status(), ownership.owner(), null);
        }
        long startNanos = System.nanoTime();
        try {
            Optional<Boolean> executed = withHandleSelfHealing(seriesKey, handle -> {
                operation.accept(handle);
                return Boolean.TRUE;
            });
            if (latency != null) {
                latency.record(System.nanoTime() - startNanos);
            }
            if (executed.isEmpty()) {
                recordError(SeriesStatus.NOT_OPEN);
                return new SeriesStatusResponse(SeriesStatus.NOT_OPEN, self.value(), null);
            }
            return new SeriesStatusResponse(SeriesStatus.OK, self.value(), null);
        } catch (RuntimeException e) {
            if (latency != null) {
                latency.record(System.nanoTime() - startNanos);
            }
            recordError(SeriesStatus.ERROR);
            return new SeriesStatusResponse(SeriesStatus.ERROR, self.value(), describe(e));
        }
    }

    private ReadResponse handleRead(ReadRequest request) {
        Ownership ownership = ownership(request.seriesKey(), null);
        if (ownership.status() != SeriesStatus.OK) {
            recordError(ownership.status());
            return new ReadResponse(ownership.status(), ownership.owner(), null, null);
        }
        // m7: só conta a leitura depois que a checagem de dono passou.
        readsCount.increment();
        long startNanos = System.nanoTime();
        try {
            Optional<SeriesResult> result = withHandleSelfHealing(request.seriesKey(), handle ->
                    request.endExclusiveEpochMs() != null
                            ? handle.read(request.dsName(), request.toViewQuery(), request.endExclusiveEpochMs())
                            : handle.read(request.dsName(), request.toViewQuery()));
            readLatency.record(System.nanoTime() - startNanos);
            if (result.isEmpty()) {
                recordError(SeriesStatus.NOT_OPEN);
                return new ReadResponse(SeriesStatus.NOT_OPEN, self.value(), null, null);
            }
            return new ReadResponse(SeriesStatus.OK, self.value(), result.get(), null);
        } catch (RuntimeException e) {
            readLatency.record(System.nanoTime() - startNanos);
            recordError(SeriesStatus.ERROR);
            return new ReadResponse(SeriesStatus.ERROR, self.value(), null, describe(e));
        }
    }

    private ReadPresetResponse handleReadPreset(ReadPresetRequest request) {
        Ownership ownership = ownership(request.seriesKey(), null);
        if (ownership.status() != SeriesStatus.OK) {
            recordError(ownership.status());
            return new ReadPresetResponse(ownership.status(), ownership.owner(), null, null);
        }
        // m7: só conta a leitura depois que a checagem de dono passou.
        readsCount.increment();
        long startNanos = System.nanoTime();
        try {
            Optional<Map<String, SeriesResult>> results = withHandleSelfHealing(request.seriesKey(), handle ->
                    request.endExclusiveEpochMs() != null
                            ? handle.read(request.presetName(), request.endExclusiveEpochMs())
                            : handle.read(request.presetName()));
            readLatency.record(System.nanoTime() - startNanos);
            if (results.isEmpty()) {
                recordError(SeriesStatus.NOT_OPEN);
                return new ReadPresetResponse(SeriesStatus.NOT_OPEN, self.value(), null, null);
            }
            return new ReadPresetResponse(SeriesStatus.OK, self.value(), results.get(), null);
        } catch (RuntimeException e) {
            readLatency.record(System.nanoTime() - startNanos);
            recordError(SeriesStatus.ERROR);
            return new ReadPresetResponse(SeriesStatus.ERROR, self.value(), null, describe(e));
        }
    }

    private SeriesStatusResponse handleClose(SeriesCommandRequest request) {
        registry.close(request.seriesKey());
        return new SeriesStatusResponse(SeriesStatus.OK, self.value(), null);
    }

    /**
     * {@link SeriesHandleRegistry#withHandle}; se a série não estiver aberta,
     * tenta {@link SeriesHandleRegistry#reopenIfKnown} (reabre pelo cache de
     * definição) e tenta de novo — a operação em si sempre roda dentro do lock
     * de {@code withHandle}, nunca sobre o {@link NgrrdHandle} bruto devolvido
     * por {@code reopenIfKnown}.
     */
    private <R> Optional<R> withHandleSelfHealing(String seriesKey, Function<NgrrdHandle, R> fn) {
        Optional<R> result = registry.withHandle(seriesKey, fn);
        if (result.isPresent()) {
            return result;
        }
        if (registry.reopenIfKnown(seriesKey).isEmpty()) {
            return Optional.empty();
        }
        return registry.withHandle(seriesKey, fn);
    }

    private Ownership ownership(String seriesKey, SeriesPlacement placementHint) {
        if (registry.isMigrationFrozen(seriesKey)) {
            return Ownership.local(SeriesStatus.MIGRATING, null);
        }
        Optional<SeriesPlacement> placement = placementLookup.placementLocal(seriesKey);
        if (registry.isForgotten(seriesKey)) {
            if (placement.isEmpty() || placement.get().isOwnedBy(self.value())) {
                return ownershipForgotten(seriesKey);
            }
            // Issue #174: a réplica local já mostra outro dono — convergiu para além do FINISH, então a
            // marca não protege mais nada. Descarta e segue pelo caminho normal (WRONG_OWNER com o dono
            // local, sem ir ao líder).
            registry.dropForgotten(seriesKey);
        }
        if (placement.isPresent()) {
            SeriesPlacement current = placement.get();
            if (current.state() == PlacementState.MIGRATING) {
                if (self.value().equals(current.ownerNodeId()) && registry.isCopying(seriesKey)) {
                    return Ownership.local(SeriesStatus.OK, current.ownerNodeId());
                }
                return Ownership.local(SeriesStatus.MIGRATING, current.ownerNodeId());
            }
            if (!current.isOwnedBy(self.value())) {
                return Ownership.local(SeriesStatus.WRONG_OWNER, current.ownerNodeId());
            }
            return Ownership.local(SeriesStatus.OK, current.ownerNodeId());
        }
        if (registry.isOpen(seriesKey)) {
            return Ownership.local(SeriesStatus.OK, self.value());
        }
        // Seção 0 do M3 (achado do Refuter do M2, reproduzido A/B): sob churn de liderança, o líder
        // pode não encontrar no catálogo uma série já colocada (réplica local do novo líder ainda
        // convergindo) e criar um placement NOVO noutro nó; o cliente então abre a série aqui com um
        // placementHint que, se aceito de cara, faz este open() criar uma cópia VAZIA no lugar errado
        // — o catálogo passa a apontar para ela e os dados reais ficam órfãos no dono antigo. Por isso
        // o hint NUNCA é aceito por si só: serve apenas de atalho para consultar o líder já sabendo
        // qual dono verificar — quem decide é sempre placementStrong (round-trip real ao líder).
        if (placementHint != null) {
            Optional<SeriesPlacement> strong = placementLookup.placementStrong(seriesKey);
            if (strong.isPresent()) {
                negativeLookupCacheExpiryMs.remove(seriesKey);
                return ownershipFromLeader(seriesKey, strong.get());
            }
            // Sem placement no líder — WRONG_OWNER sem dono (a série realmente ainda não existe lá).
            return Ownership.leader(SeriesStatus.WRONG_OWNER, null);
        }
        // Réplica local vazia (ex.: logo após um restart, antes do catálogo persistido convergir via
        // replicação) e o registry local não confirma o dono: consulta o líder (placementStrong) antes
        // de desistir. NUNCA responder WRONG_OWNER(null) só porque a cópia local está vazia — era
        // exatamente isso que fazia o WriteDispatcher do cliente re-enfileirar para sempre num nó que,
        // na verdade, é o dono correto (ver F1.2 do achado do Debugger).
        long now = clock.millis();
        Long negativeCacheExpiry = negativeLookupCacheExpiryMs.get(seriesKey);
        if (negativeCacheExpiry != null) {
            if (now < negativeCacheExpiry) {
                return Ownership.local(SeriesStatus.WRONG_OWNER, null);
            }
            // item 7 (achado do Refuter): entrada expirada por tempo — remove já aqui em vez de
            // deixá-la parada no mapa até uma eventual nova consulta desta MESMA série.
            negativeLookupCacheExpiryMs.remove(seriesKey, negativeCacheExpiry);
        }
        Optional<SeriesPlacement> strong = placementLookup.placementStrong(seriesKey);
        if (strong.isEmpty()) {
            // Cache negativo curto: uma série de fato não colocada não deve martelar o líder a cada
            // requisição enquanto o cliente insiste (backoff dele à parte).
            putNegativeCacheEntry(seriesKey, now);
            return Ownership.leader(SeriesStatus.WRONG_OWNER, null);
        }
        negativeLookupCacheExpiryMs.remove(seriesKey);
        // Dono confirmado pelo líder, mas ainda sem handle nem definição em cache localmente (registry
        // não tinha a série aberta) — o self-healing do write/read/checkpoint decide NOT_OPEN a partir
        // daqui; open() sempre tem a definição YAML no corpo da requisição.
        return ownershipFromLeader(seriesKey, strong.get());
    }

    /**
     * Decisão de dono a partir do placement que o líder confirmou — mesmo critério da réplica local em
     * {@link #ownership}: {@code MIGRATING} só é atendido por este nó durante a cópia online da própria
     * origem ({@link SeriesHandleRegistry#isCopying}); fora dela, {@code MIGRATING}, nunca {@code OK} só
     * porque o dono ainda é este nó (achado do Refuter na issue #174).
     */
    private Ownership ownershipFromLeader(String seriesKey, SeriesPlacement current) {
        if (current.state() == PlacementState.MIGRATING) {
            if (current.isOwnedBy(self.value()) && registry.isCopying(seriesKey)) {
                return Ownership.leader(SeriesStatus.OK, current.ownerNodeId());
            }
            return Ownership.leader(SeriesStatus.MIGRATING, current.ownerNodeId());
        }
        if (!current.isOwnedBy(self.value())) {
            return Ownership.leader(SeriesStatus.WRONG_OWNER, current.ownerNodeId());
        }
        return Ownership.leader(SeriesStatus.OK, current.ownerNodeId());
    }

    /**
     * Próximo passo do M3 (mesma família da seção 0): depois de {@code MIGRATE_FINISH} a origem chama
     * {@link SeriesHandleRegistry#forget}, mas a réplica LOCAL do catálogo pode continuar dizendo
     * {@code ACTIVE(self)} por um instante — se {@link #ownership} confiasse nela (ou no {@code
     * placementHint} do cliente) para uma série esquecida, um {@code OPEN} nessa janela recriaria a
     * série VAZIA aqui, exatamente o defeito da seção 0, só que pelo caminho da réplica local em vez do
     * hint. Por isso, enquanto {@link SeriesHandleRegistry#isForgotten} for verdadeiro, nem {@code
     * placementLocal} nem o hint são consultados: só {@code placementStrong} (round-trip real ao líder)
     * decide. Se o líder confirmar {@code ACTIVE(self)}, o {@code OPEN} pode prosseguir — a marca é
     * limpa por {@link SeriesHandleRegistry#open} quando o handler efetivamente reabre a série; qualquer
     * outro resultado responde {@code WRONG_OWNER} com o dono que o líder de fato conhece. Só é chamado
     * enquanto a réplica local está vazia ou ainda diz que o dono é este nó — com outro dono nela, a
     * marca é descartada em {@link #ownership}.
     */
    private Ownership ownershipForgotten(String seriesKey) {
        Optional<SeriesPlacement> strong = placementLookup.placementStrong(seriesKey);
        if (strong.isEmpty()) {
            return Ownership.leader(SeriesStatus.WRONG_OWNER, null);
        }
        negativeLookupCacheExpiryMs.remove(seriesKey);
        return ownershipFromLeader(seriesKey, strong.get());
    }

    /**
     * item 7 (achado do Refuter): registra a entrada negativa e, antes disso, varre o mapa removendo
     * toda entrada já expirada — sem essa varredura, uma série que nunca mais é consultada depois de
     * expirar ficaria parada no mapa para sempre (o {@code get} de {@link #ownership} só limpa a
     * própria chave que está olhando, nunca as outras), crescendo sem limite ao longo do tempo.
     */
    private void putNegativeCacheEntry(String seriesKey, long now) {
        negativeLookupCacheExpiryMs.entrySet().removeIf(entry -> entry.getValue() <= now);
        negativeLookupCacheExpiryMs.put(seriesKey, now + NEGATIVE_LOOKUP_CACHE_TTL.toMillis());
    }

    private void recordError(SeriesStatus status) {
        if (status == SeriesStatus.OK) {
            return;
        }
        errorsByStatus.computeIfAbsent(status, ignored -> new LongAdder()).increment();
    }

    private static String describe(RuntimeException e) {
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message != null ? ": " + message : "");
    }

    /**
     * Resultado da checagem de dono: status a responder e o dono conhecido (pode ser {@code null}).
     *
     * @param confirmedByLeader {@code true} quando a decisão veio de {@link PlacementLookup#placementStrong}
     *                          (round-trip ao líder) — o {@code OPEN} com criação não repete a consulta
     */
    private record Ownership(SeriesStatus status, String owner, boolean confirmedByLeader) {

        /** Decisão tomada só com informação local (réplica eventual ou registry). */
        static Ownership local(SeriesStatus status, String owner) {
            return new Ownership(status, owner, false);
        }

        /** Decisão confirmada pelo líder. */
        static Ownership leader(SeriesStatus status, String owner) {
            return new Ownership(status, owner, true);
        }
    }
}
