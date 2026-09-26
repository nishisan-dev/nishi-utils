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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Atende, no dono da série, os comandos {@link Commands#OWNER_COMMANDS}:
 * {@code open}, {@code writeBatch}, {@code checkpoint}, {@code flush},
 * {@code read}, {@code readPreset} e {@code close}.
 *
 * <p>Toda operação passa antes por {@link #ownership} ({@link #ownershipBatch} no
 * {@code writeBatch}), que decide se este nó é mesmo o dono segundo sua cópia local
 * (eventual) do catálogo — cobrindo o caso de corrida líder→dono (via
 * {@code placementHint}) e o de catálogo local atrasado (via
 * {@link SeriesHandleRegistry#isOpen}). Desde a issue #177, um redirecionamento
 * ({@code WRONG_OWNER}/{@code MIGRATING}) tirado da réplica local só sai depois de
 * confirmado no líder (ver {@link #ownershipBatch}).</p>
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

        /**
         * Placements de {@code seriesKeys} no líder, numa consulta em lote com prazo total {@code maxWait}
         * (issue #177) — usada para confirmar todo redirecionamento derivado da réplica local. Chaves
         * ausentes no líder ficam fora do mapa. Nunca deve bloquear além de {@code maxWait}: o adaptador de
         * produção usa {@code ngrrd.catalog.lookup}, não {@link #placementStrong} (que, num seguidor, pode
         * bloquear por muito mais que o prazo dentro do core).
         *
         * <p>O default consulta {@link #placementStrong} chave a chave e não respeita o prazo — existe só
         * para implementações de teste que não se importam com isso.</p>
         *
         * @throws RuntimeException em qualquer falha da consulta (sem líder, prazo esgotado, transporte);
         *         nunca resposta parcial
         */
        default Map<String, SeriesPlacement> placementsAtLeader(Collection<String> seriesKeys, Duration maxWait) {
            Map<String, SeriesPlacement> found = new LinkedHashMap<>();
            for (String seriesKey : seriesKeys) {
                placementStrong(seriesKey).ifPresent(placement -> found.put(seriesKey, placement));
            }
            return found;
        }

        /**
         * Se a réplica local do catálogo já é a autoritativa — este nó é o líder — e um redirecionamento
         * derivado dela dispensa a confirmação de {@link #placementsAtLeader}.
         */
        default boolean localIsAuthoritative() {
            return false;
        }

        /**
         * Se este nó conhece um líder a quem perguntar. Sem líder (eleição em curso), um redirecionamento
         * derivado da réplica local é respondido por ela na hora, sem pagar o prazo de
         * {@link #placementsAtLeader} para descobrir o óbvio.
         */
        default boolean leaderKnown() {
            return true;
        }
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
     * @param redirectConfirmations        séries cujo redirecionamento derivado da réplica local foi enviado
     *                                     ao líder para confirmação (issue #177), incluindo as consultas que
     *                                     falharam
     * @param redirectOverrides            redirecionamentos em que a resposta do líder divergiu da réplica
     *                                     local (a réplica estava atrasada)
     * @param redirectConfirmationFailures redirecionamentos respondidos pela réplica local porque a
     *                                     confirmação no líder falhou ou estava em cooldown após uma falha
     * @param redirectCacheHits            redirecionamentos respondidos por uma confirmação recente do líder
     *                                     em cache
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
            LatencySnapshot leaderConfirmationLatency,
            long redirectConfirmations,
            long redirectOverrides,
            long redirectConfirmationFailures,
            long redirectCacheHits) {

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

        /** Assinatura da 8.6.0, sem as métricas de confirmação de redirecionamento (zeradas). */
        public StorageHandlerMetrics(long writeBatches, long samplesWritten, long samplesFailed, long reads,
                long checkpoints, long flushes, Map<SeriesStatus, Long> errorsByStatus,
                LatencySnapshot writeBatchLatency, LatencySnapshot checkpointLatency, LatencySnapshot readLatency,
                long leaderConfirmations, LatencySnapshot leaderConfirmationLatency) {
            this(writeBatches, samplesWritten, samplesFailed, reads, checkpoints, flushes, errorsByStatus,
                    writeBatchLatency, checkpointLatency, readLatency, leaderConfirmations, leaderConfirmationLatency,
                    0L, 0L, 0L, 0L);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(StorageRequestHandler.class.getName());

    /** Prazo do cache negativo de {@code placementStrong}: evita martelar o líder por chave. */
    private static final Duration NEGATIVE_LOOKUP_CACHE_TTL = Duration.ofSeconds(5);
    /**
     * Prazo total da confirmação no líder dos redirecionamentos derivados da réplica local (issue #177) —
     * uma consulta por requisição. Curto de propósito: o {@code OPEN} confirma segurando um lock de
     * coordenação da série, e uma escrita em lote espera por ela antes de responder.
     */
    private static final Duration OWNER_CONFIRMATION_TIMEOUT = Duration.ofSeconds(2);
    /** Validade de uma confirmação do líder no {@link #confirmedPlacements}. */
    private static final Duration CONFIRMED_PLACEMENT_TTL = Duration.ofSeconds(5);
    /** Teto de entradas do {@link #confirmedPlacements}. */
    private static final int CONFIRMED_PLACEMENT_MAX_ENTRIES = 100_000;
    /**
     * Depois de uma falha da confirmação no líder, os redirecionamentos seguem a réplica local (o
     * comportamento da 8.6.0) por este tempo, sem nova consulta — com o líder indisponível, cada
     * requisição pagaria o prazo inteiro de {@link #OWNER_CONFIRMATION_TIMEOUT}.
     */
    private static final Duration REDIRECT_CONFIRMATION_COOLDOWN = Duration.ofSeconds(1);
    /**
     * Uma confirmação no líder em curso há mais que isto é tratada como "presa" (líder lento ou
     * inalcançável): enquanto ela não termina, as requisições seguintes respondem pela réplica local em vez
     * de abrir outra consulta que também esperaria o prazo inteiro. No caminho saudável a consulta leva
     * milissegundos e nunca chega aqui.
     */
    private static final Duration STALLED_CONFIRMATION_THRESHOLD = Duration.ofMillis(500);
    /** Faixas do contador de geração de posse ({@link #ownershipGenerations}). */
    private static final int OWNERSHIP_GENERATION_STRIPES = 1024;

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
    private final LongAdder redirectConfirmationsCount = new LongAdder();
    private final LongAdder redirectOverridesCount = new LongAdder();
    private final LongAdder redirectConfirmationFailuresCount = new LongAdder();
    private final LongAdder redirectCacheHitsCount = new LongAdder();
    /**
     * Cache positivo das confirmações do líder (issue #177), por série. Uma entrada só vale dentro do prazo
     * e enquanto a réplica local não tiver avançado além dela ({@code updatedAtEpochMs} local maior que o
     * confirmado — os dois carimbados pelo relógio do líder). Invalidada por
     * {@link SeriesHandleRegistry#addOwnershipChangeListener} a cada mudança local de posse.
     */
    private final ConcurrentMap<String, ConfirmedPlacement> confirmedPlacements = new ConcurrentHashMap<>();
    /** Até quando (relógio {@link #clock}) os redirecionamentos seguem a réplica local após uma falha. */
    private volatile long redirectConfirmationCooldownUntilMs;
    /**
     * Se a última confirmação no líder terminou em falha. Enquanto verdadeiro, passado o cooldown, só UMA
     * consulta de sonda ({@link #confirmationProbeInFlight}) vai ao líder por vez; as demais respondem pela
     * réplica local. Com o líder saudável ({@code false}) as confirmações correm em paralelo, sem trava.
     */
    private volatile boolean leaderConfirmationDegraded;
    private final AtomicBoolean confirmationProbeInFlight = new AtomicBoolean();
    /** Confirmações no líder em curso: ficha → instante de início (relógio {@link #clock}). */
    private final ConcurrentMap<Long, Long> confirmationsInFlight = new ConcurrentHashMap<>();
    private final AtomicLong confirmationTokens = new AtomicLong();
    /**
     * Geração de posse por faixa de {@code seriesKey}, incrementada a cada mudança local de posse
     * ({@link SeriesHandleRegistry#addOwnershipChangeListener}). Uma confirmação lê a geração antes da
     * consulta ao líder e só grava no {@link #confirmedPlacements} se ela não mudou — senão uma consulta em
     * voo durante a mudança ressuscitaria no cache a posse anterior. Colisão de faixa só faz deixar de
     * cachear (nunca cacheia a mais).
     */
    private final AtomicLongArray ownershipGenerations = new AtomicLongArray(OWNERSHIP_GENERATION_STRIPES);

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
        registry.addOwnershipChangeListener(this::onOwnershipChanged);
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
                leaderConfirmationLatency.snapshot(), redirectConfirmationsCount.sum(), redirectOverridesCount.sum(),
                redirectConfirmationFailuresCount.sum(), redirectCacheHitsCount.sum());
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

        // Dono de todas as séries do lote de uma vez: os redirecionamentos que precisam de confirmação no
        // líder saem numa única consulta (issue #177).
        Map<String, Ownership> ownerships = ownershipBatch(writesBySeries.keySet(), null);
        for (Map.Entry<String, List<SeriesWrite>> entry : writesBySeries.entrySet()) {
            String seriesKey = entry.getKey();
            Ownership ownership = ownerships.get(seriesKey);
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
        return ownershipBatch(List.of(seriesKey), placementHint).get(seriesKey);
    }

    /**
     * Decide o dono de cada série de {@code seriesKeys} ({@link #localOwnership} por chave, na ordem de
     * sempre) e confirma no líder, numa ÚNICA consulta em lote, todo redirecionamento derivado da réplica
     * local — {@code ACTIVE(outro)} e {@code MIGRATING} fora da cópia online da própria origem (issue #177).
     *
     * <p>Defeito que isto corrige: o destino de uma migração com a réplica atrasada ({@code ACTIVE(origem)})
     * respondia {@code WRONG_OWNER(origem)}, enquanto a origem (esquecida, que já consulta o líder)
     * respondia {@code WRONG_OWNER(destino)} — o cliente ficava em pingue-pongue por minutos. Agora, por
     * redirecionamento a confirmar:</p>
     * <ul>
     *   <li>este nó é o líder ({@link PlacementLookup#localIsAuthoritative}) → resposta local;</li>
     *   <li>confirmação válida em {@link #confirmedPlacements} → resposta por ela, SEM
     *       {@code confirmedByLeader} (nunca autoriza criar uma série);</li>
     *   <li>sem autorização de {@link #tryBeginConfirmation} (sem líder conhecido, cooldown após falha,
     *       consulta presa ou sonda já em curso) → resposta local (8.6.0);</li>
     *   <li>senão entra no lote de {@link PlacementLookup#placementsAtLeader}: encontrado → decisão do
     *       líder (e cache); ausente → {@code WRONG_OWNER} sem dono (e cache negativo); falha → resposta
     *       local e cooldown.</li>
     * </ul>
     *
     * <p>Se o líder confirmar {@code ACTIVE(self)} sem handle aberto (destino logo após o
     * {@code MIGRATE_COMMIT}, que descarta o handle), a decisão é {@code OK}: a auto-cura não tem definição
     * para reabrir e a operação responde {@code NOT_OPEN}; o cliente reabre e o {@code OPEN} abre o objeto
     * já commitado (presente no volume, nada é criado).</p>
     *
     * @param hintForSingle {@code placementHint} de um {@code OPEN} (só faz sentido com uma chave)
     */
    private Map<String, Ownership> ownershipBatch(Collection<String> seriesKeys, SeriesPlacement hintForSingle) {
        Map<String, Ownership> decisions = new LinkedHashMap<>();
        Map<String, SeriesPlacement> toConfirm = new LinkedHashMap<>();
        long now = clock.millis();
        Boolean authoritative = null;
        for (String seriesKey : seriesKeys) {
            LocalDecision local = localOwnership(seriesKey, hintForSingle);
            decisions.put(seriesKey, local.ownership());
            if (local.redirectPlacement() == null) {
                continue;
            }
            if (authoritative == null) {
                authoritative = placementLookup.localIsAuthoritative();
            }
            if (authoritative) {
                continue;
            }
            Ownership cached = cachedConfirmation(seriesKey, local.redirectPlacement(), now);
            if (cached != null) {
                redirectCacheHitsCount.increment();
                decisions.put(seriesKey, cached);
            } else {
                toConfirm.put(seriesKey, local.redirectPlacement());
            }
        }
        if (toConfirm.isEmpty()) {
            return decisions;
        }
        ConfirmationPermit permit = tryBeginConfirmation(now);
        if (permit == null) {
            // Sem líder conhecido, em cooldown, com uma consulta presa ou com a sonda já em curso: a réplica
            // local responde (comportamento da 8.6.0) sem pagar o prazo da consulta.
            redirectConfirmationFailuresCount.add(toConfirm.size());
            return decisions;
        }
        confirmRedirectsAtLeader(toConfirm, decisions, permit);
        return decisions;
    }

    /**
     * Autoriza uma confirmação no líder agora, ou {@code null} se ela deve ser pulada: nenhum líder
     * conhecido; dentro do {@link #REDIRECT_CONFIRMATION_COOLDOWN} após uma falha; alguma confirmação em
     * curso há mais que {@link #STALLED_CONFIRMATION_THRESHOLD}; ou, com a última confirmação falha, outra
     * requisição já sondando o líder. No caminho saudável não há trava: confirmações concorrentes correm em
     * paralelo. Toda autorização concedida precisa terminar em {@link #finishConfirmation}.
     */
    private ConfirmationPermit tryBeginConfirmation(long now) {
        if (!placementLookup.leaderKnown() || now < redirectConfirmationCooldownUntilMs) {
            return null;
        }
        long stalledIfStartedBefore = now - STALLED_CONFIRMATION_THRESHOLD.toMillis();
        for (long startedAt : confirmationsInFlight.values()) {
            if (startedAt <= stalledIfStartedBefore) {
                return null;
            }
        }
        boolean probe = leaderConfirmationDegraded;
        if (probe && !confirmationProbeInFlight.compareAndSet(false, true)) {
            return null;
        }
        long token = confirmationTokens.incrementAndGet();
        confirmationsInFlight.put(token, now);
        return new ConfirmationPermit(token, probe);
    }

    /** Encerra uma confirmação autorizada por {@link #tryBeginConfirmation}, atualizando o estado do líder. */
    private void finishConfirmation(ConfirmationPermit permit, boolean succeeded) {
        confirmationsInFlight.remove(permit.token());
        if (succeeded) {
            leaderConfirmationDegraded = false;
        } else {
            leaderConfirmationDegraded = true;
            redirectConfirmationCooldownUntilMs = clock.millis() + REDIRECT_CONFIRMATION_COOLDOWN.toMillis();
        }
        if (permit.probe()) {
            confirmationProbeInFlight.set(false);
        }
    }

    /**
     * Aviso de {@link SeriesHandleRegistry} de que a posse local de {@code seriesKey} pode ter mudado:
     * avança a geração ANTES de remover a entrada do cache — a ordem que {@link #putConfirmedPlacement}
     * precisa para nunca deixar uma confirmação anterior à mudança no cache.
     */
    private void onOwnershipChanged(String seriesKey) {
        ownershipGenerations.incrementAndGet(generationStripe(seriesKey));
        confirmedPlacements.remove(seriesKey);
    }

    private static int generationStripe(String seriesKey) {
        return Math.floorMod(seriesKey.hashCode(), OWNERSHIP_GENERATION_STRIPES);
    }

    /**
     * Decisão de dono só com informação local (réplica eventual do catálogo e registry), ou a consulta
     * forte dos caminhos que já a faziam (série esquecida, réplica vazia). Um redirecionamento derivado
     * da réplica local volta marcado para confirmação no líder ({@link #ownershipBatch}).
     */
    private LocalDecision localOwnership(String seriesKey, SeriesPlacement placementHint) {
        if (registry.isMigrationFrozen(seriesKey)) {
            return LocalDecision.decided(Ownership.local(SeriesStatus.MIGRATING, null));
        }
        Optional<SeriesPlacement> placement = placementLookup.placementLocal(seriesKey);
        if (registry.isForgotten(seriesKey)) {
            if (placement.isEmpty() || placement.get().isOwnedBy(self.value())) {
                return LocalDecision.decided(ownershipForgotten(seriesKey));
            }
            // Issue #174: a réplica local já mostra outro dono — convergiu para além do FINISH, então a
            // marca não protege mais nada. Descarta e segue pelo caminho normal (redirecionamento pela
            // réplica local, confirmado no líder desde a issue #177).
            registry.dropForgotten(seriesKey);
        }
        if (placement.isPresent()) {
            SeriesPlacement current = placement.get();
            if (current.state() == PlacementState.MIGRATING) {
                if (self.value().equals(current.ownerNodeId()) && registry.isCopying(seriesKey)) {
                    return LocalDecision.decided(Ownership.local(SeriesStatus.OK, current.ownerNodeId()));
                }
                return LocalDecision.redirect(SeriesStatus.MIGRATING, current);
            }
            if (!current.isOwnedBy(self.value())) {
                return LocalDecision.redirect(SeriesStatus.WRONG_OWNER, current);
            }
            return LocalDecision.decided(Ownership.local(SeriesStatus.OK, current.ownerNodeId()));
        }
        if (registry.isOpen(seriesKey)) {
            return LocalDecision.decided(Ownership.local(SeriesStatus.OK, self.value()));
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
                return LocalDecision.decided(ownershipFromLeader(seriesKey, strong.get()));
            }
            // Sem placement no líder — WRONG_OWNER sem dono (a série realmente ainda não existe lá).
            return LocalDecision.decided(Ownership.leader(SeriesStatus.WRONG_OWNER, null));
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
                return LocalDecision.decided(Ownership.local(SeriesStatus.WRONG_OWNER, null));
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
            return LocalDecision.decided(Ownership.leader(SeriesStatus.WRONG_OWNER, null));
        }
        negativeLookupCacheExpiryMs.remove(seriesKey);
        // Dono confirmado pelo líder, mas ainda sem handle nem definição em cache localmente (registry
        // não tinha a série aberta) — o self-healing do write/read/checkpoint decide NOT_OPEN a partir
        // daqui; open() sempre tem a definição YAML no corpo da requisição.
        return LocalDecision.decided(ownershipFromLeader(seriesKey, strong.get()));
    }

    /**
     * Decisão a partir de uma confirmação do líder ainda válida em {@link #confirmedPlacements}, ou
     * {@code null}. Vale dentro do prazo e enquanto a réplica local não tiver avançado além dela; a decisão
     * sai sem {@code confirmedByLeader}.
     */
    private Ownership cachedConfirmation(String seriesKey, SeriesPlacement replica, long now) {
        ConfirmedPlacement cached = confirmedPlacements.get(seriesKey);
        if (cached == null) {
            return null;
        }
        if (now >= cached.expiresAtMs() || replica.updatedAtEpochMs() > cached.placement().updatedAtEpochMs()) {
            confirmedPlacements.remove(seriesKey, cached);
            return null;
        }
        return ownershipFromLeader(seriesKey, cached.placement(), false);
    }

    /**
     * Confirma {@code toConfirm} no líder numa única consulta com prazo {@link #OWNER_CONFIRMATION_TIMEOUT}
     * e grava as decisões em {@code decisions} (que já traz as respostas locais, mantidas se a consulta
     * falhar).
     */
    private void confirmRedirectsAtLeader(Map<String, SeriesPlacement> toConfirm, Map<String, Ownership> decisions,
            ConfirmationPermit permit) {
        redirectConfirmationsCount.add(toConfirm.size());
        Map<String, Long> generationsBefore = new HashMap<>();
        for (String seriesKey : toConfirm.keySet()) {
            generationsBefore.put(seriesKey, ownershipGenerations.get(generationStripe(seriesKey)));
        }
        Map<String, SeriesPlacement> atLeader;
        try {
            atLeader = placementLookup.placementsAtLeader(toConfirm.keySet(), OWNER_CONFIRMATION_TIMEOUT);
            if (atLeader == null) {
                throw new IllegalStateException("consulta ao líder devolveu null");
            }
            finishConfirmation(permit, true);
        } catch (RuntimeException e) {
            finishConfirmation(permit, false);
            redirectConfirmationFailuresCount.add(toConfirm.size());
            LOGGER.log(Level.FINE, e, () -> "Falha ao confirmar no líder o redirecionamento de " + toConfirm.size()
                    + " série(s); respondendo pela réplica local por " + REDIRECT_CONFIRMATION_COOLDOWN.toMillis()
                    + " ms");
            return;
        }
        long now = clock.millis();
        for (String seriesKey : toConfirm.keySet()) {
            SeriesPlacement leaderPlacement = atLeader.get(seriesKey);
            Ownership decided;
            if (leaderPlacement == null) {
                putNegativeCacheEntry(seriesKey, now);
                decided = Ownership.leader(SeriesStatus.WRONG_OWNER, null);
            } else {
                negativeLookupCacheExpiryMs.remove(seriesKey);
                putConfirmedPlacement(seriesKey, leaderPlacement, now, generationsBefore.get(seriesKey));
                decided = ownershipFromLeader(seriesKey, leaderPlacement);
            }
            Ownership local = decisions.put(seriesKey, decided);
            if (local != null && !local.sameAnswerAs(decided)) {
                redirectOverridesCount.increment();
                LOGGER.log(Level.FINE, () -> "NGRRD_OWNER_REDIRECT_OVERRIDE série=" + seriesKey + " local="
                        + local.status() + "(" + local.owner() + ") líder=" + decided.status() + "("
                        + decided.owner() + ")");
            }
        }
    }

    /**
     * Decisão de dono a partir do placement que o líder confirmou — mesmo critério da réplica local em
     * {@link #ownership}: {@code MIGRATING} só é atendido por este nó durante a cópia online da própria
     * origem ({@link SeriesHandleRegistry#isCopying}); fora dela, {@code MIGRATING}, nunca {@code OK} só
     * porque o dono ainda é este nó (achado do Refuter na issue #174).
     */
    private Ownership ownershipFromLeader(String seriesKey, SeriesPlacement current) {
        return ownershipFromLeader(seriesKey, current, true);
    }

    /**
     * @param fresh {@code true} quando {@code current} acabou de vir do líder; {@code false} quando veio do
     *              {@link #confirmedPlacements} — a decisão então NUNCA sai com {@code confirmedByLeader},
     *              para que o {@code OPEN} com criação repita a leitura forte antes de criar (issue #174)
     */
    private Ownership ownershipFromLeader(String seriesKey, SeriesPlacement current, boolean fresh) {
        SeriesStatus status;
        if (current.state() == PlacementState.MIGRATING) {
            status = current.isOwnedBy(self.value()) && registry.isCopying(seriesKey)
                    ? SeriesStatus.OK
                    : SeriesStatus.MIGRATING;
        } else {
            status = current.isOwnedBy(self.value()) ? SeriesStatus.OK : SeriesStatus.WRONG_OWNER;
        }
        return new Ownership(status, current.ownerNodeId(), fresh);
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

    /**
     * Registra a confirmação do líder no {@link #confirmedPlacements}. Só placements {@code ACTIVE}: um
     * {@code MIGRATING} vai mudar em breve e é justamente o que se quer confirmar a cada requisição. Não
     * grava se a geração de posse da série mudou desde antes da consulta ({@code generationBefore}); a
     * rechecagem depois do {@code put} fecha a corrida com {@link #onOwnershipChanged} (que avança a geração
     * antes de remover). Só ao passar do teto: varre as expiradas e, se ainda acima, esvazia o cache (ele é
     * só um atalho — a próxima requisição de cada série consulta o líder de novo).
     */
    private void putConfirmedPlacement(String seriesKey, SeriesPlacement placement, long now, long generationBefore) {
        if (placement.state() != PlacementState.ACTIVE) {
            return;
        }
        int stripe = generationStripe(seriesKey);
        if (ownershipGenerations.get(stripe) != generationBefore) {
            return;
        }
        if (confirmedPlacements.size() >= CONFIRMED_PLACEMENT_MAX_ENTRIES) {
            confirmedPlacements.values().removeIf(entry -> entry.expiresAtMs() <= now);
            if (confirmedPlacements.size() >= CONFIRMED_PLACEMENT_MAX_ENTRIES) {
                confirmedPlacements.clear();
            }
        }
        ConfirmedPlacement entry = new ConfirmedPlacement(placement, now + CONFIRMED_PLACEMENT_TTL.toMillis());
        confirmedPlacements.put(seriesKey, entry);
        if (ownershipGenerations.get(stripe) != generationBefore) {
            confirmedPlacements.remove(seriesKey, entry);
        }
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
     * @param confirmedByLeader {@code true} quando a decisão veio agora do líder ({@link PlacementLookup#placementStrong}
     *                          ou {@link PlacementLookup#placementsAtLeader}) — o {@code OPEN} com criação não
     *                          repete a consulta; sempre {@code false} para uma resposta do
     *                          {@link #confirmedPlacements} (issue #174)
     */
    private record Ownership(SeriesStatus status, String owner, boolean confirmedByLeader) {

        /** Mesma resposta ao cliente (status e dono), independentemente da origem da decisão. */
        boolean sameAnswerAs(Ownership other) {
            return status == other.status && Objects.equals(owner, other.owner);
        }

        /** Decisão tomada só com informação local (réplica eventual ou registry). */
        static Ownership local(SeriesStatus status, String owner) {
            return new Ownership(status, owner, false);
        }

        /** Decisão confirmada pelo líder. */
        static Ownership leader(SeriesStatus status, String owner) {
            return new Ownership(status, owner, true);
        }
    }

    /**
     * Decisão de {@link #localOwnership}: {@code ownership} é a resposta pela informação local; se
     * {@code redirectPlacement} não for {@code null}, é um redirecionamento derivado da réplica local
     * ({@code redirectPlacement} é a entrada dela) que ainda precisa ser confirmado no líder.
     */
    private record LocalDecision(Ownership ownership, SeriesPlacement redirectPlacement) {

        static LocalDecision decided(Ownership ownership) {
            return new LocalDecision(ownership, null);
        }

        static LocalDecision redirect(SeriesStatus status, SeriesPlacement replica) {
            return new LocalDecision(Ownership.local(status, replica.ownerNodeId()), replica);
        }
    }

    /**
     * Confirmação do líder guardada em {@link #confirmedPlacements}.
     *
     * @param placement   o placement que o líder devolveu
     * @param expiresAtMs instante (relógio {@link #clock}) a partir do qual a entrada não vale mais
     */
    private record ConfirmedPlacement(SeriesPlacement placement, long expiresAtMs) {
    }

    /**
     * Autorização de uma confirmação no líder ({@link #tryBeginConfirmation}).
     *
     * @param token ficha em {@link #confirmationsInFlight}
     * @param probe se é a sonda única de um líder em falha (libera {@link #confirmationProbeInFlight})
     */
    private record ConfirmationPermit(long token, boolean probe) {
    }
}
