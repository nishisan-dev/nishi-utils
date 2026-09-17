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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
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
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
 * próprio registro — {@link SeriesHandleRegistry#reopenIfKnown} recusa reabrir
 * uma série marcada {@code closedByClient}, então a auto-cura aqui só se aplica
 * ao caso de ociosidade/LRU.</p>
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
            LatencySnapshot readLatency) {

        public StorageHandlerMetrics {
            errorsByStatus = Map.copyOf(Objects.requireNonNullElse(errorsByStatus, Map.of()));
            writeBatchLatency = Objects.requireNonNullElse(writeBatchLatency, LatencySnapshot.EMPTY);
            checkpointLatency = Objects.requireNonNullElse(checkpointLatency, LatencySnapshot.EMPTY);
            readLatency = Objects.requireNonNullElse(readLatency, LatencySnapshot.EMPTY);
        }
    }

    /** Prazo do cache negativo de {@code placementStrong}: evita martelar o líder por chave. */
    private static final Duration NEGATIVE_LOOKUP_CACHE_TTL = Duration.ofSeconds(5);

    private final PlacementLookup placementLookup;
    private final SeriesHandleRegistry registry;
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
    private final ConcurrentMap<String, Long> negativeLookupCacheExpiryMs = new ConcurrentHashMap<>();

    public StorageRequestHandler(Transport transport, PlacementLookup placementLookup,
            SeriesHandleRegistry registry, NodeId self, Durability defaultDurability,
            OnGeometryChange defaultOnGeometryChange, Clock clock) {
        super(transport, Commands.OWNER_COMMANDS);
        this.placementLookup = Objects.requireNonNull(placementLookup, "placementLookup");
        this.registry = Objects.requireNonNull(registry, "registry");
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
            default -> throw new IllegalArgumentException("Comando não suportado por StorageRequestHandler: " + command);
        };
    }

    /** Snapshot atual das métricas do handler. */
    public StorageHandlerMetrics metricsSnapshot() {
        Map<SeriesStatus, Long> errors = errorsByStatus.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sum()));
        return new StorageHandlerMetrics(writeBatchesCount.sum(), samplesWrittenCount.sum(), samplesFailedCount.sum(),
                readsCount.sum(), checkpointsCount.sum(), flushesCount.sum(), errors, writeBatchLatency.snapshot(),
                checkpointLatency.snapshot(), readLatency.snapshot());
    }

    private SeriesStatusResponse handleOpen(OpenRequest request) {
        Ownership ownership = ownership(request.seriesKey(), request.placementHint());
        if (ownership.status() != SeriesStatus.OK) {
            recordError(ownership.status());
            return new SeriesStatusResponse(ownership.status(), ownership.owner(), null);
        }
        try {
            Durability durability = request.durability() != null ? request.durability() : defaultDurability;
            OnGeometryChange onGeometryChange = request.onGeometryChange() != null
                    ? request.onGeometryChange() : defaultOnGeometryChange;
            registry.open(request.seriesKey(), request.yaml(), Ngrrd.OpenOptions.of(durability, onGeometryChange));
            return new SeriesStatusResponse(SeriesStatus.OK, self.value(), null);
        } catch (RuntimeException e) {
            recordError(SeriesStatus.ERROR);
            return new SeriesStatusResponse(SeriesStatus.ERROR, self.value(), describe(e));
        }
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
        if (registry.isMigrating(seriesKey)) {
            return new Ownership(SeriesStatus.MIGRATING, null);
        }
        Optional<SeriesPlacement> placement = placementLookup.placementLocal(seriesKey);
        if (placement.isPresent()) {
            SeriesPlacement current = placement.get();
            if (current.state() == PlacementState.MIGRATING) {
                return new Ownership(SeriesStatus.MIGRATING, current.ownerNodeId());
            }
            if (!current.isOwnedBy(self.value())) {
                return new Ownership(SeriesStatus.WRONG_OWNER, current.ownerNodeId());
            }
            return new Ownership(SeriesStatus.OK, current.ownerNodeId());
        }
        if (placementHint != null && placementHint.isOwnedBy(self.value())) {
            return new Ownership(SeriesStatus.OK, placementHint.ownerNodeId());
        }
        if (registry.isOpen(seriesKey)) {
            return new Ownership(SeriesStatus.OK, self.value());
        }
        // Réplica local vazia (ex.: logo após um restart, antes do catálogo persistido convergir via
        // replicação) e nem o placementHint nem o registry local confirmam o dono: consulta o líder
        // (placementStrong) antes de desistir. NUNCA responder WRONG_OWNER(null) só porque a cópia
        // local está vazia — era exatamente isso que fazia o WriteDispatcher do cliente re-enfileirar
        // para sempre num nó que, na verdade, é o dono correto (ver F1.2 do achado do Debugger).
        long now = clock.millis();
        Long negativeCacheExpiry = negativeLookupCacheExpiryMs.get(seriesKey);
        if (negativeCacheExpiry != null) {
            if (now < negativeCacheExpiry) {
                return new Ownership(SeriesStatus.WRONG_OWNER, null);
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
            return new Ownership(SeriesStatus.WRONG_OWNER, null);
        }
        negativeLookupCacheExpiryMs.remove(seriesKey);
        SeriesPlacement current = strong.get();
        if (!current.isOwnedBy(self.value())) {
            return new Ownership(SeriesStatus.WRONG_OWNER, current.ownerNodeId());
        }
        // Dono confirmado pelo líder, mas ainda sem handle nem definição em cache localmente (registry
        // não tinha a série aberta) — o self-healing do write/read/checkpoint decide NOT_OPEN a partir
        // daqui; open() sempre tem a definição YAML no corpo da requisição.
        return new Ownership(SeriesStatus.OK, current.ownerNodeId());
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

    /** Resultado da checagem de dono: status a responder e o dono conhecido (pode ser {@code null}). */
    private record Ownership(SeriesStatus status, String owner) {
    }
}
