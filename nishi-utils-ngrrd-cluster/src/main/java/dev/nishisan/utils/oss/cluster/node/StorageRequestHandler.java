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
     * Consulta de placement local consumida por este handler — isola a
     * dependência de {@code CatalogService} para permitir testes com fake, sem
     * subir um {@code NGrid} real.
     */
    public interface PlacementLookup {
        Optional<SeriesPlacement> placementLocal(String seriesKey);
    }

    /** Snapshot das métricas mínimas deste handler. O M2 amplia. */
    public record StorageHandlerMetrics(
            long writeBatches,
            long samplesWritten,
            long reads,
            long checkpoints,
            Map<SeriesStatus, Long> errorsByStatus) {

        public StorageHandlerMetrics {
            errorsByStatus = Map.copyOf(Objects.requireNonNullElse(errorsByStatus, Map.of()));
        }
    }

    private final PlacementLookup placementLookup;
    private final SeriesHandleRegistry registry;
    private final NodeId self;
    private final Durability defaultDurability;
    private final OnGeometryChange defaultOnGeometryChange;

    private final LongAdder writeBatchesCount = new LongAdder();
    private final LongAdder samplesWrittenCount = new LongAdder();
    private final LongAdder readsCount = new LongAdder();
    private final LongAdder checkpointsCount = new LongAdder();
    private final ConcurrentMap<SeriesStatus, LongAdder> errorsByStatus = new ConcurrentHashMap<>();

    public StorageRequestHandler(Transport transport, PlacementLookup placementLookup,
            SeriesHandleRegistry registry, NodeId self, Durability defaultDurability,
            OnGeometryChange defaultOnGeometryChange) {
        super(transport, Commands.OWNER_COMMANDS);
        this.placementLookup = Objects.requireNonNull(placementLookup, "placementLookup");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.self = Objects.requireNonNull(self, "self");
        this.defaultDurability = Objects.requireNonNull(defaultDurability, "defaultDurability");
        this.defaultOnGeometryChange = Objects.requireNonNull(defaultOnGeometryChange, "defaultOnGeometryChange");
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

    /** Snapshot atual das métricas mínimas do handler. */
    public StorageHandlerMetrics metricsSnapshot() {
        Map<SeriesStatus, Long> errors = errorsByStatus.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sum()));
        return new StorageHandlerMetrics(writeBatchesCount.sum(), samplesWrittenCount.sum(), readsCount.sum(),
                checkpointsCount.sum(), errors);
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
            try {
                Optional<Long> written = withHandleSelfHealing(seriesKey, handle -> {
                    for (SeriesWrite write : entry.getValue()) {
                        handle.write(write.dsName(), new Sample(write.tsEpochMs(), write.value()));
                        writtenSoFar[0]++;
                    }
                    return writtenSoFar[0];
                });
                if (written.isEmpty()) {
                    statusBySeries.put(seriesKey, SeriesStatus.NOT_OPEN);
                    recordError(SeriesStatus.NOT_OPEN);
                } else {
                    samplesWrittenCount.add(written.get());
                    statusBySeries.put(seriesKey, SeriesStatus.OK);
                }
            } catch (RuntimeException e) {
                samplesWrittenCount.add(writtenSoFar[0]);
                statusBySeries.put(seriesKey, SeriesStatus.ERROR);
                errorBySeries.put(seriesKey, describe(e));
                recordError(SeriesStatus.ERROR);
            }
        }
        return new WriteBatchResponse(statusBySeries, ownerBySeries, errorBySeries);
    }

    private SeriesStatusResponse handleCheckpoint(SeriesCommandRequest request) {
        SeriesStatusResponse response = handleSeriesOp(request.seriesKey(), NgrrdHandle::checkpoint);
        if (response.status() == SeriesStatus.OK) {
            checkpointsCount.increment();
        }
        return response;
    }

    private SeriesStatusResponse handleFlush(SeriesCommandRequest request) {
        return handleSeriesOp(request.seriesKey(), NgrrdHandle::flush);
    }

    private SeriesStatusResponse handleSeriesOp(String seriesKey, Consumer<NgrrdHandle> operation) {
        Ownership ownership = ownership(seriesKey, null);
        if (ownership.status() != SeriesStatus.OK) {
            recordError(ownership.status());
            return new SeriesStatusResponse(ownership.status(), ownership.owner(), null);
        }
        try {
            Optional<Boolean> executed = withHandleSelfHealing(seriesKey, handle -> {
                operation.accept(handle);
                return Boolean.TRUE;
            });
            if (executed.isEmpty()) {
                recordError(SeriesStatus.NOT_OPEN);
                return new SeriesStatusResponse(SeriesStatus.NOT_OPEN, self.value(), null);
            }
            return new SeriesStatusResponse(SeriesStatus.OK, self.value(), null);
        } catch (RuntimeException e) {
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
        try {
            Optional<SeriesResult> result = withHandleSelfHealing(request.seriesKey(), handle ->
                    request.endExclusiveEpochMs() != null
                            ? handle.read(request.dsName(), request.toViewQuery(), request.endExclusiveEpochMs())
                            : handle.read(request.dsName(), request.toViewQuery()));
            if (result.isEmpty()) {
                recordError(SeriesStatus.NOT_OPEN);
                return new ReadResponse(SeriesStatus.NOT_OPEN, self.value(), null, null);
            }
            return new ReadResponse(SeriesStatus.OK, self.value(), result.get(), null);
        } catch (RuntimeException e) {
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
        try {
            Optional<Map<String, SeriesResult>> results = withHandleSelfHealing(request.seriesKey(), handle ->
                    request.endExclusiveEpochMs() != null
                            ? handle.read(request.presetName(), request.endExclusiveEpochMs())
                            : handle.read(request.presetName()));
            if (results.isEmpty()) {
                recordError(SeriesStatus.NOT_OPEN);
                return new ReadPresetResponse(SeriesStatus.NOT_OPEN, self.value(), null, null);
            }
            return new ReadPresetResponse(SeriesStatus.OK, self.value(), results.get(), null);
        } catch (RuntimeException e) {
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
        return new Ownership(SeriesStatus.WRONG_OWNER, null);
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
