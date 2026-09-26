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

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.SeriesHandleRegistry;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import dev.nishisan.utils.oss.storage.StorageKey;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateChunkRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigratePatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateCommitRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStartRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigratePrepareRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executa, no nó, os dois lados do protocolo de migração de uma série
 * ({@link Commands#MIGRATION_COMMANDS}): origem ({@link Role#SOURCE}, que lê a
 * imagem do volume local e a transfere em chunks) e destino ({@link Role#TARGET},
 * que acumula os chunks e ativa a cópia após validar o SHA-256).
 *
 * <p>Estado por migração (chave {@code migrationId}, não {@code seriesKey}: os
 * dois lados de uma mesma migração guardam entradas independentes, um em papel
 * {@code SOURCE} e outro em {@code TARGET}) em {@link #states}, sujeito a
 * varredura periódica ({@link #sweepExpiredStates(Duration)}) — entradas fora de
 * {@link MigratePhase#STARTED}/{@link MigratePhase#TRANSFERRING} (isto é, que já
 * chegaram a um resultado) expiram depois de um TTL: a origem só precisa da
 * entrada até {@code MIGRATE_FINISH} chegar (ou nunca chegar — nesse caso a
 * cópia órfã é responsabilidade do reconciliador do M4, não deste mapa em
 * memória) e o destino só até o próximo ciclo de rebalanceamento decidir o que
 * fazer com a série.</p>
 */
public final class MigrationExecutor extends RequestHandlerSupport {

    private static final Logger LOGGER = Logger.getLogger(MigrationExecutor.class.getName());

    /** Papel deste nó numa migração específica. */
    public enum Role {
        /** Este nó é o dono atual da série, lendo a imagem e enviando os chunks. */
        SOURCE,
        /** Este nó é o destino, acumulando os chunks recebidos. */
        TARGET
    }

    /** Fase de uma migração, do ponto de vista de um dos dois lados. */
    public enum MigratePhase {
        /** {@code MIGRATE_START} aceito; a transferência ainda não começou de fato. */
        STARTED,
        /** Chunks em trânsito (origem) ou sendo recebidos (destino). */
        TRANSFERRING,
        /** SHA-256 confirmado; a cópia no destino está ativa. */
        COMMITTED,
        /** A migração falhou (transporte, hash divergente, ou erro de aplicação). */
        FAILED,
        /** A migração foi abortada explicitamente pelo coordenador. */
        ABORTED,
        /** ({@code SOURCE} apenas) {@code MIGRATE_FINISH} processado: cópia local apagada. */
        FINISHED
    }

    /**
     * Estado local (de um dos dois lados) de uma migração em curso ou concluída.
     *
     * @param storageKey chave física do objeto no {@code BlobStorage} (ex.: {@code series/<seriesKey>.ngrr})
     *                   — resolvida pela origem a partir do prefixo configurado no nó; {@code null} até
     *                   ser conhecida (origem: desde {@code MIGRATE_START}; destino: só a partir do
     *                   {@code MIGRATE_COMMIT}, que é a primeira mensagem a carregá-la)
     */
    public record MigrationState(String seriesKey, String migrationId, Role role, MigratePhase phase,
            int chunksReceived, int chunksExpected, long bytes, String sha256Hex, String storageKey, String message,
            long updatedAtMs) {
    }

    /** Contagem de migrações concluídas com sucesso, por papel, ainda presentes em {@link #states}. */
    public record ExecutorMetrics(long migrationsIn, long migrationsOut) {
    }

    private final SeriesHandleRegistry registry;
    private final BlobVolume volume;
    private final ClusterRpc rpc;
    private final CatalogView catalog;
    private final NodeId self;
    private final ObjectNaming objectNaming;
    private final long migrationChunkBytes;
    private final long maxSeriesBytes;
    /** Cota dura deste nó como destino ({@code ngrrd.quota.*}, issue #167 item 3); {@code 0} = sem limite. */
    private final long quotaMaxSeries;
    private final long quotaMaxBytes;
    private final Clock clock;
    private final MigrationBandwidth bandwidth;
    private final ExecutorService transferExecutor;

    // Shared with OPEN and physical metadata inspection; transfers release it before network chunks.
    private Object seriesLock(String seriesKey) {
        return registry.operationLock(seriesKey);
    }

    private final ConcurrentMap<String, MigrationState> states = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, StagingBuffer> staging = new ConcurrentHashMap<>();

    public MigrationExecutor(Transport transport, SeriesHandleRegistry registry, BlobVolume volume, ClusterRpc rpc,
            CatalogView catalog, NodeId self, long migrationChunkBytes, long maxSeriesBytes, Clock clock) {
        this(transport, registry, volume, rpc, catalog, self, "series", migrationChunkBytes, maxSeriesBytes, clock);
    }

    /** Uses the node's naming contract, including for series never opened in this process. */
    public MigrationExecutor(Transport transport, SeriesHandleRegistry registry, BlobVolume volume, ClusterRpc rpc,
            CatalogView catalog, NodeId self, String seriesObjectPrefix, long migrationChunkBytes,
            long maxSeriesBytes, Clock clock) {
        this(transport, registry, volume, rpc, catalog, self, seriesObjectPrefix, migrationChunkBytes,
                maxSeriesBytes, MigrationBandwidth.DEFAULT_BYTES_PER_SECOND, clock);
    }

    /** Uses an aggregate byte budget for all outgoing transfers on this node. */
    public MigrationExecutor(Transport transport, SeriesHandleRegistry registry, BlobVolume volume, ClusterRpc rpc,
            CatalogView catalog, NodeId self, String seriesObjectPrefix, long migrationChunkBytes,
            long maxSeriesBytes, long migrationBytesPerSecond, Clock clock) {
        this(transport, registry, volume, rpc, catalog, self, seriesObjectPrefix, migrationChunkBytes, maxSeriesBytes,
                migrationBytesPerSecond, clock, 0L, 0L);
    }

    /**
     * @param quotaMaxSeries cota dura de séries deste nó (issue #167, item 3): um {@code MIGRATE_PREPARE} é
     *                       recusado com {@link MigrateStatus#QUOTA_EXCEEDED} quando as entradas do volume mais
     *                       os alvos de migração abertos já a atingem; {@code 0} = sem limite
     * @param quotaMaxBytes  cota dura de bytes: recusa quando usados + reservados + pedidos a ultrapassam;
     *                       {@code 0} = sem limite
     */
    public MigrationExecutor(Transport transport, SeriesHandleRegistry registry, BlobVolume volume, ClusterRpc rpc,
            CatalogView catalog, NodeId self, String seriesObjectPrefix, long migrationChunkBytes,
            long maxSeriesBytes, long migrationBytesPerSecond, Clock clock, long quotaMaxSeries, long quotaMaxBytes) {
        super(transport, Commands.MIGRATION_COMMANDS);
        if (quotaMaxSeries < 0 || quotaMaxBytes < 0) {
            throw new IllegalArgumentException("quota deve ser >= 0 (0 = sem limite)");
        }
        this.quotaMaxSeries = quotaMaxSeries;
        this.quotaMaxBytes = quotaMaxBytes;
        this.bandwidth = new MigrationBandwidth(migrationBytesPerSecond);
        this.registry = Objects.requireNonNull(registry, "registry");
        this.volume = Objects.requireNonNull(volume, "volume");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.self = Objects.requireNonNull(self, "self");
        Objects.requireNonNull(seriesObjectPrefix, "seriesObjectPrefix");
        if (seriesObjectPrefix.isBlank()) {
            throw new IllegalArgumentException("seriesObjectPrefix não pode ser vazio");
        }
        this.objectNaming = new ObjectNaming(null, null, seriesObjectPrefix);
        if (migrationChunkBytes <= 0) {
            throw new IllegalArgumentException("migrationChunkBytes deve ser > 0: " + migrationChunkBytes);
        }
        this.migrationChunkBytes = migrationChunkBytes;
        if (maxSeriesBytes <= 0) {
            throw new IllegalArgumentException("maxSeriesBytes deve ser > 0: " + maxSeriesBytes);
        }
        this.maxSeriesBytes = maxSeriesBytes;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.transferExecutor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "ngrrd-migration-src");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    protected Object handle(String command, Object body, NodeId source) {
        String seriesKey = switch (body) {
            case MigrateStartRequest r -> r.seriesKey();
            case MigratePrepareRequest r -> r.seriesKey();
            case MigrateChunkRequest r -> r.seriesKey();
            case MigratePatchRequest r -> r.seriesKey();
            case MigrateCommitRequest r -> r.seriesKey();
            case MigrateControlRequest r -> r.seriesKey();
            default -> throw new IllegalArgumentException("Corpo de migração inválido");
        };
        try (var guard = CoordinationLocks.acquire(seriesLock(seriesKey))) {
            return dispatch(command, body);
        }
    }

    private Object dispatch(String command, Object body) {
        return switch (command) {
            case Commands.MIGRATE_PREPARE -> handlePrepare((MigratePrepareRequest) body);
            case Commands.MIGRATE_START -> handleStart((MigrateStartRequest) body);
            case Commands.MIGRATE_CHUNK -> handleChunk((MigrateChunkRequest) body);
            case Commands.MIGRATE_PATCH -> handlePatch((MigratePatchRequest) body);
            case Commands.MIGRATE_COMMIT -> handleCommit((MigrateCommitRequest) body);
            case Commands.MIGRATE_ABORT -> handleAbort((MigrateControlRequest) body);
            case Commands.MIGRATE_FINISH -> handleFinish((MigrateControlRequest) body);
            case Commands.MIGRATE_STATUS -> handleStatus((MigrateControlRequest) body);
            default -> throw new IllegalArgumentException("Comando não suportado por MigrationExecutor: " + command);
        };
    }

    // ---------------------------------------------------------------- origem (SOURCE)

    private MigrateResponse handleStart(MigrateStartRequest request) {
        String seriesKey = request.seriesKey();
        String migrationId = request.migrationId();
        MigrationState previous = states.get(migrationId);
        if (previous != null) {
            if (!previous.seriesKey().equals(seriesKey) || previous.role() != Role.SOURCE
                    || previous.phase() == MigratePhase.ABORTED || previous.phase() == MigratePhase.FINISHED
                    || previous.phase() == MigratePhase.FAILED) {
                return MigrateResponse.of(MigrateStatus.ERROR, "migração encerrada ou identidade divergente");
            }
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        Optional<MigrationState> activeOther = activeSourceStateFor(seriesKey);
        if (activeOther.isPresent()) {
            if (!activeOther.get().migrationId().equals(migrationId)) {
                return MigrateResponse.of(MigrateStatus.ERROR,
                        "já existe migração ativa para a série " + seriesKey);
            }
            // Reenvio idempotente do mesmo START — já em andamento (ou concluído) sob este id.
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        String storageKey;
        Optional<byte[]> image;
        try {
            registry.beginMigrationCopy(seriesKey);
            storageKey = resolveStorageKey(seriesKey);
            image = Optional.ofNullable(registry.migrationSnapshot(seriesKey,
                    () -> volume.storage().get(storageKey).orElse(null)));
        } catch (RuntimeException e) {
            registry.clearMigrating(seriesKey);
            return MigrateResponse.of(MigrateStatus.ERROR,
                    "falha ao preparar a imagem de " + seriesKey + ": " + describe(e));
        }
        if (image.isEmpty()) {
            registry.clearMigrating(seriesKey);
            return MigrateResponse.of(MigrateStatus.ERROR, "série ausente no volume local: " + seriesKey);
        }
        byte[] bytes = image.get();
        if (bytes.length > maxSeriesBytes) {
            registry.clearMigrating(seriesKey);
            return MigrateResponse.of(MigrateStatus.ERROR,
                    "série (" + bytes.length + " bytes) excede maxSeriesBytes (" + maxSeriesBytes + ")");
        }
        try {
            MigrateResponse prepared = rpc.call(NodeId.of(request.targetNodeId()), Commands.MIGRATE_PREPARE,
                    new MigratePrepareRequest(seriesKey, migrationId, storageKey, bytes.length, true), MigrateResponse.class);
            if (prepared.status() != MigrateStatus.COPY_READY) {
                registry.clearMigrating(seriesKey);
                return MigrateResponse.of(MigrateStatus.ERROR, "destination must support live copy: " + prepared.status() + " " + prepared.message());
            }
        } catch (RuntimeException e) {
            registry.clearMigrating(seriesKey);
            return MigrateResponse.of(MigrateStatus.ERROR, "destination reservation failed: " + describe(e));
        }
        String sha256Hex = sha256Hex(bytes);
        int totalChunks = chunkCount(bytes.length);
        states.put(migrationId, new MigrationState(seriesKey, migrationId, Role.SOURCE, MigratePhase.STARTED,
                0, totalChunks, bytes.length, sha256Hex, storageKey, null, clock.millis()));
        // The runnable must not retain the initial full image after catch-up replaces it.
        var pendingImage = new java.util.concurrent.atomic.AtomicReference<>(bytes);
        transferExecutor.execute(() -> transfer(request.targetNodeId(), seriesKey, migrationId,
                pendingImage.getAndSet(null), sha256Hex, storageKey));
        return MigrateResponse.of(MigrateStatus.OK, null);
    }

    /** The configured prefix is shared by OPEN, reconciliation and migration; no cached YAML is needed. */
    private String resolveStorageKey(String seriesKey) {
        return StorageKey.series(objectNaming, seriesKey);
    }

    /** Roda em {@code ngrrd-migration-src}: envia os chunks em ordem, depois o commit. */
    private void transfer(String targetNodeId, String seriesKey, String migrationId, byte[] bytes,
            String sha256Hex, String storageKey) {
        updatePhase(migrationId, MigratePhase.TRANSFERRING, null);
        NodeId target = NodeId.of(targetNodeId);
        int total = chunkCount(bytes.length);
        for (int seq = 0; seq < total; seq++) {
            if (!transferActive(migrationId)) {
                return;
            }
            int start = (int) Math.min((long) seq * migrationChunkBytes, bytes.length);
            int end = (int) Math.min((long) start + migrationChunkBytes, bytes.length);
            if (!bandwidth.acquire(end - start, () -> transferActive(migrationId))) { return; }
            byte[] chunk = Arrays.copyOfRange(bytes, start, end);
            MigrateResponse response;
            try {
                response = rpc.call(target, Commands.MIGRATE_CHUNK,
                        new MigrateChunkRequest(seriesKey, migrationId, seq, total, chunk), MigrateResponse.class);
            } catch (NgrrdClusterException e) {
                LOGGER.log(Level.WARNING, "Falha de transporte ao enviar o chunk " + seq + "/" + total
                        + " da série " + seriesKey + " (migração " + migrationId + ") para " + targetNodeId, e);
                updatePhase(migrationId, MigratePhase.FAILED,
                        "falha de transporte no chunk " + seq + ": " + e.getMessage());
                return;
            }
            if (response.status() != MigrateStatus.OK) {
                updatePhase(migrationId, MigratePhase.FAILED,
                        "destino recusou o chunk " + seq + ": " + response.status()
                                + (response.message() != null ? " (" + response.message() + ")" : ""));
                return;
            }
        }
        if (!transferActive(migrationId)) {
            return;
        }
        // Catch up once while writes are still admitted, then freeze only the final dirty ranges.
        try {
            byte[] current = snapshotWhileCopying(seriesKey, migrationId, storageKey);
            int patchSequence = sendPatches(target, seriesKey, migrationId, bytes, current, 0, false);
            bytes = current;
            byte[] frozen;
            try (var guard = CoordinationLocks.acquire(seriesLock(seriesKey))) {
                if (!transferActive(migrationId)) { return; }
                registry.markMigrating(seriesKey);
                frozen = volume.storage().get(storageKey).orElseThrow();
            }
            var finalRanges = changedRanges(current, frozen);
            long finalBytes = finalRanges.stream().mapToLong(PatchRange::length).sum();
            if (finalBytes > 256 * 1024L) {
                throw new IllegalStateException("series changed too fast for a bounded cutover; retry migration later");
            }
            // A série já está congelada aqui (clientes recebendo MIGRATING): estes são os ÚNICOS
            // patches urgentes — não podem esperar atrás dos chunks de 256 KiB de outras cópias na
            // mesma banda do nó. Continuam contando no orçamento (acquireUrgent debita, só não espera
            // a vez); os chunks concorrentes absorvem o atraso.
            sendPatches(target, seriesKey, migrationId, current, frozen, patchSequence, true);
            bytes = frozen;
            sha256Hex = sha256Hex(frozen);
        } catch (RuntimeException e) {
            updatePhase(migrationId, MigratePhase.FAILED, "live-copy catch-up failed: " + describe(e));
            return;
        }
        if (!transferActive(migrationId)) { return; }
        MigrateResponse commitResponse;
        try {
            commitResponse = rpc.call(target, Commands.MIGRATE_COMMIT,
                    new MigrateCommitRequest(seriesKey, migrationId, sha256Hex, bytes.length, storageKey),
                    MigrateResponse.class);
        } catch (NgrrdClusterException e) {
            LOGGER.log(Level.WARNING, "Falha de transporte no commit da série " + seriesKey
                    + " (migração " + migrationId + ") em " + targetNodeId, e);
            updatePhase(migrationId, MigratePhase.FAILED, "falha de transporte no commit: " + e.getMessage());
            return;
        }
        if (commitResponse.status() == MigrateStatus.COMMITTED) {
            updatePhase(migrationId, MigratePhase.COMMITTED, null);
        } else {
            updatePhase(migrationId, MigratePhase.FAILED,
                    "commit recusado pelo destino: " + commitResponse.status()
                            + (commitResponse.message() != null ? " (" + commitResponse.message() + ")" : ""));
        }
    }

    private byte[] snapshotWhileCopying(String key, String migrationId, String storageKey) {
        try (var guard = CoordinationLocks.acquire(seriesLock(key))) {
            if (!transferActive(migrationId)) { throw new IllegalStateException("migration ended"); }
            return registry.migrationSnapshot(key, () -> volume.storage().get(storageKey).orElseThrow());
        }
    }

    private record PatchRange(int offset, int length) { }

    private java.util.List<PatchRange> changedRanges(byte[] before, byte[] after) {
        if (before.length != after.length) { throw new IllegalStateException("geometry changed during live copy"); }
        var ranges = new java.util.ArrayList<PatchRange>();
        int block = (int) Math.min(4096, migrationChunkBytes);
        for (int offset = 0; offset < after.length; offset += block) {
            int end = Math.min(after.length, offset + block);
            if (Arrays.mismatch(before, offset, end, after, offset, end) >= 0) {
                if (!ranges.isEmpty()) {
                    PatchRange last = ranges.getLast();
                    if (last.offset() + last.length() == offset && end - last.offset() <= migrationChunkBytes) {
                        ranges.set(ranges.size() - 1, new PatchRange(last.offset(), end - last.offset()));
                        continue;
                    }
                }
                ranges.add(new PatchRange(offset, end - offset));
            }
        }
        return ranges;
    }

    /**
     * Envia os patches de {@code before} para {@code after}. {@code urgent} distingue os dois momentos
     * do cutover: {@code false} para o catch-up (série ainda recebendo escrita, chunks/patches disputam
     * a banda em pé de igualdade via {@link MigrationBandwidth#acquire}); {@code true} só para os
     * patches enviados DEPOIS de {@code markMigrating} (série já congelada), que usam {@link
     * MigrationBandwidth#acquireUrgent} — furam a fila sem esperar, mas continuam debitando o orçamento.
     *
     * <p>Visibilidade de pacote (não {@code private}) só para {@code MigrationExecutorTest} poder
     * exercitar o laço isoladamente, com um {@link ClusterRpc} fake; não é API estável do cliente.</p>
     */
    int sendPatches(NodeId target, String key, String id, byte[] before, byte[] after, int sequence,
            boolean urgent) {
        for (PatchRange range : changedRanges(before, after)) {
            if (urgent) {
                // acquireUrgent nunca espera, então não há ponto natural de checagem de "migração ainda
                // ativa" como no acquire (que recebe transferActive como BooleanSupplier do laço de
                // espera) — sem esta checagem explícita, um abort concorrente durante o cutover final
                // não interrompia o envio dos patches restantes, gastando RPCs inúteis contra um destino
                // que já não espera por eles.
                if (!transferActive(id)) {
                    throw new IllegalStateException("migration ended while pacing patches");
                }
                bandwidth.acquireUrgent(range.length());
            } else if (!bandwidth.acquire(range.length(), () -> transferActive(id))) {
                throw new IllegalStateException("migration ended while pacing patches");
            }
            byte[] changed = Arrays.copyOfRange(after, range.offset(), range.offset() + range.length());
            MigrateResponse response = rpc.call(target, Commands.MIGRATE_PATCH,
                    new MigratePatchRequest(key, id, sequence++, range.offset(), changed), MigrateResponse.class);
            if (response.status() != MigrateStatus.OK) {
                throw new IllegalStateException("patch refused: " + response.status() + " " + response.message());
            }
        }
        return sequence;
    }

    private MigrateResponse handleStatus(MigrateControlRequest request) {
        MigrationState state = states.get(request.migrationId());
        if (state == null) {
            return MigrateResponse.of(MigrateStatus.UNKNOWN, null);
        }
        return switch (state.phase()) {
            case COMMITTED -> new MigrateResponse(MigrateStatus.COMMITTED, null, state.bytes());
            case STARTED, TRANSFERRING -> MigrateResponse.of(MigrateStatus.PARTIAL, null);
            case FAILED, ABORTED -> MigrateResponse.of(MigrateStatus.ERROR, state.message());
            case FINISHED -> MigrateResponse.of(MigrateStatus.UNKNOWN, null);
        };
    }

    /** Records even an early abort, so delayed chunks cannot resurrect its staging. */
    private MigrateResponse handleAbort(MigrateControlRequest request) {
        String seriesKey = request.seriesKey();
        String migrationId = request.migrationId();
        MigrationState state = states.get(migrationId);
        if (state == null) {
            updateState(migrationId, seriesKey, Role.TARGET, MigratePhase.ABORTED, null, "abort antecipado");
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        if (!state.seriesKey().equals(seriesKey)) {
            return MigrateResponse.of(MigrateStatus.ERROR, "migrationId pertence a outra série");
        }
        if (state.phase() == MigratePhase.ABORTED || state.phase() == MigratePhase.FINISHED) {
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        Optional<SeriesPlacement> current = catalog.placementStrong(seriesKey);
        if (current.isPresent() && current.get().state() == PlacementState.MIGRATING
                && !migrationId.equals(current.get().migrationId())) {
            // A delayed abort must not clear a newer source's guard or delete its target image.
            staging.remove(migrationId);
            volume.storage().releaseReservation(migrationId);
            updateState(migrationId, seriesKey, state.role(), MigratePhase.ABORTED, state.storageKey(), null);
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        if (state.role() == Role.SOURCE) {
            registry.clearMigrating(seriesKey);
            updateState(migrationId, seriesKey, Role.SOURCE, MigratePhase.ABORTED, state.storageKey(), null);
        } else {
            staging.remove(migrationId);
            volume.storage().releaseReservation(migrationId);
            if (state.phase() == MigratePhase.COMMITTED && state.storageKey() != null) {
                // Achado bloqueante do Refuter (perda de dados): um ABORT pode chegar depois que esta
                // migração já foi dada como concluída de verdade por outro líder (dual-leader/partição —
                // ver Javadoc de MigrationCoordinator#abort). Sem esta confirmação, apagar aqui apagaria
                // a ÚNICA cópia real da série (a origem já a apagou via MIGRATE_FINISH), deixando o
                // catálogo revertido para ACTIVE(src) sem imagem em lugar nenhum — o próximo OPEN recria
                // a série VAZIA. Só apaga se o líder NÃO confirmar que esta cópia é a ativa.
                Optional<SeriesPlacement> strong = catalog.placementStrong(seriesKey);
                if (strong.isPresent() && strong.get().isOwnedBy(self.value())) {
                    LOGGER.log(Level.WARNING, "MIGRATE_ABORT (destino, COMMITTED) para " + seriesKey
                            + " (migrationId=" + migrationId + "): líder confirma ACTIVE(self) — recusando "
                            + "apagar a cópia já ativa");
                    updateState(migrationId, seriesKey, Role.TARGET, MigratePhase.ABORTED, state.storageKey(), null);
                    return MigrateResponse.of(MigrateStatus.OK, null);
                }
                volume.storage().delete(state.storageKey());
            }
            updateState(migrationId, seriesKey, Role.TARGET, MigratePhase.ABORTED, state.storageKey(), null);
        }
        return MigrateResponse.of(MigrateStatus.OK, null);
    }

    private MigrateResponse handleFinish(MigrateControlRequest request) {
        String seriesKey = request.seriesKey();
        String migrationId = request.migrationId();
        MigrationState state = states.get(migrationId);
        String storageKey = state != null && state.storageKey() != null
                ? state.storageKey() : resolveStorageKey(seriesKey);
        // Mesmo achado bloqueante do Refuter que motivou o guard de handleAbort (destino): um FINISH
        // atrasado/duplicado não pode esquecer nem apagar a cópia local se o líder, na leitura FORTE,
        // ainda confirma que ESTE nó é o dono ativo — nesse caso o flip para o destino nunca aconteceu
        // de verdade (ou foi revertido), e apagar aqui deixaria a série sem NENHUMA cópia.
        Optional<SeriesPlacement> strong = catalog.placementStrong(seriesKey);
        if (strong.isPresent() && strong.get().isOwnedBy(self.value())) {
            LOGGER.log(Level.WARNING, "MIGRATE_FINISH para " + seriesKey + " (migrationId=" + migrationId
                    + "): líder confirma ACTIVE(self) — recusando esquecer/apagar a cópia local");
            registry.clearMigrating(seriesKey);
            updateState(migrationId, seriesKey, Role.SOURCE, MigratePhase.FINISHED, storageKey, null);
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        applyFinishLocally(seriesKey, migrationId, storageKey);
        return MigrateResponse.of(MigrateStatus.OK, null);
    }

    /**
     * Efetivamente esquece e apaga a cópia local da origem — extraído de {@link #handleFinish} para ser
     * reusado por {@link #healStuckMigrations} (achado dos MÉDIOS do Refuter: um {@code MIGRATE_FINISH}
     * que nunca chega deixa a origem presa em {@code markMigrating} para sempre; a autocura aplica esta
     * MESMA limpeza depois de confirmar, ela própria, {@code ACTIVE(outro)} via {@code placementStrong}).
     */
    private void applyFinishLocally(String seriesKey, String migrationId, String storageKey) {
        // forget (não discard): além de soltar o handle, ESQUECE a definição desta série neste nó. Com
        // discard, o YAML/opções continuavam em cache e a auto-cura de StorageRequestHandler
        // (reopenIfKnown) RECRIAVA o arquivo — vazio — assim que chegasse uma escrita atrasada do
        // cliente enquanto a réplica local do catálogo ainda apontasse para cá, deixando uma órfã
        // permanente no dono antigo. Ver Javadoc de SeriesHandleRegistry#forget.
        // Idempotente: forget e storage().delete não fazem nada se já não há nada a apagar.
        registry.forget(seriesKey);
        if (storageKey != null) {
            volume.storage().delete(storageKey);
            // Confere o próprio delete: se a imagem ainda existir logo em seguida (mesma thread, sem
            // concorrência possível sobre esta chave — só MIGRATE_FINISH apaga), é sinal de que
            // BlobStorage#delete não fez o que prometeu (achado ao investigar RebalanceClusterTest:
            // imagem nunca some no dono antigo mesmo com este handler respondendo OK). WARNING só nesse
            // caso anômalo — o caminho normal fica em FINE para não poluir o log de produção.
            if (volume.storage().get(storageKey).isPresent()) {
                LOGGER.log(Level.WARNING, "MIGRATE_FINISH: storage().delete(" + storageKey + ") não removeu a "
                        + "imagem de " + seriesKey + " (migrationId=" + migrationId + ")");
            } else {
                LOGGER.log(Level.FINE, "MIGRATE_FINISH apagou " + storageKey + " (seriesKey=" + seriesKey + ")");
            }
        }
        updateState(migrationId, seriesKey, Role.SOURCE, MigratePhase.FINISHED, storageKey, null);
        // A partir daqui o catálogo já aponta para o novo dono. Sair de "migrating" é seguro porque a
        // série está esquecida: sem um OPEN explícito (que só acontece com o dono confirmado pelo
        // líder), nada reabre nem recria a cópia local.
        registry.clearMigrating(seriesKey);
    }

    // ---------------------------------------------------------------- destino (TARGET)

    private MigrateResponse handlePrepare(MigratePrepareRequest request) {
        MigrationState previous = states.get(request.migrationId());
        if (previous != null && (!previous.seriesKey().equals(request.seriesKey()) || previous.role() != Role.TARGET
                || isTerminal(previous.phase()))) {
            return MigrateResponse.of(MigrateStatus.ERROR, "migration ended or identity mismatch");
        }
        if (!isCurrentTarget(request.seriesKey(), request.migrationId())
                || !resolveStorageKey(request.seriesKey()).equals(request.storageKey())) {
            return MigrateResponse.of(MigrateStatus.ERROR, "reservation not authorized by catalog");
        }
        if (!destinationActive()) {
            return failTarget(request.seriesKey(), request.migrationId(), "destination is not ACTIVE");
        }
        if (request.totalBytes() <= 0 || request.totalBytes() > maxSeriesBytes) {
            return failTarget(request.seriesKey(), request.migrationId(), "invalid migration size");
        }
        for (MigrationState other : states.values()) {
            if (other.role() == Role.TARGET && other.seriesKey().equals(request.seriesKey())
                    && !other.migrationId().equals(request.migrationId()) && !isTerminal(other.phase())) {
                failTarget(other.seriesKey(), other.migrationId(), "superseded by current catalog migration");
            }
        }
        StagingBuffer old = staging.get(request.migrationId());
        if (old != null) {
            return old.expectedBytes == request.totalBytes() && old.storageKey.equals(request.storageKey())
                    && old.liveCopy == request.liveCopy()
                    ? MigrateResponse.of(old.liveCopy ? MigrateStatus.COPY_READY : MigrateStatus.OK, null)
                    : MigrateResponse.of(MigrateStatus.ERROR, "reservation identity mismatch");
        }
        // Issue #167 (item 3): a cota é do PRÓPRIO destino (config local), rechecada aqui porque o líder
        // decide com um status possivelmente defasado. Só migrações novas chegam aqui (o re-PREPARE
        // idempotente já retornou acima).
        Optional<String> quota = quotaRefusal(request);
        if (quota.isPresent()) {
            return failTarget(request.seriesKey(), request.migrationId(), MigrateStatus.QUOTA_EXCEEDED, quota.get());
        }
        try {
            volume.storage().reserve(request.migrationId(), request.storageKey(), request.totalBytes());
        } catch (RuntimeException e) {
            return failTarget(request.seriesKey(), request.migrationId(), describe(e));
        }
        staging.put(request.migrationId(), new StagingBuffer(request.totalBytes(), request.storageKey(), request.liveCopy(), clock.millis()));
        states.put(request.migrationId(), new MigrationState(request.seriesKey(), request.migrationId(), Role.TARGET,
                MigratePhase.STARTED, 0, 0, request.totalBytes(), null, request.storageKey(), null, clock.millis()));
        return MigrateResponse.of(request.liveCopy() ? MigrateStatus.COPY_READY : MigrateStatus.OK, null);
    }

    private boolean destinationActive() {
        var status = catalog.nodeStatusStrong(self.value());
        return !(catalog.geometryTrackingEnabled() && status.isEmpty())
                && status.filter(n -> n.state() != dev.nishisan.utils.oss.cluster.catalog.NodeState.ACTIVE).isEmpty();
    }

    private MigrateResponse failTarget(String seriesKey, String migrationId, String message) {
        return failTarget(seriesKey, migrationId, MigrateStatus.ERROR, message);
    }

    private MigrateResponse failTarget(String seriesKey, String migrationId, MigrateStatus status, String message) {
        staging.remove(migrationId);
        volume.storage().releaseReservation(migrationId);
        updateState(migrationId, seriesKey, Role.TARGET, MigratePhase.FAILED, null, message);
        return MigrateResponse.of(status, message);
    }

    /**
     * Motivo de cota pelo qual este destino recusa a reserva: séries = entradas vivas do volume + alvos de
     * migração ainda abertos (excluindo esta) {@code >= quotaMaxSeries}; bytes = usados + reservados +
     * {@code totalBytes} {@code > quotaMaxBytes}. Vazio sem cota ou com folga.
     */
    private Optional<String> quotaRefusal(MigratePrepareRequest request) {
        if (quotaMaxSeries <= 0 && quotaMaxBytes <= 0) {
            return Optional.empty();
        }
        var stats = volume.stats();
        if (quotaMaxSeries > 0) {
            long openTargets = states.values().stream()
                    .filter(s -> s.role() == Role.TARGET && !isTerminal(s.phase())
                            && !s.migrationId().equals(request.migrationId()))
                    .count();
            long effective = stats.catalogEntryCount() + openTargets;
            if (effective >= quotaMaxSeries) {
                return Optional.of("quota_series(" + (effective + 1) + "/" + quotaMaxSeries + ")");
            }
        }
        if (quotaMaxBytes > 0) {
            long used = Arrays.stream(stats.shardUsedBytes()).sum();
            long effective = used + volume.storage().reservedBytes() + request.totalBytes();
            if (effective > quotaMaxBytes) {
                return Optional.of("quota_bytes(" + effective + "/" + quotaMaxBytes + ")");
            }
        }
        return Optional.empty();
    }

    private MigrateResponse handleChunk(MigrateChunkRequest request) {
        MigrationState existing = states.get(request.migrationId());
        if (existing != null && (!existing.seriesKey().equals(request.seriesKey())
                || existing.role() != Role.TARGET || isTerminal(existing.phase()))) {
            return MigrateResponse.of(MigrateStatus.ERROR, "migração encerrada ou identidade divergente");
        }
        if (existing == null && !isCurrentTarget(request.seriesKey(), request.migrationId())) {
            return MigrateResponse.of(MigrateStatus.ERROR, "migração não autorizada pelo catálogo");
        }
        StagingBuffer buffer = staging.get(request.migrationId());
        if (buffer == null) {
            return MigrateResponse.of(MigrateStatus.ERROR, "MIGRATE_PREPARE required before chunks");
        }
        if (buffer.image != null) {
            return MigrateResponse.of(MigrateStatus.ERROR, "base image is already complete");
        }
        if (request.total() <= 0 || request.seq() < 0 || request.seq() >= request.total() || request.data() == null) {
            return failTarget(request.seriesKey(), request.migrationId(), "invalid chunk");
        }
        int chunksReceived;
        int totalExpected;
        int bufferedBytes;
        synchronized (buffer) {
            if (request.seq() != buffer.chunksReceived) {
                return failTarget(request.seriesKey(), request.migrationId(),
                        "chunk fora de ordem para " + request.migrationId() + ": esperado "
                                + buffer.chunksReceived + ", recebido " + request.seq());
            }
            if (buffer.totalExpected < 0) {
                buffer.totalExpected = request.total();
            } else if (buffer.totalExpected != request.total()) {
                return failTarget(request.seriesKey(), request.migrationId(),
                        "total de chunks inconsistente para " + request.migrationId());
            }
            if ((long) buffer.out.size() + request.data().length > buffer.expectedBytes) {
                return failTarget(request.seriesKey(), request.migrationId(),
                        "staging de " + request.migrationId() + " excede maxSeriesBytes (" + maxSeriesBytes + ")");
            }
            buffer.out.write(request.data(), 0, request.data().length);
            buffer.chunksReceived++;
            chunksReceived = buffer.chunksReceived;
            totalExpected = buffer.totalExpected;
            bufferedBytes = buffer.out.size();
        }
        states.put(request.migrationId(), new MigrationState(request.seriesKey(), request.migrationId(),
                Role.TARGET, MigratePhase.TRANSFERRING, chunksReceived, totalExpected, bufferedBytes, null, null,
                null, clock.millis()));
        return MigrateResponse.of(MigrateStatus.OK, null);
    }

    private MigrateResponse handlePatch(MigratePatchRequest request) {
        MigrationState state = states.get(request.migrationId());
        if (state == null || state.role() != Role.TARGET || !state.seriesKey().equals(request.seriesKey())
                || isTerminal(state.phase())) {
            return MigrateResponse.of(MigrateStatus.ERROR, "patch identity is not active");
        }
        StagingBuffer buffer = staging.get(request.migrationId());
        if (buffer == null || !buffer.liveCopy || buffer.totalExpected < 0
                || buffer.chunksReceived != buffer.totalExpected || request.data() == null
                || request.data().length == 0 || request.offset() < 0
                || (long) request.offset() + request.data().length > buffer.expectedBytes) {
            return failTarget(request.seriesKey(), request.migrationId(), "invalid patch or incomplete base image");
        }
        if (request.sequence() == buffer.patchSequence - 1 && buffer.lastPatch != null
                && request.offset() == buffer.lastPatch.offset()
                && Arrays.equals(request.data(), buffer.lastPatch.data())) {
            return MigrateResponse.of(MigrateStatus.OK, null);
        }
        if (request.sequence() != buffer.patchSequence) {
            return failTarget(request.seriesKey(), request.migrationId(), "patch out of order");
        }
        System.arraycopy(request.data(), 0, buffer.image(), request.offset(), request.data().length);
        buffer.lastPatch = new MigratePatchRequest(request.seriesKey(), request.migrationId(), request.sequence(),
                request.offset(), request.data().clone());
        buffer.patchSequence++;
        return MigrateResponse.of(MigrateStatus.OK, null);
    }

    private MigrateResponse handleCommit(MigrateCommitRequest request) {
        MigrationState existing = states.get(request.migrationId());
        if (existing != null && (!existing.seriesKey().equals(request.seriesKey())
                || existing.role() != Role.TARGET)) {
            return MigrateResponse.of(MigrateStatus.ERROR, "migrationId pertence a outra série ou papel");
        }
        if (existing != null && existing.phase() == MigratePhase.COMMITTED) {
            // Reenvio idempotente do mesmo COMMIT depois de já confirmado.
            return new MigrateResponse(MigrateStatus.COMMITTED, null, existing.bytes());
        }
        if (existing != null && isTerminal(existing.phase())) {
            return MigrateResponse.of(MigrateStatus.ERROR, "migração encerrada");
        }
        // Strong validation is required even after tombstone expiry or a process restart.
        // Under the series lock, no newer local commit can pass this check and install first.
        if (!isCurrentTarget(request.seriesKey(), request.migrationId())) {
            staging.remove(request.migrationId());
            volume.storage().releaseReservation(request.migrationId());
            return MigrateResponse.of(MigrateStatus.ERROR, "migração não autorizada pelo catálogo");
        }
        StagingBuffer buffer = staging.get(request.migrationId());
        if (buffer == null) {
            return MigrateResponse.of(MigrateStatus.ERROR,
                    "nenhum staging em andamento para " + request.migrationId());
        }
        if (!buffer.storageKey.equals(request.storageKey())) {
            return failTarget(request.seriesKey(), request.migrationId(), "physical key differs from reservation");
        }
        byte[] bytes;
        synchronized (buffer) {
            bytes = buffer.image();
        }
        if (bytes.length != request.totalBytes() || bytes.length != buffer.expectedBytes
                || buffer.chunksReceived != buffer.totalExpected) {
            staging.remove(request.migrationId());
            volume.storage().releaseReservation(request.migrationId());
            updateState(request.migrationId(), request.seriesKey(), Role.TARGET, MigratePhase.FAILED,
                    request.storageKey(), "tamanho recebido (" + bytes.length + ") difere do esperado ("
                            + request.totalBytes() + ")");
            return MigrateResponse.of(MigrateStatus.HASH_MISMATCH, "tamanho divergente");
        }
        String actualSha256Hex = sha256Hex(bytes);
        if (!actualSha256Hex.equalsIgnoreCase(request.sha256Hex())) {
            staging.remove(request.migrationId());
            volume.storage().releaseReservation(request.migrationId());
            updateState(request.migrationId(), request.seriesKey(), Role.TARGET, MigratePhase.FAILED,
                    request.storageKey(), "SHA-256 divergente");
            return MigrateResponse.of(MigrateStatus.HASH_MISMATCH, "SHA-256 divergente");
        }
        if (!destinationActive()) {
            return failTarget(request.seriesKey(), request.migrationId(), "destination stopped accepting migrations");
        }
        // Descarta qualquer handle antigo desta série ANTES de sobrescrever a imagem por baixo — nunca
        // deixar um NgrrdHandle em memória sobreviver à substituição do arquivo (ver Javadoc de
        // SeriesHandleRegistry#discard).
        try {
            registry.discard(request.seriesKey());
            volume.storage().atomicReplaceReserved(request.storageKey(), bytes, request.migrationId());
        } catch (RuntimeException e) {
            return failTarget(request.seriesKey(), request.migrationId(), describe(e));
        }
        staging.remove(request.migrationId());
        volume.storage().releaseReservation(request.migrationId());
        states.put(request.migrationId(), new MigrationState(request.seriesKey(), request.migrationId(),
                Role.TARGET, MigratePhase.COMMITTED, 0, 0, bytes.length, actualSha256Hex, request.storageKey(), null,
                clock.millis()));
        return new MigrateResponse(MigrateStatus.COMMITTED, null, bytes.length);
    }

    // ---------------------------------------------------------------- observabilidade

    /** Snapshot das migrações concluídas com sucesso ainda presentes em {@link #states}, por papel. */
    public ExecutorMetrics metricsSnapshot() {
        long in = 0L;
        long out = 0L;
        for (MigrationState state : states.values()) {
            if (state.role() == Role.TARGET && state.phase() == MigratePhase.COMMITTED) {
                in++;
            } else if (state.role() == Role.SOURCE && state.phase() == MigratePhase.FINISHED) {
                out++;
            }
        }
        return new ExecutorMetrics(in, out);
    }

    /** Expires even unfinished transfers, releasing their admission reservations. */
    public void expireReservations(Duration timeout) {
        long threshold = clock.millis() - timeout.toMillis();
        for (var entry : states.entrySet()) {
            MigrationState state = entry.getValue();
            try (var guard = CoordinationLocks.acquire(seriesLock(state.seriesKey()))) {
                StagingBuffer buffer = staging.get(entry.getKey());
                if (buffer != null && buffer.createdAtMs < threshold) {
                    failTarget(state.seriesKey(), entry.getKey(), "migration reservation expired");
                }
            }
        }
    }

    /**
     * Remove entradas de {@link #states} (e o staging associado) que já chegaram a um resultado —
     * qualquer fase exceto {@link MigratePhase#STARTED}/{@link MigratePhase#TRANSFERRING} — há mais de
     * {@code ttl}. Chamado periodicamente pelo {@code NodeStatusReporter} (mesmo tick de
     * {@code registry.closeIdle()}).
     *
     * <p>Achado do Refuter (r2, MÉDIO): nunca remove uma entrada cuja série ainda está {@code
     * registry.isMigrating} — mesmo terminal (ex.: {@code SOURCE}/{@link MigratePhase#COMMITTED}, a
     * transferência terminou mas o {@code MIGRATE_FINISH} nunca confirmou aqui). Apagar a entrada SEM
     * antes liberar a marca deixaria a série bloqueada para sempre, sem nenhum registro em {@link
     * #states} para {@link #healStuckMigrations} encontrar e resolver depois.</p>
     *
     * @return quantas entradas foram removidas
     */
    public int sweepExpiredStates(Duration ttl) {
        long threshold = clock.millis() - ttl.toMillis();
        int removed = 0;
        for (Map.Entry<String, MigrationState> entry : states.entrySet()) {
            MigrationState state = entry.getValue();
            try (var guard = CoordinationLocks.acquire(seriesLock(state.seriesKey()))) {
                if (states.get(entry.getKey()) != state) {
                    continue;
                }
                if (isTerminal(state.phase()) && state.updatedAtMs() < threshold
                        && !registry.isMigrating(state.seriesKey())) {
                    if (states.remove(entry.getKey(), state)) {
                        staging.remove(entry.getKey());
                        volume.storage().releaseReservation(entry.getKey());
                        removed++;
                    }
                }
            }
        }
        return removed;
    }

    /**
     * Autocura séries presas em {@code markMigrating} (papel {@link Role#SOURCE}, {@code
     * registry.isMigrating} ainda bloqueando {@code OPEN}/{@code withHandle}) sem atualização há mais
     * de {@code timeout}.
     *
     * <p>Candidatas: fase {@link MigratePhase#STARTED}/{@link MigratePhase#TRANSFERRING} (a migração
     * nem começou a transferir, ou está no meio), {@link MigratePhase#COMMITTED} e {@link
     * MigratePhase#FAILED} — nenhuma delas limpa {@code markMigrating} sozinha, só {@code
     * MIGRATE_ABORT}/{@code MIGRATE_FINISH} fazem isso, e é justamente a entrega deles que pode se
     * perder. {@code COMMITTED} (achado do Refuter r2, MÉDIO): a transferência terminou com sucesso e o
     * destino já confirmou (ver {@link #transfer}), mas sem {@code MIGRATE_FINISH} a marca fica —
     * "transferência concluída, FINISH perdido". {@code FAILED} (achado do Refuter r3): a transferência
     * falhou (chunk/commit recusado, falha de transporte — ver {@link #transfer}) e o coordenador
     * decide abortar, mas sem o {@code MIGRATE_ABORT} chegar aqui a marca também fica presa — com a
     * revalidação do item 1 da rodada anterior (r2), isso pode acontecer mesmo SEM perda de rede, se a
     * reversão do catálogo em {@code MigrationCoordinator#abort} não for gravada (precondição falhou):
     * nesse caso nenhum ABORT é enviado a lugar nenhum, de propósito. Nenhuma das duas é terminal do
     * ponto de vista de {@code markMigrating} continuar bloqueando a série, mesmo sendo terminal para
     * {@link #isTerminal} (que rege só {@link #sweepExpiredStates}).</p>
     *
     * <p>Achado dos MÉDIOS do Refuter: se o {@code MIGRATE_ABORT} ou o {@code MIGRATE_FINISH} do
     * coordenador nunca chegam a este nó (rede, nó reiniciado entre o envio e a entrega), a série fica
     * bloqueada para sempre — mesmo depois de o catálogo já ter um desfecho definitivo. Chamado
     * periodicamente pelo {@code NodeStatusReporter} (mesmo tick de {@link #sweepExpiredStates}),
     * consulta {@code placementStrong} — NUNCA a réplica local — para decidir: {@code ACTIVE(self)} → o
     * ABORT se perdeu, {@link SeriesHandleRegistry#clearMigrating} libera a série de volta ao serviço
     * normal aqui; {@code ACTIVE(outro)} → o FINISH se perdeu, aplica a MESMA limpeza de {@link
     * #handleFinish} (via {@link #applyFinishLocally}), agora com a confirmação forte já em mãos.
     * Qualquer outro resultado ({@code MIGRATING}, ausente) não mexe: o desfecho real ainda não está
     * definido, agir seria um chute.</p>
     *
     * @return quantas séries presas foram resolvidas (nos dois sentidos combinados)
     */
    public int healStuckMigrations(Duration timeout) {
        long threshold = clock.millis() - timeout.toMillis();
        int healed = 0;
        for (Map.Entry<String, MigrationState> entry : states.entrySet()) {
            MigrationState state = entry.getValue();
            try (var guard = CoordinationLocks.acquire(seriesLock(state.seriesKey()))) {
                if (states.get(entry.getKey()) != state) {
                    continue;
                }
                if (state.role() != Role.SOURCE || !isStuckCandidatePhase(state.phase())
                        || state.updatedAtMs() >= threshold) {
                    continue;
                }
                String seriesKey = state.seriesKey();
                if (!registry.isMigrating(seriesKey)) {
                    // Já resolvida por outro caminho (ex.: MIGRATE_ABORT/FINISH chegou entre esta varredura
                    // e a anterior) — nada a curar.
                    continue;
                }
                Optional<SeriesPlacement> strong = catalog.placementStrong(seriesKey);
                if (strong.isEmpty() || strong.get().state() != PlacementState.ACTIVE) {
                    // MIGRATING (ainda em curso de verdade, ou outra migração já a sucedeu) ou ausente: o
                    // desfecho real ainda não está definido — não é seguro agir.
                    continue;
                }
                SeriesPlacement placement = strong.get();
                String migrationId = entry.getKey();
                if (placement.isOwnedBy(self.value())) {
                    LOGGER.log(Level.INFO, "Autocura de " + seriesKey + " (migrationId=" + migrationId + "): presa em "
                            + "markMigrating há mais de " + timeout + " sem MIGRATE_ABORT — líder confirma "
                            + "ACTIVE(self), liberando de volta ao serviço normal");
                    registry.clearMigrating(seriesKey);
                    updateState(migrationId, seriesKey, Role.SOURCE, MigratePhase.ABORTED, state.storageKey(),
                            "autocura: MIGRATE_ABORT nunca chegou, placement forte confirma ACTIVE(self)");
                } else {
                    LOGGER.log(Level.WARNING, "Autocura de " + seriesKey + " (migrationId=" + migrationId + "): presa "
                            + "em markMigrating há mais de " + timeout + " sem MIGRATE_FINISH — líder confirma "
                            + "ACTIVE(" + placement.ownerNodeId() + "), aplicando o FINISH local agora");
                    applyFinishLocally(seriesKey, migrationId, state.storageKey());
                }
                healed++;
            }
        }
        return healed;
    }

    /** Fecha o pool de transferência ({@code ngrrd-migration-src}). Não fecha o volume nem o registry. */
    public void close() {
        staging.keySet().forEach(volume.storage()::releaseReservation);
        staging.clear();
        transferExecutor.shutdownNow();
    }

    // ---------------------------------------------------------------- internos

    private Optional<MigrationState> activeSourceStateFor(String seriesKey) {
        return states.values().stream()
                .filter(state -> state.role() == Role.SOURCE && state.seriesKey().equals(seriesKey)
                        && (!isTerminal(state.phase())
                            || state.phase() == MigratePhase.COMMITTED && registry.isMigrating(seriesKey)))
                .findFirst();
    }

    private static boolean isTerminal(MigratePhase phase) {
        return phase != MigratePhase.STARTED && phase != MigratePhase.TRANSFERRING;
    }

    /**
     * Fases candidatas a {@link #healStuckMigrations} — ver Javadoc do método para o porquê de
     * COMMITTED e FAILED entrarem (nenhuma delas é {@link #isTerminal} do ponto de vista de {@code
     * markMigrating} continuar bloqueando a série: só {@code MIGRATE_ABORT}/{@code MIGRATE_FINISH}
     * limpam a marca, e é exatamente a chegada deles que pode se perder).
     */
    private static boolean isStuckCandidatePhase(MigratePhase phase) {
        return phase == MigratePhase.STARTED || phase == MigratePhase.TRANSFERRING
                || phase == MigratePhase.COMMITTED || phase == MigratePhase.FAILED;
    }

    private void updateState(String migrationId, String seriesKey, Role role, MigratePhase phase, String storageKey,
            String message) {
        states.compute(migrationId, (id, old) -> new MigrationState(seriesKey, migrationId, role, phase,
                old != null ? old.chunksReceived() : 0, old != null ? old.chunksExpected() : 0,
                old != null ? old.bytes() : 0L, old != null ? old.sha256Hex() : null,
                storageKey != null ? storageKey : (old != null ? old.storageKey() : null), message, clock.millis()));
    }

    private boolean isCurrentTarget(String seriesKey, String migrationId) {
        return catalog.placementStrong(seriesKey).filter(p -> p.state() == PlacementState.MIGRATING
                && migrationId.equals(p.migrationId()) && self.value().equals(p.targetNodeId())).isPresent();
    }

    private boolean transferActive(String migrationId) {
        MigrationState state = states.get(migrationId);
        return !Thread.currentThread().isInterrupted() && state != null
                && state.role() == Role.SOURCE && !isTerminal(state.phase());
    }

    private void updatePhase(String migrationId, MigratePhase phase, String message) {
        states.computeIfPresent(migrationId, (id, old) -> isTerminal(old.phase()) ? old
                : new MigrationState(old.seriesKey(), old.migrationId(), old.role(), phase,
                        old.chunksReceived(), old.chunksExpected(), old.bytes(), old.sha256Hex(),
                        old.storageKey(), message, clock.millis()));
    }

    private static String describe(RuntimeException e) {
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message != null ? ": " + message : "");
    }

    private int chunkCount(long totalBytes) {
        if (totalBytes <= 0) {
            return 1;
        }
        return (int) ((totalBytes + migrationChunkBytes - 1) / migrationChunkBytes);
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }

    /** Acumulador de chunks recebidos por uma migração em curso no destino. */
    private static final class StagingBuffer {
        private final long expectedBytes;
        private final String storageKey;
        private final long createdAtMs;
        private final boolean liveCopy;
        private byte[] image;
        private int patchSequence;
        private MigratePatchRequest lastPatch;
        private StagingBuffer(long expectedBytes, String storageKey, boolean liveCopy, long createdAtMs) {
            this.expectedBytes = expectedBytes;
            this.storageKey = storageKey;
            this.createdAtMs = createdAtMs;
            this.liveCopy = liveCopy;
        }
        private byte[] image() {
            if (image == null) {
                image = out.toByteArray();
                out = null;
            }
            return image;
        }
        private ByteArrayOutputStream out = new ByteArrayOutputStream();
        private int chunksReceived;
        private int totalExpected = -1;
    }
}
