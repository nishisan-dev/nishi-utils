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

import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.NGridNodeBuilder;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.cluster.api.ClientMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.SeriesInfo;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeRequest;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminRebalanceResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import dev.nishisan.utils.oss.cluster.rpc.TransportClusterRpc;
import dev.nishisan.utils.oss.format.DefinitionHash;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Implementação padrão de {@link NgrrdClusterClient}: entra no cluster NGrid
 * como membro sem volume ({@code client} + {@link NodeInfo#ROLE_LEADER_INELIGIBLE}),
 * monta os colaboradores ({@link PlacementResolver}, {@link WriteDispatcher})
 * e devolve {@link RemoteSeriesHandle} por série, com deduplicação por
 * {@code seriesKey}.
 */
public final class DefaultNgrrdClusterClient implements NgrrdClusterClient {

    private static final Logger LOGGER = Logger.getLogger(DefaultNgrrdClusterClient.class.getName());
    private static final long LEADER_POLL_INTERVAL_MS = 50L;
    private static final String CLIENT_ROLE = "client";
    /** Mesmo papel usado por {@code NgrrdStorageNode} para se anunciar ao cluster. */
    private static final String STORAGE_ROLE = "storage";

    private static final int MAX_NOT_LEADER_ATTEMPTS = 5;
    /** Placeholder do supplier de métricas do {@code WriteDispatcher} até {@code clientRef} ser publicado em {@link #connect}. */
    private static final ClientMetricsSnapshot EMPTY_METRICS = new ClientMetricsSnapshot(0L, 0L, 0L, 0L,
            Map.of(), Map.of(), 0, LatencySnapshot.EMPTY, 0L);

    private final NgrrdClusterConfig config;
    private final NGridNode node;
    private final Path dataDir;
    private final boolean temporaryDataDir;
    private final ClusterRpc rpc;
    /** Mesma instância de {@link #rpc}, com o tipo concreto — só para expor {@code latencySnapshot()}/{@code placeCount()} em {@link #metrics()}. */
    private final MetricsTrackingClusterRpc metricsRpc;
    private final PlacementResolver resolver;
    private final SeriesExistence existence;
    private final WriteDispatcher dispatcher;
    private final ConcurrentMap<String, RemoteSeriesHandle> handles;
    /**
     * item 10 (achado do Refuter): lock por {@code seriesKey} usado só para serializar aberturas
     * concorrentes DA MESMA série — ver {@link #open(String, Map, Ngrrd.OpenOptions)}.
     */
    private final ConcurrentMap<String, Object> openLocks;

    private volatile boolean closed;

    private DefaultNgrrdClusterClient(NgrrdClusterConfig config, NGridNode node, Path dataDir,
            boolean temporaryDataDir, MetricsTrackingClusterRpc rpc, PlacementResolver resolver,
            SeriesExistence existence, WriteDispatcher dispatcher, ConcurrentMap<String, RemoteSeriesHandle> handles) {
        this.config = config;
        this.node = node;
        this.dataDir = dataDir;
        this.temporaryDataDir = temporaryDataDir;
        this.rpc = rpc;
        this.metricsRpc = rpc;
        this.resolver = resolver;
        this.existence = existence;
        this.dispatcher = dispatcher;
        this.handles = handles;
        this.openLocks = new ConcurrentHashMap<>();
    }

    /** Conecta ao cluster ngrrd e devolve um cliente pronto para {@link #open}. */
    public static DefaultNgrrdClusterClient connect(NgrrdClusterConfig cfg) {
        Objects.requireNonNull(cfg, "cfg");
        Path dataDir;
        boolean temporaryDataDir;
        try {
            if (cfg.dataDir() != null) {
                Files.createDirectories(cfg.dataDir());
                dataDir = cfg.dataDir();
                temporaryDataDir = false;
            } else {
                dataDir = Files.createTempDirectory("ngrrd-client");
                temporaryDataDir = true;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("falha ao preparar o dataDir do cliente ngrrd", e);
        }

        NGridNodeBuilder builder = NGrid.node(cfg.host(), cfg.port())
                .id(cfg.clientId())
                .priority(0)
                .roles(CLIENT_ROLE, NodeInfo.ROLE_LEADER_INELIGIBLE)
                .dataDir(dataDir);
        CatalogService.declareMaps(builder);
        if (cfg.seed() != null && !cfg.seed().isBlank()) {
            builder.seed(cfg.seed());
        }
        if (!cfg.peers().isEmpty()) {
            builder.peers(cfg.peers().toArray(new String[0]));
        }

        NGridNode node;
        try {
            node = builder.start();
        } catch (IOException e) {
            throw new UncheckedIOException("falha ao iniciar o nó cliente do cluster ngrrd", e);
        }
        try {
            awaitLeaderOrThrow(node, cfg.leaderWaitTimeout());
            awaitStorageConnectionsOrThrow(node, cfg.leaderWaitTimeout());

            CatalogService catalog = CatalogService.from(node);
            TransportClusterRpc transportRpc = new TransportClusterRpc(node.transport(), node.coordinator(),
                    cfg.requestTimeout());
            MetricsTrackingClusterRpc rpc = new MetricsTrackingClusterRpc(transportRpc);
            RetryPolicy leaderRetry = new RetryPolicy(cfg.leaderWaitTimeout(), cfg.retryBackoffMin(),
                    cfg.retryBackoffMax());
            CatalogLookupClient catalogLookupClient = new CatalogLookupClient(rpc, leaderRetry, Clock.systemUTC(),
                    cfg.catalogLookupBatchSize());
            PlacementResolver resolver = new PlacementResolver(catalog, rpc, leaderRetry, Clock.systemUTC(),
                    catalogLookupClient);
            SeriesExistence existence = new SeriesExistence(resolver, catalogLookupClient);

            ConcurrentMap<String, RemoteSeriesHandle> handles = new ConcurrentHashMap<>();
            RetryPolicy opRetry = new RetryPolicy(cfg.retryTimeout(), cfg.retryBackoffMin(), cfg.retryBackoffMax());
            // Referência publicada com segurança (AtomicReference = volatile) para a thread do
            // tickLoop do WriteDispatcher, que só a lê ~METRICS_TICK_INTERVAL ticks depois de criada
            // (bem depois deste método retornar) — resolve o auto-referenciamento (o supplier de
            // métricas do cliente precisa do próprio DefaultNgrrdClusterClient, que só existe depois
            // do WriteDispatcher já estar construído).
            AtomicReference<DefaultNgrrdClusterClient> clientRef = new AtomicReference<>();
            Supplier<ClientMetricsSnapshot> metricsSupplier = cfg.metricsListener() == null ? null : () -> {
                DefaultNgrrdClusterClient client = clientRef.get();
                return client != null ? client.metrics() : EMPTY_METRICS;
            };
            // B1 (achado do Refuter): este era `cfg.requestTimeout()` — o WriteDispatcher usava o
            // requestTimeout (tipicamente segundos) como se fosse o closeTimeout (tipicamente dezenas
            // de segundos) em TODO close()/flushAllSync(), truncando o dreno bem antes do que o
            // cliente anuncia via NgrrdClusterConfig#closeTimeout().
            WriteDispatcher dispatcher = new WriteDispatcher(rpc, resolver, opRetry, cfg.batchMaxSamples(),
                    cfg.batchMaxDelay(), cfg.maxBufferedSamplesPerNode(), cfg.bufferFullPolicy(),
                    cfg.closeTimeout(), seriesKey -> {
                        RemoteSeriesHandle handle = handles.get(seriesKey);
                        return handle != null && handle.reopen();
                    }, (seriesKey, newOwner) -> {
                        RemoteSeriesHandle handle = handles.get(seriesKey);
                        if (handle != null) {
                            handle.ownerChanged(newOwner);
                        }
                    }, Clock.systemUTC(), cfg.metricsListener(), metricsSupplier);
            DefaultNgrrdClusterClient client = new DefaultNgrrdClusterClient(cfg, node, dataDir, temporaryDataDir,
                    rpc, resolver, existence, dispatcher, handles);
            clientRef.set(client);
            return client;
        } catch (RuntimeException e) {
            try {
                node.close();
            } catch (IOException | RuntimeException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    private static void awaitLeaderOrThrow(NGridNode node, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        Optional<NodeInfo> leader = node.coordinator().leaderInfo();
        while (leader.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(LEADER_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NgrrdClusterException(ErrorCode.NO_LEADER, "interrompido aguardando líder do cluster", e);
            }
            leader = node.coordinator().leaderInfo();
        }
        if (leader.isEmpty()) {
            throw new NgrrdClusterException(ErrorCode.NO_LEADER,
                    "nenhum líder eleito ao conectar em " + timeout);
        }
    }

    /**
     * B3(i) (achado do Refuter do M1c): esperar só o líder estar eleito não bastava — o cliente podia
     * ficar "pronto" antes do {@code Transport} TCP terminar de conectar com algum storage node, e a 1a
     * chamada RPC para esse nó falhava com "No connection available for storage-N".
     *
     * <p>M3: passa a exigir só que <strong>ao menos UM</strong> storage node ativo esteja conectado —
     * não mais todos (nota do checkpoint do M1c: "connect() falha se qualquer storage node ativo
     * estiver inalcançável dentro de leaderWaitTimeout, sem disponibilidade parcial"). A conectividade
     * com os demais é responsabilidade da retentativa de transporte por operação
     * ({@code TransportRetry}); nós ainda inalcançáveis ao final da espera só geram um WARN.</p>
     */
    private static void awaitStorageConnectionsOrThrow(NGridNode node, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        boolean anyConnected = hasAnyConnectedStorageMember(node);
        while (!anyConnected && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(LEADER_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                        "interrompido aguardando conexão de transporte com os storage nodes", e);
            }
            anyConnected = hasAnyConnectedStorageMember(node);
        }
        if (!anyConnected) {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                    "nenhum storage node alcançável via transporte dentro de " + timeout);
        }
        List<NodeId> stillDisconnected = disconnectedStorageMembers(node);
        if (!stillDisconnected.isEmpty()) {
            LOGGER.log(Level.WARNING, "connect(): " + stillDisconnected.size() + " storage node(s) ainda "
                    + "inalcançável(is) via transporte após " + timeout + " — prosseguindo com disponibilidade "
                    + "parcial (as chamadas para eles retentam por conta própria): " + stillDisconnected);
        }
    }

    private static boolean hasAnyConnectedStorageMember(NGridNode node) {
        List<NodeId> storageMembers = storageMemberIds(node);
        return !storageMembers.isEmpty()
                && storageMembers.stream().anyMatch(id -> node.transport().isConnected(id));
    }

    private static List<NodeId> storageMemberIds(NGridNode node) {
        return node.coordinator().activeMembers().stream()
                .filter(info -> info.roles().contains(STORAGE_ROLE))
                .map(NodeInfo::nodeId)
                .toList();
    }

    private static List<NodeId> disconnectedStorageMembers(NGridNode node) {
        return storageMemberIds(node).stream()
                .filter(id -> !node.transport().isConnected(id))
                .toList();
    }

    @Override
    public NgrrdHandle open(String yaml, Map<String, String> tags) {
        return open(yaml, tags, Ngrrd.OpenOptions.defaults());
    }

    @Override
    public NgrrdHandle open(String yaml, Map<String, String> tags, Ngrrd.OpenOptions options) {
        ensureOpen();
        Objects.requireNonNull(yaml, "yaml");
        Objects.requireNonNull(tags, "tags");
        String template = SeriesKeyTemplate.templateOf(yaml);
        String seriesKey = SeriesKeyTemplate.resolve(template, tags);
        RemoteSeriesHandle existing = handles.get(seriesKey);
        if (existing != null && existing.isOpen()) {
            return existing;
        }
        // item 10 (achado do Refuter): nunca fazer RPC dentro de computeIfAbsent — isso mantinha o bin
        // lock interno do ConcurrentHashMap preso durante toda a chamada de rede de openNewHandle
        // (OPEN no dono), bloqueando get()/put() de QUALQUER OUTRA série neste mesmo mapa até a rede
        // responder. Em vez disso, um lock por seriesKey (openLocks, construído sem I/O) serializa só
        // as aberturas concorrentes DA MESMA série; handles só é tocado com get/put simples.
        Object lock = openLocks.computeIfAbsent(seriesKey, key -> new Object());
        try {
            try (var guard = CoordinationLocks.acquire(lock)) {
                existing = handles.get(seriesKey);
                // Um handle em cache já fechado/marcado inexistente (ex.: onClose ainda não rodou, ou
                // perdeu a corrida de remoção condicional contra um handle mais novo) nunca é devolvido
                // — substituído por um novo abaixo, que refaz o open do zero.
                if (existing != null && existing.isOpen()) {
                    return existing;
                }
                // openNewHandle publica o handle em handles (dentro de resetSeries), só se o OPEN der certo.
                return openNewHandle(seriesKey, yaml, tags, options);
            }
        } finally {
            openLocks.remove(seriesKey, lock);
        }
    }

    @Override
    public NgrrdHandle open(Path yamlFile, Map<String, String> tags) {
        Objects.requireNonNull(yamlFile, "yamlFile");
        try {
            String yaml = Files.readString(yamlFile, StandardCharsets.UTF_8);
            return open(yaml, tags);
        } catch (IOException e) {
            throw new UncheckedIOException("falha ao ler a definição YAML: " + yamlFile, e);
        }
    }

    @Override
    public boolean exists(String seriesKey) {
        ensureOpen();
        Objects.requireNonNull(seriesKey, "seriesKey");
        return existence.exists(seriesKey, config.retryTimeout());
    }

    @Override
    public Map<String, Boolean> exists(Collection<String> seriesKeys) {
        ensureOpen();
        Objects.requireNonNull(seriesKeys, "seriesKeys");
        return existence.exists(seriesKeys, config.retryTimeout());
    }

    @Override
    public Optional<SeriesInfo> find(String seriesKey) {
        ensureOpen();
        Objects.requireNonNull(seriesKey, "seriesKey");
        return existence.find(seriesKey, config.retryTimeout());
    }

    private RemoteSeriesHandle openNewHandle(String seriesKey, String yaml, Map<String, String> tags,
            Ngrrd.OpenOptions options) {
        String definitionHashHex = DefinitionHash.hex(yaml);
        RetryPolicy opRetry = new RetryPolicy(config.retryTimeout(), config.retryBackoffMin(),
                config.retryBackoffMax());
        RemoteSeriesHandle handle = new RemoteSeriesHandle(seriesKey, yaml, definitionHashHex, tags, options,
                resolver, rpc, dispatcher, opRetry, config.requestTimeout(), config.closeTimeout(),
                Clock.systemUTC(), handles::remove, dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor.from(
                        new dev.nishisan.utils.oss.format.SeriesGeometry(
                                dev.nishisan.utils.oss.config.NgrrdYamlLoader.parse(yaml, System::getenv))));
        handle.open(opened -> handles.put(seriesKey, opened));
        return handle;
    }

    @Override
    public void flushAll() {
        ensureOpen();
        dispatcher.flushAllSync();
    }

    @Override
    public ClientMetricsSnapshot metrics() {
        return new ClientMetricsSnapshot(dispatcher.samplesEnqueued(), dispatcher.samplesSent(),
                dispatcher.samplesFailed(), dispatcher.batchesSent(), dispatcher.retriesByStatus(),
                dispatcher.bufferedSamples(), handles.size(), metricsRpc.latencySnapshot(), metricsRpc.placeCount());
    }

    @Override
    public AdminStatusResponse clusterStatus() {
        ensureOpen();
        int attempt = 0;
        NodeId leaderHint = null;
        for (;;) {
            attempt++;
            NodeId leader = leaderHint != null ? leaderHint : awaitLeaderIdOrThrow();
            leaderHint = null;
            AdminStatusResponse response = rpc.call(leader, Commands.ADMIN_STATUS, null, AdminStatusResponse.class);
            if (response.status() == SeriesStatus.OK) {
                return response;
            }
            if (response.status() != SeriesStatus.NOT_LEADER) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "ngrrd.admin.status respondeu " + response.status());
            }
            if (attempt >= MAX_NOT_LEADER_ATTEMPTS) {
                throw new NgrrdClusterException(ErrorCode.NO_LEADER,
                        "NOT_LEADER persistente ao consultar o status do cluster após " + attempt + " tentativas");
            }
            // Menor (achado do Refuter): só dorme quando NÃO sabemos já para onde ir — com leaderHint
            // preenchido (a resposta já indicou o líder atual), a próxima tentativa vai direto a ele,
            // sem uma espera artificial de LEADER_POLL_INTERVAL_MS no meio do caminho.
            if (response.leaderNodeId() != null) {
                leaderHint = NodeId.of(response.leaderNodeId());
            } else {
                sleepQuietly(LEADER_POLL_INTERVAL_MS);
            }
        }
    }

    @Override
    public void rebalanceNow() {
        ensureOpen();
        int attempt = 0;
        NodeId leaderHint = null;
        for (;;) {
            attempt++;
            NodeId leader = leaderHint != null ? leaderHint : awaitLeaderIdOrThrow();
            leaderHint = null;
            AdminRebalanceResponse response =
                    rpc.call(leader, Commands.ADMIN_REBALANCE, null, AdminRebalanceResponse.class);
            if (response.status() == SeriesStatus.OK) {
                return;
            }
            if (response.status() != SeriesStatus.NOT_LEADER) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        "ngrrd.admin.rebalance respondeu " + response.status());
            }
            if (attempt >= MAX_NOT_LEADER_ATTEMPTS) {
                throw new NgrrdClusterException(ErrorCode.NO_LEADER,
                        "NOT_LEADER persistente ao disparar o rebalanceamento após " + attempt + " tentativas");
            }
            if (response.leaderNodeId() != null) {
                leaderHint = NodeId.of(response.leaderNodeId());
            } else {
                sleepQuietly(LEADER_POLL_INTERVAL_MS);
            }
        }
    }

    @Override
    public StorageNodeStatus drainNode(String nodeId) {
        return adminTransition(Commands.ADMIN_DRAIN, nodeId, "ngrrd.admin.drain");
    }

    @Override
    public StorageNodeStatus activateNode(String nodeId) {
        return adminTransition(Commands.ADMIN_ACTIVATE, nodeId, "ngrrd.admin.activate");
    }

    /** Implementação comum de {@link #drainNode(String)}/{@link #activateNode(String)}. */
    private StorageNodeStatus adminTransition(String command, String nodeId, String commandLabel) {
        ensureOpen();
        Objects.requireNonNull(nodeId, "nodeId");
        int attempt = 0;
        NodeId leaderHint = null;
        for (;;) {
            attempt++;
            NodeId leader = leaderHint != null ? leaderHint : awaitLeaderIdOrThrow();
            leaderHint = null;
            AdminNodeStatusResponse response = rpc.call(leader, command, new AdminNodeRequest(nodeId, false),
                    AdminNodeStatusResponse.class);
            if (response.status() == SeriesStatus.OK) {
                return response.nodeStatus();
            }
            if (response.status() != SeriesStatus.NOT_LEADER) {
                throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                        commandLabel + " respondeu " + response.status()
                                + (response.message() != null ? " (" + response.message() + ")" : ""));
            }
            if (attempt >= MAX_NOT_LEADER_ATTEMPTS) {
                throw new NgrrdClusterException(ErrorCode.NO_LEADER,
                        "NOT_LEADER persistente ao executar " + commandLabel + " após " + attempt + " tentativas");
            }
            if (response.leaderNodeId() != null) {
                leaderHint = NodeId.of(response.leaderNodeId());
            } else {
                sleepQuietly(LEADER_POLL_INTERVAL_MS);
            }
        }
    }

    @Override
    public NodeMetricsSnapshot nodeMetrics(String nodeId) {
        ensureOpen();
        Objects.requireNonNull(nodeId, "nodeId");
        return rpc.call(NodeId.of(nodeId), Commands.ADMIN_METRICS, new AdminNodeRequest(nodeId, false),
                NodeMetricsSnapshot.class);
    }

    /** Mesma lógica de {@link #awaitLeaderOrThrow(NGridNode, Duration)}, mas via {@link #rpc} já conectado. */
    private NodeId awaitLeaderIdOrThrow() {
        long deadline = System.currentTimeMillis() + config.leaderWaitTimeout().toMillis();
        Optional<NodeId> leader = rpc.leaderId();
        while (leader.isEmpty() && System.currentTimeMillis() < deadline) {
            sleepQuietly(LEADER_POLL_INTERVAL_MS);
            leader = rpc.leaderId();
        }
        return leader.orElseThrow(() -> new NgrrdClusterException(ErrorCode.NO_LEADER,
                "nenhum líder eleito para consultar o status do cluster"));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.CLOSED, "interrompido aguardando retentativa", e);
        }
    }

    @Override
    public NodeId clientNodeId() {
        return node.transport().local().nodeId();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // B1 (achado do Refuter): UM ÚNICO deadline alimenta handles -> dispatcher -> node.close, não
        // um closeTimeout inteiro "renovado" para cada fase — do contrário N handles lentos, mais o
        // dispatcher, podiam multiplicar o tempo total de close() por várias vezes closeTimeout.
        long deadline = System.currentTimeMillis() + config.closeTimeout().toMillis();
        for (RemoteSeriesHandle handle : List.copyOf(handles.values())) {
            long remainingMs = deadline - System.currentTimeMillis();
            Duration flushBudget = remainingMs > 0 ? Duration.ofMillis(remainingMs) : Duration.ZERO;
            try {
                handle.close(flushBudget);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Falha ao fechar a série " + handle.seriesKey() + " durante o close do cliente", e);
            }
        }
        long dispatcherRemainingMs = deadline - System.currentTimeMillis();
        Duration dispatcherBudget = dispatcherRemainingMs > 0 ? Duration.ofMillis(dispatcherRemainingMs) : Duration.ZERO;
        try {
            dispatcher.close(dispatcherBudget);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao fechar o write dispatcher do cliente", e);
        }
        try {
            node.close();
        } catch (IOException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao fechar o nó cliente do cluster ngrrd", e);
        }
        if (temporaryDataDir) {
            deleteRecursivelyQuietly(dataDir);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new NgrrdClusterException(ErrorCode.CLOSED, "cliente do cluster ngrrd já foi fechado");
        }
    }

    private static void deleteRecursivelyQuietly(Path dir) {
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Falha ao apagar " + path + " do dataDir temporário do cliente", e);
                }
            });
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Falha ao apagar o dataDir temporário do cliente: " + dir, e);
        }
    }
}
