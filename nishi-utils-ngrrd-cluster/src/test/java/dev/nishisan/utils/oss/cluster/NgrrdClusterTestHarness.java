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

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.node.StorageNodeConfig;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Harness reutilizável para os testes de cluster real (profile
 * {@code ngrrd-cluster}) deste módulo: sobe N {@link NgrrdStorageNode} em
 * malha completa via loopback com portas pré-alocadas, e conecta clientes
 * transparentes ({@link NgrrdClusterClient}) a eles. Pensado para sobreviver
 * aos marcos seguintes (M2/M3): {@link #addStorageNode} e
 * {@link #restartStorageNode} cobrem os cenários de rebalanceamento e reinício
 * de nó com o mesmo {@code nodeId}/volume.
 */
public final class NgrrdClusterTestHarness implements Closeable {

    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_STATUS_REPORT_INTERVAL = Duration.ofSeconds(2);
    /**
     * Checagens seguidas exigidas antes de considerar a malha estável — mesmo valor de
     * {@code StorageNodeClusterTest} (M1b). Tentativa de aumentar bastante esse número (20, ~3 s)
     * para tentar absorver de vez o churn de bootstrap do M0 (epoch 1→N, "dual-leader" ocasional —
     * ver checkpoint do M1b) teve o efeito <strong>oposto</strong> na prática: em vez de reduzir o
     * risco, aumentou a chance de a espera cair bem no meio de um live-lock ainda em curso e nunca
     * completar as {@value #STABLE_CHECKS_REQUIRED} checagens seguidas, travando o teste por minutos
     * (observado empiricamente). Mantido curto de propósito; a robustez contra o churn residual fica
     * por conta da retentativa em {@code DistributedWriteReadClusterTest} e do relaxamento da
     * asserção de distribuição (mesmo tratamento dado pelo M1b), não de esperar mais aqui.
     */
    private static final int STABLE_CHECKS_REQUIRED = 5;

    private final Path base;
    private final List<StorageNodeConfig> configs;
    private final List<NgrrdStorageNode> storageNodes;
    private final List<NgrrdClusterClient> clients = new CopyOnWriteArrayList<>();

    private NgrrdClusterTestHarness(Path base, List<StorageNodeConfig> configs, List<NgrrdStorageNode> storageNodes) {
        this.base = base;
        this.configs = new ArrayList<>(configs);
        this.storageNodes = new ArrayList<>(storageNodes);
    }

    /**
     * Sobe {@code storageNodeCount} storage nodes em malha completa
     * (peers cruzados), cada um sob {@code base/storage-<i>/{data,volume}}.
     * {@code customize} é aplicado a cada {@link StorageNodeConfig.Builder}
     * depois de nodeId/porta/peers/diretórios já preenchidos — pode
     * sobrescrever qualquer um deles.
     */
    public static NgrrdClusterTestHarness start(Path base, int storageNodeCount,
            Consumer<StorageNodeConfig.Builder> customize) throws IOException {
        Objects.requireNonNull(base, "base");
        if (storageNodeCount <= 0) {
            throw new IllegalArgumentException("storageNodeCount deve ser > 0: " + storageNodeCount);
        }
        int[] ports = allocateFreePorts(storageNodeCount);
        List<String> addresses = new ArrayList<>(storageNodeCount);
        for (int port : ports) {
            addresses.add("127.0.0.1:" + port);
        }

        List<StorageNodeConfig> configs = new ArrayList<>(storageNodeCount);
        List<NgrrdStorageNode> nodes = new ArrayList<>(storageNodeCount);
        for (int i = 0; i < storageNodeCount; i++) {
            StorageNodeConfig.Builder builder = StorageNodeConfig.builder()
                    .nodeId("storage-" + i)
                    .port(ports[i])
                    .peers(peersExcept(addresses, i))
                    .dataDir(base.resolve("storage-" + i + "/data"))
                    .volumeDir(base.resolve("storage-" + i + "/volume"))
                    .statusReportInterval(DEFAULT_STATUS_REPORT_INTERVAL);
            customize.accept(builder);
            StorageNodeConfig config = builder.build();
            configs.add(config);
            nodes.add(NgrrdStorageNode.start(config));
        }
        return new NgrrdClusterTestHarness(base, configs, nodes);
    }

    private static String[] peersExcept(List<String> addresses, int index) {
        List<String> peers = new ArrayList<>(addresses);
        peers.remove(index);
        return peers.toArray(new String[0]);
    }

    /**
     * Conecta um cliente transparente à malha de storage nodes deste harness.
     * {@code customize} é aplicado depois de {@code peers}/{@code dataDir} já
     * preenchidos.
     */
    public NgrrdClusterClient connectClient(Consumer<NgrrdClusterConfig.Builder> customize) {
        NgrrdClusterConfig.Builder builder = NgrrdClusterConfig.builder()
                .peers(storagePeerAddresses())
                .dataDir(base.resolve("client-" + clients.size() + "/data"));
        customize.accept(builder);
        NgrrdClusterClient client = NgrrdCluster.connect(builder.build());
        clients.add(client);
        // Entrar (ou sair) um membro pode reabrir a eleição por um instante (mesmo "churn" de
        // bootstrap já observado com storage nodes puros — ver checkpoint do M1b): espera a malha
        // assentar num único líder visto por todos os storage nodes antes de devolver o cliente — do
        // contrário um PLACE imediato pode esgotar as retentativas de NOT_LEADER do PlacementResolver
        // em pleno reshuffle. Não fixa a contagem total de membros esperada (um cliente anterior pode
        // ter sido fechado fora do harness, via `client.close()` direto) — exige só que os storage
        // nodes concordem entre si (mesmo líder, mesma contagem de membros ativos).
        awaitMeshStable();
        return client;
    }

    private String[] storagePeerAddresses() {
        List<String> addresses = new ArrayList<>(storageNodes.size());
        for (NgrrdStorageNode node : storageNodes) {
            addresses.add(node.config().host() + ":" + node.config().port());
        }
        return addresses.toArray(new String[0]);
    }

    /** Espera até que algum storage node enxergue um líder eleito. */
    public void awaitLeader() {
        awaitTrue("líder eleito entre os storage nodes", () ->
                storageNodes.stream().anyMatch(node -> node.node().coordinator().leaderInfo().isPresent()));
    }

    /** Espera até que o catálogo local do primeiro storage node reporte {@code n} nós. */
    public void awaitNodeStatuses(int n) {
        CatalogService catalog = storageNodes.get(0).catalog();
        awaitTrue(n + " storage node(s) reportados no catálogo", () -> catalog.nodesLocal().size() == n);
    }

    /** Espera até que o catálogo local do primeiro storage node tenha {@code n} placements {@code ACTIVE}. */
    public void awaitPlacements(int n) {
        CatalogService catalog = storageNodes.get(0).catalog();
        awaitTrue(n + " placement(s) ACTIVE no catálogo", () -> {
            Map<String, SeriesPlacement> placements = catalog.placementsLocal();
            return placements.size() == n
                    && placements.values().stream().allMatch(p -> p.state() == PlacementState.ACTIVE);
        });
    }

    /** O storage node que atualmente se considera líder. */
    public NgrrdStorageNode leaderNode() {
        return storageNodes.stream()
                .filter(NgrrdStorageNode::isLeader)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("nenhum storage node deste harness é líder agora"));
    }

    /** Snapshot imutável dos storage nodes atuais deste harness. */
    public List<NgrrdStorageNode> nodes() {
        return List.copyOf(storageNodes);
    }

    /**
     * Sobe um novo storage node, com peers apontando para todos os já
     * existentes (e vice-versa na próxima vez que eles reconectarem via
     * gossip). Preparação para o rebalanceamento do M3.
     */
    public NgrrdStorageNode addStorageNode(Consumer<StorageNodeConfig.Builder> customize) throws IOException {
        int port = allocateFreePorts(1)[0];
        int index = storageNodes.size();
        StorageNodeConfig.Builder builder = StorageNodeConfig.builder()
                .nodeId("storage-" + index)
                .port(port)
                .peers(storagePeerAddresses())
                .dataDir(base.resolve("storage-" + index + "/data"))
                .volumeDir(base.resolve("storage-" + index + "/volume"))
                .statusReportInterval(DEFAULT_STATUS_REPORT_INTERVAL);
        customize.accept(builder);
        StorageNodeConfig config = builder.build();
        NgrrdStorageNode node = NgrrdStorageNode.start(config);
        configs.add(config);
        storageNodes.add(node);
        return node;
    }

    /**
     * Fecha o storage node em {@code index} e sobe um novo processo com
     * exatamente a mesma {@link StorageNodeConfig} (mesmo {@code nodeId},
     * {@code dataDir} e {@code volumeDir}) — simula reinício/manutenção sem
     * perder identidade nem dados.
     */
    public NgrrdStorageNode restartStorageNode(int index) throws IOException {
        storageNodes.get(index).close();
        NgrrdStorageNode restarted = NgrrdStorageNode.start(configs.get(index));
        storageNodes.set(index, restarted);
        return restarted;
    }

    /**
     * Espera a malha convergir de verdade do ponto de vista dos storage nodes: o mesmo líder visto por
     * todos eles e a mesma contagem de membros ativos entre si, por {@value #STABLE_CHECKS_REQUIRED}
     * checagens seguidas — não apenas uma vez (mesmo critério de {@code StorageNodeClusterTest.awaitClusterStable},
     * indisponível aqui por viver noutro pacote de teste). De propósito, não exige um total exato de
     * membros: um cliente conectado por este harness pode ter sido fechado diretamente pelo chamador
     * (sem passar por {@link #close()}), então o único invariante verificável de fora é o consenso
     * entre os storage nodes, não um número fixo.
     */
    private void awaitMeshStable() {
        long deadline = System.currentTimeMillis() + DEFAULT_AWAIT_TIMEOUT.toMillis();
        Optional<NodeInfo> stableLeader = Optional.empty();
        int stableChecks = 0;
        while (System.currentTimeMillis() < deadline) {
            Optional<NodeInfo> leader = consensusLeader();
            if (leader.isPresent() && leader.equals(stableLeader)) {
                stableChecks++;
            } else if (leader.isPresent()) {
                stableLeader = leader;
                stableChecks = 1;
            } else {
                stableLeader = Optional.empty();
                stableChecks = 0;
            }
            if (stableChecks >= STABLE_CHECKS_REQUIRED) {
                return;
            }
            try {
                Thread.sleep(150L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrompido aguardando a malha estabilizar");
            }
        }
        fail("malha não estabilizou dentro de " + DEFAULT_AWAIT_TIMEOUT);
    }

    private Optional<NodeInfo> consensusLeader() {
        Optional<NodeInfo> firstLeader = storageNodes.get(0).node().coordinator().leaderInfo();
        if (firstLeader.isEmpty()) {
            return Optional.empty();
        }
        int firstMemberCount = storageNodes.get(0).node().coordinator().activeMembers().size();
        for (NgrrdStorageNode node : storageNodes) {
            if (!firstLeader.equals(node.node().coordinator().leaderInfo())) {
                return Optional.empty();
            }
            if (node.node().coordinator().activeMembers().size() != firstMemberCount) {
                return Optional.empty();
            }
        }
        return firstLeader;
    }

    private void awaitTrue(String description, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + DEFAULT_AWAIT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrompido aguardando: " + description);
            }
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + DEFAULT_AWAIT_TIMEOUT + "): " + description);
        }
    }

    private static int[] allocateFreePorts(int count) throws IOException {
        int[] ports = new int[count];
        List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                socket.setReuseAddress(true);
                sockets.add(socket);
                ports[i] = socket.getLocalPort();
            }
        } finally {
            for (ServerSocket socket : sockets) {
                socket.close();
            }
        }
        return ports;
    }

    @Override
    public void close() {
        for (NgrrdClusterClient client : clients) {
            client.close();
        }
        for (NgrrdStorageNode node : storageNodes) {
            node.close();
        }
    }
}
