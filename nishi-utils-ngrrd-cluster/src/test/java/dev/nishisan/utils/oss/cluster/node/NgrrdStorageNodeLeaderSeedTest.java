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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.LeastLoadedPlacementPolicy;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupRequest;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cobre o "seed" de liderança em {@link NgrrdStorageNode#start}: um nó SEM peers se autoelege líder
 * durante {@code builder.start()}, ANTES de {@code placementHandler} ser registrado como
 * {@code LeadershipListener} logo em seguida. {@code addLeadershipListener} não dispara um callback
 * sintético para quem já registra o listener com o coordenador JÁ líder — sem o seed explícito
 * ({@code placementHandler.onLeaderChanged(self)}, feito por {@code NgrrdStorageNode#wirePlacementHandler}
 * antes de o handler atender requisições), {@code becameLeaderAtMs} ficaria em {@code 0}
 * (epoch) para sempre neste mandato, e a janela de graça pós-liderança (seção 0 do M3) nunca se
 * abriria — um miss de {@link Commands#CATALOG_LOOKUP} logo após o boot viraria {@code OK} sem a
 * chave ("não existe" definitivo) mesmo a réplica local do catálogo podendo não ter convergido ainda.
 *
 * <p>Não é {@code *ClusterTest}: um único nó sem peers ({@code minClusterSize} efetivo 1) se autoelege
 * quase instantaneamente, sem a espera de handshake/gossip entre múltiplos nós — roda na suíte
 * padrão do módulo (não fica atrás do profile {@code ngrrd-cluster}).</p>
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class NgrrdStorageNodeLeaderSeedTest {

    @Test
    void liderDesdeOBootAplicaJanelaDeGracaAoCatalogLookup(@TempDir Path tempDir) throws Exception {
        int port = allocateFreeLocalPort();
        StorageNodeConfig cfg = StorageNodeConfig.builder()
                .nodeId("storage-solo")
                .port(port)
                .dataDir(tempDir.resolve("data"))
                .volumeDir(tempDir.resolve("volume"))
                .bootDiscoveryWindow(Duration.ZERO)
                .placementGraceAfterLeadership(Duration.ofSeconds(20))
                .build();

        try (NgrrdStorageNode node = NgrrdStorageNode.start(cfg)) {
            awaitTrue("nó solo (sem peers) se autoelege líder", Duration.ofSeconds(15), node::isLeader);

            CatalogLookupResponse response = node.rpc().call(NodeId.of(node.nodeId()), Commands.CATALOG_LOOKUP,
                    new CatalogLookupRequest(List.of("serie-inexistente")), CatalogLookupResponse.class);

            // Sem o seed, becameLeaderAtMs ainda seria 0: a diferença para o relógio real de agora é
            // enorme (décadas), a janela de graça de 20s pareceria sempre expirada, e este miss viraria
            // OK sem a chave — exatamente o "não existe" indevido logo após o boot que este teste cobre.
            assertEquals(SeriesStatus.NOT_LEADER, response.status(),
                    "o miss logo após o boot deveria cair na janela de graça pós-liderança, não virar OK");
        }
    }

    @Test
    void placementHandlerEhSemeadoAntesDeAtenderRequisicoes() throws Exception {
        // O seed precisa marcar becameLeaderAtMs ANTES de o handler ficar alcançável por rede ou pelo
        // caminho local — senão um CATALOG_LOOKUP que chegue nesse intervalo vê a marca em 0, acha a
        // janela de graça expirada e responde o miss como "não existe" definitivo. E o listener de
        // liderança vem antes do seed, para nenhuma troca de líder posterior ao seed se perder.
        List<String> events = new ArrayList<>();
        try (NGridCluster cluster = NGrid.local(1).start()) {
            PlacementRequestHandler handler = new PlacementRequestHandler(cluster.node(0).transport(),
                    new SeedRecordingCatalog(events), new AlwaysLeaderView(), () -> false,
                    new LeastLoadedPlacementPolicy(), Duration.ofSeconds(10), Duration.ofSeconds(3),
                    Clock.systemUTC());

            NgrrdStorageNode.wirePlacementHandler(handler, NodeId.of("self"),
                    listener -> events.add("leadership-listener"),
                    listener -> events.add("transport-listener"),
                    localHandler -> events.add("local-handler"));
        }

        assertEquals(List.of("leadership-listener", "seed", "transport-listener", "local-handler"), events);
    }

    private static int allocateFreeLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void awaitTrue(String description, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + timeout + "): " + description);
        }
    }

    /** Líder sempre — o seed entra no ramo "virei líder" e recalcula as pendências pelo catálogo. */
    private static final class AlwaysLeaderView implements PlacementRequestHandler.LeaderView {
        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.of("self");
        }

        @Override
        public Set<String> reachableNodeIds() {
            return Set.of();
        }
    }

    /**
     * {@link CatalogView} que registra {@code "seed"} quando a recontagem de pendências do ramo "virei
     * líder" de {@code onLeaderChanged} lê o catálogo local — o sinal observável de que o seed rodou.
     */
    private static final class SeedRecordingCatalog implements CatalogView {
        private final List<String> events;

        SeedRecordingCatalog(List<String> events) {
            this.events = events;
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return Optional.empty();
        }

        @Override
        public Optional<StorageNodeStatus> nodeStatusStrong(String nodeId) {
            return Optional.empty();
        }

        @Override
        public Collection<StorageNodeStatus> nodesLocal() {
            return List.of();
        }

        @Override
        public Map<String, SeriesPlacement> placementsLocal() {
            events.add("seed");
            return Map.of();
        }

        @Override
        public void putPlacement(String seriesKey, SeriesPlacement placement) {
            throw new UnsupportedOperationException("não usado neste teste");
        }

        @Override
        public void putNodeStatus(StorageNodeStatus status) {
            throw new UnsupportedOperationException("não usado neste teste");
        }
    }
}
