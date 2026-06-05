package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste de cenário/regressão da issue #117 em rede real (Docker): três nós em
 * <b>full-mesh</b> iniciados <b>concorrentemente</b> devem formar uma malha
 * <b>direta</b> completa (cada nó conectado diretamente aos outros dois,
 * {@code DIRECT_PEERS_COUNT == 2}) e, ao perder um nó, eleger um novo líder.
 *
 * <p>
 * <b>Escopo da evidência.</b> Este IT valida o comportamento <i>corrigido</i>
 * no ambiente real (full-mesh + start concorrente + netem + failover). Ele
 * <b>não</b> é um witness RED→GREEN: a corrida original do <i>simultaneous
 * open</i> não é reproduzível sob demanda — numa rede funcional (mesmo com
 * perda, pois o TCP retransmite o FIN) o código não corrigido se recupera via
 * reconexão. O estado "preso em proxy" reportado exigiu uma falha de caminho
 * real, não simulável por netem num único host. A correção é garantida pelo
 * invariante determinístico de {@code registerLiveConnection} (coberto também
 * por {@code TcpTransportConcurrentMeshTest}); este IT guarda contra regressões
 * de formação de malha e failover.
 *
 * <p>
 * A latência {@code netem} (env {@code NG_IT_NETEM_DELAY}, padrão {@code 60ms})
 * estressa o caminho de rede; requer {@code NET_ADMIN} e, na ausência, o
 * entrypoint degrada graciosamente (sem latência) e o teste segue válido.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NGridConcurrentMeshFailoverIT {

    private static final String NETEM_DELAY =
            System.getenv().getOrDefault("NG_IT_NETEM_DELAY", "60ms");

    private static Network network;
    private static NGridMeshNodeContainer nodeA;
    private static NGridMeshNodeContainer nodeB;
    private static NGridMeshNodeContainer nodeC;

    @BeforeAll
    static void startCluster() {
        network = Network.newNetwork();
        // Full-mesh: cada nó conhece os outros dois (host:porta na rede bridge).
        nodeA = new NGridMeshNodeContainer("node-a", "node-b:9000", "node-c:9000", NETEM_DELAY, network);
        nodeB = new NGridMeshNodeContainer("node-b", "node-a:9000", "node-c:9000", NETEM_DELAY, network);
        nodeC = new NGridMeshNodeContainer("node-c", "node-a:9000", "node-b:9000", NETEM_DELAY, network);

        // Start concorrente — condição necessária para o simultaneous-open.
        Startables.deepStart(nodeA, nodeB, nodeC).join();
    }

    @AfterAll
    static void stopCluster() {
        Stream.of(nodeA, nodeB, nodeC)
                .filter(c -> c != null && c.isRunning())
                .forEach(c -> {
                    try {
                        c.stop();
                    } catch (Exception ignored) {
                    }
                });
        if (network != null) {
            try {
                network.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Cerne da issue: a malha direta total deve se formar apesar do start
     * concorrente. Cada nó deve enxergar exatamente 2 peers DIRETOS (não via
     * proxy) e o cluster deve ter um líder.
     */
    @Test
    @Order(1)
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void concurrentlyStartedNodesFormFullDirectMesh() {
        await("malha direta total (DIRECT_PEERS_COUNT==2 em todos) + líder")
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertFullDirectMesh());
    }

    /**
     * Failover: ao matar o nó iniciado primeiro (potencial hub), os dois
     * sobreviventes — que precisam estar diretamente conectados entre si —
     * devem eleger um novo líder.
     */
    @Test
    @Order(2)
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void survivorsReelectAfterFirstNodeKilled() {
        // Garante o estado convergido (malha direta completa) antes do failover —
        // mesmo sinal robusto verificado no teste 1.
        await("cluster convergido (malha direta completa)")
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertFullDirectMesh());

        // Mata node-a (crash real via docker stop / SIGTERM).
        nodeA.stop();

        await("re-eleição entre os sobreviventes")
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    long leaders = Stream.of(nodeB, nodeC)
                            .filter(NGridNodeContainer::isRunning)
                            .filter(NGridNodeContainer::isLeader)
                            .count();
                    assertTrue(leaders >= 1,
                            "Ao menos um sobrevivente deve ser líder após a queda do node-a.\n"
                                    + survivorStateDump());
                    assertTrue(Stream.of(nodeB, nodeC)
                                    .filter(NGridNodeContainer::isRunning)
                                    .allMatch(c -> c.latestActiveMembersCount() >= 2),
                            "Os sobreviventes devem convergir para 2 membros ativos.\n"
                                    + survivorStateDump());
                });
    }

    private void assertFullDirectMesh() {
        assertTrue(countLeaders() == 1,
                "Deve haver exatamente 1 líder.\n" + clusterStateDump());
        Stream.of(nodeA, nodeB, nodeC).forEach(c ->
                assertTrue(c.latestDirectPeersCount() == 2,
                        c.nodeId() + " deveria ter 2 peers DIRETOS (malha completa); "
                                + "valor < 2 indica par proxy-only (bug da issue #117).\n"
                                + clusterStateDump()));
    }

    private long countLeaders() {
        return Stream.of(nodeA, nodeB, nodeC)
                .filter(c -> c != null && c.isRunning())
                .filter(NGridNodeContainer::isLeader)
                .count();
    }

    private static String clusterStateDump() {
        return Stream.of(nodeA, nodeB, nodeC).map(NGridConcurrentMeshFailoverIT::nodeState)
                .reduce("", (a, b) -> a + b);
    }

    private static String survivorStateDump() {
        return Stream.of(nodeB, nodeC).map(NGridConcurrentMeshFailoverIT::nodeState)
                .reduce("", (a, b) -> a + b);
    }

    private static String nodeState(NGridNodeContainer c) {
        if (c == null) {
            return "";
        }
        return String.format("  %s running=%s leader=%s direct=%d active=%d reachable=%d%n",
                c.nodeId(), c.isRunning(),
                c.isRunning() && c.isLeader(),
                safe(c::latestDirectPeersCount), safe(c::latestActiveMembersCount),
                safe(c::latestReachableNodesCount));
    }

    private static int safe(java.util.function.IntSupplier s) {
        try {
            return s.getAsInt();
        } catch (Exception e) {
            return -1;
        }
    }
}
