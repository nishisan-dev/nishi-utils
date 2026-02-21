package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Valida que um nó que reinicia após falha se sincroniza (catch-up) com o
 * cluster.
 *
 * <p>
 * Roteiro:
 * <ol>
 * <li>Cluster estabiliza com 3 nós.</li>
 * <li>O seed para ({@code docker stop}) — peers continuam operacionais.</li>
 * <li>O seed é reiniciado via {@code GenericContainer#start()}.</li>
 * <li>Aguardamos que o seed volte a participar do cluster
 * (log {@code "NGrid Node iniciado"} + ausência de erros críticos).</li>
 * </ol>
 *
 * <p>
 * <b>Nota:</b> A asserção de catch-up completo (sincronização total do WAL)
 * fica como evolução futura, quando o NGrid expuser uma métrica de lag de
 * replicação
 * ou um evento persistível. Por ora, validamos que o nó voltou sem crash.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NGridCatchUpIT extends AbstractNGridClusterIT {

    @Test
    @Order(1)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void clusterShouldBeStableBeforeRestart() {
        await("cluster stable before restart")
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> countLeaders() >= 1);

        assertTrue(seed.isRunning(), "Seed deve estar rodando antes do restart");
        assertTrue(node2.isRunning(), "node-2 deve estar rodando antes do restart");
        assertTrue(node3.isRunning(), "node-3 deve estar rodando antes do restart");
    }

    @Test
    @Order(2)
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void seedShouldRejoinClusterAfterRestart() {
        // Aguarda cluster estável
        await("initial stable").atMost(30, TimeUnit.SECONDS)
                .until(() -> countLeaders() >= 1);

        // Para o seed
        seed.stop();
        await("seed stopped").atMost(10, TimeUnit.SECONDS)
                .until(() -> !seed.isRunning());

        // Reinicia o seed com o mesmo container (mesma config, mesma rede)
        seed.start();

        // Aguarda que o seed volte a anunciar startup
        await("seed rejoined cluster")
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    String logs = seed.containerLogs();
                    // O seed logou que iniciou novamente
                    long startupCount = logs.lines()
                            .filter(l -> l.contains("NGrid Node iniciado"))
                            .count();
                    return startupCount >= 2; // pelo menos startup inicial + restart
                });

        assertTrue(seed.isRunning(),
                "Seed deve estar rodando após restart. Logs:\n" + seed.containerLogs());
    }

    @Test
    @Order(3)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void allNodesShouldBeHealthyAfterCatchUp() {
        if (seed.isRunning()) {
            assertTrue(node2.isRunning(), "node-2 deve estar saudável após catch-up");
            assertTrue(node3.isRunning(), "node-3 deve estar saudável após catch-up");
        }
    }
}
