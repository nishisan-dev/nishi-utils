package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Valida que o cluster elege um novo líder quando o seed falha.
 *
 * <p>
 * O teste para o container {@code seed-1} via {@code docker stop} (SIGTERM
 * real,
 * não apenas fechamento de socket), simulando um crash de processo isolado.
 * Em seguida aguarda que um dos dois nós restantes assuma a liderança.
 *
 * <p>
 * <b>Ordem dos testes:</b> necessária porque o failover destrói {@code seed}
 * — os testes subsequentes rodam com apenas 2 nós ativos.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NGridLeaderFailoverIT extends AbstractNGridClusterIT {

    @Test
    @Order(1)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldHaveLeaderBeforeFailover() {
        await("initial leader elected")
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> countLeaders() == 1);

        assertEquals(1, countLeaders(),
                "Deve haver exatamente 1 líder antes do failover");
    }

    @Test
    @Order(2)
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void shouldElectNewLeaderAfterSeedDies() {
        // Garante estabilização inicial
        await("initial cluster stable")
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> countLeaders() >= 1);

        // Para o seed — simula crash real (docker stop → SIGTERM no processo)
        seed.stop();

        // Aguarda que um dos peers sobreviventes assuma a liderança
        await("new leader elected after seed death")
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    long leaders = Stream.of(node2, node3)
                            .filter(c -> c.isRunning() && c.isLeader())
                            .count();
                    assertTrue(leaders >= 1,
                            "Pelo menos 1 nó sobrevivente deve ser líder após o seed cair."
                                    + "\nnode-2 líder: " + node2.isLeader()
                                    + "\nnode-3 líder: " + node3.isLeader());
                });
    }

    @Test
    @Order(3)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void survivingNodesShouldStillBeRunning() {
        if (!seed.isRunning()) {
            assertTrue(node2.isRunning(), "node-2 deve estar rodando");
            assertTrue(node3.isRunning(), "node-3 deve estar rodando");
        }
    }
}
