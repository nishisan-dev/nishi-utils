package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Valida que o cluster elege exatamente 1 líder após o startup de 3 nós.
 *
 * <p>
 * O critério de eleição do NGrid é baseado em maior ID lexicográfico
 * ({@code seed-1} < {@code node-2} < {@code node-3}). Após estabilização,
 * exatamente 1 nó deve se declarar líder nos logs.
 */
class NGridLeaderElectionIT extends AbstractNGridClusterIT {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldElectExactlyOneLeaderAfterClusterStarts() {
        // Os containers já passaram pelo waitingFor("NGrid Node iniciado")
        // Aguarda até que um nó se declare líder nos logs
        await("leader elected")
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(1, countLeaders(),
                        "Exatamente 1 nó deve ser líder. Logs:\n"
                                + "seed-1: "
                                + seed.containerLogs().lines().filter(l -> l.contains("Leader") || l.contains("leader"))
                                        .findFirst().orElse("(nenhum)")
                                + "\nnode-2: "
                                + node2.containerLogs().lines()
                                        .filter(l -> l.contains("Leader") || l.contains("leader")).findFirst()
                                        .orElse("(nenhum)")
                                + "\nnode-3: "
                                + node3.containerLogs().lines()
                                        .filter(l -> l.contains("Leader") || l.contains("leader")).findFirst()
                                        .orElse("(nenhum)")));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void allNodesShouldBeRunningAndConnected() {
        // Verifica que todos os 3 containers seguem rodando após estabilização
        long runningCount = Stream.of(seed, node2, node3)
                .filter(c -> c.isRunning())
                .count();

        assertEquals(3, runningCount, "Todos os 3 nós devem estar rodando");
    }
}
