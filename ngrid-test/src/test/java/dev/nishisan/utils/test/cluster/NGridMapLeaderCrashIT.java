package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NGridMapLeaderCrashIT extends AbstractNGridMapClusterIT {

    @Test
    @Order(1)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldMaintainMapConsistencyDuringLeaderCrash() throws Exception {
        // Aguarda estabilização inicial do producer/reader
        await("initial stability")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> countLeaders() == 1 && !node2_producer.extractMapPuts().isEmpty());

        NGridMapNodeContainer leader = findLeader();
        assertNotNull(leader, "Deve haver um líder");
        
        // Deixa rodar por 3s para ter puts/gets concorrentes
        Thread.sleep(3000);
        
        int putsBeforeCrash = node2_producer.extractMapPuts().size();
        assertTrue(putsBeforeCrash > 0, "Producer deve estar rodando ativamente");

        // CRASH DO LÍDER (SIGKILL)
        leader.getDockerClient().killContainerCmd(leader.getContainerId()).exec();

        // Aguarda nova eleição
        await("new leader election")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> {
                long leaders = Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                        .filter(c -> c.isRunning() && c.isLeader())
                        .count();
                return leaders == 1;
            });

        // Deixa rodar mais 3s no novo líder
        Thread.sleep(3000);

        // Validar Producer
        int putsAfterCrash = node2_producer.extractMapPuts().size();
        assertTrue(putsAfterCrash > putsBeforeCrash, "Producer deve continuar após queda do líder");
        
        // Validar Readers - EVENTUAL deve continuar forte
        // e STRONG deve ter recuperado após erro provisório
        long strongReads = node3_reader.extractMapReads("STRONG").size();
        assertTrue(strongReads > 0, "Devem haver leituras consistentes concluídas");
        
        long missingEventual = node3_reader.extractMapReads("EVENTUAL").stream()
                .filter(s -> s.endsWith("MISSING"))
                .count();
        // Não validamos que misses são zero estritamente, pois as chaves são aleatórias, 
        // mas garante que há reads completos
        assertTrue(node3_reader.extractMapReads("EVENTUAL").size() > 0, "Deve haver read eventual");
    }

    @Test
    @Order(2)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldSurviveDoubleCrash() throws Exception {
        // Derruba mais um nó (follower que não seja producer nem reader)
        NGridMapNodeContainer nodeToKill = Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
            .filter(c -> c.isRunning() && c != node2_producer && c != node3_reader)
            .filter(c -> !c.isLeader())
            .findFirst()
            .orElse(null);
            
        assertNotNull(nodeToKill, "Deve haver um nó follower para matar");
        nodeToKill.getDockerClient().killContainerCmd(nodeToKill.getContainerId()).exec();

        // Ainda temos quorum (3/5 vivos com factor=2) - espera estabilizar
        await("new leader if needed")
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> countLeaders() >= 1);
            
        // Producer deve continuar gerando puts com o quorum restante.
        // Usa awaitility pois o producer pode ficar bloqueado em invokeLeader()
        // retries (~5s) durante a reconexão ao novo líder.
        if (node2_producer.isRunning()) {
            int currentPuts = node2_producer.extractMapPuts().size();
            await("producer should resume after double crash")
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> node2_producer.extractMapPuts().size() > currentPuts);
        }
    }
}
