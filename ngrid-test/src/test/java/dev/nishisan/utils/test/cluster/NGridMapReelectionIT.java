package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NGridMapReelectionIT extends AbstractNGridMapClusterIT {

    @Test
    @Order(1)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldMaintainOperationsDuringGracefulReelection() throws Exception {
        await("cluster stable")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> countLeaders() == 1);

        NGridMapNodeContainer leader = findLeader();
        assertNotNull(leader, "Deve haver um líder");
        
        Thread.sleep(3000);
        int putsBefore = node2_producer.extractMapPuts().size();
        assertTrue(putsBefore > 0, "Producer rodando");

        // GRACEFUL STOP (SIGTERM)
        leader.stop();

        // Aguarda reeleição
        await("graceful reelection")
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> countLeaders() == 1);
            
        Thread.sleep(3000);
        
        int putsAfter = node2_producer.extractMapPuts().size();
        assertTrue(putsAfter > putsBefore, "Producer retoma após graceful shutdown");
        
        // Failures count
        long failures = node2_producer.getLogs().lines()
            .filter(l -> l.contains("MAP-PUT-FAIL"))
            .count();
        // Em reeleição graceful, esperamos falhas apenas na janela de reeleição (~5s)
        assertTrue(failures < 200, "Poucas escritas podem falhar na transição, mas a maioria passa: " + failures);
        
        // EVENTUAL reads NUNCA falham mesmo que o líder caia, porque usa a réplica local
        long eventualReads = node3_reader.extractMapReads("EVENTUAL").size();
        assertTrue(eventualReads > 0);
        long eventualFails = node3_reader.getLogs().lines()
            .filter(l -> l.contains("MAP-READ-FAIL") && l.contains("EVENTUAL"))
            .count();
        assertTrue(eventualFails == 0, "EVENTUAL never fails because it reads local replica");
    }

    @Test
    @Order(2)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldPreventSplitBrainWrites() throws Exception {
        // Agora resta quorum de 4/5. 
        // Vamos isolar o líder cortando 2 followers (agora sobram 2/5 -> NO QUORUM)
        NGridMapNodeContainer newLeader = findLeader();
        assertNotNull(newLeader);
        
        Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
            .filter(c -> c.isRunning() && c != newLeader)
            .limit(2) // Corta mais 2 nós
            .forEach(c -> c.stop());
            
        // Agora temos no máximo 2 nós vivos (sem quorum de 3)
        // Lease do líder isolado vai expirar
        Thread.sleep(6000);
        
        // Gets eventuais em nodes isolados funcionam:
        int readsBeforeIso = node3_reader.extractMapReads("EVENTUAL").size();
        Thread.sleep(2000);
        
        if (node3_reader.isRunning()) {
            int readsAfterIso = node3_reader.extractMapReads("EVENTUAL").size();
            assertTrue(readsAfterIso > readsBeforeIso, "EVENTUAL funciona no minishard isolado");
        }
        
        // PUTs falham pois o lider perdeu o LEASE
        long putsIsolados = node2_producer.getLogs().lines()
            .filter(l -> l.contains("MAP-PUT-FAIL"))
            .count();
        
        assertTrue(putsIsolados > 0, "Writes isolados devem gerar exceptions (falta quorum)");
    }
}
