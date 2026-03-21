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
        // Com factor=2 e totalExpected=5, o lease renova se activeCount > 5/2 = 2.
        // Precisamos ter no máximo 2 nós ativos para que o lease expire.
        // Paramos TODOS os nós exceto producer e reader (incluindo o líder).
        Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
            .filter(c -> c.isRunning() && c != node2_producer && c != node3_reader)
            .forEach(c -> c.stop());
            
        // Com 2/5 nós ativos: activeCount(2) > totalExpected/2(2) é FALSE.
        // O novo líder (se eleito entre producer/reader) não consegue renovar
        // o lease, o lease expira, e o líder faz stepDown.
        // Writes falham com "No leader available" ou "Leader lease expired".
        
        // Espera que MAP-PUT-FAIL apareça (indica que o líder perdeu quorum)
        if (node2_producer.isRunning()) {
            await("writes should fail without quorum")
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> node2_producer.getLogs().lines()
                    .filter(l -> l.contains("MAP-PUT-FAIL"))
                    .count() > 0);
        }
        
        // EVENTUAL reads continuam porque leem da réplica local.
        // O reader alterna STRONG/EVENTUAL e pode ficar bloqueado em STRONG
        // retries (~5s cada), então usamos awaitility com window generosa.
        if (node3_reader.isRunning()) {
            int readsBeforeIso = node3_reader.extractMapReads("EVENTUAL").size();
            await("EVENTUAL reads should continue locally")
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> node3_reader.extractMapReads("EVENTUAL").size() > readsBeforeIso);
        }
    }
}
