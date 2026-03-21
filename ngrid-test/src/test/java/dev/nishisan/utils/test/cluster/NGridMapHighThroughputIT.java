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
class NGridMapHighThroughputIT extends AbstractNGridMapClusterIT {

    @Test
    @Order(1)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldAchieveSustainedThroughputAndConsistency() throws Exception {
        await("cluster stable")
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> countLeaders() == 1);

        Thread.sleep(10000); // 10s de throughput sustentado (50 puts/s * 10s = ~500 puts)
        
        int puts = node2_producer.extractMapPuts().size();
        assertTrue(puts > 200, "Deve manter vazão sustentada > 20 puts/sec. Puts: " + puts);
        
        long totalKeys = node3_reader.getLogs().lines()
            .filter(l -> l.contains("MAP-KEYSET:count="))
            .map(l -> l.substring(l.indexOf("count=") + 6))
            .mapToLong(Long::parseLong)
            .max().orElse(0);
            
        assertTrue(totalKeys >= puts * 0.9, 
                "Reader deve enxergar as escritas (converge). Count=" + totalKeys + ", Puts=" + puts);
    }
    
    @Test
    @Order(2)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldCatchupAfterNodeCrashAndRecovery() throws Exception {
        NGridMapNodeContainer toRestart = node5_reader; // Node reader extra
        assertTrue(toRestart.isRunning());
        
        toRestart.stop();
        Thread.sleep(3000); // Producer roda 3s enquanto o nó 5 está caido
        
        // Reinicia
        toRestart.start();
        
        // Espera estabilizar
        await("catch up")
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> {
                long totalKeysNode5 = toRestart.getLogs().lines()
                    .filter(l -> l.contains("MAP-KEYSET:count="))
                    .map(l -> l.substring(l.indexOf("count=") + 6))
                    .mapToLong(Long::parseLong)
                    .max().orElse(0);
                    
                long totalPuts = node2_producer.extractMapPuts().size();
                
                return totalKeysNode5 > 0 && totalKeysNode5 >= totalPuts - 100;
            });
            
        assertTrue(true, "Node recuperou dados passados (snapshot ou logs)");
    }
}
