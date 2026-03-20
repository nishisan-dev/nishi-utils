package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/**
 * Classe base para testes do DistributedMap com topologia de 5 nós.
 * Executa APENAS se -Dngrid.test.docker=true (baseline TDD que pode falhar).
 */
@EnabledIfSystemProperty(named = "ngrid.test.docker", matches = "true")
public abstract class AbstractNGridMapClusterIT {

    protected static Network network;
    protected static NGridMapNodeContainer seed;
    protected static NGridMapNodeContainer node2_producer;
    protected static NGridMapNodeContainer node3_reader;
    protected static NGridMapNodeContainer node4;
    protected static NGridMapNodeContainer node5_reader;

    @BeforeAll
    static void startCluster() {
        network = Network.newNetwork();
        
        seed = new NGridMapNodeContainer("seed-1", "server", "seed-1:9000", network);
        // Producer
        node2_producer = new NGridMapNodeContainer("node-2", "map-stress", "seed-1:9000", network)
                .withMapRate(50)
                .withEpoch(1);
        // Reader
        node3_reader = new NGridMapNodeContainer("node-3", "map-reader", "seed-1:9000", network);
        // Node comum
        node4 = new NGridMapNodeContainer("node-4", "server", "seed-1:9000", network);
        // Reader secundário
        node5_reader = new NGridMapNodeContainer("node-5", "map-reader", "seed-1:9000", network);

        // Inicia sequencial em vez de deepStart paralelo pesado
        seed.start();
        node2_producer.start();
        node3_reader.start();
        node4.start();
        node5_reader.start();
    }

    @AfterAll
    static void stopCluster() {
        Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                .filter(c -> c != null && c.isRunning())
                .forEach(c -> {
                    try { c.stop(); } catch (Exception ignored) {}
                });
        if (network != null) {
            try { network.close(); } catch (Exception ignored) {}
        }
    }

    protected long countLeaders() {
        return Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                .filter(c -> c != null && c.isRunning() && c.isLeader())
                .count();
    }
    
    protected NGridMapNodeContainer findLeader() {
        return Stream.of(seed, node2_producer, node3_reader, node4, node5_reader)
                .filter(c -> c != null && c.isRunning() && c.isLeader())
                .findFirst()
                .orElse(null);
    }
}
