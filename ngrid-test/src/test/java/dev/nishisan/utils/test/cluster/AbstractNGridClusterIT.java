package dev.nishisan.utils.test.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/**
 * Classe base abstrata para todos os testes de cluster NGrid com Docker.
 *
 * <p>
 * Sobe um cluster de 3 nós ({@code seed-1}, {@code node-2}, {@code node-3})
 * em paralelo usando uma rede bridge dedicada. O {@code seed-1} é o ponto de
 * entrada inicial (seed); os demais se conectam a ele para bootstrapping.
 *
 * <p>
 * A subclasse herda o cluster já estabilizado no {@code @BeforeAll}. O
 * teardown é automático via {@code @AfterAll}.
 *
 * <h2>Modelo de rede</h2>
 * 
 * <pre>
 * [seed-1:9000] ← DNS alias "seed-1" na rede bridge
 *     ↑
 * [node-2:9000] ← DNS alias "node-2"
 * [node-3:9000] ← DNS alias "node-3"
 * </pre>
 *
 * <p>
 * Os containers se comunicam entre si via DNS interno do Docker.
 * A porta 9000 de cada container é mapeada em porta aleatória no host,
 * acessível via {@link NGridNodeContainer#mappedPort()}.
 */
public abstract class AbstractNGridClusterIT {

    protected static Network network;
    protected static NGridNodeContainer seed;
    protected static NGridNodeContainer node2;
    protected static NGridNodeContainer node3;

    @BeforeAll
    static void startCluster() {
        network = Network.newNetwork();
        seed = new NGridNodeContainer("seed-1", "seed-1:9000", network);
        node2 = new NGridNodeContainer("node-2", "seed-1:9000", network);
        node3 = new NGridNodeContainer("node-3", "seed-1:9000", network);

        // Inicia os 3 containers em paralelo para reduzir tempo de setup
        Startables.deepStart(seed, node2, node3).join();
    }

    @AfterAll
    static void stopCluster() {
        Stream.of(seed, node2, node3)
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
     * Conta quantos nós (entre seed, node2, node3) se declararam líder nos logs.
     */
    protected long countLeaders() {
        return Stream.of(seed, node2, node3)
                .filter(NGridNodeContainer::isLeader)
                .count();
    }
}
