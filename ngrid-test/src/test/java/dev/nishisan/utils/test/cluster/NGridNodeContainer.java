package dev.nishisan.utils.test.cluster;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Testcontainers wrapper para um nó NGrid rodando como processo Docker isolado.
 *
 * <p>
 * Cada instância representa um {@code NGridNode} em um container separado,
 * comunicando-se com os pares via rede bridge dedicada. O host do nó dentro da
 * rede é o próprio {@code nodeId}, resolvido via Docker DNS (alias de rede).
 *
 * <p>
 * O container aguarda a mensagem {@code "NGrid Node iniciado"} nos logs antes
 * de ser considerado pronto — tornando o startup assertion-driven, não
 * time-driven.
 */
public class NGridNodeContainer extends GenericContainer<NGridNodeContainer> {

    public static final int NODE_PORT = 9000;

    private final String nodeId;

    /**
     * Cria um container para um nó NGrid no modo {@code "server"}.
     *
     * @param nodeId   identificador único do nó (também usado como alias DNS na
     *                 rede)
     * @param seedHost host:porta do seed, ex.: {@code "seed-1:9000"}
     * @param network  rede Docker compartilhada pelo cluster
     */
    public NGridNodeContainer(String nodeId, String seedHost, Network network) {
        super("ngrid-test:latest");
        this.nodeId = nodeId;

        withNetwork(network);
        withNetworkAliases(nodeId); // DNS interno: seed-1, node-2, node-3
        withEnv("NG_NODE_ID", nodeId);
        withEnv("NG_NODE_HOST", nodeId); // NGrid anuncia este host aos peers
        withEnv("NG_NODE_PORT", String.valueOf(NODE_PORT));
        withEnv("NG_BASE_DIR", "/data/" + nodeId);
        withEnv("SEED_HOST", seedHost);
        withExposedPorts(NODE_PORT);
        withCommand("server");
        withStartupTimeout(Duration.ofSeconds(60));
        waitingFor(
                Wait.forLogMessage(".*NGrid Node iniciado.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));

        // Redireciona logs do container para SLF4J com prefixo do nodeId
        withLogConsumer(new Slf4jLogConsumer(
                LoggerFactory.getLogger("ngrid." + nodeId)));
    }

    /**
     * Retorna o identificador do nó.
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Retorna a porta mapeada no host para conexões externas ao cluster.
     */
    public int mappedPort() {
        return getMappedPort(NODE_PORT);
    }

    /**
     * Retorna os logs acumulados do container como string.
     */
    public String containerLogs() {
        return getLogs();
    }

    /**
     * Informa se o container logou que se tornou líder.
     */
    public boolean isLeader() {
        String logs = getLogs();
        Boolean latest = null;
        for (String line : logs.split("\\R")) {
            Boolean fromLoop = parseLeaderValue(line, "CURRENT_LEADER_STATUS:");
            if (fromLoop != null) {
                latest = fromLoop;
                continue;
            }
            Boolean fromStartup = parseLeaderValue(line, "Is Leader:");
            if (fromStartup != null) {
                latest = fromStartup;
            }
        }
        return latest != null ? latest : false;
    }

    public int latestActiveMembersCount() {
        return parseLatestInt("ACTIVE_MEMBERS_COUNT:");
    }

    public int latestReachableNodesCount() {
        return parseLatestInt("REACHABLE_NODES_COUNT:");
    }

    private Boolean parseLeaderValue(String line, String marker) {
        int idx = line.indexOf(marker);
        if (idx < 0) {
            return null;
        }
        String suffix = line.substring(idx + marker.length()).trim();
        if (suffix.startsWith("true")) {
            return true;
        }
        if (suffix.startsWith("false")) {
            return false;
        }
        return null;
    }

    private int parseLatestInt(String marker) {
        Integer latest = null;
        for (String line : getLogs().split("\\R")) {
            int idx = line.indexOf(marker);
            if (idx < 0) {
                continue;
            }
            String suffix = line.substring(idx + marker.length()).trim();
            try {
                latest = Integer.parseInt(suffix);
            } catch (NumberFormatException ignored) {
            }
        }
        return latest != null ? latest : -1;
    }
}
