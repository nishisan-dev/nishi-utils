package dev.nishisan.utils.test.cluster;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class NGridMapNodeContainer extends GenericContainer<NGridMapNodeContainer> {
    public static final int NODE_PORT = 9000;
    private final String nodeId;

    public NGridMapNodeContainer(String nodeId, String role, String seedHost, Network network) {
        super("ngrid-test:latest");
        this.nodeId = nodeId;

        withNetwork(network);
        withNetworkAliases(nodeId);
        withEnv("NG_NODE_ID", nodeId);
        withEnv("NG_NODE_HOST", nodeId);
        withEnv("NG_NODE_PORT", String.valueOf(NODE_PORT));
        withEnv("NG_BASE_DIR", "/data/" + nodeId);
        withEnv("SEED_HOST", seedHost);
        withExposedPorts(NODE_PORT);
        
        withCommand(role);
        
        withStartupTimeout(Duration.ofSeconds(60));
        waitingFor(Wait.forLogMessage(".*NGrid (Node|Map-Stress|Map-Reader) iniciado.*", 1)
                .withStartupTimeout(Duration.ofSeconds(60)));

        withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("ngrid." + nodeId)));
    }

    public NGridMapNodeContainer withMapRate(int ratePerSec) {
        return withEnv("MAP_RATE_PER_SEC", String.valueOf(ratePerSec));
    }

    public NGridMapNodeContainer withEpoch(int epoch) {
        return withEnv("NG_MESSAGE_EPOCH", String.valueOf(epoch));
    }

    public String nodeId() {
        return nodeId;
    }

    public int mappedPort() {
        return getMappedPort(NODE_PORT);
    }

    public String containerLogs() {
        return getLogs();
    }

    public boolean isLeader() {
        String logs = getLogs();
        Boolean latest = null;
        for (String line : logs.split("\\R")) {
            int idx = line.indexOf("CURRENT_LEADER_STATUS:");
            if (idx >= 0) {
                latest = line.substring(idx + 22).trim().startsWith("true");
                continue;
            }
            idx = line.indexOf("Is Leader:");
            if (idx >= 0) {
                latest = line.substring(idx + 10).trim().startsWith("true");
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

    public List<String> extractMapPuts() {
        List<String> puts = new ArrayList<>();
        for (String line : getLogs().split("\\R")) {
            int idx = line.indexOf("MAP-PUT:");
            if (idx >= 0) {
                puts.add(line.substring(idx + 8).trim());
            }
        }
        return puts;
    }

    public List<String> extractMapReads(String consistency) {
        List<String> reads = new ArrayList<>();
        String marker = "MAP-READ-" + consistency + ":";
        for (String line : getLogs().split("\\R")) {
            int idx = line.indexOf(marker);
            if (idx >= 0) {
                reads.add(line.substring(idx + marker.length()).trim());
            }
        }
        return reads;
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
