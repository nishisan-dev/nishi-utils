package dev.nishisan.utils.ngrid.testcontainers;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class NGridTestcontainersSmokeTest {
    private static final ImageFromDockerfile IMAGE = new ImageFromDockerfile("ngrid-test-tc", false)
            .withFileFromPath("Dockerfile", Path.of("ngrid-test/Dockerfile"))
            .withFileFromPath("pom.xml", Path.of("pom.xml"))
            .withFileFromPath("src", Path.of("src"))
            .withFileFromPath("ngrid-test", Path.of("ngrid-test"));
    private static final int CLIENT_COUNT = 5;
    private static final int RESTART_CYCLES = 2;

    @Test
    void shouldDeliverMessagesAcrossNodes() throws Exception {
        try (Network network = Network.newNetwork()) {
            Path configDir = Files.createTempDirectory("ngrid-tc-config");
            Path seedConfig = configDir.resolve("server-config.yml");
            Path seedData = Files.createTempDirectory("ngrid-seed-data");
            Files.writeString(seedConfig, seedConfigYaml("/data/seed"));

            GenericContainer<?>[] clients = new GenericContainer<?>[CLIENT_COUNT];
            try (GenericContainer<?> seed = createSeedContainer(network, seedConfig, seedData, "1")) {
                seed.start();

                for (int i = 0; i < CLIENT_COUNT; i++) {
                    int idx = i + 1;
                    Path clientConfig = configDir.resolve("client-" + idx + ".yml");
                    Path clientData = Files.createTempDirectory("ngrid-client-" + idx + "-data");
                    Files.writeString(clientConfig, clientConfigYaml("client-" + idx, 9000 + idx,
                            "/data/client-" + idx));
                    clients[i] = createClientContainer(network, clientConfig, clientData, "client-" + idx);
                    clients[i].start();
                }

                boolean receivedAny = waitForAnyLog(clients, "INDEX-", Duration.ofSeconds(60));
                if (!receivedAny) {
                    String message = "Clients did not receive any messages from seed."
                            + "\n--- seed logs (last 60 lines) ---\n" + tailLogs(seed, 60);
                    for (int i = 0; i < CLIENT_COUNT; i++) {
                        message += "\n--- client-" + (i + 1) + " logs (last 60 lines) ---\n"
                                + tailLogs(clients[i], 60);
                    }
                    assertTrue(false, message);
                }

                for (int i = 0; i < CLIENT_COUNT; i++) {
                    assertNoDuplicateMessages("client-" + (i + 1), clients[i].getLogs());
                }
            } finally {
                for (int i = 0; i < CLIENT_COUNT; i++) {
                    if (clients[i] != null) {
                        clients[i].close();
                    }
                }
            }
        }
    }

    @Test
    void shouldRecoverAfterSeedRestartWithoutDuplicatesOrLoss() throws Exception {
        try (Network network = Network.newNetwork()) {
            Path configDir = Files.createTempDirectory("ngrid-tc-config-restart");
            Path seedConfig = configDir.resolve("server-config.yml");
            Path seedData = Files.createTempDirectory("ngrid-seed-data-restart");
            Files.writeString(seedConfig, seedConfigYaml("/data/seed"));

            GenericContainer<?>[] clients = new GenericContainer<?>[CLIENT_COUNT];
            GenericContainer<?> currentSeed = createSeedContainer(network, seedConfig, seedData, "1");
            try {
                currentSeed.start();

                for (int i = 0; i < CLIENT_COUNT; i++) {
                    int idx = i + 1;
                    Path clientConfig = configDir.resolve("client-" + idx + ".yml");
                    Path clientData = Files.createTempDirectory("ngrid-client-" + idx + "-data-restart");
                    Files.writeString(clientConfig, clientConfigYaml("client-" + idx, 9000 + idx,
                            "/data/client-" + idx));
                    clients[i] = createClientContainer(network, clientConfig, clientData, "client-" + idx);
                    clients[i].start();
                }

                assertAllRunning(currentSeed, clients, "after startup");

                boolean beforeRestart = waitForAnyLog(clients, "INDEX-1-", Duration.ofSeconds(60));
                assertTrue(beforeRestart, "No messages received before seed restart");

                for (int cycle = 1; cycle <= RESTART_CYCLES; cycle++) {
                    assertAllRunning(currentSeed, clients, "before kill (cycle=" + cycle + ")");
                    killContainer(currentSeed);
                    currentSeed.close();
                    String epoch = String.valueOf(cycle + 1);
                    currentSeed = createSeedContainer(network, seedConfig, seedData, epoch);
                    currentSeed.start();
                    assertAllRunning(currentSeed, clients, "after restart (cycle=" + cycle + ")");
                    boolean afterRestart = waitForAnyLog(clients, "INDEX-" + epoch + "-", Duration.ofSeconds(60));
                    if (!afterRestart) {
                        String message = "No messages received after seed restart (epoch=" + epoch + ")."
                                + "\n--- seed logs (last 60 lines) ---\n" + tailLogs(currentSeed, 60);
                        for (int i = 0; i < CLIENT_COUNT; i++) {
                            message += "\n--- client-" + (i + 1) + " logs (last 60 lines) ---\n"
                                    + tailLogs(clients[i], 60);
                        }
                        assertTrue(false, message);
                    }
                }

                for (int i = 0; i < CLIENT_COUNT; i++) {
                    String logs = clients[i].getLogs();
                    String clientName = "client-" + (i + 1);
                    assertNoDuplicateMessages(clientName, logs);
                    for (int epoch = 1; epoch <= RESTART_CYCLES + 1; epoch++) {
                        assertNoMessageLoss(logs, epoch);
                    }
                }
            } finally {
                if (currentSeed != null) {
                    currentSeed.close();
                }
                for (int i = 0; i < CLIENT_COUNT; i++) {
                    if (clients[i] != null) {
                        clients[i].close();
                    }
                }
            }
        }
    }

    private boolean waitForLog(GenericContainer<?> container, String needle, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (container.getLogs().contains(needle)) {
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }

    private boolean waitForAnyLog(GenericContainer<?>[] containers, String needle, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (GenericContainer<?> container : containers) {
                if (container != null && container.isRunning() && container.getLogs().contains(needle)) {
                    return true;
                }
            }
            Thread.sleep(500);
        }
        return false;
    }

    private String tailLogs(GenericContainer<?> container, int maxLines) {
        String logs = container.getLogs();
        String[] lines = logs.split("\\R");
        int start = Math.max(0, lines.length - maxLines);
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < lines.length; i++) {
            sb.append(lines[i]).append("\n");
        }
        return sb.toString();
    }

    private void assertNoDuplicateMessages(String clientName, String logs) {
        Map<String, Integer> counts = new HashMap<>();
        Pattern pattern = Pattern.compile("INDEX-\\d+-\\d+");
        Matcher matcher = pattern.matcher(logs);
        while (matcher.find()) {
            String msg = matcher.group();
            counts.merge(msg, 1, Integer::sum);
        }
        java.util.List<String> duplicates = new java.util.ArrayList<>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() > 1) {
                duplicates.add(entry.getKey() + " x" + entry.getValue());
            }
        }
        if (!duplicates.isEmpty()) {
            String message = "Duplicate messages detected for " + clientName + ": " + duplicates;
            assertTrue(false, message);
        }
    }

    private void assertNoMessageLoss(String logs, int epoch) {
        Map<Integer, Set<Integer>> seen = parseMessages(logs);
        Set<Integer> indices = seen.get(epoch);
        if (indices == null || indices.isEmpty()) {
            return;
        }
        int max = indices.stream().mapToInt(Integer::intValue).max().orElse(-1);
        java.util.List<Integer> missing = new java.util.ArrayList<>();
        for (int i = 0; i <= max; i++) {
            if (!indices.contains(i)) {
                missing.add(i);
            }
        }
        if (!missing.isEmpty()) {
            String message = "Missing messages for epoch " + epoch + ": " + missing;
            assertTrue(false, message);
        }
    }

    private Map<Integer, Set<Integer>> parseMessages(String logs) {
        Map<Integer, Set<Integer>> result = new HashMap<>();
        Pattern pattern = Pattern.compile("INDEX-(\\d+)-(\\d+)");
        Matcher matcher = pattern.matcher(logs);
        while (matcher.find()) {
            int epoch = Integer.parseInt(matcher.group(1));
            int index = Integer.parseInt(matcher.group(2));
            result.computeIfAbsent(epoch, key -> new HashSet<>()).add(index);
        }
        return result;
    }

    private GenericContainer<?> createClientContainer(Network network, Path clientConfig, Path clientData,
            String nodeId) {
        return new GenericContainer<>(IMAGE)
                .withNetwork(network)
                .withNetworkAliases(nodeId)
                .withCopyFileToContainer(MountableFile.forHostPath(clientConfig),
                        "/app/config/client-config.yml")
                .withFileSystemBind(clientData.toString(), "/data/" + nodeId)
                .withCommand("client")
                .waitingFor(Wait.forLogMessage(".*NGrid Node iniciado com sucesso!.*", 1))
                .withStartupTimeout(Duration.ofSeconds(90));
    }

    private GenericContainer<?> createSeedContainer(Network network, Path seedConfig, Path seedData, String epoch) {
        return new GenericContainer<>(IMAGE)
                .withNetwork(network)
                .withNetworkAliases("seed")
                .withCopyFileToContainer(MountableFile.forHostPath(seedConfig),
                        "/app/config/server-config.yml")
                .withFileSystemBind(seedData.toString(), "/data/seed")
                .withEnv("NG_MESSAGE_EPOCH", epoch)
                .withCommand("server")
                .waitingFor(Wait.forLogMessage(".*NGrid Node iniciado com sucesso!.*", 1))
                .withStartupTimeout(Duration.ofSeconds(90));
    }

    private void killContainer(GenericContainer<?> container) {
        if (container == null || !container.isRunning()) {
            return;
        }
        try {
            container.getDockerClient()
                    .killContainerCmd(container.getContainerId())
                    .exec();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to kill container " + container.getContainerId(), e);
        }
    }

    private void assertAllRunning(GenericContainer<?> seed, GenericContainer<?>[] clients, String phase) {
        StringBuilder message = new StringBuilder("Some containers are not running (" + phase + ").");
        boolean ok = true;
        if (seed == null || !seed.isRunning()) {
            ok = false;
            message.append("\n- seed not running");
            if (seed != null) {
                message.append("\n--- seed logs (last 60 lines) ---\n")
                        .append(tailLogs(seed, 60));
            }
        }
        for (int i = 0; i < clients.length; i++) {
            GenericContainer<?> client = clients[i];
            if (client == null || !client.isRunning()) {
                ok = false;
                message.append("\n- client-").append(i + 1).append(" not running");
            }
        }
        if (!ok) {
            assertTrue(false, message.toString());
        }
    }

    private String seedConfigYaml(String baseDir) {
        return ("""
                node:
                  id: "seed-1"
                  host: "seed"
                  port: 9000
                  roles:
                    - "SEED"
                    - "COORDINATOR"
                  dirs:
                    base: "%s"

                autodiscover:
                  enabled: false
                  secret: "tc-secret"

                cluster:
                  name: "tc-cluster"
                  replication:
                    factor: 2
                    strict: true
                  transport:
                    workers: 4
                  seeds:
                    - "seed:9000"

                queues:
                  - name: "global-events"
                    retention:
                      policy: "TIME_BASED"
                      duration: "48h"
                    performance:
                      fsync: false
                      memory-buffer:
                        enabled: true
                        size: 1000
                      short-circuit: false

                maps:
                  - name: "_ngrid-queue-offsets"
                    persistence: "ASYNC_WITH_FSYNC"
                """).formatted(baseDir);
    }

    private String clientConfigYaml(String nodeId, int port, String baseDir) {
        return ("""
                node:
                  id: "%s"
                  host: "%s"
                  port: %d
                  dirs:
                    base: "%s"

                autodiscover:
                  enabled: false
                  secret: "tc-secret"

                cluster:
                  name: "tc-cluster"
                  replication:
                    factor: 2
                    strict: true
                  transport:
                    workers: 4
                  seeds:
                    - "seed:9000"

                queues:
                  - name: "global-events"
                    retention:
                      policy: "TIME_BASED"
                      duration: "48h"
                    performance:
                      fsync: false
                      memory-buffer:
                        enabled: true
                        size: 1000
                      short-circuit: false

                maps:
                  - name: "_ngrid-queue-offsets"
                    persistence: "ASYNC_WITH_FSYNC"
                """).formatted(nodeId, nodeId, port, baseDir);
    }
}
