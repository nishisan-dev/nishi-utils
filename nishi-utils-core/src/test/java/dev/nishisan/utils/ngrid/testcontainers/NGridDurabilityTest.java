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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Testes de durabilidade para validar que offsets e mensagens são
 * corretamente persistidos e recuperados após crash abrupto (SIGKILL).
 * 
 * <p>
 * Estes testes complementam {@link NGridTestcontainersSmokeTest} que
 * valida corretude funcional (sem duplicatas, sem perdas), focando em
 * validar explicitamente a durabilidade de estado persistido.
 * </p>
 */
@EnabledIfSystemProperty(named = "ngrid.test.docker", matches = "true")
class NGridDurabilityTest {
  private static final ImageFromDockerfile IMAGE = new ImageFromDockerfile("ngrid-test-durability", false)
      .withFileFromPath("Dockerfile", Path.of("ngrid-test/Dockerfile"))
      .withFileFromPath("pom.xml", Path.of("pom.xml"))
      .withFileFromPath("src", Path.of("src"))
      .withFileFromPath("ngrid-test", Path.of("ngrid-test"));

  /**
   * Teste 1: Valida que após crash abrupto (SIGKILL), o cliente não recebe
   * mensagens duplicadas ao reiniciar, provando que o offset foi durabilizado.
   * 
   * <p>
   * Cenário:
   * <ol>
   * <li>Seed produz mensagens continuamente</li>
   * <li>Client consome mensagens e persiste offset</li>
   * <li>Client é morto abruptamente (SIGKILL via docker kill)</li>
   * <li>Client reinicia com mesmo volume de dados</li>
   * <li>Validar: novas mensagens recebidas, sem duplicatas das anteriores</li>
   * </ol>
   */
  @Test
  void shouldRecoverOffsetAfterCrashAndNotReceiveDuplicates() throws Exception {
    try (Network network = Network.newNetwork()) {
      Path configDir = Files.createTempDirectory("ngrid-durability-offset");
      Path seedConfig = configDir.resolve("server-config.yml");
      Path seedData = Files.createTempDirectory("ngrid-durability-seed");
      Path clientData = Files.createTempDirectory("ngrid-durability-client");

      Files.writeString(seedConfig, seedConfigYaml("/data/seed"));
      Path clientConfig = configDir.resolve("client-config.yml");
      Files.writeString(clientConfig, clientConfigYaml("client-1", 9001, "/data/client-1"));

      GenericContainer<?> currentSeed = createSeedContainer(network, seedConfig, seedData, "1");
      GenericContainer<?> client = null;

      try {
        currentSeed.start();

        client = createClientContainer(network, clientConfig, clientData, "client-1");
        client.start();

        // Aguardar cliente receber algumas mensagens
        boolean receivedInitial = waitForLog(client, "INDEX-1-5", Duration.ofSeconds(60));
        assertTrue(receivedInitial, "Client should receive at least 5 messages before crash");

        // Capturar mensagens recebidas antes do crash
        Set<String> messagesBeforeCrash = extractMessages(client.getLogs());
        int countBeforeCrash = messagesBeforeCrash.size();
        assertTrue(countBeforeCrash >= 5, "Should have received at least 5 messages, got: " + countBeforeCrash);

        // SIGKILL no cliente (crash abrupto)
        killContainer(client);
        client.close();

        // Reiniciar cliente com MESMO volume de dados
        client = createClientContainer(network, clientConfig, clientData, "client-1");
        client.start();

        // Aguardar novas mensagens após restart
        boolean receivedAfterRestart = waitForLogPattern(client,
            Pattern.compile("INDEX-1-(\\d+)"),
            idx -> idx > countBeforeCrash + 2,
            Duration.ofSeconds(60));
        assertTrue(receivedAfterRestart, "Client should receive new messages after restart");

        // Validar que não houve duplicatas
        String logsAfterRestart = client.getLogs();
        Set<String> messagesAfterRestart = extractMessages(logsAfterRestart);

        // Verificar interseção - mensagens anteriores não devem aparecer novamente
        Set<String> duplicates = new HashSet<>(messagesBeforeCrash);
        duplicates.retainAll(messagesAfterRestart);

        assertTrue(duplicates.isEmpty(),
            "Should not receive duplicate messages after restart. Duplicates found: " + duplicates);

      } finally {
        if (client != null)
          client.close();
        currentSeed.close();
      }
    }
  }

  /**
   * Teste 2: Valida durabilidade após crash do seed.
   * 
   * <p>
   * Cenário:
   * <ol>
   * <li>Seed produz mensagens</li>
   * <li>SIGKILL no seed (crash abrupto)</li>
   * <li>Restart seed com mesmo volume</li>
   * <li>Client valida que recebe mensagens após restart</li>
   * </ol>
   */
  @Test
  void shouldRecoverMessagesAfterSeedCrash() throws Exception {
    try (Network network = Network.newNetwork()) {
      Path configDir = Files.createTempDirectory("ngrid-durability-seed-crash");
      Path seedConfig = configDir.resolve("server-config.yml");
      Path seedData = Files.createTempDirectory("ngrid-durability-seed-crash-seed");
      Path clientData = Files.createTempDirectory("ngrid-durability-seed-crash-client");

      // Usar config padrão (mesma dos outros testes que funcionam)
      Files.writeString(seedConfig, seedConfigYaml("/data/seed"));
      Path clientConfig = configDir.resolve("client-config.yml");
      Files.writeString(clientConfig, clientConfigYaml("client-1", 9001, "/data/client-1"));

      GenericContainer<?> client = null;
      GenericContainer<?> currentSeed = createSeedContainer(network, seedConfig, seedData, "1");

      try {
        currentSeed.start();

        // Iniciar cliente para consumir mensagens do epoch 1
        client = createClientContainer(network, clientConfig, clientData, "client-1");
        client.start();

        // Aguardar cliente receber algumas mensagens do epoch 1
        boolean receivedEpoch1 = waitForLog(client, "INDEX-1-3", Duration.ofSeconds(60));
        assertTrue(receivedEpoch1, "Client should receive messages from epoch 1");

        Set<String> messagesBeforeCrash = extractMessages(client.getLogs());
        int countBeforeCrash = messagesBeforeCrash.size();

        // SIGKILL no seed (crash abrupto)
        killContainer(currentSeed);
        currentSeed.close();

        // Reiniciar seed com mesmo volume (epoch 2 para diferenciar)
        currentSeed = createSeedContainer(network, seedConfig, seedData, "2");
        currentSeed.start();

        // Aguardar cliente receber mensagens do epoch 2 (novas após restart)
        boolean receivedEpoch2 = waitForLog(client, "INDEX-2-", Duration.ofSeconds(60));
        assertTrue(receivedEpoch2,
            "Client should receive messages from epoch 2 after seed restart. " +
                "Messages before crash: " + countBeforeCrash);

      } finally {
        if (client != null)
          client.close();
        if (currentSeed != null)
          currentSeed.close();
      }
    }
  }

  /**
   * Teste 3: Valida que múltiplos clientes mantêm offsets independentes após
   * crash.
   * 
   * <p>
   * Cenário:
   * <ol>
   * <li>Seed produz mensagens</li>
   * <li>Client-1 consome 10 mensagens, Client-2 consome 5 mensagens</li>
   * <li>Ambos são mortos abruptamente</li>
   * <li>Ambos reiniciam</li>
   * <li>Validar: cada um continua do seu próprio offset</li>
   * </ol>
   */
  @Test
  void shouldMaintainIndependentOffsetsAfterMultiClientCrash() throws Exception {
    try (Network network = Network.newNetwork()) {
      Path configDir = Files.createTempDirectory("ngrid-durability-multi");
      Path seedConfig = configDir.resolve("server-config.yml");
      Path seedData = Files.createTempDirectory("ngrid-durability-multi-seed");
      Path client1Data = Files.createTempDirectory("ngrid-durability-multi-client1");
      Path client2Data = Files.createTempDirectory("ngrid-durability-multi-client2");

      Files.writeString(seedConfig, seedConfigYaml("/data/seed"));
      Path client1Config = configDir.resolve("client-1.yml");
      Path client2Config = configDir.resolve("client-2.yml");
      Files.writeString(client1Config, clientConfigYaml("client-1", 9001, "/data/client-1"));
      Files.writeString(client2Config, clientConfigYaml("client-2", 9002, "/data/client-2"));

      GenericContainer<?> currentSeed = createSeedContainer(network, seedConfig, seedData, "1");
      GenericContainer<?> client1 = null;
      GenericContainer<?> client2 = null;

      try {
        currentSeed.start();

        client1 = createClientContainer(network, client1Config, client1Data, "client-1");
        client2 = createClientContainer(network, client2Config, client2Data, "client-2");
        client1.start();
        client2.start();

        // Aguardar ambos receberem mensagens (com offsets potencialmente diferentes)
        boolean client1Received = waitForLog(client1, "INDEX-1-5", Duration.ofSeconds(60));
        boolean client2Received = waitForLog(client2, "INDEX-1-3", Duration.ofSeconds(60));
        assertTrue(client1Received && client2Received, "Both clients should receive messages");

        Set<String> client1MsgsBefore = extractMessages(client1.getLogs());
        Set<String> client2MsgsBefore = extractMessages(client2.getLogs());

        // SIGKILL em ambos
        killContainer(client1);
        killContainer(client2);
        client1.close();
        client2.close();

        // Reiniciar ambos com mesmos volumes
        client1 = createClientContainer(network, client1Config, client1Data, "client-1");
        client2 = createClientContainer(network, client2Config, client2Data, "client-2");
        client1.start();
        client2.start();

        // Aguardar novas mensagens
        Thread.sleep(5000);

        // Validar: sem duplicatas em nenhum dos clientes
        Set<String> client1MsgsAfter = extractMessages(client1.getLogs());
        Set<String> client2MsgsAfter = extractMessages(client2.getLogs());

        Set<String> client1Dups = new HashSet<>(client1MsgsBefore);
        client1Dups.retainAll(client1MsgsAfter);

        Set<String> client2Dups = new HashSet<>(client2MsgsBefore);
        client2Dups.retainAll(client2MsgsAfter);

        assertTrue(client1Dups.isEmpty(),
            "Client-1 should not receive duplicates. Found: " + client1Dups);
        assertTrue(client2Dups.isEmpty(),
            "Client-2 should not receive duplicates. Found: " + client2Dups);

      } finally {
        if (client1 != null)
          client1.close();
        if (client2 != null)
          client2.close();
        currentSeed.close();
      }
    }
  }

  // ==================== Métodos Utilitários ====================

  private boolean waitForLog(GenericContainer<?> container, String needle, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      if (container.getLogs().contains(needle)) {
        return true;
      }
      Thread.sleep(500);
    }
    return false;
  }

  private boolean waitForLogPattern(GenericContainer<?> container, Pattern pattern,
      java.util.function.IntPredicate condition, Duration timeout) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      Matcher matcher = pattern.matcher(container.getLogs());
      while (matcher.find()) {
        int index = Integer.parseInt(matcher.group(1));
        if (condition.test(index)) {
          return true;
        }
      }
      Thread.sleep(500);
    }
    return false;
  }

  private Set<String> extractMessages(String logs) {
    Set<String> messages = new HashSet<>();
    Pattern pattern = Pattern.compile("INDEX-\\d+-\\d+");
    Matcher matcher = pattern.matcher(logs);
    while (matcher.find()) {
      messages.add(matcher.group());
    }
    return messages;
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

  private GenericContainer<?> createClientContainer(Network network, Path clientConfig,
      Path clientData, String nodeId) {
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

  private GenericContainer<?> createSeedContainer(Network network, Path seedConfig,
      Path seedData, String epoch) {
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
          name: "durability-cluster"
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

  private String seedConfigYamlWithFsync(String baseDir) {
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
          name: "durability-fsync-cluster"
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
              fsync: true
              memory-buffer:
                enabled: false
                size: 1
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
          name: "durability-cluster"
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

  private String clientConfigYamlWithFsync(String nodeId, int port, String baseDir) {
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
          name: "durability-fsync-cluster"
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
              fsync: true
              memory-buffer:
                enabled: false
                size: 1
              short-circuit: false

        maps:
          - name: "_ngrid-queue-offsets"
            persistence: "ASYNC_WITH_FSYNC"
        """).formatted(nodeId, nodeId, port, baseDir);
  }
}
