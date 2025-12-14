# nishi-utils

Coleção de utilitários em Java, com foco em:

- **NQueue**: fila **persistente** (FIFO) baseada em arquivos, segura para múltiplas threads.
- **NGrid**: infraestrutura **distribuída** via TCP com **fila** e **mapa** (replicação por líder + quorum).
- **Stats**: utilitários para **métricas/estatísticas** simples (contadores, médias, valores, memória).

## Funcionalidades

### NQueue (fila persistente)

- **Persistência em disco** (reconstrução do estado após restart)
- **Concorrência** (múltiplos produtores/consumidores)
- Operações: `offer`, `poll`, `peek`
- **Compactação** automática/parametrizável do arquivo de dados

### NGrid (fila/mapa distribuídos)

- Cluster via **TCP** com descoberta/gossip de peers
- **Eleição determinística de líder** (por ID)
- **Replicação com quorum** configurável
- API cliente transparente: qualquer nó pode encaminhar a operação ao líder
- Estruturas:
  - `DistributedQueue`: `offer`, `poll`, `peek`
  - `DistributedMap`: `put`, `get`, `remove`

## Requisitos

- Java **21**
- Maven

## Instalação (Maven)

Versão do artefato no projeto: **1.0.14** (`pom.xml`).

```xml
<dependency>
  <groupId>dev.nishisan</groupId>
  <artifactId>nishi-utils</artifactId>
  <version>1.0.14</version>
</dependency>
```

## Uso: NQueue

### Exemplo básico

```java
import dev.nishisan.utils.queue.NQueue;

import java.nio.file.Path;
import java.util.Optional;

public class Example {
  public static void main(String[] args) throws Exception {
    Path baseDir = Path.of("/tmp/queues");

    try (NQueue<String> queue = NQueue.open(baseDir, "minha-fila")) {
      queue.offer("primeira");
      queue.offer("segunda");

      Optional<String> next = queue.peek();
      System.out.println("peek=" + next.orElse("<vazio>"));

      Optional<String> msg = queue.poll();
      System.out.println("poll=" + msg.orElse("<vazio>"));
    }
  }
}
```

### Configurações avançadas

```java
import dev.nishisan.utils.queue.NQueue;

import java.nio.file.Path;
import java.time.Duration;

public class ExampleOptions {
  public static void main(String[] args) throws Exception {
    Path baseDir = Path.of("/tmp/queues");

    NQueue.Options options = NQueue.Options.defaults()
        .withFsync(false)
        .withCompactionWasteThreshold(0.30)
        .withCompactionInterval(Duration.ofMinutes(10))
        .withCompactionBufferSize(256 * 1024);

    try (NQueue<String> queue = NQueue.open(baseDir, "minha-fila", options)) {
      queue.offer("hello");
    }
  }
}
```

### Exemplo produtor/consumidor (threads)

Veja também: `src/main/java/dev/nishisan/utils/queue/NQueueExample.java`.

```java
import dev.nishisan.utils.queue.NQueue;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerConsumer {
  public static void main(String[] args) throws Exception {
    Path baseDir = Path.of("/tmp");
    try (NQueue<String> queue = NQueue.open(baseDir, "demo")) {
      ExecutorService ex = Executors.newFixedThreadPool(2);

      ex.submit(() -> {
        try {
          for (int i = 0; i < 1000; i++) {
            queue.offer("message-" + i);
          }
        } catch (Exception ignored) {
        }
      });

      ex.submit(() -> {
        try {
          // espera itens aparecerem
          Thread.sleep(200);
          while (!queue.isEmpty()) {
            queue.poll();
          }
        } catch (Exception ignored) {
        }
      });

      ex.shutdown();
      ex.awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
```

## Uso: NGrid

### Criando um cluster (exemplo em um único processo)

O exemplo abaixo segue a mesma ideia do teste de integração:
`src/test/java/dev/nishisan/utils/ngrid/NGridIntegrationTest.java`.

```java
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class NGridClusterExample {
  public static void main(String[] args) throws Exception {
    NodeInfo n1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 9001);
    NodeInfo n2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", 9002);
    NodeInfo n3 = new NodeInfo(NodeId.of("node-3"), "127.0.0.1", 9003);

    Path base = Files.createTempDirectory("ngrid-demo");
    Path d1 = Files.createDirectories(base.resolve("node1"));
    Path d2 = Files.createDirectories(base.resolve("node2"));
    Path d3 = Files.createDirectories(base.resolve("node3"));

    try (NGridNode node1 = new NGridNode(NGridConfig.builder(n1)
        .addPeer(n2).addPeer(n3)
        .queueDirectory(d1).queueName("queue")
        .replicationQuorum(2)
        .build());
         NGridNode node2 = new NGridNode(NGridConfig.builder(n2)
             .addPeer(n1).addPeer(n3)
             .queueDirectory(d2).queueName("queue")
             .replicationQuorum(2)
             .build());
         NGridNode node3 = new NGridNode(NGridConfig.builder(n3)
             .addPeer(n1).addPeer(n2)
             .queueDirectory(d3).queueName("queue")
             .replicationQuorum(2)
             .build())) {

      node1.start();
      node2.start();
      node3.start();

      DistributedQueue<String> queue = node1.queue(String.class);
      queue.offer("job-1");
      Optional<String> job = queue.poll();
      System.out.println("job=" + job.orElse("<vazio>"));

      DistributedMap<String, String> map = node2.map(String.class, String.class);
      map.put("k1", "v1");
      System.out.println("get(k1)=" + map.get("k1").orElse("<vazio>"));

      // Múltiplos mapas (nomeados) no mesmo cluster:
      DistributedMap<String, String> users = node1.getMap("users", String.class, String.class);
      DistributedMap<String, String> sessions = node3.getMap("sessions", String.class, String.class);
      users.put("u1", "alice");
      sessions.put("s1", "token-123");
    }
  }
}
```

### Observações/limitações atuais (MVP)

- Operações são **roteadas ao líder** para consistência.
- O mapa é mantido em memória (e replicado); snapshot/catch-up completo ainda é uma evolução natural.
- O `replicationQuorum` define quantos nós (incluindo o líder) precisam confirmar para a operação ser considerada commitada.

## Arquitetura (resumo)

- **Transporte/Cluster**: TCP, troca de mensagens, descoberta de peers
- **Coordenação**: membros, heartbeats e eleição de líder
- **Replicação**: operação com ID, replicação e ACK até atingir quorum
- **Estruturas**: `DistributedQueue` e `DistributedMap`

```mermaid
flowchart TD
  subgraph nishiUtils [nishi-utils]
    subgraph nqueue [NQueue]
      NQueueAPI[NQueue<T>]
      NQueueFiles["Arquivos: data.log + queue.meta"]
      NQueueAPI --> NQueueFiles
    end

    subgraph ngrid [NGrid]
      NGridNode[NGridNode]
      Transport[TcpTransport]
      Coordinator[ClusterCoordinator]
      Replication[ReplicationManager]
      DQueue[DistributedQueue]
      DMap[DistributedMap]
      QSvc[QueueClusterService]
      MSvc[MapClusterService]
      MPersist[MapPersistence\n(WAL + Snapshot)]

      NGridNode --> Transport
      NGridNode --> Coordinator
      NGridNode --> Replication
      NGridNode --> DQueue
      NGridNode --> DMap

      DQueue --> QSvc
      QSvc --> NQueueAPI

      DMap --> MSvc
      MSvc --> MPersist
    end

    subgraph stats [Stats]
      StatsUtils[StatsUtils]
    end
  end
```

Para detalhes (docs em pt-BR):

- `doc/ngrid/arquitetura.md`
- `doc/ngrid/guia-utilizacao.md`
- `doc/ngrid/map-design.md`
- `doc/ngrid/nqueue-integration.md`
- `doc/nqueue-readme.md`

## Stats (métricas)

O pacote `dev.nishisan.utils.stats` inclui a classe `StatsUtils`, que mantém:

- contadores de hits (`notifyHitCounter`)
- médias com janela fixa (`notifyAverageCounter`)
- valores atuais (`notifyCurrentValue`)
- cálculo periódico de memória/counters (thread daemon interna)

Veja: `src/main/java/dev/nishisan/utils/stats/StatsUtils.java`.

## Rodando testes

```bash
mvn test
```

## Licença

Este projeto é distribuído sob **GNU GPL v3** (ou posterior). Veja os cabeçalhos dos arquivos-fonte para detalhes.



