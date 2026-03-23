# NGrid — Guia de utilização (com exemplos)

Este documento mostra como **configurar e usar** o NGrid e suas estruturas (fila e mapa distribuídos), além de exemplos com **NQueue** e utilitários.

> Recomendação: leia também `doc/ngrid/arquitetura.md` para entender os fluxos de cluster/eleição/replicação.

## Dependência (Maven)

Adicione a dependência do projeto (ajuste a versão conforme seu release):

```xml
<dependency>
  <groupId>dev.nishisan</groupId>
  <artifactId>nishi-utils</artifactId>
  <version>3.1.0</version>
</dependency>
```

## Conceitos rápidos

- **NQueue**: fila persistente local em disco (por diretório), com `offer/poll/peek`.
- **NGridNode**: "processo" de um nó do cluster (inicia transporte TCP, coordenação, replicação e expõe fila/mapa).
- **DistributedQueue / DistributedMap**: fachadas que roteiam chamadas ao **líder**.
- **Quorum**: número mínimo de confirmações para "commit" de operações replicadas.
- **Data Directory**: raiz única (`node.dirs.base`) para filas, mapas e estado de replicação.
- **Tópicos por fila/mapa**: cada estrutura replica com sequências independentes (evita bloqueios entre filas).
- **DeploymentProfile**: perfil de deploy (`DEV`, `STAGING`, `PRODUCTION`) que ativa guardrails de segurança em produção.

## 1) NQueue (fila local persistente)

### Exemplo básico

```java
import dev.nishisan.utils.queue.NQueue;

import java.nio.file.Path;
import java.util.Optional;

public class NQueueBasicExample {
  public static void main(String[] args) throws Exception {
    Path baseDir = Path.of("/tmp/queues");
    String queueName = "minha-fila";

    try (NQueue<String> queue = NQueue.open(baseDir, queueName)) {
      queue.offer("msg-1");
      queue.offer("msg-2");

      Optional<String> peek = queue.peek(); // não remove
      System.out.println("peek=" + peek.orElse("<vazio>"));

      Optional<String> polled = queue.poll(); // remove
      System.out.println("poll=" + polled.orElse("<vazio>"));
    }
  }
}
```

### Configurando opções (ex.: fsync e compactação)

```java
import dev.nishisan.utils.queue.NQueue;

import java.nio.file.Path;
import java.time.Duration;

public class NQueueOptionsExample {
  public static void main(String[] args) throws Exception {
    Path baseDir = Path.of("/tmp/queues");
    String queueName = "fila-com-opcoes";

    NQueue.Options options = NQueue.Options.defaults()
        .withFsync(false) // desempenho maior, menor durabilidade
        .withCompactionWasteThreshold(0.3)
        .withCompactionInterval(Duration.ofMinutes(10))
        .withCompactionBufferSize(256 * 1024);

    try (NQueue<String> queue = NQueue.open(baseDir, queueName, options)) {
      queue.offer("hello");
    }
  }
}
```

### Modo Stream (Log Distribuído)

Para usar a fila como um log de eventos (Stream) onde as mensagens persistem por tempo e múltiplos consumidores leem independentemente:

```java
NQueue.Options options = NQueue.Options.defaults()
    .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
    .withRetentionTime(Duration.ofHours(24)); // Retém por 24h
```

## 2) NGrid (cluster): configuração e bootstrap

O NGrid funciona com nós descritos por `NodeInfo(nodeId, host, port)` e com uma lista de **peers iniciais**. A descoberta completa tende a convergir via handshake e peer updates. Após o `start()`, você também pode adicionar peers dinamicamente com `node.join(...)`.

### Configuração do nó (NGridConfig)

Campos principais:

- `local(NodeInfo)`: identidade do nó (host/porta para bind).
- `addPeer(NodeInfo)`: peers iniciais para bootstrap (opcional).
- `replicationFactor(int)`: fator de replicação para quorum de escrita (quorum efetivo é limitado pelo tamanho do cluster ativo).
- `dataDirectory(Path)`: diretório base para filas, mapas e estado de replicação (recomendado).
- `addQueue(QueueConfig)`: adiciona uma fila distribuída (multi-queue).
- `addMap(MapConfig)`: adiciona um mapa distribuído (multi-map).
- `queueDirectory(Path)` e `queueName(String)`: modo legado de fila única (compatibilidade, **deprecated**).
- **`strictConsistency(boolean)`**: Define o modelo de consistência. `true` (padrão) prioriza consistência (CP), exigindo quorum fixo (`replicationFactor`) mesmo que nós falhem. `false` prioriza disponibilidade (AP), ajustando o quorum aos nós ativos. **Nota:** No modo estrito, o líder aguarda o quórum antes de aplicar a mudança localmente.
- **`transportWorkerThreads(int)`**: Número de threads dedicadas ao processamento de IO e conexões (padrão: 2). Aumente em clusters com muitos nós ou alta latência de handshake.
- **`deploymentProfile(DeploymentProfile)`**: Perfil de deploy que ativa guardrails de segurança (veja seção dedicada).

#### Configurações de transporte

O Builder aceita configurações adicionais que afetam a comunicação entre nós:

| Método | Default | Descrição |
|---|---|---|
| `clusterName(String)` | `"default-cluster"` | Nome lógico do cluster |
| `connectTimeout(Duration)` | 5s | Timeout para conexão TCP com peers |
| `reconnectInterval(Duration)` | 500ms | Intervalo entre tentativas de reconexão |
| `requestTimeout(Duration)` | 20s | Timeout para requisições CLIENT_REQUEST |
| `heartbeatInterval(Duration)` | 1s | Intervalo de heartbeat entre nós |
| `leaseTimeout(Duration)` | `3 × heartbeat` | Tempo sem ACK antes do líder fazer step-down |
| `rttProbeInterval(Duration)` | 2s | Intervalo de sondagem de RTT entre nós |

#### Configuração via YAML

Você também pode carregar a configuração a partir de um arquivo YAML, com suporte a variáveis de ambiente.
Para detalhes completos e exemplos, consulte: [doc/ngrid/configuracao.md](configuracao.md).

```java
// Opção 1: Carregar manualmente
NGridYamlConfig yamlConfig = NGridConfigLoader.load(Path.of("config.yaml"));
NGridConfig config = NGridConfigLoader.convertToDomain(yamlConfig);

// Opção 2: Construtor direto (recomendado)
NGridNode node = new NGridNode(Path.of("config.yaml"));
node.start();
```

### DeploymentProfile (guardrails de produção)

O `DeploymentProfile` controla validações de segurança no `build()`:

| Perfil | Comportamento |
|---|---|
| `DEV` (padrão) | Nenhum guardrail aplicado |
| `STAGING` | Nenhum guardrail aplicado |
| `PRODUCTION` | Valida invariantes de segurança |

Quando `PRODUCTION` é selecionado, o `build()` exige:
- `strictConsistency` = `true`
- `replicationFactor` ≥ 2
- Todos os mapas configurados via `addMap()` devem ter persistência habilitada (não pode ser `DISABLED`)

```java
NGridConfig cfg = NGridConfig.builder(local)
    .deploymentProfile(DeploymentProfile.PRODUCTION)
    .strictConsistency(true)
    .replicationFactor(3)
    .dataDirectory(Path.of("/var/ngrid/node-1"))
    .addQueue(QueueConfig.builder("orders").build())
    .addMap(MapConfig.builder("sessions")
        .persistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
        .build())
    .build();
```

### Proteção contra Split-Brain

O NGrid agora exige um número mínimo de membros ativos para eleger um líder. Por padrão, esse valor é 1 (o nó pode ser líder sozinho). Em produção, recomenda-se configurar o cluster para exigir a maioria dos nós para evitar que partições de rede criem múltiplos líderes. (Veja `ClusterCoordinatorConfig` para customização avançada).

> Para produção, normalmente cada nó roda em seu processo/host. Aqui é apenas uma demo local.

```java
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.QueueConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class NGridClusterExample {
  public static void main(String[] args) throws Exception {
    // Dica: em testes/ambientes compartilhados (CI), prefira portas efêmeras para evitar colisões.
    // Para simplificar a demo, aqui ainda usamos portas fixas.
    NodeInfo n1 = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 9011);
    NodeInfo n2 = new NodeInfo(NodeId.of("node-2"), "127.0.0.1", 9012);
    NodeInfo n3 = new NodeInfo(NodeId.of("node-3"), "127.0.0.1", 9013);

    Path baseDir = Files.createTempDirectory("ngrid-demo");
    Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
    Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
    Path dir3 = Files.createDirectories(baseDir.resolve("node3"));
    QueueConfig queueConfig = QueueConfig.builder("queue").build();

    try (NGridNode node1 = new NGridNode(NGridConfig.builder(n1)
            .addPeer(n2).addPeer(n3)
            .dataDirectory(dir1).addQueue(queueConfig)
            .replicationFactor(2)
            .build());
         NGridNode node2 = new NGridNode(NGridConfig.builder(n2)
            .addPeer(n1).addPeer(n3)
            .dataDirectory(dir2).addQueue(queueConfig)
            .replicationFactor(2)
            .build());
         NGridNode node3 = new NGridNode(NGridConfig.builder(n3)
            .addPeer(n1).addPeer(n2)
            .dataDirectory(dir3).addQueue(queueConfig)
            .replicationFactor(2)
            .build())) {

      node1.start();
      node2.start();
      node3.start();

      // APIs distribuídas
      DistributedQueue<String> q1 = node1.queue(String.class);
      DistributedQueue<String> q2 = node2.queue(String.class);
      DistributedQueue<String> q3 = node3.queue(String.class);

      DistributedMap<String, String> m1 = node1.map(String.class, String.class);
      DistributedMap<String, String> m3 = node3.map(String.class, String.class);

      // Por política atual: maior NodeId vence => node-3 será líder
      System.out.println("Leader=" + node1.coordinator().leaderInfo().orElseThrow());

      // Fila distribuída
      q3.offer("payload-1");
      q3.offer("payload-2");

      Optional<String> peekFollower = q1.peek(); // roteia para líder
      System.out.println("peekFollower=" + peekFollower.orElse("<vazio>"));

      // Consumo tradicional (Queue Mode - destrutivo)
      Optional<String> polledFollower = q2.poll(); // roteia para líder
      System.out.println("polledFollower=" + polledFollower.orElse("<vazio>"));
      
      // Consumo persistente (Log Mode - por consumidor)
      // O offset é salvo no cluster associado ao NodeId do chamador (neste caso n2)
      // Isso permite que n2 continue de onde parou mesmo após reconexão.
      // Requer que a NQueue esteja configurada com RetentionPolicy.TIME_BASED
      // Optional<String> streamItem = q2.poll(); 

      // Mapa distribuído
      m3.put("shared-key", "value-1");
      System.out.println("m1.get=" + m1.get("shared-key").orElse("<vazio>"));
    }
  }
}
```

### 2.1) Multiplas filas e TypedQueue

Quando `queues` esta configurado (via YAML ou `addQueue(...)`), o NGrid cria cada fila com topico e sequencia proprios. Isso evita bloqueios entre filas e permite politicas de retencao diferentes.

```java
import dev.nishisan.utils.ngrid.structures.TypedQueue;
import dev.nishisan.utils.ngrid.structures.QueueConfig;

public final class AppQueues {
  public static final TypedQueue<Order> ORDERS = TypedQueue.of("orders", Order.class);
  public static final TypedQueue<Event> EVENTS = TypedQueue.of("events", Event.class);
}

QueueConfig ordersCfg = QueueConfig.builder("orders").build();
QueueConfig eventsCfg = QueueConfig.builder("events").build();

NGridNode node = new NGridNode(NGridConfig.builder(local)
    .dataDirectory(Path.of("/var/ngrid/node-1"))
    .addQueue(ordersCfg)
    .addQueue(eventsCfg)
    .build());
node.start();

DistributedQueue<Order> orders = node.getQueue(AppQueues.ORDERS);
DistributedQueue<Event> events = node.getQueue(AppQueues.EVENTS);
```

> Dica: em clusters multi-queue, os comandos sao roteados por nome (`queue.offer:{queue}`), entao o lider sempre sabe qual fila atender.

#### Configuração avançada de QueueConfig

O `QueueConfig.Builder` suporta opções adicionais:

```java
QueueConfig cfg = QueueConfig.builder("orders")
    .retention(QueueConfig.RetentionPolicy.timeBased(Duration.ofDays(7))) // padrão
    // .retention(QueueConfig.RetentionPolicy.sizeBased(1_000_000_000L)) // 1GB
    // .retention(QueueConfig.RetentionPolicy.countBased(100_000))        // 100k items
    .maxSizeBytes(500_000_000L)  // limite de tamanho
    .maxItems(50_000)            // limite de itens
    .compressionEnabled(false)   // compressão de items
    .nqueueOptions(NQueue.Options.defaults().withFsync(true)) // opções do NQueue
    .build();
```

### 2.2) Multiplos mapas e MapConfig

Análogo às filas, o Builder aceita `.addMap(MapConfig)` para declarar mapas no config:

```java
import dev.nishisan.utils.ngrid.structures.MapConfig;
import dev.nishisan.utils.map.NMapPersistenceMode;

MapConfig usersCfg = MapConfig.builder("users")
    .persistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
    .build();

MapConfig sessionsCfg = MapConfig.builder("sessions")
    .persistenceMode(NMapPersistenceMode.ASYNC_NO_FSYNC)
    .build();

NGridConfig cfg = NGridConfig.builder(local)
    .dataDirectory(Path.of("/var/ngrid/node-1"))
    .addQueue(QueueConfig.builder("events").build())
    .addMap(usersCfg)
    .addMap(sessionsCfg)
    .replicationFactor(2)
    .build();
```

> Nota: mapas configurados via `addMap()` são eagerly registrados no `start()`, garantindo que o líder esteja pronto para servir requests desde o início.

### Adicionando peers dinamicamente (join)

```java
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.NGridNode;

public class NGridJoinExample {
  public static void main(String[] args) {
    NGridNode node = null; // suponha start() chamado
    NodeInfo seed = null;  // peer conhecido

    node.join(seed);
  }
}
```

### Autodiscovery via seed node

O NGrid suporta autodiscovery: um nó novo pode buscar automaticamente a configuração do cluster conectando-se a um seed node. Configure via YAML:

```yaml
autodiscover:
  enabled: true
  seed: "192.168.1.10:9011"
  secret: "meu-segredo-compartilhado"
```

Quando habilitado, durante o `start()`:
1. O nó conecta ao seed via socket TCP.
2. Envia um handshake seguido de `CONFIG_FETCH_REQUEST`.
3. Recebe a configuração do cluster (peers, filas, mapas).
4. Salva a configuração localmente e desativa o autodiscover para reinícios futuros.

### Ciclo de vida do nó (start/close) e boas práticas

- **`start()`**: sobe transporte TCP, coordenação (heartbeat + eleição), replicação (quorum + timeout), e por fim as fachadas `DistributedQueue`/`DistributedMap`.
- **`close()`**: encerra as fachadas e serviços, para o replicador, coordenação, scheduler e transporte (best-effort; se houver erro de IO, propaga o primeiro).
- **Boas práticas**:
  - Sempre use `try-with-resources` com `NGridNode`.
  - Evite reusar o mesmo `nodeId` em dois processos ao mesmo tempo.
  - Garanta que `dataDirectory` seja persistente se você quiser durabilidade/restart.

```mermaid
sequenceDiagram
participant App as App
participant Node as NGridNode
participant C as ClusterCoordinator
participant R as ReplicationManager

App->>Node: start()
Node->>C: start()
Node->>R: start()
Note over Node: Node pronto para aceitar chamadas via DistributedQueue/Map
App->>Node: close()
Node->>R: close()
Node->>C: close()
```

## 3) DistributedQueue (fila distribuída)

### API principal

- `offer(T value)` — insere sem key/headers
- `offer(byte[] key, T value)` — insere com routing key
- `offer(byte[] key, NQueueHeaders headers, T value)` — insere com routing key e headers customizados
- `Optional<T> peek()`
- `Optional<T> poll()`
- `Optional<T> pollWhenAvailable(Duration timeout)` (long-poll com notificação)

### Observações importantes

- Se você chamar em um **follower**, a operação será encaminhada ao **líder** via `CLIENT_REQUEST/CLIENT_RESPONSE`.
- No backend, o NGrid usa `QueueClusterService` + `ReplicationManager` e persiste em `NQueue` local (um diretório por nó).
- **Consumo Persistente**: Ao chamar `poll()`, o sistema identifica automaticamente o `NodeId` do nó que fez a requisição. Se a fila estiver configurada em modo `RetentionPolicy.TIME_BASED`, o cluster gerenciará um offset persistente para este consumidor. Isso garante que, se o nó cair e voltar, continuará lendo a partir da última mensagem não consumida por ele.
- **Subscribe/Notify (reduz storm)**: filas fazem subscribe automático no líder. Use `pollWhenAvailable(...)` para esperar notificação antes de buscar dados.

### Modos de consumo (Queue vs Log)

No NGrid moderno (configurado via `dataDirectory` + `queues`), o modo padrao e **TIME_BASED** (log/stream).
O modo **DELETE_ON_CONSUME** fica restrito ao legado (`queueDirectory`) e nao e exposto no YAML do NGrid.

- **TIME_BASED (log/stream)**: o item permanece por tempo e cada consumidor avanca seu **offset** individual.
- **DELETE_ON_CONSUME (legado)**: o item e removido no commit. Todos os consumidores avancam o mesmo ponteiro global.

Exemplo de log distribuido com dois consumidores distintos (NodeId diferentes):

```java
DistributedQueue<String> events = node.getQueue("events", String.class);

// Consumidor A (node-A)
Optional<String> a1 = events.poll();

// Consumidor B (node-B)
Optional<String> b1 = events.poll();
```

> Importante: offsets sao persistidos por `NodeId`. Use IDs estaveis e, em log mode, execute consumidores em nos distintos para isolar offsets. Se o retention expirar, o offset pode ser avancado para o item mais antigo disponivel.

### Resiliencia em filas (catch-up e reenvio)

Quando um follower fica atrasado, o NGrid combina **reenvio de sequencias** com **snapshot em chunks**:

- **Gap curto**: o follower solicita ao lider o reenvio das sequencias faltantes e aplica na ordem correta.
- **Gap grande**: se o atraso ultrapassar o limiar de sincronizacao (por padrao ~500 operacoes), o follower inicia **catch-up por snapshot**, em chunks (ex.: 1000 itens por chunk).

Isso garante que um no que ficou offline consiga recuperar rapidamente o estado da fila, mesmo que muitos eventos tenham ocorrido.

```java
// Exemplo conceitual: follower atrasado entra no cluster
NGridNode follower = new NGridNode(NGridConfig.builder(infoF)
    .addPeer(infoL)
    .dataDirectory(dirF)
    .addQueue(QueueConfig.builder("orders").build())
    .replicationFactor(1)
    .build());
follower.start();

// O follower detecta lag e dispara sincronizacao automatica
```

## 4) DistributedMap (mapa distribuído)

### API principal

- `Optional<V> put(K key, V value)`
- `Optional<V> get(K key)` — consistência `STRONG` (padrão, roteia ao líder)
- `Optional<V> get(K key, Consistency consistency)` — com nível de consistência configurável
- `Optional<V> remove(K key)`
- `Set<K> keySet()` — leitura local (eventual consistency)
- `boolean containsKey(K key)` — leitura local (eventual consistency)
- `int size()` — contagem local (eventual consistency)
- `boolean isEmpty()` — verifica se vazio (local)
- `void putAll(Map<K, V> entries)` — insere múltiplos entries (cada put é replicado individualmente)
- `void removeByPrefix(String prefix)` — remove entries por prefixo (operação local, sem replicação)

### Níveis de consistência para leitura

O `get()` aceita um segundo argumento `Consistency` para controlar o trade-off entre latência e frescor dos dados:

```java
import dev.nishisan.utils.ngrid.structures.Consistency;

DistributedMap<String, String> map = node.getMap("users", String.class, String.class);

// STRONG (padrão): sempre roteia ao líder — dado mais fresco, maior latência
Optional<String> strong = map.get("user-1"); // equivale a get("user-1", Consistency.STRONG)

// EVENTUAL: lê da réplica local — menor latência, pode estar defasado
Optional<String> eventual = map.get("user-1", Consistency.EVENTUAL);

// BOUNDED: lê da réplica local se o lag de replicação for aceitável
Optional<String> bounded = map.get("user-1", Consistency.bounded(5)); // maxLag=5 sequências
```

| Nível | Roteia ao líder? | Garantia |
|---|---|---|
| `STRONG` | Sempre | Dado mais recente |
| `EVENTUAL` | Nunca | Dado potencialmente defasado |
| `BOUNDED` | Se lag > maxLag | Frescor limitado pelo maxLag |

### Múltiplos mapas (nomeados)

- `node.map(K.class, V.class)` continua existindo e retorna o **mapa padrão**, cujo nome é `config.mapName()` (padrão: `default-map`).
- Para criar/usar **vários mapas independentes** no mesmo cluster, use `node.getMap("map-name", K.class, V.class)`:

```java
DistributedMap<String, String> users = node1.getMap("users", String.class, String.class);
DistributedMap<String, String> sessions = node2.getMap("sessions", String.class, String.class);

users.put("u1", "alice");
sessions.put("s1", "token-123");
```

> Nota importante: como cada mapa registra handlers de replicação por nome, **crie o mesmo mapa em todos os nós** (chamando `getMap(...)`) antes de começar a escrever, para evitar que followers recebam replicações de um mapa que ainda não foi inicializado localmente.

### Observações importantes

- No estado atual, o mapa é mantido em memória (`ConcurrentHashMap`) e replicado via `ReplicationManager`.
- `get()` com `Consistency.STRONG` (padrão) é roteado ao líder na fachada (`DistributedMap`), mantendo um modelo simples de consistência.
- Operações de escrita (`put`/`remove`) verificam se o líder possui lease válido antes de aceitar.

### Persistência local do mapa (opcional)

Você pode habilitar persistência em disco para o mapa **por nó** (cada nó escolhe).

Modos:

- `DISABLED` (padrão)
- `ASYNC_NO_FSYNC`
- `ASYNC_WITH_FSYNC`

Arquivos por mapa:

```
{mapDirectory}/{mapName}/
├── wal.log
├── snapshot.dat
└── map.meta
```

Exemplo de configuração (API legada — mapa único):

```java
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.QueueConfig;

import java.nio.file.Path;

public class NGridMapPersistenceExample {
  public static void main(String[] args) {
    NodeInfo local = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 9011);

    NGridConfig cfg = NGridConfig.builder(local)
        // bootstrap do cluster (exemplo)
        // .addPeer(...)
        .replicationFactor(2)
        // fila distribuída (obrigatório hoje)
        .dataDirectory(Path.of("/tmp/ngrid/node-1"))
        .addQueue(QueueConfig.builder("queue").build())
        // mapa: persistência local (opcional)
        .mapDirectory(Path.of("/tmp/ngrid/node-1/maps"))
        .mapName("default-map") // nome do mapa padrão (usado por node.map(...))
        .mapPersistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
        .build();

    try (NGridNode node = new NGridNode(cfg)) {
      node.start();
      // node.map(String.class, String.class)...
      // node.getMap("users", String.class, String.class)...
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
```

Exemplo de configuração (API nova — multi-map via `MapConfig`):

```java
import dev.nishisan.utils.ngrid.structures.MapConfig;
import dev.nishisan.utils.map.NMapPersistenceMode;

NGridConfig cfg = NGridConfig.builder(local)
    .dataDirectory(Path.of("/var/ngrid/node-1"))
    .addQueue(QueueConfig.builder("events").build())
    .addMap(MapConfig.builder("users")
        .persistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
        .build())
    .addMap(MapConfig.builder("cache")
        .persistenceMode(NMapPersistenceMode.DISABLED) // OK para DEV
        .build())
    .replicationFactor(2)
    .build();
```

Notas:

- A persistência é **local** e não altera o modelo de consistência do cluster.
- `ASYNC_WITH_FSYNC` prioriza durabilidade em crash; `ASYNC_NO_FSYNC` prioriza performance.

## 5) Leader Reelection (baseada em RTT)

O NGrid suporta reeleição de líder baseada em latência de rede (RTT). Quando habilitada, o cluster pode sugerir a migração da liderança para um nó com melhor RTT.

```java
NGridConfig cfg = NGridConfig.builder(local)
    .leaderReelectionEnabled(true)
    .leaderReelectionInterval(Duration.ofSeconds(5))       // intervalo de avaliação
    .leaderReelectionCooldown(Duration.ofSeconds(60))      // cooldown entre reeleições
    .leaderReelectionMinDelta(20.0)                         // delta mínimo de RTT (ms)
    // ... demais configs
    .build();
```

| Parâmetro | Default | Descrição |
|---|---|---|
| `leaderReelectionEnabled` | `false` | Habilita/desabilita a reeleição |
| `leaderReelectionInterval` | 5s | Frequência de avaliação de RTT |
| `leaderReelectionCooldown` | 60s | Tempo mínimo entre reeleições |
| `leaderReelectionSuggestionTtl` | 30s | TTL de uma sugestão de reeleição |
| `leaderReelectionMinDelta` | 20.0 | Delta mínimo de RTT (ms) para disparar sugestão |

## 6) Métricas e Observabilidade

O NGrid expõe métricas e alertas para monitoramento operacional.

### Snapshot operacional

```java
NGridOperationalSnapshot snapshot = node.operationalSnapshot();

System.out.println("Leader: " + snapshot.leaderId());
System.out.println("Is Leader: " + snapshot.isLeader());
System.out.println("Active Members: " + snapshot.activeMembersCount());
System.out.println("Replication Lag: " + snapshot.lag());
System.out.println("Pending Ops: " + snapshot.pendingOps());
System.out.println("Reachable: " + snapshot.reachable() + "/" + snapshot.totalNodes());
```

### Alert Engine

O `NGridAlertEngine` monitora condições operacionais e dispara alertas automaticamente:

```java
node.addAlertListener(alert -> {
  System.out.println("[ALERT] " + alert);
});
```

### Dashboard Reporter

Quando o `dataDirectory` está configurado, o NGrid grava automaticamente um arquivo `dashboard.yaml` com o estado operacional a cada 10 segundos.

## 7) Utilitários

### 7.1) LeaderElectionUtils (somente cluster + eleição)

Quando você quer apenas "descoberta + eleição de líder" (sem usar `NGridNode` completo), use `LeaderElectionUtils`.

```java
import dev.nishisan.utils.ngrid.LeaderElectionListener;
import dev.nishisan.utils.ngrid.LeaderElectionUtils;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransportConfig;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LeaderElectionOnlyExample {
  public static void main(String[] args) {
    NodeInfo local = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 9021);

    TcpTransport transport = new TcpTransport(
        TcpTransportConfig.builder(local)
            .connectTimeout(Duration.ofSeconds(5))
            .reconnectInterval(Duration.ofSeconds(2))
            .build()
    );
    transport.start();

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    LeaderElectionUtils.LeaderElectionService svc =
        LeaderElectionUtils.create(transport, ClusterCoordinatorConfig.defaults(), scheduler);

    svc.addLeaderElectionListener(new LeaderElectionListener() {
      @Override
      public void onLeadershipChanged(boolean isLeader, NodeId leaderId) {
        System.out.println("isLeader=" + isLeader + " leaderId=" + leaderId);
      }
    });

    svc.start();
  }
}
```

> Nota: o `LeaderElectionService#close()` fecha internamente o `ClusterCoordinator`, que encerra o scheduler (veja o javadoc do utilitário). Use um scheduler dedicado.

### 7.1.1) Eventos de liderança (diferença entre `LeadershipListener` e `LeaderElectionListener`)

- **`LeadershipListener`**: notificado quando o líder observado muda (mesmo que o nó local não ganhe/perca liderança).
- **`LeaderElectionListener`**: notificado quando o nó local ganha/perde liderança.

```mermaid
sequenceDiagram
participant C as ClusterCoordinator
participant LL as LeadershipListener
participant EL as LeaderElectionListener

C->>C: recomputeLeader()
alt leader mudou
  C-->>LL: onLeaderChanged(newLeader)
  alt local ganhou/perdeu liderança
    C-->>EL: onLeadershipChanged(isLeader, newLeader)
  end
end
```

### 7.2) StatsUtils (métricas simples)

O `NQueue` já expõe contadores internos por `StatsUtils` (ex.: `queue.getStats()`), mas você também pode usar `StatsUtils` diretamente:

```java
import dev.nishisan.utils.stats.StatsUtils;

public class StatsUtilsExample {
  public static void main(String[] args) {
    StatsUtils stats = new StatsUtils();
    stats.notifyHitCounter("requests.total");
    stats.notifyAverageCounter("latency.ms", 10L);
    stats.notifyAverageCounter("latency.ms", 20L);

    System.out.println("requests.total=" + stats.getCounterValue("requests.total"));
    System.out.println("latency.avg=" + stats.getAverage("latency.ms"));
  }
}
```

## Boas práticas e troubleshooting

### Portas e bind

- Cada nó precisa de uma porta TCP única (host/port em `NodeInfo`) para bind no `TcpTransport`.
- Em testes automatizados, prefira portas efêmeras (exemplo: `NGridIntegrationTest`).

### Quorum

- Ajuste `replicationFactor` para equilibrar disponibilidade e consistência.
- Se o cluster tiver menos membros ativos do que o quorum configurado, o NGrid reduz para um quorum efetivo viável (se `strictConsistency=false`) ou falha a operação (se `strictConsistency=true`, que é o padrão).

### Persistência (fila)

- Se você quer mais durabilidade, mantenha `NQueue.Options.withFsync(true)` (default).
- Para alta taxa de escrita em dev/benchmark, `withFsync(false)` pode ser aceitável (com risco de perda no crash).
