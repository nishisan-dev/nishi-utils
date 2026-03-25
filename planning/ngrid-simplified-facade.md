# NGrid Simplified Facade API

Criar uma camada facade (`NGrid`) que encapsula a verbosidade de `NGridConfig.builder()` + `NGridNode`, oferecendo uma API fluente e concisa para criar clusters. A API atual permanece intacta — coexistência total.

## Proposta de API

### Modo Local (dev/testes)

```java
// Cluster efêmero com 2 nós — zero configuração de rede
try (NGridCluster cluster = NGrid.local(2)
        .queue("orders")
        .map("users")
        .start()) {

    DistributedQueue<String> q = cluster.queue("orders", String.class);
    DistributedMap<String, String> m = cluster.map("users", String.class, String.class);

    q.offer("pedido-1");
    m.put("u1", "Alice");
}
```

### Modo Node (produção)

```java
// Nó individual com seed discovery
try (NGridNode node = NGrid.node("192.168.1.10", 9011)
        .seed("192.168.1.11:9011")
        .queue("orders")
        .map("users")
        .dataDir(Path.of("/var/ngrid/data"))
        .replication(2)
        .start()) {

    node.getQueue("orders", String.class).offer("pedido-1");
}

// Nó individual com peers explícitos
try (NGridNode node = NGrid.node("192.168.1.10", 9011)
        .peers("192.168.1.11:9011", "192.168.1.12:9011")
        .queue("orders")
        .replication(2)
        .start()) {
    // ...
}

// Porta auto-alocada
try (NGridNode node = NGrid.node("192.168.1.10")
        .seed("192.168.1.11:9011")
        .queue("orders")
        .start()) {
    // ...
}
```

---

## User Review Required

> [!IMPORTANT]
> **`NGridCluster` vs `NGridNode`**: No modo `NGrid.local(n)`, o retorno é um `NGridCluster` que gerencia N nós internamente. No modo `NGrid.node(ip)`, o retorno é um `NGridNode` padrão (já existente). Essa distinção é intencional — `NGridCluster` é um conceito novo, apenas para dev/testes onde múltiplos nós coexistem no mesmo processo.

> [!IMPORTANT]
> **`NodeId` automático**: No modo local, os IDs serão `local-1`, `local-2`, etc. No modo node, o ID será derivado de `host:port` (ex: `192.168.1.10:9011`). O usuário pode sobrescrever com `.id("custom-id")`.

---

## Proposed Changes

### Facade Entry Point

#### [NEW] [NGrid.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGrid.java)

Classe estática com dois entry points:

- `NGrid.local(int nodeCount)` → retorna `NGridLocalBuilder`
- `NGrid.node(String host)` / `NGrid.node(String host, int port)` → retorna `NGridNodeBuilder`

---

### Builders

#### [NEW] [NGridLocalBuilder.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridLocalBuilder.java)

Builder para o modo dev/test. Fluent API:

| Método | Descrição |
|---|---|
| `.queue(String name)` | Adiciona fila ao cluster |
| `.map(String name)` | Adiciona mapa ao cluster |
| `.replication(int factor)` | Fator de replicação (default: `nodeCount`) |
| `.dataDir(Path dir)` | Diretório base (default: temp) |
| `.strictConsistency(boolean)` | Modo CP vs AP (default: `false` em local) |
| `.start()` | Cria e inicia todos os nós, retorna `NGridCluster` |

Internamente:
- Aloca N portas efêmeras via `ServerSocket(0)` (reutiliza o padrão do `NGridIntegrationTest`)
- Cria diretórios temporários para cada nó
- Gera `NodeId` automáticos: `local-1`, `local-2`, etc.
- Configura mesh completo (cada nó conhece todos os outros como peers)
- Chama `start()` em cada nó e aguarda consenso via `ClusterTestUtils.awaitClusterConsensus()`

#### [NEW] [NGridNodeBuilder.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNodeBuilder.java)

Builder para nó individual (produção). Fluent API:

| Método | Descrição |
|---|---|
| `.id(String nodeId)` | NodeId customizado (default: `host:port`) |
| `.seed(String hostPort)` | Seed para autodiscovery |
| `.peers(String... hostPorts)` | Peers explícitos (`"host:port"` parsing) |
| `.queue(String name)` | Adiciona fila |
| `.map(String name)` | Adiciona mapa |
| `.replication(int factor)` | Fator de replicação |
| `.dataDir(Path dir)` | Diretório de dados |
| `.strictConsistency(boolean)` | Modo CP vs AP |
| `.start()` | Constrói config, cria `NGridNode`, chama `start()`, retorna `NGridNode` |

Internamente: resolve `"host:port"` string para `NodeInfo`, gera `NodeId` de `host:port` se não explícito, delega para `NGridConfig.builder()` existente.

---

### Container de Cluster

#### [NEW] [NGridCluster.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridCluster.java)

Container que gerencia N nós locais. Implementa `Closeable`.

| Método | Descrição |
|---|---|
| `.queue(String name, Class<T> type)` | Retorna `DistributedQueue` do primeiro nó |
| `.map(String name, Class<K>, Class<V>)` | Retorna `DistributedMap` do primeiro nó |
| `.node(int index)` | Acesso direto a um nó específico |
| `.nodes()` | Lista de todos os nós |
| `.leader()` | Retorna o nó líder |
| `.close()` | Fecha todos os nós |

---

### Testes

#### [NEW] [NGridFacadeLocalTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/structures/NGridFacadeLocalTest.java)

Testes de integração para o modo local:

1. **`localClusterFormsAndElectsLeader`**: `NGrid.local(2).queue("q").start()` → valida que líder é eleito
2. **`localClusterQueueReplicates`**: Offer em um nó, poll em outro
3. **`localClusterMapReplicates`**: Put em um nó, get em outro
4. **`localClusterCloseReleasesResources`**: Verifica que `close()` não lança exceção

#### [NEW] [NGridFacadeNodeTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/structures/NGridFacadeNodeTest.java)

Testes unitários para o `NGridNodeBuilder`:

1. **`nodeBuilderParsesHostPort`**: Valida parsing de `"host:port"` em peers
2. **`nodeBuilderGeneratesNodeIdFromHostPort`**: Valida geração automática de NodeId
3. **`nodeBuilderWithExplicitId`**: Valida `.id("custom")`
4. **`nodeBuilderRequiresDataDir`**: Valida que sem `dataDir` lança exceção (modo produção)

---

## Verification Plan

### Automated Tests

Executar via Maven no módulo `nishi-utils-core`:

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn -pl nishi-utils-core test -Dtest="NGridFacadeLocalTest,NGridFacadeNodeTest" -Dsurefire.useFile=false
```

### Testes existentes (regressão)

Garantir que a API existente continua funcionando (nenhum teste existente deve quebrar):

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn -pl nishi-utils-core test -Dsurefire.useFile=false
```
