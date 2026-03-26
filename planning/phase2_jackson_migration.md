# Fase 2 — Migração do Wire Protocol: Java Serialization → Jackson

Migrar o protocolo de rede do NGrid de `ObjectOutputStream`/`ObjectInputStream` (Java Serialization) para Jackson JSON com length-prefixed framing. Esta é a mudança de maior impacto na redução de CPU idle (~40-60%), conforme analisado na [issue_81_analysis.md](file:///home/lucas/Projects/nishisan/nishi-utils/planning/issue_81_analysis.md).

> [!WARNING]
> Esta é uma **breaking change** no wire protocol. Nós na versão antiga não se comunicarão com nós na versão nova. Exige rolling restart coordenado ou bump de major version.

## User Review Required

> [!IMPORTANT]
> **Versão alvo**: O bump deve ser para `4.0.0` (major) ou é aceitável em `3.6.0`/`3.7.0` como minor/breaking?

> [!IMPORTANT]
> **Campos `Serializable` genéricos**: Os payloads `ClientRequestPayload.body`, `ClientResponsePayload.body`, `ReplicationPayload.data`, `SyncResponsePayload.data` e `OfferPayload.value` atualmente aceitam qualquer `Serializable`. A proposta é migrar para `Object` com Jackson Default Typing habilitado via `@JsonTypeInfo(use = Id.CLASS)` nesses campos, para preservar a flexibilidade. **Alternativa**: restringir esses campos a `String`/`byte[]` e serializar o conteúdo no caller. Qual abordagem prefere?

> [!IMPORTANT]
> **`NQueueHeaders`**: Já possui serialização binária própria (`writeTo`/`readFrom`). Proponho criar um Jackson serializer/deserializer customizado que reutilize essa lógica binária (escreve como Base64 no JSON). Ou posso simplesmente serializar o `Map<String, byte[]>` como JSON nativo. Qual prefere?

---

## Proposed Changes

### Componente 1: MessageCodec (camada de abstração)

Criar uma interface `MessageCodec` e a implementação `JacksonMessageCodec` no pacote `transport.codec`.

#### [NEW] [MessageCodec.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/codec/MessageCodec.java)

Interface com dois métodos:
```java
public interface MessageCodec {
    byte[] encode(ClusterMessage message) throws IOException;
    ClusterMessage decode(byte[] data) throws IOException;
}
```

#### [NEW] [JacksonMessageCodec.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/codec/JacksonMessageCodec.java)

Implementação usando `ObjectMapper` com:
- Módulos para `NodeId` (serializa como string), `NQueueHeaders` (custom serializer)
- `@JsonTypeInfo` polimórfico para o campo `payload` do `ClusterMessage`, mapeando `MessageType` → classe de payload
- Registro de mixins ou subtypes via `ObjectMapper`

---

### Componente 2: Wire Protocol — Length-Prefixed Framing

#### [MODIFY] [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java)

Refatorar a inner class `Connection` (linhas 702-811):

**Antes:**
```java
private final ObjectOutputStream outputStream;  // linha 704
private final ObjectInputStream inputStream;     // linha 705
```

**Depois:**
```java
private final DataOutputStream outputStream;
private final DataInputStream inputStream;
private final MessageCodec codec;
```

**`drainOutbound()` (linha 740):** Substituir `outputStream.writeObject(message)` por:
```java
byte[] data = codec.encode(message);
outputStream.writeInt(data.length);  // 4-byte length prefix
outputStream.write(data);
outputStream.flush();
```

**`readLoop()` (linha 762):** Substituir `(ClusterMessage) inputStream.readObject()` por:
```java
int length = inputStream.readInt();
byte[] data = inputStream.readNBytes(length);
ClusterMessage message = codec.decode(data);
```

**Construtor da `Connection` (linha 711):** Remover `ObjectOutputStream`/`ObjectInputStream`, usar `DataOutputStream`/`DataInputStream`, receber `MessageCodec` por parâmetro.

O `MessageCodec` será passado pelo `TcpTransport` para cada `Connection`, instanciado uma única vez e compartilhado (thread-safe).

---

### Componente 3: Anotações Jackson nos modelos de domínio

#### [MODIFY] [ClusterMessage.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/ClusterMessage.java)

- Remover `implements Serializable`, `@Serial`, `serialVersionUID`
- Adicionar `@JsonCreator` no construtor principal (8 args)
- Adicionar `@JsonProperty` em todos os parâmetros
- Mudar campo `payload` de `Serializable` para `Object`
- Adicionar `@JsonTypeInfo(use = Id.CLASS)` no campo `payload` para suportar polimorfismo
- Ajustar métodos `payload()` e `payload(Class<T>)` para usar `Object` em vez de `Serializable`
- Ajustar factory methods `request()` e `response()` para aceitar `Object` em vez de `Serializable`

#### [MODIFY] [NodeId.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/NodeId.java)

- Remover `implements Serializable`, `@Serial`, `serialVersionUID`
- Adicionar `@JsonCreator` em `NodeId.of(String)` e `@JsonValue` em `value()`

#### [MODIFY] [NodeInfo.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/NodeInfo.java)

- Remover `implements Serializable`, `@Serial`, `serialVersionUID`
- Adicionar `@JsonCreator` no construtor de 4 args e `@JsonProperty` nos parâmetros

#### [MODIFY] 19 Payloads (pacote `common/` + `structures/OfferPayload`)

Para **cada** payload:
- Remover `implements Serializable`, `@Serial`, `serialVersionUID`
- Adicionar `@JsonCreator` + `@JsonProperty` nos construtores
- Para records (`SequenceResendRequestPayload`, `SequenceResendResponsePayload`): Jackson suporta records nativamente, só precisa remover `implements Serializable`
- Campos `Serializable` → `Object` com `@JsonTypeInfo(use = Id.CLASS)` nos payloads que possuem dados genéricos:
  - `ClientRequestPayload.body`
  - `ClientResponsePayload.body`
  - `ReplicationPayload.data`
  - `SyncResponsePayload.data`
  - `OfferPayload.value` (+ remover bounded generic `<T extends Serializable>` → `<T>`)

#### [MODIFY] [NQueueHeaders.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/queue/NQueueHeaders.java)

- Remover `implements Serializable`, `serialVersionUID`
- Adicionar `@JsonCreator`/`@JsonProperty` ou custom serializer/deserializer (pendente decisão do usuário)

---

### Componente 4: Classes de configuração

#### [MODIFY] Configs no pacote `config/`

Para as seguintes classes e suas inner classes:
- `ClusterPolicyConfig` (+ `SeedNodeConfig`, `ReplicationConfig`, `TransportConfig`)
- `QueuePolicyConfig` (+ `RetentionConfig`, `PerformanceConfig`, `MemoryBufferConfig`, `CompactionConfig`)
- `MapPolicyConfig`

Ações:
- Remover `implements Serializable` e `serialVersionUID`
- Adicionar `@JsonCreator` + `@JsonProperty` se necessário (verificar se Jackson consegue desserializar via getters/setters existentes)

---

### Componente 5: Impactos colaterais

#### [MODIFY] Quaisquer classes que referenciam `Serializable` como tipo de parâmetro

Buscar e ajustar referências de `Serializable` nos parâmetros de métodos/construtores que passam payloads:
- `ClusterMessage.request(...)` e `ClusterMessage.response(...)`
- Callers no `ClusterCoordinator`, `ReplicationManager`, `DistributedQueue`, `DistributedMap`, etc.

---

## Verification Plan

### Testes Unitários

#### [NEW] `JacksonMessageCodecTest.java`

Teste de roundtrip (encode → decode) para **todos os 19 tipos de payload** + `ClusterMessage` completo.

Cenários obrigatórios:
1. Cada `MessageType` com seu respectivo payload
2. `ClusterMessage` com `correlationId` presente e ausente
3. Payloads com campos genéricos (`Object`) contendo `String`, `Map`, `List`
4. `NQueueHeaders` com múltiplos headers
5. `OfferPayload` genérico
6. Records (`SequenceResendRequest/Response`)

```bash
mvn test -pl nishi-utils-core -Dtest=JacksonMessageCodecTest
```

### Testes de Integração (existentes — validação de regressão)

Os 55+ testes existentes validam o cluster end-to-end (formação, heartbeat, replicação, queues, maps, failover). Se a migração estiver correta, **todos devem passar sem alteração**, pois a serialização é transparente para o resto do sistema.

```bash
# Testes rápidos de integração (in-process, sem Docker)
mvn test -pl nishi-utils-core -Presilience

# Teste focado principal
mvn test -pl nishi-utils-core -Dtest=NGridIntegrationTest
```

### Verificação Manual

Não aplicável — os testes de integração existentes já cobrem formação de cluster, troca de mensagens, replicação, failover e restart. Se todos passarem, a migração está validada de ponta a ponta.
