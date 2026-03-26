# Fase 3 — Heartbeat Coalescing & Binary Frames (Issue #81)

Otimizar o protocolo de rede do NGrid para as mensagens mais frequentes (HEARTBEAT, PING/PONG), substituindo a serialização JSON por frames binários de tamanho fixo e eliminando a geração de `UUID.randomUUID()` para cada heartbeat broadcast.

**Impacto estimado:** -5-10% adicional de CPU idle sobre a Fase 2 (Jackson).

## Contexto Atual

A Fase 2 (Jackson) **já está implementada**:
- `MessageCodec` interface em `transport/codec/`
- `JacksonMessageCodec` como implementação default
- `TcpTransport.Connection` usa o padrão **length-prefixed** (4-byte int header + JSON bytes)
- Cada heartbeat gera **2× `UUID.randomUUID()`** — uma no `ClusterMessage.request()` e outra no `messageForPeer()` dentro do `broadcast()`

### Dados do Frame Atual vs Proposto

| Campo | JSON (atual) | Binário (proposto) |
|-------|-------------|-------------------|
| Tipo de mensagem | ~20 bytes string | 1 byte |
| messageId UUID | 36 bytes string | **eliminado** |
| source NodeId | ~50 bytes (UUID string + JSON key) | 36 bytes UTF-8 fixo |
| HeartbeatPayload | ~80 bytes JSON | 24 bytes (3× long) |
| Overhead Jackson | ~200 bytes (tipo info, chaves, aspas) | 0 bytes |
| **Total estimado** | **~500-600 bytes** | **~61 bytes** |

---

## Proposed Changes

### Componente 1: Codec Binário

#### [NEW] [BinaryFrameCodec.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/codec/BinaryFrameCodec.java)

Codec binário especializado para mensagens `HEARTBEAT` e `PING`. O frame tem tamanho fixo:

```
| offset | tamanho | campo              |
|--------|---------|-------------------|
| 0      | 1 byte  | messageType (0x01=HEARTBEAT, 0x02=PING) |
| 1      | varies  | sourceNodeId (length-prefixed UTF-8)     |
| varies | 8 bytes | epochMilli         |
| varies | 8 bytes | leaderHighWatermark|
| varies | 8 bytes | leaderEpoch        |
```

- `encode(ClusterMessage)` → `byte[]` — serializa o frame
- `decode(byte[])` → `ClusterMessage` — reconstrói o `ClusterMessage` com `HeartbeatPayload` sem UUID

> [!NOTE]
> O `messageId` é **eliminado** para heartbeats e PINGs. Estas mensagens são fire-and-forget e não precisam de correlação. Para o PING do `RttMonitor`, a correlação `sendAndAwait()` usa o `messageId` do request. Solução: manter um `messageId` fixo por nó (derivado do `NodeId`) ou usar um contador atômico simples (long) em vez de UUID.

**Decisão de design:** Usar um **contador monotônico por conexão** (`AtomicLong`) para o messageId do PING, convertendo-o em UUID deterministicamente (`new UUID(0, counter)`) para manter compatibilidade com `PendingResponse` sem gerar entropia.

---

#### [NEW] [CompositeMessageCodec.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/codec/CompositeMessageCodec.java)

Wrapper que decide qual codec usar baseado em um **magic byte** no header:

```
| byte[0]  | Codec usado        |
|----------|--------------------|
| 0x01-0x02| BinaryFrameCodec   |
| qualquer outro | JacksonMessageCodec |
```

**Encode:** Se `message.type()` é `HEARTBEAT` ou `PING`, delega para `BinaryFrameCodec`. Caso contrário, prepende um byte marker `0x00` + bytes JSON via `JacksonMessageCodec`.

**Decode:** Lê o primeiro byte. Se é `0x01` ou `0x02`, delega para `BinaryFrameCodec`. Se é `0x00`, remove o marker e delega para `JacksonMessageCodec`. Se é `{` (0x7B, início de JSON), trata como JSON legacy puro.

> [!IMPORTANT]
> O byte de discriminação no `CompositeMessageCodec` exige alterar o protocolo de framing levemente: os frames JSON passam a ter 1 byte extra de overhead (marker `0x00`). Isso é aceitável pois JSON já tem centenas de bytes. Mensagens existentes começam com `{` (0x7B), portanto a detecção de "legacy JSON sem marker" funciona como backward-compat temporário durante rolling upgrade.

---

### Componente 2: Eliminação de UUID nos Heartbeats

#### [MODIFY] [ClusterMessage.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/ClusterMessage.java)

Adicionar factory method `lightweight()` para mensagens que não precisam de `messageId` UUID:

```java
public static ClusterMessage lightweight(MessageType type, String qualifier,
        NodeId source, NodeId destination, Object payload) {
    return new ClusterMessage(null, null, type, qualifier, source, destination, payload, 1);
}
```

O construtor já faz `messageId == null ? UUID.randomUUID() : messageId`. Para o caso lightweight, passamos um **UUID fixo** (e.g. `new UUID(0, 0)`) em vez de `null` para evitar a geração:

```java
private static final UUID ZERO_UUID = new UUID(0, 0);

public static ClusterMessage lightweight(MessageType type, String qualifier,
        NodeId source, NodeId destination, Object payload) {
    return new ClusterMessage(ZERO_UUID, null, type, qualifier, source, destination, payload, 1);
}
```

---

#### [MODIFY] [ClusterCoordinator.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java)

Alterar `sendHeartbeat()` (L463) para usar `ClusterMessage.lightweight()` em vez de `ClusterMessage.request()`.

---

#### [MODIFY] [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java)

1. **L87**: Trocar `new JacksonMessageCodec()` por `new CompositeMessageCodec()`
2. **L166 `messageForPeer()`**: Para heartbeats, não gerar novo UUID — reutilizar o `ZERO_UUID`

---

#### [MODIFY] [RttMonitor.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/metrics/RttMonitor.java)

Alterar `sendPing()` (L99): O PING **precisa** de `messageId` para correlação via `sendAndAwait()`. Manter `ClusterMessage.request()` aqui, mas considerar usar um **contador monotônico** em vez de UUID:

```java
private final AtomicLong pingCounter = new AtomicLong();

private UUID nextPingId() {
    return new UUID(0, pingCounter.incrementAndGet());
}
```

Isso evita o custo de SecureRandom do UUID.randomUUID() mas mantém unicidade local para correlação.

---

### Componente 3: Broadcast otimizado

#### [MODIFY] [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java)

No método `broadcast()` e `messageForPeer()`:
- Para HEARTBEAT: em vez de criar um novo `ClusterMessage` com `UUID.randomUUID()` para cada peer, reutilizar a **mesma instância** alterando apenas o `destination`. Adicionar método factory `ClusterMessage.withDestination()`:

```java
public ClusterMessage withDestination(NodeId newDestination) {
    return new ClusterMessage(this.messageId, this.correlationId, this.type,
            this.qualifier, this.source, newDestination, this.payload, this.ttl);
}
```

E no `broadcast()`:
```java
@Override
public void broadcast(ClusterMessage message) {
    for (NodeId nodeId : knownPeers.keySet()) {
        if (!nodeId.equals(config.local().nodeId())) {
            send(message.withDestination(nodeId));
        }
    }
}
```

Isso elimina `messageForPeer()` e o `UUID.randomUUID()` extra que ele gerava.

---

## Verification Plan

### Testes Automatizados

#### Teste unitário novo: `BinaryFrameCodecTest`

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl nishi-utils-core -Dtest="dev.nishisan.utils.ngrid.cluster.transport.BinaryFrameCodecTest" -Dsurefire.useFile=false
```

Cenários:
- Round-trip encode/decode de HEARTBEAT com os 3 campos do `HeartbeatPayload`
- Round-trip encode/decode de PING
- Verificar que o tamanho do frame binário é significativamente menor que JSON

#### Teste unitário novo: `CompositeMessageCodecTest`

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl nishi-utils-core -Dtest="dev.nishisan.utils.ngrid.cluster.transport.CompositeMessageCodecTest" -Dsurefire.useFile=false
```

Cenários:
- HEARTBEAT vai para BinaryFrameCodec e volta
- HANDSHAKE vai para JacksonMessageCodec e volta
- Decode de JSON legacy (sem marker byte) funciona

#### Testes existentes (regressão)

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl nishi-utils-core -Dtest="dev.nishisan.utils.ngrid.cluster.transport.JacksonMessageCodecTest" -Dsurefire.useFile=false
```

#### Build completo

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn clean install -DskipTests=false
```

### Verificação Manual

Não aplicável nesta fase — a mudança é transparente no wire protocol. Os testes de integração existentes cobrem o fluxo completo de cluster com heartbeats, PING/PONG e eleição de líder.
