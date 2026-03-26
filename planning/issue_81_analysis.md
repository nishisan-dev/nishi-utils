# Parecer Técnico — Issue #81: NGrid Alto Consumo de CPU em Idle

## Veredicto: ✅ A issue é válida e bem fundamentada

Após análise detalhada do código-fonte, **todos os pontos levantados na issue se confirmam**. O diagnóstico está preciso tanto na identificação dos ofensores quanto na estimativa de impacto.

---

## Validação Ponto a Ponto

### 1. 🔴 Java Serialization no TcpTransport — **CONFIRMADO, maior vilão**

Em [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java):

- **Linha 713**: `new ObjectOutputStream(socket.getOutputStream())`
- **Linha 715**: `new ObjectInputStream(socket.getInputStream())`
- **Linha 747**: `outputStream.writeObject(message)` — serializa todas as mensagens
- **Linha 764**: `(ClusterMessage) inputStream.readObject()` — desserializa todas as mensagens

Existem **18 tipos de Payload** diferentes que passam por Java Serialization, todos `implements Serializable`. O grafo de objetos é profundo — `ClusterMessage` contém `NodeId`, que contém `Serializable`, e os payloads como `HandshakePayload` carregam `Set<NodeInfo>` e `Map<NodeId, Double>`.

> [!CAUTION]
> Java Serialization gera ~5-10x mais overhead de CPU por operação comparado a Jackson/MessagePack. Em idle com 3 nós, são no mínimo **8 operações de serialização/s por nó** (heartbeats + RTT), cada uma traversando grafos de objetos complexos.

### 2. 🟡 Heartbeat a cada 1s — **CONFIRMADO**

Em [ClusterCoordinator.java:210-213](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java#L210-L213):
```java
scheduler.scheduleAtFixedRate(this::sendHeartbeat,
        0,
        config.heartbeatInterval().toMillis(),  // default: 1000ms
        TimeUnit.MILLISECONDS);
```

Default em [NGridConfig.java:308](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridConfig.java#L308): `Duration.ofSeconds(1)`

**Agravante confirmado:** `recomputeLeader()` é chamado em [ClusterCoordinator.java:701](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java#L701) para **cada heartbeat recebido**, executando `stream().filter().max()`.

### 3. 🟡 RTT Probe a cada 2s — **CONFIRMADO**

Em [RttMonitor.java:69-70](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/metrics/RttMonitor.java#L69-L70):
```java
long periodMs = Math.max(100L, interval.toMillis());  // default: 2000ms
scheduler.scheduleAtFixedRate(this::probePeers, periodMs, periodMs, TimeUnit.MILLISECONDS);
```

Cada probe usa `sendAndAwait()` que serializa PING + espera PONG serializado via Java Serialization.

### 4. Contribuintes Menores — **CONFIRMADOS**

| Task | Intervalo | Confirmação |
|------|-----------|-------------|
| Dead member eviction | = heartbeatInterval (1s) | [ClusterCoordinator.java:214-217](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java#L214-L217) |
| Reconnect loop | default 3s | [TcpTransportConfig.java:87](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransportConfig.java#L87) |
| Route probe | 3s hardcoded | [TcpTransport.java:128](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java#L128) |

---

## Plano Evolutivo Proposto

### Fase 1 — Quick Wins: Ajuste de Intervalos (v3.6.0)

**Impacto estimado: -25-35% de CPU idle. Risco: baixo.**

| Mudança | Arquivo | De | Para |
|---------|---------|-----|------|
| Default heartbeat | `NGridConfig.Builder` | 1s | **3s** |
| Default heartbeat timeout | `ClusterCoordinatorConfig.defaults()` | 5s | **10s** |
| Default lease timeout | `ClusterCoordinatorConfig.defaults()` | 15s (3×5) | **30s** (3×10) |
| Default RTT probe | `NGridConfig.Builder` | 2s | **10s** |
| Route probe | `TcpTransport.java:128` | 3s hardcoded | **10s** (configurável) |
| Eviction interval | `ClusterCoordinator.start()` | = heartbeatInterval | `heartbeatInterval × 2` |

> [!IMPORTANT]
> Aumentar o heartbeat para 3s é seguro. Sistemas como Consul usam 5s, Kubernetes usa 10s. O heartbeat timeout de 10s (com eviction check a cada 6s) ainda detecta falhas rapidamente.

Inclui também:
- Cachear resultado de `recomputeLeader()` e só recomputar quando membership muda (não a cada heartbeat)
- Tornar o route probe interval configurável via `TcpTransportConfig`

---

### Fase 2 — Migração de Serialização: Jackson (v3.7.0 ou v4.0.0)

**Impacto estimado: -40-60% de CPU idle. Risco: médio-alto (breaking change no wire protocol).**

Substituir `ObjectOutputStream`/`ObjectInputStream` por Jackson no `TcpTransport.Connection`:

1. Criar uma interface `MessageCodec` com implementações:
   - `JacksonMessageCodec` (novo default)
   - ~~`JavaSerializationCodec` (fallback legacy)~~ → **Sem fallback**, conforme regras

2. Adaptar `TcpTransport.Connection`:
   - `drainOutbound()` → usar codec para escrever
   - `readLoop()` → usar codec para ler

3. Remover `implements Serializable` de:
   - `ClusterMessage`, `NodeId`, `NodeInfo`
   - Todos os 18 tipos de Payload
   - Adicionar anotações Jackson (`@JsonCreator`, `@JsonProperty`)

4. Frame protocol: length-prefixed messages (4-byte header + JSON bytes)

> [!WARNING]
> Esta fase é uma **breaking change** no protocolo de rede. Nós na versão antiga não poderão se comunicar com nós na versão nova. Exige rolling restart coordenado ou bump de major version.

---

### Fase 3 — Heartbeat Coalescing & Binary Frames (v3.8.0 / v4.1.0)

**Impacto estimado: -5-10% adicional. Risco: baixo.**

- Para heartbeats (e PING/PONG), usar um formato **binary fixo** (não JSON):
  - 1 byte tipo + 8 bytes timestamp + 8 bytes watermark + 8 bytes epoch = **25 bytes** vs ~500+ bytes com Java Serialization
- Eliminar criação de `UUID.randomUUID()` para cada heartbeat broadcast (gasta entropia)

---

### Fase 4 — Observabilidade & Benchmarks (paralelo)

- Adicionar métricas de CPU/serialização ao `StatsUtils` para tracking contínuo
- Criar um benchmark JMH comparando `JavaSerialization` vs `Jackson` vs `BinaryFrame` para as mensagens do NGrid
- Documentar os novos defaults e trade-offs na documentação

---

## Priorização Recomendada

```
Fase 1 ──────▶ Fase 2 ──────▶ Fase 3
 (dias)        (1-2 semanas)    (dias)
  Low risk     Breaking change   Low risk
  -25-35%      -40-60%           -5-10%
```

A **Fase 1 é independente** e pode ser entregue imediatamente como v3.6.0. As Fases 2 e 3 podem ser planejadas para a próxima major version se preferir evitar breaking changes no minor.
