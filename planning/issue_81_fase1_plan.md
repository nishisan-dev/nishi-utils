# Fase 1 — Quick Wins: Ajuste de Intervalos NGrid (v3.6.0)

Reduzir consumo de CPU idle do NGrid (~25-35%) através de ajustes nos intervalos de tarefas periódicas e otimização do `recomputeLeader()`. Todas as mudanças são **backward-compatible** na API pública (apenas defaults mudam).

## Proposed Changes

### Defaults de Intervalos

#### [MODIFY] [NGridConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridConfig.java)

Alterar defaults no `Builder`:

```diff
- private Duration rttProbeInterval = Duration.ofSeconds(2);
+ private Duration rttProbeInterval = Duration.ofSeconds(10);

- private Duration heartbeatInterval = Duration.ofSeconds(1);
+ private Duration heartbeatInterval = Duration.ofSeconds(3);
```

---

#### [MODIFY] [ClusterCoordinatorConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinatorConfig.java)

Alterar `defaults()`:

```diff
  public static ClusterCoordinatorConfig defaults() {
-     Duration defaultTimeout = Duration.ofSeconds(5);
-     return new ClusterCoordinatorConfig(Duration.ofSeconds(1), defaultTimeout,
+     Duration defaultTimeout = Duration.ofSeconds(10);
+     return new ClusterCoordinatorConfig(Duration.ofSeconds(3), defaultTimeout,
              defaultTimeout.multipliedBy(3), 1, null);
  }
```

Resultado: heartbeat=3s, timeout=10s, lease=30s.

---

#### [MODIFY] [NGridNode.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java)

Na L312-314, o `heartbeatTimeout` está **hardcoded como 5s**, ignorando qualquer config. Alterar para ser proporcional ao heartbeatInterval:

```diff
  ClusterCoordinatorConfig coordinatorConfig = ClusterCoordinatorConfig.of(
          config.heartbeatInterval(),
-         Duration.ofSeconds(5),
+         config.heartbeatInterval().multipliedBy(3),
          config.leaseTimeout(),
          minClusterSize,
          null);
```

> [!IMPORTANT]
> Com heartbeat=3s, o timeout derivado será **9s** (3×3). Se o usuário configurar heartbeat=1s, será 3s. Esta abordagem é mais correta do que hardcodar 5s, pois mantém a proporção timeout/interval consistente independente do valor configurado.

---

### Eviction Interval

#### [MODIFY] [ClusterCoordinator.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java)

L214-217: Eviction a cada `heartbeatInterval × 2` em vez de `heartbeatInterval`:

```diff
  scheduler.scheduleAtFixedRate(this::evictDeadMembers,
-         config.heartbeatInterval().toMillis(),
-         config.heartbeatInterval().toMillis(),
+         config.heartbeatInterval().toMillis() * 2,
+         config.heartbeatInterval().toMillis() * 2,
          TimeUnit.MILLISECONDS);
```

---

### Route Probe Configurável

#### [MODIFY] [TcpTransportConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransportConfig.java)

Adicionar campo `routeProbeInterval` com default 10s:

```diff
  private final int workerThreads;
+ private final Duration routeProbeInterval;

  private TcpTransportConfig(Builder builder) {
      // ... existing fields ...
      this.workerThreads = builder.workerThreads;
+     this.routeProbeInterval = builder.routeProbeInterval;
  }

+ public Duration routeProbeInterval() {
+     return routeProbeInterval;
+ }

  // No Builder:
+ private Duration routeProbeInterval = Duration.ofSeconds(10);

+ public Builder routeProbeInterval(Duration interval) {
+     this.routeProbeInterval = Objects.requireNonNull(interval, "interval");
+     return this;
+ }
```

#### [MODIFY] [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java)

L128: Usar o novo campo de config:

```diff
- scheduler.scheduleAtFixedRate(this::probeLoop, 3_000, 3_000, TimeUnit.MILLISECONDS);
+ long probeMs = config.routeProbeInterval().toMillis();
+ scheduler.scheduleAtFixedRate(this::probeLoop, probeMs, probeMs, TimeUnit.MILLISECONDS);
```

---

### Otimização do `recomputeLeader()`

#### [MODIFY] [ClusterCoordinator.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/coordination/ClusterCoordinator.java)

L699-701: No handler de heartbeat, remover a chamada incondicional a `recomputeLeader()`. O heartbeat apenas faz `touch()` no membro — não muda membership. O `recomputeLeader()` já é chamado em todos os pontos onde membership realmente muda:
- `onPeerConnected()` (L658)
- `onPeerDisconnected()` (L672)
- `evictDeadMembers()` quando `changed=true` (L515)
- `start()` (L218)

```diff
  members.computeIfAbsent(source,
          id -> new ClusterMember(...)).touch();
- recomputeLeader();
+ // recomputeLeader() is only needed when the member is NEW (computeIfAbsent created one)
```

Para garantir que um **novo membro** descoberto via heartbeat (antes do handshake) acione a recomputação:

```java
members.compute(source, (id, existing) -> {
    if (existing != null) {
        existing.touch();
        return existing;
    }
    // New member discovered via heartbeat — trigger recompute
    ClusterMember newMember = new ClusterMember(
            findPeerInfo(source).orElseGet(() -> new NodeInfo(source, "", 0)));
    return newMember;
});
// Only recompute if member was new (i.e., just added)
if (memberIsNew) {
    recomputeLeader();
}
```

A implementação exata usará o retorno de `compute()` para detectar se o membro é novo:

```java
boolean[] isNew = {false};
members.compute(source, (id, existing) -> {
    if (existing != null) {
        existing.touch();
        return existing;
    }
    isNew[0] = true;
    return new ClusterMember(
            findPeerInfo(source).orElseGet(() -> new NodeInfo(source, "", 0)));
});
if (isNew[0]) {
    recomputeLeader();
}
```

---

## Verification Plan

### Automated Tests

Todos os testes de integração existentes definem intervalos **explícitos** (150-500ms) via API programática, portanto não serão impactados pela mudança de defaults. Os testes relevantes:

```bash
# Build completo e execução de todos os testes
cd /home/lucas/Projects/nishisan/nishi-utils
mvn clean install
```

Testes específicos de lease/heartbeat que validam o comportamento:
- `LeaderLeaseStepDownTest` — usa heartbeat=200ms explícito
- `LeaderLeaseIntegrationTest` — usa heartbeat configurável
- `NGridIntegrationTest` — teste geral de cluster

```bash
# Rodar apenas os testes de lease/coordenação
mvn test -pl nishi-utils-core -Dtest="LeaderLeaseStepDownTest,LeaderLeaseIntegrationTest,NGridIntegrationTest"
```

### Manual Verification

Não se aplica nesta fase — as mudanças são puramente de defaults e lógica interna. A verificação por deploy em staging e medição de CPU será feita pelo usuário no ambiente NGrid real (ishin-gateway).
