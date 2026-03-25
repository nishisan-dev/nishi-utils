# NGrid — Otimização de CPU em Idle

## Problema

Com um cluster de 2 nós **completamente ocioso** (sem ofertas, sem gets), o NGrid mantém ~15 tarefas periódicas/threads ativas que geram wakeups e consumo de CPU desnecessários.

## Proposed Changes

### 1. TcpTransport — Eliminar busy-wait do `drainOutbound` (**CRÍTICO**)

O ofensor principal. Cada conexão mantém uma virtual thread em loop com `poll() + Thread.sleep(5)` — 200 wakeups/seg **por conexão**.

#### [MODIFY] [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java)

- Trocar `ConcurrentLinkedQueue<ClusterMessage> outbound` por `LinkedBlockingQueue<ClusterMessage>`
- Substituir `outbound.poll()` + `Thread.sleep(5)` por `outbound.poll(1, TimeUnit.SECONDS)` (blocking poll com timeout)
- O timeout de 1s garante que a thread checa `isOpen()` periodicamente sem spin

```diff
- private final ConcurrentLinkedQueue<ClusterMessage> outbound = new ConcurrentLinkedQueue<>();
+ private final LinkedBlockingQueue<ClusterMessage> outbound = new LinkedBlockingQueue<>();

  private void drainOutbound() {
      try {
          while (isOpen()) {
-             ClusterMessage message = outbound.poll();
-             if (message == null) {
-                 try {
-                     Thread.sleep(5);
-                 } catch (InterruptedException e) {
-                     Thread.currentThread().interrupt();
-                     break;
-                 }
-                 continue;
-             }
+             ClusterMessage message = outbound.poll(1, TimeUnit.SECONDS);
+             if (message == null) {
+                 continue; // timeout — recheck isOpen()
+             }
              synchronized (outputStream) {
                  outputStream.writeObject(message);
                  outputStream.flush();
              }
          }
+     } catch (InterruptedException e) {
+         Thread.currentThread().interrupt();
      } catch (Exception e) {
```

**Impacto**: Elimina ~200 wakeups/seg por conexão → 0 em idle.

---

### 2. ReplicationManager — Consolidar tasks de alta frequência

6 tasks programadas, 3 delas a cada 500ms. Consolidar em 2 ciclos:

#### [MODIFY] [ReplicationManager.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java)

**Antes (6 tasks):**
```
checkTimeouts       → periodMs (~500ms)
retryPending        → retryMs  (~500ms)
checkLagAndSync     → 2000ms
retryLeaderSync     → 500ms
checkResendTimeouts → 500ms
trimLog             → cleanupPeriodMs (~5-60s)
```

**Depois (2 tasks):**
```
maintenanceTick()   → 1000ms (consolida: checkTimeouts, retryPending,
                               retryLeaderSync, checkResendTimeouts)
backgroundTick()    → 5000ms (consolida: checkLagAndSync, trimLog)
```

O método `maintenanceTick()` chama sequencialmente `checkTimeouts()`, `retryPending()`, `retryLeaderSync()` e `checkResendTimeouts()`. Relaxar para 1s ao invés de 500ms é seguro porque o SLA de replicação não exige sub-segundo e o `operationTimeout` é tipicamente 5-10s.

**Impacto**: 6 scheduled tasks → 2. Wakeups: 12/s → 1.2/s.

---

### 3. TcpTransport — Relaxar `reconnectLoop`

#### [MODIFY] [TcpTransport.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/TcpTransport.java)

- Intervalo default de `reconnectInterval` permanece o que vier da config
- Adicionar early-exit: se todos os peers conhecidos estão conectados, o loop retorna imediatamente sem iterar

```java
private void reconnectLoop() {
    if (!running) return;
    // Fast path: skip iteration if all known peers are connected
    boolean allConnected = knownPeers.values().stream()
        .filter(p -> !p.nodeId().equals(config.local().nodeId()) && p.port() > 0)
        .allMatch(p -> isConnected(p.nodeId()));
    if (allConnected) return;
    // ... existing reconnection logic
}
```

**Impacto**: Em estado estável (todos conectados), o loop custa ~1 stream check vs iterar + submeter tasks.

---

### 4. Observabilidade — `NGridDashboardReporter` desligável

#### [MODIFY] [NGridNode.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java)

Atualmente o dashboard reporter é **sempre** criado se `dataDirectory != null`. Adicionar flag `dashboardEnabled` ao `NGridConfig` (default: `true` para PRODUCTION, `false` para DEV).

#### [MODIFY] [NGridConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridConfig.java)

Adicionar campo `dashboardEnabled` ao builder com default baseado no `DeploymentProfile`.

**Impacto**: Elimina IO de disco a cada 10s + geração de snapshot em cenários dev/test.

---

### 5. RttMonitor — Piggybacking no heartbeat

#### [MODIFY] [RttMonitor.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/metrics/RttMonitor.java)

O RTT probe envia uma mensagem PING dedicada a cada `rttProbeInterval` (default 2s). Otimização: calcular RTT a partir do heartbeat que já flui naturalmente (incluir `sentAt` no `HeartbeatPayload`, calcular na recepção).

> [!WARNING]
> Esta mudança requer alterar `HeartbeatPayload` — que é serializado cross-node. Para manter compatibilidade, adicionar o campo como `Optional` (null-safe na desserialização). Alternativa mais segura: manter o PING mas aumentar o intervalo default para 10s.

**Decisão recomendada**: Aumentar `rttProbeInterval` default para 10s (mudança trivial, sem risco).

**Impacto**: 1 PING/2s por peer → 1 PING/10s por peer.

---

## Resumo de Impacto

| Métrica | Antes (2 nós idle) | Depois |
|---|---|---|
| Wakeups/seg (scheduled) | ~14 | ~3 |
| Writer threads em spin | 2 (busy-wait) | 0 (blocking) |
| Dashboard IO | 1 write/10s | 0 (desabilitado em DEV) |
| RTT probes | 1/2s por peer | 1/10s por peer |

## Verification Plan

### Automated Tests

```bash
# Regressão completa
cd /home/lucas/Projects/nishisan/nishi-utils
mvn -pl nishi-utils-core test -Dsurefire.useFile=false
```

### Manual Verification

1. Iniciar cluster local com 2 nós via `NGrid.local(2)`
2. Monitorar threads ativas com `jcmd <pid> Thread.print` ou VisualVM
3. Confirmar ausência de spin em `drainOutbound` (thread em WAITING, não TIMED_WAITING a cada 5ms)
