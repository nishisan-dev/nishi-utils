# Runbook: Validação de Consistência Pós-Incidente

## Visão Geral
Procedimento para validar a consistência dos dados do NGrid após qualquer incidente
(partição de rede, crash, split-brain, rollback manual). Este é o runbook de "última milha"
que deve ser executado ao final de qualquer outro procedimento de recuperação.

---

## Quando Executar

- Após recuperação de partição de rede
- Após restart de nó(s) por crash
- Após detecção de split-brain
- Após intervenção manual no cluster
- Quando alerta `HIGH_GAP_RATE` ou `SNAPSHOT_FALLBACK_SPIKE` foi disparado

---

## Procedimento

### 1. Verificar alinhamento de epochs

Em cada nó, verificar o dashboard:

```bash
cat <data-dir>/dashboard.yaml
```

Comparar `leaderEpoch` e `trackedLeaderEpoch` entre todos os nós:

```yaml
# Nó 1 (Líder)
cluster:
  leaderEpoch: 12
  trackedLeaderEpoch: 12

# Nó 2 (Follower)
cluster:
  leaderEpoch: 12           # ← deve ser igual ao líder
  trackedLeaderEpoch: 12    # ← deve ser igual ao líder
```

> [!WARNING]
> Se `trackedLeaderEpoch` de algum follower for **menor** que o `leaderEpoch` do líder,
> o follower ainda está convergindo. Aguardar.

### 2. Verificar convergência de sequência

Comparar `globalSequence` (líder) com `lastAppliedSequence` (followers):

```yaml
# Líder
replication:
  globalSequence: 100000

# Follower
replication:
  lastAppliedSequence: 100000    # ← deve ser igual ao globalSequence do líder
  replicationLag: 0              # ← deve ser 0
```

### 3. Verificar high watermark

O `trackedLeaderHighWatermark` dos followers deve ser igual ao `globalSequence` do líder:

```yaml
cluster:
  trackedLeaderHighWatermark: 100000   # ← deve ser igual ao globalSequence do líder
```

### 4. Verificar contadores de gap e fallback

```yaml
replication:
  gapsDetected: 5           # ← anotar valor
  resendSuccessCount: 5     # ← deve ser >= gapsDetected
  snapshotFallbackCount: 0  # ← idealmente 0 ou estável
```

Se `gapsDetected > resendSuccessCount`, há gaps não resolvidos que podem indicar
entradas perdidas.

### 5. Verificar pendingOperationsCount

```yaml
replication:
  pendingOperationsCount: 0   # ← deve ser 0 em estado estável
```

Se > 0 por mais de 30 segundos, investigar se há followers inalcançáveis.

### 6. Teste funcional de read/write

Executar uma operação de escrita e leitura em cada nó para confirmar operabilidade:

```java
// Via API
node.getQueue("test-queue", Serializable.class).offer("probe-" + System.nanoTime());
// Verificar no follower que o item aparece
```

---

## Critérios de Validação

| Critério | Condição |
|:---|:---|
| Epoch alinhado | `trackedLeaderEpoch` igual em todos os nós |
| Lag zero | `replicationLag == 0` em todos os followers |
| Watermark alinhado | `trackedLeaderHighWatermark` == `globalSequence` do líder |
| Sem gaps pendentes | `gapsDetected <= resendSuccessCount` |
| Sem pending | `pendingOperationsCount == 0` |
| I/O funcional | Write + read completam sem erro |
| Alertas limpos | Nenhum alerta ativo no cluster |

---

## Se Validação Falhar

### Lag persistente (> 0 por mais de 60s)

1. Verificar conectividade de rede → [Runbook Partição de Rede](network_partition.md)
2. Verificar carga do líder → [Runbook Líder Instável](unstable_leader.md)

### Epoch divergente

1. Forçar restart do follower com epoch menor.
2. O follower vai sincronizar do zero (snapshot fallback).

### Gaps não resolvidos

1. Verificar logs por `RetransmitFailed` ou `GapDetected`.
2. Se resend falhou repetidamente, reiniciar o follower para forçar snapshot fallback.

### PendingOperations crescente

1. Verificar se algum follower ficou permanentemente inalcançável.
2. Remover follower morto da configuração ou reiniciá-lo.

---

## Métricas Relacionadas

| Métrica | Localização |
|:---|:---|
| `leaderEpoch` | `NGridOperationalSnapshot.leaderEpoch()` |
| `trackedLeaderEpoch` | `NGridOperationalSnapshot.trackedLeaderEpoch()` |
| `globalSequence` | `NGridOperationalSnapshot.globalSequence()` |
| `lastAppliedSequence` | `NGridOperationalSnapshot.lastAppliedSequence()` |
| `replicationLag` | `NGridOperationalSnapshot.replicationLag()` |
| `trackedLeaderHighWatermark` | `NGridOperationalSnapshot.trackedLeaderHighWatermark()` |
| `gapsDetected` | `NGridOperationalSnapshot.gapsDetected()` |
| `resendSuccessCount` | `NGridOperationalSnapshot.resendSuccessCount()` |
| `pendingOperationsCount` | `NGridOperationalSnapshot.pendingOperationsCount()` |
