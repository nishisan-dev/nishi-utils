# NGrid — Baseline de Maturidade de Produção

**Data:** 2026-02-07  
**Fase:** 0 — Baseline e Gate Inicial  
**Escopo:** NGrid Queue, Map, Replication, Coordination

---

## 1. Comportamento por Cenário de Falha

### 1.1 Crash do Líder

**Configurações relevantes:**
- `heartbeatInterval`: 1s (default)
- `heartbeatTimeout`: 5s (default)
- `strictConsistency`: `false` (default)

**Fluxo:**
1. Followers detectam ausência de heartbeat após `heartbeatTimeout` (5s default).
2. `ClusterCoordinator` remove líder dos `activeMembers` e inicia eleição.
3. Eleição determinística: **node com maior NodeId** é eleito.
4. Novo líder incrementa epoch (persistido em `leader-epoch.dat`).
5. `ReplicationManager` no novo líder restaura `globalSequence` de `sequence-state.dat`.
6. Followers aplicam Hybrid Offset Sync: comparam `lastAppliedSequence` com líder.
   - **Lag < snapshotThreshold (500):** Natural Replay (líder reenvia commandos).
   - **Lag ≥ snapshotThreshold:** Full Snapshot Transfer (chunked, ~1000 itens/chunk).

**Resultado esperado:**
- Dados escritos **antes** do crash são preservados (já replicados nos followers).
- Writes **em trânsito** (pendentes de ACK) podem ser perdidos — semântica **at-least-once**.
- O epoch fencing garante que o líder antigo (zombie), ao reconectar, terá comandos rejeitados.

**Resultado observado (testes):**
- `QueueNodeFailoverIntegrationTest.testDataPersistsAfterLeaderFailover` ✅ — dados preservados.
- `QueueNodeFailoverIntegrationTest.testWritesDuringFailover` ✅ — writes sob stress durante failover.

**Gaps e riscos:**
- Com `strictConsistency=false`, o novo líder pode aceitar writes com um único ACK local (quorum dinâmico reduz para 1).
- **Não há timeout de lease:** líder isolado não faz step-down automático — depende apenas de reconexão/fencing passivo.

---

### 1.2 Crash de Follower

**Configurações relevantes:**
- `replicationQuorum`: 2 (default via NGridConfig)
- `strictConsistency`: `false` (default)

**Fluxo:**
1. Líder detecta desconexão TCP do follower.
2. `ClusterCoordinator` remove follower dos `activeMembers`.
3. `ReplicationManager.requiredQuorum()` adapta dinamicamente:
   - Com `strictConsistency=false`: `Math.min(requested, activeMembers.size())` → pode chegar a 1.
   - Com `strictConsistency=true`: mantém quorum configurado → operação falha com `TimeoutException`.
4. Quando follower retorna:
   - Reconecta via `TcpTransport`.
   - Compara `lastAppliedSequence` com líder `highWatermark`.
   - Catch-up via Natural Replay ou Snapshot conforme o lag.

**Resultado observado (testes):**
- `QueueCatchUpIntegrationTest.testQueueCatchUpAfterSignificantLag` ✅ — 600 itens, snapshot sync.
- `QueueCatchUpIntegrationTest.testMultiChunkQueueCatchUp` ✅ — 2500 itens, multi-chunk transfer.

**Gaps e riscos:**
- Com quorum dinâmico, cluster de 3 nós com 2 followers offline aceita writes com 1 ACK — risco de perda se o líder fizer crash subsequente.

---

### 1.3 Partição de Rede

#### 1.3.1 Partição Curta (< `heartbeatTimeout`)

**Fluxo:**
1. Heartbeats falham mas dentro da janela de tolerância.
2. Nenhuma ação drástica — líder continua operando.
3. Ao reconectar, followers recebem heartbeat com epoch e `highWatermark` atualizados.
4. Catch-up natural (Natural Replay) para lag acumulado.

**Resultado esperado:** Transparente. Nenhuma reeleição ou perda de dados.

#### 1.3.2 Partição Longa (> `heartbeatTimeout`)

**Fluxo:**
1. Partição detectada via heartbeat timeout (5s).
2. **Lado majoritário:** reeleição ocorre se líder ficou isolado; novo líder com epoch incrementado.
3. **Lado minoritário com líder antigo:**
   - Sem lease: líder **não** faz step-down automático.
   - Com `strictConsistency=false`: aceita writes com quorum=1 → **divergência de dados**.
   - Com `strictConsistency=true`: writes falham com `TimeoutException` (quorum inalcançável).
4. **Reconexão:** epoch fencing rejeita líder antigo. Dados escritos no lado minoritário são descartados silenciosamente.

**Resultado observado (testes):**
- `ReplicationQuorumFailureTest` ✅ — valida que quorum inalcançável retorna timeout (strict mode).
- **Não há teste automatizado** para partição longa com dados divergentes.

**Gaps e riscos:**
- **GAP CRÍTICO:** sem lease, o líder isolado continua aceitando writes que serão descartados.
- **GAP CRÍTICO:** sem `strictConsistency=true`, admite-se split-brain write com quorum=1.
- Dados no lado minoritário são perdidos silenciosamente na reconexão — sem log de divergência.

---

### 1.4 Restart Total do Cluster

**Configurações relevantes:**
- `queueDirectory`: diretório de persistência para filas.
- `NQueue.Options.withFsync`: garante durabilidade na escrita (default: `false`).
- `mapPersistenceMode`: `DISABLED` | `ASYNC_NO_FSYNC` | `ASYNC_WITH_FSYNC` (default: `DISABLED`).
- `dataDirectory`: usado para `leader-epoch.dat`, `sequence-state.dat`.

**Fluxo:**
1. Todos os nós param (graceful close ou crash).
2. Ao reiniciar:
   - `ClusterCoordinator` carrega epoch de `leader-epoch.dat`.
   - `ReplicationManager` restaura `globalSequence` de `sequence-state.dat`.
   - `NQueue` recarrega segmentos do `queueDirectory` (se persistência ativada).
   - `MapPersistence` carrega snapshot + replays WAL (se `mode != DISABLED`).
   - `MonotonicOffsetStore` carrega offsets de arquivo (se map de offsets persistente).
3. Eleição completa ocorre do zero (como se fosse primeiro boot).

**Resultado observado (testes):**
- `QueueRestartConsistencyTest.testQueueDataPersistsAcrossRestart` ✅ — dados preservados.
- `QueueRestartConsistencyTest.testPartiallyConsumedQueueRestart` ✅ — offsets preservados.
- `QueueRestartConsistencyTest.testSequenceStatePreservedAfterRestart` ✅ — sequence continua.
- `NGridDurabilityTest.shouldRecoverOffsetAfterCrashAndNotReceiveDuplicates` ✅ — crash abrupto (SIGKILL), sem duplicatas (requer fsync).
- `NGridDurabilityTest.shouldRecoverMessagesAfterSeedCrash` ✅ — mensagens sobrevivem crash do seed.
- `NGridDurabilityTest.shouldMaintainIndependentOffsetsAfterMultiClientCrash` ✅ — offsets independentes.

**Gaps e riscos:**
- Com defaults (`mapPersistenceMode=DISABLED`, `withFsync=false`): **perda total de offsets e dados do map em crash abrupto**.
- `MapPersistence` é *best-effort*: falhas são logadas (`Level.WARNING`) mas não propagam exceção — operação continua silenciosamente sem persistência.
- Sem healthcheck que detecte falha de persistência — operador não é notificado.

---

## 2. Matriz de Modo de Operação por Ambiente

| Config | Default Atual | Dev | Staging | Prod (Recomendado) |
|---|---|---|---|---|
| `strictConsistency` | `false` | `false` | `true` | `true` |
| `mapPersistenceMode` | `DISABLED` | `DISABLED` | `ASYNC_WITH_FSYNC` | `ASYNC_WITH_FSYNC` |
| `NQueue.withFsync` | `false` | `false` | `true` | `true` |
| `replicationQuorum` | `2` | `1` | `⌊N/2⌋+1` | `⌊N/2⌋+1` |
| `minClusterSize` | `1` | `1` | `⌊N/2⌋+1` | `⌊N/2⌋+1` |
| `heartbeatInterval` | `1s` | `1s` | `1s` | `1s` |
| `heartbeatTimeout` | `5s` | `5s` | `5s` | `3s` |
| `operationTimeout` | `30s` | `30s` | `10s` | `5s` |
| `retryInterval` | `1s` | `1s` | `1s` | `500ms` |
| Retenção (Queue) | `DELETE_ON_CONSUME` | `DELETE_ON_CONSUME` | `TIME_BASED` | `TIME_BASED` |

### Justificativa por Perfil

**Dev:**
- Performance e velocidade de iteração. Persistência desligada para testes rápidos.
- `replicationFactor=1` para desenvolvimento single-node.

**Staging:**
- Reproduz condições de produção. `strictConsistency=true` ativa quorum estático — falhas de quorum são visíveis.
- Persistência completa com fsync — valida durabilidade antes de prod.

**Prod:**
- Tolerância zero a perda de dados. Timeouts mais agressivos para detecção rápida de falhas.
- `minClusterSize` impede writes sem quorum suficiente.

---

## 3. Definição de SLOs Iniciais

> **Nota:** Estes SLOs são propostas baseadas na análise do código e dos testes existentes. A instrumentação real para medição contínua será implementada nas Fases 4-5.

### 3.1 Disponibilidade

| SLO | Métrica | Alvo | Condição |
|---|---|---|---|
| Disponibilidade de escrita | % de `offer()`/`put()` com sucesso | ≥ 99.9% | Cluster estável (N nós, ≥ quorum ativos) |
| Disponibilidade de leitura | % de `poll()`/`peek()`/`get()` com sucesso | ≥ 99.99% | Qualquer nó ativo (leitura local) |

### 3.2 Latência

| SLO | Métrica | Alvo | Condição |
|---|---|---|---|
| Latência `offer` P95 | Tempo até ACK de quorum | < 50ms | Cluster local, rede saudável |
| Latência `offer` P99 | Tempo até ACK de quorum | < 200ms | Cluster local, rede saudável |
| Latência `poll` P95 | Tempo de poll + offset commit | < 100ms | Cluster local |
| Latência `put` (Map) P95 | Tempo até ACK de quorum | < 50ms | Cluster local |

### 3.3 Resiliência

| SLO | Métrica | Alvo | Condição |
|---|---|---|---|
| Tempo de failover | Tempo entre detecção de crash e novo líder operacional | < 10s | `heartbeatTimeout=5s` |
| Taxa de erro de replicação | % de `TimeoutException` em `ReplicationManager` | < 0.1% | Cluster estável |
| Frequência de sync completo | `SYNC_REQUEST` por hora | ≤ 1/hora | Operação normal, sem crashes |

### 3.4 Durabilidade

| SLO | Métrica | Alvo | Condição |
|---|---|---|---|
| Perda de dados em crash graceful | Mensagens perdidas | 0 | `withFsync=true`, `mapPersistenceMode=ASYNC_WITH_FSYNC` |
| Perda de dados em SIGKILL | Mensagens perdidas | ≤ 1 batch de WAL | `withFsync=true` |
| Perda de offsets em crash | Offset regression | 0 | `mapPersistenceMode=ASYNC_WITH_FSYNC` |

---

## 4. Mapa de Cobertura de Testes

| Teste | Cenário Coberto | Status |
|---|---|---|
| `QueueNodeFailoverIntegrationTest` | Crash do líder, writes durante failover | ✅ |
| `QueueCatchUpIntegrationTest` | Catch-up de follower (snapshot + multi-chunk) | ✅ |
| `QueueRestartConsistencyTest` | Restart: dados, offsets, sequence state | ✅ |
| `NGridDurabilityTest` | SIGKILL: offsets, mensagens, multi-client | ✅ (Docker) |
| `ReplicationQuorumFailureTest` | Quorum inalcançável, timeout | ✅ |
| `LeaderReelectionIntegrationTest` | Reeleição após perda de líder | ✅ |
| `ConsistencyIntegrationTest` | Consistência geral de replicação | ✅ |
| `ReplicationSuccessIntegrationTest` | Replicação com sucesso | ✅ |
| `CatchUpIntegrationTest` | Catch-up genérico (não-queue) | ✅ |
| **Partição longa com divergência** | Split-brain write, resolução | ❌ Sem cobertura |
| **Falha de persistência silenciosa** | MapPersistence WAL/snapshot failure | ❌ Sem cobertura |
| **Lease/auto step-down** | Líder isolado faz step-down | ❌ Não implementado |

---

## 5. Resumo de Gaps Críticos (Input para Fase 1+)

| # | Gap | Severidade | Fase Alvo |
|---|---|---|---|
| G1 | `strictConsistency=false` como default permite quorum=1 | **P0** | Fase 1 |
| G2 | `mapPersistenceMode=DISABLED` como default perde offsets | **P0** | Fase 1 |
| G3 | `MapPersistence` best-effort sem healthcheck | **P0** | Fase 2 |
| G4 | Sem lease: líder isolado não faz step-down | **P1** | Fase 3 |
| G5 | Sem métricas exportáveis (apenas java.util.logging) | **P1** | Fase 4 |
| G6 | Sem teste de partição longa com divergência | **P1** | Fase 2 |
| G7 | Dados do lado minoritário descartados sem log | **P1** | Fase 2 |
