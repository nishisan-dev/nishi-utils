# Checkpoint — Issue #78: Epoch Tracking & Truncation

**Data:** 2026-02-22T21:44 BRT  
**Branch:** (working tree — não commitado ainda)  
**Status:** 🟡 Implementação funcional, teste de integração com follower pendente

---

## 1. Problema

Ao reiniciar o seed node com dados persistidos (Docker bind mount), o `resetState()` + `installSnapshot()` reintroduz dados da epoch anterior. Clientes re-consumindo a partir do offset 0 recebem mensagens duplicadas.

## 2. Solução Implementada

Três camadas de proteção:

### 2.1 `NGridNode.cleanStaleReplicationState()` — **[NOVO]**
- **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java`
- **O que faz:** Antes de `replicationManager.start()`, varre os diretórios de queue por `.epoch` files. Se existem (indicando restart com dados persistidos), deleta `sequence-state.dat` do `ReplicationManager` para ele iniciar com `globalSequence=0`.
- **Por quê:** Sem isso, o `ReplicationManager` carrega sequências da epoch anterior, anunciando watermark=20 quando só existem 10 operações — impossibilitando sync de followers.

### 2.2 `QueueClusterService.truncateIfStaleEpoch()` — **[NOVO]**
- **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java`
- **O que faz:** No construtor, verifica `.epoch` file. Se o UUID da epoch salva difere do atual (novo UUID gerado a cada start), trunca a queue, reseta offsets, e salva nova epoch.
- **Campos adicionados:** `epochPath`, `currentEpoch`
- **Métodos auxiliares:** `loadEpoch()`, `saveEpoch()`

### 2.3 `ReplicationManager.resetSequenceState()` — **[NOVO]**
- **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java`
- **O que faz:** Reseta `globalSequence`, `lastAppliedSequence`, `nextExpectedSequenceByTopic`, `sequenceByTopic`, e deleta `sequence-state.dat`.
- **Nota:** Disponível mas **NÃO chamado diretamente** no fluxo atual — substituído pela limpeza centralizada no `NGridNode`.

### 2.4 `ReplicationHandler.onBecameLeader()` — **[NOVO]**
- **Arquivo:** `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationHandler.java`
- Hook default para eleições subsequentes durante a mesma JVM.
- `ReplicationManager.onLeaderChanged()` chama `handler.onBecameLeader()`.

---

## 3. Arquivos Modificados

| Arquivo | Mudança |
|---------|---------|
| `NGridNode.java` | `cleanStaleReplicationState()`, import `Files` |
| `QueueClusterService.java` | `epochPath`, `currentEpoch`, `truncateIfStaleEpoch()`, `loadEpoch()`, `saveEpoch()`, reordenação do construtor |
| `ReplicationManager.java` | `resetSequenceState()` |
| `ReplicationHandler.java` | `default onBecameLeader()` |
| `QueueEpochTruncationTest.java` | **[NOVO]** — teste de integração |

---

## 4. Estado dos Testes

| Teste | Status | Notas |
|-------|--------|-------|
| `shouldCreateAndUpdateEpochFile` | ✅ PASSA | Verifica que `.epoch` é criado e atualizado entre restarts |
| `shouldTruncateStaleDataOnLeaderPromotion` | ❌ FALHA | `Applied=0 Expected=10` — follower não recebe operações |

### Diagnóstico do teste falho

A **truncação funciona corretamente** — confirmada por logs:
```
WARNING: Queue epoch-queue has 10 stale items from previous epoch (xxx). Truncating to prevent duplicate delivery.
WARNING: Deleted stale sequence-state.dat — node restarted with persisted queue data
INFO: No saved sequence state found, starting from sequence 1
```

O problema **não é a epoch/truncação**, mas sim a **replicação/catch-up do follower**:
- O follower se conecta, registra o handler (`QueueClusterService created`), vê o leader.
- **Porém `lastAppliedSequence` permanece em 0** — nenhuma operação é transferida.
- Não há logs de snapshot transfer, lag detection, ou leader sync no follower.
- O padrão é idêntico ao `QueueCatchUpIntegrationTest` (que funciona).

### Hipóteses abertas para investigar

1. **`cleanStaleReplicationState` no follower:** O follower (`followerDir`) é novo, sem `.epoch` files. `cleanStaleReplicationState` deveria ser no-op. Verificar se `queueBaseDir` resolve para um path inesperado e interfere.

2. **Leader sync com handlers limpos:** Após `cleanStaleReplicationState` deletar `sequence-state.dat` no seed2, o `leaderSyncing` e `leaderSyncTopics` são re-populados em `onLeaderChanged`. Verificar se o sync flow é interrompido quando não há `previousLeader` (single node cluster) e os handlers já aplicaram as operações localmente.

3. **Port reuse / TCP TIME_WAIT:** O delay de 500ms entre runs pode não ser suficiente para liberar ports TCP. Verificar se a conexão entre follower e leader realmente estabelece.

4. **`replicationFactor(1)` + catch-up timing:** Com fator 1, o leader não espera acks. Verificar se o snapshot transfer é trigger corretamente para followers que chegam com `lastAppliedSequence=0` quando o leader já tem 10 operações.

5. **Default queue `ngrid`:** O NGridNode cria automaticamente uma queue padrão `ngrid` que também passa por epoch check. Verificar se há interferência entre handlers.

---

## 5. Decisões Arquiteturais Tomadas

1. **Epoch como UUID por instância JVM:** Cada restart gera um novo UUID. Simples e determinístico.
2. **Truncação no construtor:** Garante que dados stale são limpos antes de qualquer operação cluster.
3. **Limpeza centralizada no NGridNode:** `sequence-state.dat` é deletado ANTES do `ReplicationManager.start()` para que ele carregue do zero.
4. **Não usar fallback:** Seguindo diretriz do usuário, implementação definitiva sem compatibilidade com versão anterior.

---

## 6. Próximos Passos

1. **Debugar o catch-up do follower:**
   - Adicionar logging temporário de debug no `ReplicationManager.checkFollowerLag()` e `attemptLeaderSync()`.
   - Verificar se o follower está em `leaderSyncing=true` e se `leaderSyncTopics` está populado.
   - Testar com `Thread.sleep(2000)` antes de iniciar o follower para port release.

2. **Alternativa de validação:**
   - Se o catch-up in-process continuar problemático, considerar validar a truncação **diretamente** (sem follower), verificando que `queue.size() == 0` após o restart com epoch diferente, e depois `queue.size() == 10` após novas ofertas.

3. **Rodar suite completa:** Após o teste passar, executar `mvn test -pl .` para verificar regressões.

4. **Rodar teste Docker:** `NGridTestcontainersSmokeTest.shouldRecoverAfterSeedRestartWithoutDuplicatesOrLoss`.

5. **Commit e PR.**

---

## 7. Como Retomar

```bash
cd /home/lucas/Projects/nishisan/nishi-utils
# Ver estado atual dos arquivos modificados
git diff --stat
# Rodar teste isolado
mvn test -pl . -Dtest=QueueEpochTruncationTest -Dsurefire.useFile=false
# Rodar catch-up test para comparação
mvn test -pl . -Dtest=QueueCatchUpIntegrationTest -Dsurefire.useFile=false
```

Ao retomar, informar ao agente: "Retome a partir do checkpoint `planning/checkpoint_issue78_epoch_truncation.md`. O foco é resolver o teste `shouldTruncateStaleDataOnLeaderPromotion` que falha com `Applied=0`."
