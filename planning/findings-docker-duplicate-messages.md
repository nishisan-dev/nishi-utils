# Finding: Duplicatas Pós-Restart no Docker (Testcontainers)

**Data:** 2026-02-22
**Teste:** `NGridTestcontainersSmokeTest.shouldRecoverAfterSeedRestartWithoutDuplicatesOrLoss`
**Status:** Bug pré-existente, não regressão

## Erro Observado

```
Duplicate messages detected for client-1: [INDEX-2-0 x2, INDEX-1-1 x2, INDEX-1-0 x2]
```

## Cenário de Reprodução

1. Cluster com 1 seed + 5 clients (followers)
2. Seed produz mensagens `INDEX-{epoch}-{index}` a cada 2s
3. Seed é killado via `docker kill` (simula crash)
4. Seed reinicia com **mesmo diretório de dados** (bind mount persistente)
5. Clients continuam rodando e recebem duplicatas

## Análise da Causa Raiz

### Fluxo de Dados

```
Seed(epoch=1) → produz INDEX-1-0..N → persiste em disco
     ↓ replicação
Followers → consomem INDEX-1-0..N via pollWhenAvailable → offset avança

Seed é killado → dados ficam no bind mount

Seed(epoch=2) reinicia → dados de epoch-1 AINDA estão no disco
     ↓
Follower detecta lag → solicita sync/snapshot do novo líder
     ↓
resetState() → close() + truncateAndReopen() + localOffsetStore.reset()
     ↓
Snapshot é instalado com TODOS os itens do líder (epoch-1 + epoch-2)
     ↓
Client reconsome do offset 0 → duplicatas de INDEX-1-0, INDEX-1-1, INDEX-2-0
```

### Problema Central

O seed reinicia com dados persistidos e se torna líder novamente. O snapshot que ele envia para os followers contém **todos os itens** acumulados (epoch-1 + epoch-2). Após `resetState()` zerar os offsets do follower, o client reconsome tudo desde o início.

## O Que Já Foi Corrigido (PR #73)

- Race condition do `MemoryStager` durante `resetState()` — substituído poll-loop por `close()` + `truncateAndReopen()`
- Funcionou correto nos testes in-process (sem persistência entre restarts)

## Possíveis Soluções

### Opção A — Seed limpa dados ao reiniciar com nova epoch
- O seed detecta que sua epoch local é diferente e faz `truncateAndReopen()` antes de começar a produzir
- **Prós:** Simples, elimina dados stale
- **Contras:** Perda de dados se o seed for o único nó com dados completos

### Opção B — Consumer offset tracking distribuído
- Os followers persistem offsets de forma que sobrevivam ao `resetState()`
- Após snapshot install, o follower consulta um offset store distribuído para saber onde parou
- **Prós:** Exato, sem re-consumo
- **Contras:** Complexidade adicional

### Opção C — Snapshot inclui watermark e follower salta itens já consumidos
- O snapshot carrega um watermark indicando até qual offset os followers já confirmaram
- Após install, o follower pula para o watermark
- **Prós:** Preciso, sem perda
- **Contras:** Requer protocolo de watermark bidirecional

### Opção D — Client-side deduplication (application layer)
- A aplicação (Main.java) já tem detecção de duplicatas, mas a lógica é imprecisa
- Melhorar para ser idempotente com um `Set<String>` persistente
- **Prós:** Fácil de implementar
- **Contras:** Não resolve o problema na camada de infraestrutura

## Impacto

- **In-Process tests:** ✅ Passando (corrigido pela PR #73)
- **Docker Resilience tests:** ❌ Falhando (bug pré-existente)
- **Produção:** Potencialmente afeta qualquer cenário onde o seed reinicia com dados persistidos e followers reconectam

## Referências

- Issue #72 e PR #73: `resetState()` race condition
- Issue #74 e PR #77: Failover test flakiness (corrigido)
- Issue #76 e PR #77: Assertion flakiness pós-failover (corrigido)
