# Correção do deadlock leaderless no restart conjunto (PR nishi-utils#140) — IMPLEMENTADO

## Contexto

A revisão do PR #140 confirmou D1/D2/D4 + knob D6-lib corretos e o cenário primário da tems#9
funcionando, mas o teste `StaleFollowerRestartLivenessE2ETest` provou um **deadlock leaderless**
quando os dois membros do par reiniciam ao mesmo tempo. Esta é a correção (implementada e verificada).

> **Diagnóstico corrigido durante a implementação.** O plano inicial atribuía o deadlock a "ambos com
> bootstrap pendente anunciando watermark -1" (cenário A). A reprodução E2E revelou um mecanismo
> **diferente e mais comum** (cenário B): o **ex-líder semeia `applied=0`** no restart. Os dois
> cenários são reais e distintos; a correção cobre ambos.

## Causa-raiz (dois cenários distintos)

### Cenário B — ex-líder semeia `applied=0` (o que o E2E reproduziu)
No restart em modo stream, `ReplicationManager.seedAppliedSequenceFromFrontier()` semeava o applied
**somando apenas o frontier de follower** (`nextExpectedSequenceByTopic`). Um **ex-líder** nunca
popula esse frontier (rastreia via `sequenceByTopic`/`_global`, que o D1 passou a filtrar), então
semeia `applied=0` — mesmo tendo aplicado tudo o que produziu (`globalSequence`). Evidência E2E:

```
node1{leader=node-2, isLeader=false, applied=0,  global=61, peerWm=61}   ← ex-líder, prio 100
node2{leader=node-1, isLeader=false, applied=61, global=0,  peerWm=0}    ← ex-follower, prio 50
```

node-1 (maior afinidade) parece eternamente atrás de node-2 → defere para sincronizar (gate A);
node-2 (à frente) defere a node-1 por afinidade (não é líder, gate B não engata) → **ninguém lidera**.
Dispara em restart **limpo ou unclean** (o seed roda em todo restart stream).

### Cenário A — ambos com bootstrap pendente (relay não-vazio)
Quando os dois armam `relayPendingBootstrap` (relay com dados no restart unclean), ambos anunciam
watermark `-1` e ficam **inelegíveis**. A guarda de inelegibilidade só tinha escape "sozinho":
node-1 segue node-2 e vice-versa → split-view, e ninguém pode servir snapshot para o outro.

## Correção implementada (3 partes)

### Fix 1 — seed do applied a partir de max(frontier, global) — `ReplicationManager.seedAppliedSequenceFromFrontier()`
Semeia `applied = max(frontierSum, globalSequence)`. Restaura o frontier aplicado real para os dois
papéis (coincidem em tópico único): ex-líder → `max(0, 61)=61`; ex-follower → `max(61, 0)=61`. Assim
os dois anunciam seu estado verdadeiro e a eleição por afinidade escolhe o líder certo, sem deadlock e
sem perda. Mantém o filtro de chaves sintéticas do D1 (o teste `seedAppliedIgnoresSyntheticSequenceStateKeys`
segue verde: `max(1000, 1000)=1000`). `loadSequenceState()` (`:277`) carrega o `globalSequence` antes
do seed (`:324`).

### Fix 2 — escape AP na guarda de inelegibilidade — `ClusterCoordinator.recomputeLeader()`
Deferir só enquanto há peer **viável** (ativo com watermark conhecido `≥0`) ou dentro da boot window;
após a janela, sem peer viável (cluster inteiro em bootstrap), cair na eleição por afinidade para o nó
de maior afinidade assumir (promoção limpa `relayPendingBootstrap` em `onLeaderChanged :2179` → volta a
anunciar watermark real → o outro faz bootstrap dele). Cobre o cenário A.

### Fix 3 — registrar watermark `-1` — `ClusterCoordinator.onMessage()`
Registra o watermark do peer mesmo quando `-1` (`reported >= -1`). Distingue "ouvido, em bootstrap" de
"nunca ouvido": o gate A (`hasActivePeerWithUnknownWatermark`) não re-defere a um peer em bootstrap, e o
1º heartbeat dispara recompute (liveness do escape). Invariantes intactos (todos comparam `> -1`).

## Arquivos alterados

- `nishi-utils-core/.../replication/ReplicationManager.java` — Fix 1.
- `nishi-utils-core/.../cluster/coordination/ClusterCoordinator.java` — Fix 2 (`recomputeLeader`) + Fix 3 (`onMessage`).
- `nishi-utils-core/.../ngrid/StaleFollowerRestartLivenessE2ETest.java` — regressão E2E (1a standby unclean; 1b ambos unclean).
- `nishi-utils-core/.../cluster/coordination/LeaderAffinityElectionTest.java` — unit `wholeClusterIneligibleHigherAffinityLeadsAfterBootWindow` (cenário A).

## Verificação (executada — tudo verde)

- `StaleFollowerRestartLivenessE2ETest` → **2/2 passam** (antes: 1b falhava com deadlock de 40s).
- `LeaderAffinityElectionTest` → **8/8** (inclui o novo teste do cenário A).
- Conjunto crítico (D1/D2 + sync-before-reclaim + afinidade + liveness) → **20/20**.
- **Suíte completa do módulo: `Tests run: 464, Failures: 0, Errors: 0, Skipped: 8` — BUILD SUCCESS.**

## Pendências (fora deste fix)

- Decidir bump `5.0.1 → 5.0.2` (PR aberto/não liberado) e commit/push (não feito — aguardando o usuário).
- Fechar a tems#9 em produção (Cardinal, outro repo): bump da nishi-utils, **D5** shutdown hook,
  **D6** setar `resendLogSegmentMaxEntries`.
- Limitação conhecida: a escala do watermark/seed coincide em **tópico único** (caso do Cardinal);
  multi-tópico permanece como ponto de atenção (já registrado na issue).
