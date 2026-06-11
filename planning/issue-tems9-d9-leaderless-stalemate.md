# D9 — Abdicação do líder pós-reclaim e impasse leaderless (62 min em pré-prod)

> Descoberto em 2026-06-10 (manhã) durante a operação pós-validação do D8. Mesmo ciclo da
> issue tems#9; fix na branch `fix/d8-divergent-history-reclaim` (PR #142, junto do D8).

## Sintoma (ambiente real, 2 nós, firehose ~350 ops/s)

1. node-1 (priority 100) religa LIMPO, alcança por streaming e **reassume por afinidade** (correto).
2. **Exatos 20 heartbeats (60s) depois**, o node-1 — líder saudável — faz **step-down voluntário**.
3. O cluster fica **leaderless por 62 minutos**: node-1 nunca mais tenta; node-2 defere à afinidade
   do node-1; ninguém consome o Kafka. Só destravou quando o node-1 foi parado (saiu da membership).

## Cadeia causal (validada por código)

1. **Frontier stale do ex-líder**: um líder nunca avança o próprio frontier de follower
   (`nextExpectedSequenceByTopic`); após restart limpo o frontier fica no ponto do último
   apply-como-follower, enquanto `_topic:{t}` guarda tudo que o nó produziu.
2. **Re-apply da própria cauda**: o cursor de fetch nasce em `nextExpected-1` (stale) → o nó re-puxa
   do binlog do novo líder e **RE-APLICA as próprias ops** (duplicação no engine; fila não é
   idempotente) — e cada uma é **contada de novo** (`recordApplied`): `lastApplied` infla em P.
3. **Reclaim prematuro**: o latch do gate A arma no contador inflado — o nó assume faltando ops
   reais, e a produção pós-reclaim **reutiliza sequências** que o follower descarta como duplicatas
   (perda silenciosa).
4. **Applied congelado**: a produção recomeça do frontier real ≪ applied inflado; o
   `Math.max(applied, op.sequence)` nunca avança → o watermark anunciado congela. O contador honesto
   do follower o cruza ~20 heartbeats depois → latch reseta → **o gate A, avaliado no PRÓPRIO líder,
   o evicta** (não excluía o incumbente).
5. **Impasse**: o node-1 (atrás) só avançaria streamando — e a recusa de um não-líder a
   `RELAY_STREAM_FETCH` era **silenciosa (log FINE)**; o node-2 deferia **só por afinidade**, sem
   comparar watermark nem verificar se o eleito assumiu. O escape do restart conjunto (18463da) só
   cobre o ramo *inelegível* — este é o ramo do gate A.
6. O caso pós-cutover (D8) não reproduz: o cutover **SETa** todos os contadores numa escala única.

## Fix (PR #142, junto do D8)

- **F1 (causa-raiz)**: `seedAppliedSequenceFromFrontier` re-ancora o frontier no contador produzido
  (`nextExpected[t] = max(nextExpected[t], _topic:t + 1)`) antes do seed — o nó é a fonte da verdade
  do que produziu. Mata o re-apply, a inflação, o reclaim prematuro e o congelamento de uma vez.
- **F2**: o gate A **nunca evicta o líder corrente** (gate A é de RECLAIM; o lado do incumbente é o
  gate B). Líder observando contador de follower acima do próprio = sintoma de dessincronia de
  escala → WARN (rate-limited) e retém.
- **F3 (escape do impasse)**: `RELAY_STREAM_FETCH` recusado por não-líder responde
  `leaderUnavailable` (campo novo na `RelayStreamBatchPayload`, retrocompatível; recusa também
  ganha WARNING rate-limited). O requester repassa ao coordinator (`noteLeaderRefusal`); na
  recompute, **o nó À FRENTE assume** quando o eleito-por-afinidade recusou recentemente e não está
  à frente dele. Nó atrás nunca assume pelo escape (guarda).

## Testes

- `ReplicationManagerSequenceStateTest.seedReanchorsExLeaderFrontierOnProducedCounter` (F1).
- `LeaderlessStalemateEscapeTest` (3): líder nunca abdica por contador de follower (F2); nó à
  frente assume na recusa do eleito (F3); nó atrás nunca assume pelo escape (guarda F3).
- `CleanRestartExLeaderRejoinE2ETest` (2 nós reais): reingresso limpo sem re-apply da própria cauda
  (zero duplicatas, prova por conteúdo), reclaim por afinidade e liderança estável além da janela
  de 20 heartbeats da abdicação.

## Follow-ups (não bloqueiam)

- Ordenação epoch-aware de linhagem (mesmo follow-up do D8).
- `joinSyncLagThreshold > 0` no lado TEMS (mitigação de borda para contadores ~iguais sob produção).
