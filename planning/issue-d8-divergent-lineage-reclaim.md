# D8 — Reclaim com linhagem divergente após kill -9 com follower atrasado

> Descoberto em 2026-06-10 na validação de resiliência pré-prod (cenário C2 da issue tems#9),
> sobre a 5.1.1. Branch do fix: `fix/d8-divergent-history-reclaim` (baseada na branch do D7).

## Cenário

Líder A (priority alta) morre em kill -9 com `applied=X` enquanto o follower B está atrasado
(`watermark=Y < X`). B promove (epoch novo), produz cauda própria — **linhagem divergente**: o
frontier X de A é de uma linhagem morta, numericamente MAIOR que o watermark real do incumbente.
A religa unclean e **reassume sem nunca incorporar a cauda de B**; pior, um `SYNC_RESPONSE`
tardio reseta o estado do líder ativo e a cadeia de sync morre sem conclusão.

## Cadeia causal (validada por código na 5.1.1)

1. **Boot do A re-stampa epochs** e força um ciclo step-down/recompute no incumbente (disruptivo,
   mas auto-curável se os gates segurassem A).
2. **[ELO CENTRAL] `drainRelayOnce` desarma o `relayPendingBootstrap` ao REQUISITAR o snapshot**
   (RM:1496-1502), não ao completar — a inelegibilidade do D3 e o watermark `-1` evaporam no
   request. O `SYNC_REQUEST` morre em silêncio no B (já follower: guard `!isLeader` em RM:975-979
   descarta sem NACK), então nada jamais completa.
3. No fim da `bootDiscoveryWindow`, a cláusula de deferral (CC:896-900) sai do caminho e o **gate A
   compara watermarks brutos sem linhagem**: `X ≥ Y` ⇒ "caught up" ⇒ A eleito.
4. A promoção limpa `syncingTopics`/pending (RM:2297-2298) e o write-gate libera por drain do
   próprio relay (vazio p/ ex-líder) em ms; **não existe leader-sync** (design: RM:2308-2313).
5. Chunk-0 tardio do sync passa o stale-check (frontier por tópico de ex-líder é vazio ⇒
   `currentApplied=0`, RM:1037-1058), **reseta o estado do líder ativo** (sem guard de papel em
   `handleSyncResponse`) e a continuação morre em self-send (`requestSync` → `leaderInfo()` = o
   próprio nó → `No route available`), com o janitor inerte em líderes (RM:1907).
6. Janela adicional de startup (wiring NGridNode): heartbeats começam antes do `registerHandler`
   armar o pending ⇒ primeiro heartbeat anuncia o `lastApplied` semeado da linhagem morta.
7. Agravante: o binlog local do ex-líder ainda contém a cauda morta `[Y+1..X]` contígua — um
   follower que fizesse fetch desse range receberia linhagem morta (fence é só por identidade).

## Por que nenhum teste pegou

Todos os cenários de reclaim existentes garantem (por `awaitApplied` ou por proporções) que o
incumbente termina **numericamente à frente** do retornante — linhagem única. Falta exatamente o
caso "follower atrasado no instante do kill" (única forma de gerar incumbente com watermark menor
que o frontier do morto).

## Fix (definitivo, sem fallback)

- **F-A (central): pending até completar.** `drainRelayOnce` não remove o tópico de
  `relayPendingBootstrap` ao requisitar o sync; a remoção já existe no `completeSnapshotCutover`
  (RM:1090) e passa a ser o único ponto. O nó unclean permanece inelegível (e anunciando `-1`) até
  o snapshot INSTALAR. O escape AP da promoção (RM:2297-2298) permanece — é o caminho legítimo do
  restart conjunto (C3).
- **F-B: `-1` desde o primeiro heartbeat.** `detectUncleanRestartAndMarkBootstrap` calcula
  `uncleanWithPriorData` (unclean && existe diretório de tópico no relay); enquanto verdadeiro e
  nenhum handler registrado ainda, `advertisedLeaderHighWatermark` retorna `-1` e
  `isLocallyEligibleForLeadership` retorna `false` — fecha a janela do wiring que registra handler
  depois do `start()`.
- **F-C: guards do sync.**
  1. `handleSyncResponse`: nó LÍDER ignora `SYNC_RESPONSE` (nunca resetar estado de líder ativo).
  2. `requestSync`: alvo == nó local ⇒ no-op (sem self-send).
  3. `completeSnapshotCutover` com install que substituiu o estado: `appliedSequence`/`globalSequence`
     passam a **SET** no watermark (não `max`) e o **resend-log local do tópico é truncado** — o
     contador e o binlog re-ancoram na linhagem do líder que serviu o snapshot (mata o item 7; o
     caso que motivava o `max` morre com o guard 1).
- **F-D (follow-up, issue separada): ordenação epoch-aware de linhagem** — `lineageEpoch` no
  heartbeat + persistido no sequence-state; gates A/B comparam (epoch, watermark). Evolução de
  protocolo; não bloqueia o D8 comportamental fechado por F-A/B/C.

## Testes

1. Unit RM: pending sobrevive ao request e só desarma no cutover (elegibilidade junto).
2. Unit RM: unclean com dados ⇒ advertised `-1`/inelegível antes do `registerHandler` (F-B).
3. Unit RM: líder ignora SYNC_RESPONSE; requestSync self é no-op; cutover SETa contadores e trunca
   o resend-log do tópico (F-C).
4. **E2E `DivergentLineageReclaimE2ETest`**: líder A produz história, é morto SEM aguardar o apply
   do follower B (B fica atrasado); B promove e produz cauda; A religa unclean. Asserta: A defere
   (inelegível) até instalar o snapshot de B; reclaim por afinidade só pós-cutover; estado final de
   A contém a cauda de B (prova por conteúdo); B segue follower sem fallback-loop.
