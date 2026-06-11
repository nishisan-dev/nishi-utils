# D7 — Buracos de ingestão concorrente no RELAY_STREAM (loop de snapshot em pré-prod)

> Descoberto em 2026-06-10 durante a validação de resiliência da issue tems#9 (Cardinal, 2 nós,
> firehose Kafka ~2k ops/s, nishi-utils 5.1.0). Branch do fix: `fix/relay-stream-concurrent-ingest`.

## Sintoma

Follower sob produção contínua: o streaming aplica dezenas/centenas de milhares de ops e então o
apply encontra o relay com head acima do esperado —

```
Relay forward-gap on follower topic=cardinal-state expected=N head=N+1..N+3
(TTL-evicted prefix); requesting snapshot bootstrap
```

— com `relayExpireAfterWrite` DESLIGADO (prefixo "TTL-evicted" impossível). O snapshot "recupera",
o ciclo recomeça e o follower vive de snapshots (~2 fallbacks/min observados), nunca de stream.

## Causa-raiz

1. `TcpTransport` despacha **cada mensagem numa virtual thread própria** (`TcpTransport.java:80,744`)
   → dois `RELAY_STREAM_BATCH` processam **concorrentes**, mesmo chegando em ordem no mesmo TCP.
2. Dois batches sobrepostos em voo acontecem em regime: re-fetch por timeout (`relayStreamFetchTimeout`
   2s sob líder carregado) e o cutover de snapshot zerando o guard com fetch pré-sync ainda em voo
   (`completeSnapshotCutover`).
3. A seção por frame de `handleRelayStreamBatch` (check do cursor → `offer` → `incrementAndGet`) não
   era atômica: frame duplamente aprovado ⇒ cursor avança 2× com 1 append ⇒ **buraco durável de 1 op
   no NQueue do relay** (buracos de 1-3 = nº de frames duplamente aprovados). O apply só encontra o
   buraco ao drenar até ele — por isso o gap aparece dezenas de milhares de ops após cada snapshot.
4. `recoverFromRelayForwardGap` tratava todo forward-gap de follower como prefixo TTL-evicted →
   snapshot → novo cutover → novo par de batches concorrentes → **loop**.
5. O líder está inocente: emissão serializada por `leaderEmissionLock` e serve hole-safe
   (`takeContiguousFrames` quebra no primeiro buraco; `from` ausente ⇒ batch vazio).

O off-by-one no boot limpo (expected=N, head=N+1 logo após restart) é o MESMO buraco, durável,
perfurado antes do shutdown.

## Fix (5.1.1)

- **F1** — `relayIngestLockByTopic`: toda ingestão e re-ancoragem de cursor serializa por tópico;
  batch em voo é **descartado** enquanto `syncingTopics`/`relayPendingBootstrap` contém o tópico;
  `completeSnapshotCutover` **purga o relay** (`NQueue.close()+truncateAndReopen()`) e re-ancora o
  cursor atomicamente sob o mesmo lock.
- **F2** — forward-gap no follower com TTL off: recupera por **re-pull do binlog** (purga o relay,
  `cursor=want-1`, refetch imediato) em vez de snapshot; contador novo `getRelayRefetchCount()`.
  Com TTL on o caminho de snapshot permanece. Também **cura na subida** relays já perfurados em
  campo pela 5.1.0. Se o líder não retiver mais o range, `needSnapshot` responde — piso correto.
- **F3** — `saveSequenceState` com tmp+`ATOMIC_MOVE` e retorno booleano; `stop()` só grava o
  clean-marker se os appliers terminaram **e** o flush final aterrissou.

## Testes

`RelayStreamConcurrentIngestTest` (3 casos): batches sobrepostos entregues concorrentemente nunca
perfuram o relay (F1); batch em voo durante install é descartado e o cutover purga/re-ancora (F1);
relay legado perfurado com TTL off cura por re-pull sem snapshot, com `RELAY_STREAM_FETCH from=`
re-ancorado (F2). Suite completa do módulo verde.

## Por que os testes anteriores não pegaram

Os E2E produzem com `put/offer` síncronos (1 op por vez) e pausam a produção antes das janelas de
install — sem dois batches em voo. O incidente exige produção concorrente contínua durante fetch
lento/cutover, o regime do firehose de pré-prod.
