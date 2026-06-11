# Op-log HA — Endurecimento sob volume real (4.1.3)

Este documento descreve o endurecimento do op-log de replicação (`ReplicationManager`) para que o HA
active/standby da Cardinal convirja e se mantenha estável sob a volumetria real do Kafka
(~milhares de ops/s, estado de dezenas de MB). As correções foram diagnosticadas a partir de logs e
thread dumps de pré-prod e validadas end-to-end.

## Modelo

O op-log faz delta-shipping ordenado por tópico: o líder atribui uma sequência por op, replica aos
followers, e o follower aplica em ordem. Um follower frio reconcilia via **snapshot**; lacunas
pequenas são preenchidas por **resend**; o **leader election** (maior NodeId, com lease/heartbeat)
decide quem escreve.

O HA da Cardinal é **eventual (LWW whole-object)**: o standby precisa de um estado "bom o suficiente"
para assumir, não de ordenação forte op-a-op. Esse princípio guia os trade-offs abaixo.

## Causas-raiz corrigidas

### 1. Índice de resend atrás do frontier (`leaderLocalApply`)

O índice de resend (`indexReplicationPayload`) rodava no fim do apply-local **assíncrono** do líder
(`completeOperation`), que ficava muito atrás do frontier de envio sob alta vazão. Resultado: o líder
reportava como "missing" ops que **já havia enviado** → snapshot infinito no follower.

**Fix:** flag `ReplicationConfig.leaderLocalApply(false)`. Quando um engine externo já é a fonte da
verdade (caso da Cardinal), o apply-local no líder é redundante; o manager **commita e indexa de
forma síncrona** ao atingir o quórum, mantendo o índice de resend no frontier de envio.

### 2. Snapshot maior que o frame do transporte (multi-chunk)

Sob volume o snapshot excede o limite de frame de 64 MB do transporte. **Fix:** snapshot multi-chunk
byte-sliced — o handler fatia o snapshot em slices de 16 MB; o follower acumula e remonta no
`onSnapshotInstalled` (novo hook do `ReplicationHandler`).

### 3. Freeze do follower por lock órfão (resiliência do lock)

Verificado por dois thread dumps idênticos a 360s de distância (CPU congelado): as 4 threads do pool
`ngrid-replication` parkadas em `sequenceBufferLock` e **zero dono** do `ReentrantLock`. Uma thread
adquiriu o lock e saiu sem `unlock()` pareado (provável `Error`/OOM); como `ReentrantLock` não libera
na morte da thread, o lock ficou eternamente "held" e o `lock()` puro parkava o pool para sempre,
derrubando a liderança do cluster.

**Fix:**
- `tryLock(timeout)` (15s) em vez de `lock()` no caminho de replicação: um lock órfão/contendido
  **degrada para timeout recuperável** (aborta e re-sincroniza) em vez de congelar o nó.
- `catch(Throwable)` nas tasks do executor: um `Error` não mata mais o worker silenciosamente.
- **Cap do `sequenceBuffer`** (`MAX_SEQUENCE_BUFFER`): ao atingir o limite, cai para snapshot em vez
  de bufferizar até OOM — remove o gatilho do lock órfão.
- Persistência da sequência **coalescida e off-lock** (dirty-flag + flush agendado): antes,
  reescrever o arquivo inteiro a cada op aplicada (~2k/s) sob o lock atrelava o tempo de retenção à
  latência de disco.

### 4. Follower vivo mas travado — O(n)/O(n²) sob o lock

Com buffer grande (follower atrás), duas operações monopolizavam o lock e faziam o `tryLock` dos
callbacks de apply expirar (o nó ficava vivo mas sem convergir):

- `buffer.stream().anyMatch(...)` — scan O(n) do buffer **por op recebida** (check de duplicata).
- `PriorityQueue.removeIf(...)` no tail-replay — **O(n²)**.

**Fix:** descarte O(log n) — `processSequenceBuffer` descarta no início as entradas já cobertas
(`seq < nextExpected`) com `poll`; o check de duplicata e o `removeIf` foram removidos (duplicatas e
o tail do watermark caem naturalmente no descarte).

### 5. Hot-loop de resend e gap evictado (skip-and-drain)

Um gap irrecuperável (op evictada do log de resend) fazia o follower martelar o mesmo offset (dezenas
de milhares de vezes), saturando o líder e atrasando seu lease (flapping).

**Fix:**
- Resend não re-checa o audit log (`isOperationCommitted`): a presença no `replicationLogBySequence`
  já implica committed; o re-check dava falso-"missing".
- `checkForMissingSequences` respeita `syncingTopics`: não dispara resend durante um snapshot.
- **skip-and-drain** (`skipEvictedGapAndDrain`): ao confirmar gap evictado (líder responde "missing"
  e o follower já tem sequências maiores no buffer), o follower **pula o gap, drena a cauda em massa
  e vai ao vivo**, em vez de bloquear no head-of-line. Métrica: `getEvictedSkipCount()`.

> **Trade-off (documentado):** skip-and-drain troca consistência forte por liveness. Chaves tocadas
> **apenas** no intervalo pulado mantêm o último valor conhecido até o próximo update ou um novo
> snapshot. Só ocorre em gap comprovadamente evictado (lag grande), nunca em operação normal. Alinha
> com o modelo eventual (LWW) e com a política de reconciliação por maior NodeId.

### 6. Liderança frágil em 2 nós (pair mode)

Em cluster de 2 nós, ao perder o peer o sobrevivente não alcançava o quórum:
`requiredActiveMembersForLeadership = max(minClusterSize, (peers/2)+1) = max(1, 2) = 2`.

**Fix:** `ClusterCoordinatorConfig.withPairMode(true)` bypassa a maioria dinâmica, exigindo só
`minClusterSize` membros ativos. Split-brain durante partição é **aceito** e reconciliado na
reconexão pelo **maior NodeId** (`recomputeLeader` já elege `max(NodeId)`; o fencing por epoch
rejeita escritas do líder obsoleto). A Cardinal habilita pair mode quando `minClusterSize <= 1`.

### 7. Janela de backlog do op-log por tempo (`replicationLogRetentionTime`)

O log de resend (`replicationLogBySequence`) era trimado **apenas por contagem**
(`replicationLogRetention`, default 1000): não havia como dizer **por quanto tempo** o backlog fica
disponível para um follower que reingressa antes de cair em snapshot. Em volumetria alta, 1000 ops
podem representar uma fração de segundo de janela; em volumetria baixa, retêm memória por horas sem
necessidade.

**Fix:** retenção **temporal** complementar — `ReplicationConfig.replicationLogRetentionTime(Duration)`.
Cada entrada do resend log carrega o instante de indexação (no commit) e o `ReplicationManager`
evicta o prefixo contíguo de entradas mais velhas que a janela:

- **oportunística** a cada commit (`indexReplicationPayload`), espelhando o
  `NQueue.skipExpiredRecordsLocked`;
- **agendada** para tópicos ociosos (estende o passe periódico `trimLog`), garantindo que um tópico
  que parou de escrever ainda libere o backlog dentro da janela.

Tempo e contagem são **complementares — o que evictar primeiro vence**: contagem limita memória,
tempo limita a janela. Default `Duration.ZERO` = desabilitado (comportamento count-only inalterado).
Quando o follower pede deltas além da janela já expirada, o líder responde `missingSequences` e o
follower cai no caminho **gap-detection → snapshot fallback** já existente — nunca divergência
silenciosa. Métrica: `getReplicationLogTimeEvictedCount()`; tamanho por tópico:
`getReplicationLogSize(topic)`.

> Caso de uso (Cardinal): `ha.replication.logRetention` (ISO-8601) passa a janela temporal de
> backlog, de modo que um nó que reingressa **dentro** da janela recebe deltas e, **fora** dela,
> snapshot.

### 8. Gate de escrita durante o leader-sync (`LeaderSyncingException`)

No failover, ao reassumir liderança o novo líder marca `leaderSyncing=true` e dispara
`attemptLeaderSync` de forma assíncrona para recuperar o que o líder anterior avançou. Porém
`replicate()` checava apenas `isLeader()` + lease — **não** `isLeaderSyncing()` — então o novo líder
**aceitava escritas imediatamente**, antes de concluir o catch-up, podendo avançar com **estado
velho** e sobrescrever o progresso do líder anterior (divergência). Além disso, `attemptLeaderSync`
deixava `leaderSyncing=true` **preso** quando não havia `syncSource` (nó sozinho / primeiro líder de
cluster novo), travando consumidores que gateiam em `isLeaderSyncing()`.

**Fix:**
- **Gate fail-fast:** `replicate()` rejeita escritas com `LeaderSyncingException` (subtipo de
  `IllegalStateException`) enquanto `isLeaderSyncing()` for `true`. Defesa em profundidade que fecha a
  janela de divergência para **todos** os backends (queue e map).
- **Clear sem `syncSource`:** quando `resolveSyncSource()` retorna `null` (nenhum peer alcançável),
  `attemptLeaderSync` limpa `leaderSyncing=false` — o nó é, por definição, a réplica mais avançada
  alcançável e pode liderar imediatamente, sem prender o consumidor.

> Caso de uso (Cardinal): o HA faz **sync-before-lead** (só ativa o consumo do Kafka após
> `isLeaderSyncing()==false`). O gate fecha a janela na própria lib e o clear correto elimina a
> necessidade do *grace* que antes era usado como salvaguarda. Quem escreve deve aguardar
> `!isLeaderSyncing()` antes da primeira escrita pós-promoção (a lib agora rejeita explicitamente).

## Configuração (Cardinal)

```java
ReplicationConfig.builder(1)
    .strictConsistency(false)
    .leaderLocalApply(false)                       // engine é a fonte da verdade
    .replicationLogRetention(100_000)              // teto de contagem (cobre a transferência do snapshot)
    .replicationLogRetentionTime(Duration.ofMinutes(30)) // janela de backlog por tempo (o que evictar primeiro vence)
    .build();

// coordinator: minClusterSize=1 → pairMode habilitado (liderança solo + reconciliação por NodeId)
```

Pela facade `NGridConfig.Builder` os dois knobs de retenção também estão expostos
(`replicationLogRetention(int)` e `replicationLogRetentionTime(Duration)`), repassados ao
`ReplicationManager` no `NGridNode`.

## Diagramas

![Resiliência do lock](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_oplog_lock_resilience.puml)

![Recuperação de gap / skip-and-drain](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_oplog_gap_recovery.puml)

![Pair mode — failover e reconciliação](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_pairmode_failover.puml)

![Gate de escrita durante leader-sync](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_leader_sync_gate.puml)

## Validação E2E (pré-prod)

- Convergência: gap ~3k ops (sub-segundo), follower acompanha ~99,5% do ritmo do líder.
- Resiliência: follower vivo no soak, **0** timeouts de lock, **0** "missing", sem freeze.
- Failover (pair mode): matar o líder → sobrevivente assume sozinho e retoma o consumo.
- Reconciliação: religar o nó de maior NodeId → ele retoma a liderança, o outro faz step-down.

## Relay-log no follower (#124)

> Evolução do modelo de ingestão do follower, **opt-in** atrás de `FollowerIngestMode.RELAY_LOG`
> (default `INLINE` preserva o comportamento 4.3.0). Elimina a **espiral de morte** do follower sob
> volume real.

### Problema (espiral de morte)

Sob ~2.8k ops/s o follower INLINE colapsava: quando o gap passava de `MAX_SEQUENCE_BUFFER` (250k) — ou o
lag passava de `SYNC_THRESHOLD` (500) — a lib pedia snapshot; `resetState()` zerava o estado em memória
e reinstalava um **full-snapshot que cresce com o estado** (379 → 716 MB). O líder ficava em starvation
servindo snapshots gigantes e o follower nunca convergia. A reação ao atraso (`apply < produção`) era
**destruir o estado e recarregar tudo** — o oposto do desejável.

### Modelo-alvo (estilo MySQL relay log)

Desacopla **recepção** de **aplicação**:

1. **IO path (barato):** cada `REPLICATION_REQUEST` é persistido num **relay-log em disco (NQueue), um
   por tópico**, em ordem de chegada, com frame `<epoch>-<seq>` + payload. O ACK sai na **recepção
   durável** (quórum = "durável em N relays"), não no apply.
2. **Apply path (próprio ritmo):** um consumer por tópico drena o relay: `peek` → **fencing por
   (epoch, seq)** → `apply` → `poll`. O fencing por sequência é o que garante *effectively-once* mesmo
   com o `OFFER` da queue (não-idempotente). Gaps de transporte usam um buffer fino de reordenação +
   resend; o backlog grande mora no disco (fim do OOM do buffer de 250k).
3. **Sem snapshot em regime:** o lag é absorvido pelo relay; `checkLagAndSync` não pede mais snapshot no
   modo relay. Snapshot fica só para o irrecuperável (bootstrap).
4. **Failover drain-gate:** ao ser promovido, o nó segura escrita (`LeaderSyncingException`) até
   **drenar o relay** (não até "snapshot instalado"); release por relay vazio, **sem depender de peer**.

### Extensão NQueue (fundação)

- **`withRetentionClampToConsumer(true)`** — a retenção `TIME_BASED` **nunca descarta registros
  não-aplicados**; só recupera o prefixo já consumido. Sem isso, a compaction por tempo apagava ops
  ainda não aplicadas (perda silenciosa) — era o ponto make-or-break.
- **Fix do `recordCount`** — recontagem do segmento vivo após compaction `TIME_BASED` (o contador ficava
  defasado, corrompendo `size()`/métricas/drain-gate).

### Durabilidade configurável (análogo ao `sync_relay_log` do MySQL)

`RelayDurability` controla o fsync do tail do relay — trade-off **taxa × janela de perda**, não de
correção (o tail perdido é re-buscado pelo resend do líder, #122):

- `OS_MANAGED` (default) — sem fsync explícito (~213k ops/s no spike); janela coberta pelo resend.
- `GROUP_COMMIT` — fsync periódico (group commit) via `NQueue.sync()`.
- `ALWAYS` — fsync por op (mais durável, ~481 ops/s no spike).

A **crash-safety contra duplicação** (o `OFFER` não-idempotente) é estrutural, não por frequência de
fsync (análogo ao `relay_log_info_repository=TABLE` do MySQL): um **clean-shutdown marker** distingue
parada limpa (resume do frontier) de crash (bootstrap que substitui o estado).

### Acumulação ilimitada / relay-only (comportamento tipo MySQL)

Com `replicationLogRetentionTime=0` (default), o relay **acumula indefinidamente** os ops não-aplicados
(até encher o disco, como o relay log do MySQL sem purge): se o apply travar por erro crítico, os ops
ficam duráveis e são aplicados depois — **lag ≠ perda**. Setar `retentionTime > 0` ativa o **cap opt-in**
(decisão E): cabeça mais velha que a janela → bootstrap.

Para o lag puro (em ordem), o follower **já usa só relay-log** — não pede snapshot por mais atrás que
fique. Os únicos snapshots remanescentes são para o que o relay/resend **não conseguem reconstruir**: gap
cuja op foi evictada do op-log do líder, estouro do reorder-buffer e restart sujo. Para um "relay-only
absoluto", basta reter o op-log do líder o suficiente (resend sempre preenche) — evolução futura: um flag
explícito (`relayOnlyNoBootstrap`) + co-location atômica do cursor.

### Configuração

```java
NGridConfig.builder(local)
    .followerIngestMode(FollowerIngestMode.RELAY_LOG)    // opt-in; default INLINE
    .relayDurability(RelayDurability.OS_MANAGED)          // OS_MANAGED | GROUP_COMMIT | ALWAYS
    .replicationLogRetentionTime(Duration.ZERO)          // 0 = relay ilimitado (acumula e aplica depois)
    .build();
```

### Diagramas

![Relay-log — recepção desacoplada do apply](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_relay_log_apply.puml)

![Failover drain-gate (relay drenado antes de liderar)](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_relay_failover_drain_gate.puml)

### Validação

- **E2E:** follower converge via relay (queue + map), sem snapshot fallback, buffer inline em 0.
- **Aceite central:** carga sustentada de 10k ops, lag muito além dos limiares → converge com **0**
  snapshot/sync e sem reset (head ainda na 1ª op) — espiral eliminada.
- **Failover 3-nós:** o novo líder só fica *ready* (`isLeaderSyncing()==false`) após drenar o relay;
  escrita volta; sem divergência.
- Suíte de resiliência completa verde; **INLINE inalterado** (default, compat 4.3.0).

---

## 10. Op-log de resend do líder em disco (#127, 4.5.0)

### Problema

O op-log de resend do líder vivia em **heap** (`Map<String, NavigableMap<Long, TimedPayload>>`,
TreeMap sincronizado). Era capado por **contagem** (`replicationLogRetention`, teto de memória) **e**
por **tempo** (`replicationLogRetentionTime`, janela de backlog) — o que evictar primeiro vence. Sob
**alta produção** (replay de ~24h a ~5x tempo-real), cobrir uma janela de 30 min em heap estouraria a
memória, então o teto por **contagem** vence e a janela temporal vira segundos. Quando o follower
instala o snapshot no ponto `S` e pede `S+1..`, o líder **já evictou** essas sequências →
`missing sequences → snapshot fallback` em loop, e o relay do follower cresce sem limite (18 GB
observados em minutos).

### Solução — `ResendLog` (store próprio, híbrido)

Backear o op-log de resend por um **store sequencial próprio em disco**, mantendo um **cache quente em
heap** para os deltas recentes:

- **`ResendLog`** (um por tópico): log **append-only segmentado**, indexado por sequência. Cada
  segmento é um arquivo (`seg-NNN.dat`) com registros `[len][timestamp][frame]`, onde `frame` reusa o
  `RelayEntryCodec` (mesma tupla do relay). O índice em memória por segmento é um par de arrays
  primitivos `sequences[]`/`offsets[]` **ordenado por inserção binária** — compacto (sem boxing) e
  **tolerante a commits fora de ordem** (o commit por quórum não respeita a ordem de sequência, então
  o índice precisa ordenar como o `TreeMap` do heap ordenava).
- **Auto-compactação barata:** a retenção descarta **segmentos inteiros** (deleta o arquivo, sem
  reescrita), governada pela **janela temporal** (`replicationLogRetentionTime`) e por um backstop de
  contagem em disco (`resendLogMaxEntries`).
- **Híbrido:** `indexReplicationPayload` faz *dual-write* — heap (cache quente, capado por
  `replicationLogRetention`) **e** disco. `handleSequenceResendRequest` lê heap primeiro e cai no disco
  para o restante; uma sequência ausente em ambos vira `missingSequences` (caminho de snapshot
  existente, sem divergência). Falha de I/O do op-log **não** falha o commit.
- **Por que não NQueue/NMap:** o padrão de acesso é *append* + **leitura aleatória por sequência**
  (`get(seq)`/range), que NQueue (FIFO) não serve e NMap só com eviction composta manual. O log
  segmentado dá os dois com índice compacto e compactação por segmento.

### Configuração

```java
.persistentResendLog(true)                       // liga o tier de disco (default false)
.replicationLogRetentionTime(Duration.ofMinutes(30)) // janela temporal (governa o disco)
// avançado (ReplicationConfig): resendLogSegmentMaxEntries, resendLogSegmentMaxAge,
//                               resendLogMaxEntries, resendLogReadBatchMax
```

### Diagrama

![Op-log de resend segmentado em disco](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_resend_oplog_segmented.puml)

### Validação

- `ResendLogTest` (9): get/range/gaps, **chegada fora de ordem**, eviction por contagem e por tempo,
  reopen limpo e recuperação de registro rasgado.
- `PersistentResendLogRegressionTest` (2): com cap por contagem baixo + janela temporal grande, o
  op-log só-heap reporta o gap de bootstrap como *missing* (espiral), enquanto o op-log em disco serve
  o gap da janela temporal (sem fallback).

---

## 11. Throughput de apply em lote + métricas de relay (#128, 4.5.0)

### Problema

O consumidor de apply do relay drenava **1-a-1** (`peek → apply → poll`). Sob burst, o apply do
follower (~1.850 ops/s) não acompanhava a produção do líder (~2.600+ ops/s) e o lag crescia
monotonicamente. Faltava também **métrica pública** de tamanho/idade do relay (o cardinal só inferia
lag por `getGlobalSequence() − getLastAppliedSequence()`).

### Solução — apply em lote (ordem estrita preservada)

- `drainRelayOnce` passa a fazer **batch-peek** (`NQueue.readRange(0, batchSize)`, leitura
  não-consumidora), aplica o prefixo contíguo em ordem e faz **um único commit de frontier por lote**
  (`commitRelayBatch`), polando o prefixo consumido **só após** apply+commit. O consumidor continua
  **single-thread por tópico** e a ordem por sequência é estrita — paralelismo por chave foi
  **descartado** (risco de ordem/fencing, a classe de bug #124). Falha de apply no meio do lote commita
  o prefixo bem-sucedido e re-lança para o backoff do loop.
- Knob **`relayApplyBatchSize`** (default 256). O ganho vem de amortizar o overhead por-op
  (lock/flush/contadores/round-trips de peek-poll) ao longo do lote.

### Métricas públicas de lag

`ReplicationManager` (alcançável via `NGridNode.replicationManager()`):

- **`getRelaySize(topic)`** — backlog durável de apply do tópico (cresce com o lag, drena com o apply).
- **`getRelayHeadAgeMillis(topic)`** — há quanto tempo a cabeça do relay espera (idade do lag).
- **`getRelaySizes()`** — mapa tópico → backlog para dashboards.

### Diagrama

![Apply do relay em lote](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_relay_batch_apply.puml)

### Validação

- `RelayLogReplicationTest#relayBacklogMetricsReflectLagAndDrain`: o backlog é observável via a métrica
  enquanto drena, drena a zero na convergência, e o apply em lote converge em ordem (cabeça = 1º item).

---

## 12. Convergência determinística no join (#129, 4.5.0)

### Problemas

1. **Bootstrap reativo:** em RELAY_LOG o follower só dispara sync ao ver gap no tráfego de op-log. Um
   follower **novo** (estado vazio) contra um líder **quiescente** (sem tráfego) **nunca sincroniza** —
   fica inerte mesmo conectado.
2. **Auto-eleição transitória no boot:** com pair mode/`minClusterSize=1`, o nó se auto-elege líder no
   boot antes de enxergar o peer e stepa para follower em ~0,4s; ao se eleger no vazio, marca-se como
   *caught-up* e carrega essa crença falsa para o papel de follower.

### Solução

- **(3a) Sync proativo no join:** `checkProactiveJoinSync` — um follower que nada aplicou para um
  tópico, com relay vazio, e que vê (pelo **watermark do heartbeat**) que o líder tem dados, dispara
  **um** snapshot por termo. Não depende de tráfego de op-log; distingue o cold-join genuíno do lag
  em-regime (que o relay absorve).
- **(3b) Leader-pause-on-join (opt-in):** espelho do drain-gate de failover no caminho de join. O
  líder rastreia o progresso dos followers (novo `FOLLOWER_PROGRESS`) e, ao detectar um follower
  atrasado entrando (`onMembershipChanged`), **pausa a produção** (`replicate` rejeita com
  `LeaderSyncingException`) até ele alcançar / desconectar / estourar `joinQuiesceMaxDuration`. Bounded:
  um follower que morre no join **não** congela o líder.
- **(3c) Sem caught-up transitório:** um nó que se auto-elege sozinho dentro da
  `joinPeerDiscoveryWindow` **não** libera o drain-gate por relay vazio (não se anuncia como líder
  sincronizado no vazio); ao ver o peer, stepa para follower e o (3a) converge.

> **(3a) e (3b) andam juntas:** pausar o líder sem o follower sincronizar proativamente pioraria o
> quadro. Por isso o (3a) é sempre ativo em RELAY_LOG e o (3b) é opt-in sobre ele.

### Configuração

```java
.leaderPauseOnJoin(true)                       // opt-in (default false)
.joinQuiesceMaxDuration(Duration.ofSeconds(10))
// avançado (ReplicationConfig): followerProgressInterval, joinPeerDiscoveryWindow, joinSyncLagThreshold
```

### Diagrama

![Sync proativo no join + leader-pause-on-join](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_join_quiesce_proactive_sync.puml)

### Validação

- `RelayLogReplicationTest#proactiveSyncConvergesFreshFollowerAgainstQuiescentLeader`: follower limpo
  reinicia contra um líder que não produz mais nada e converge **só** via sync proativo.
- `LeaderPauseOnJoinTest`: o líder rejeita escritas enquanto um follower atrasado entra e retoma quando
  ele reporta *caught-up*.

> **Fix 4.5.1 (#131):** o proativo (3a) dependia do watermark do líder via heartbeat, cujo supplier era
> fiado apenas pelo `NGridNode` (default `-1`). Em montagem **manual** do `ReplicationManager` o
> watermark chegava `-1` e o proativo era barrado (`watermark <= 0`). Agora o `ReplicationManager.start()`
> fia o supplier sozinho (qualquer montagem) e o cold-join proativo trata watermark desconhecido como
> "sincroniza por segurança". Cobertura: `ProactiveColdJoinWatermarkTest` (montagem manual + pairMode).

---

## 13. Handoff por afinidade sob produção contínua (issue tems#9, D10)

A validação do D9 em pré-prod expôs o D10: no rejoin do nó de maior afinidade sob firehose contínuo,
o cluster degenerava em **dual-leader estável** — gate B estrito (threshold 0) nunca abria porque o
delta in-flight nunca zerava; o candidato armava o latch de caught-up contra watermark de heartbeat
**stale** e assumia; nenhum lado cedia e a mecânica de epoch virava escada infinita de re-stamps
(22→105+ no incidente; contenção manual: SIGTERM + wipe + cold bootstrap). Três frentes
complementares:

### (a) Join-quiesce cobre o catch-up real (D10a)

No incidente o quiesce do (3b) liberou em **1,5s** com catch-up real de 64s. Causa primária
(confirmada em código): o release comparava o progresso reportado contra o `globalSequence` **cru**
do líder — um contador de produção local que, num líder **promovido de follower**, fica muito abaixo
do watermark real. Endurecimentos:

- release/engage comparam contra `max(globalSequence, lastApplied)` — a mesma escala do watermark
  anunciado em heartbeat;
- progresso com frontier `-1` (bootstrap gate) ou **epoch divergente** é descartado (escala
  incomparável);
- o follower reporta o watermark **anunciável**, não o `lastAppliedSequence` cru;
- rejoin descarta a entrada de progresso da sessão anterior e o release periódico exige report
  **fresco** do epoch corrente.

### (b) Quiesce-assisted reclaim — "leader pause on reclaim" (D10b, opt-in)

Espelho do (3b) no caminho do **reclaim**: quando um candidato de **maior afinidade** (prioridade,
depois NodeId — o comparator da eleição) se aproxima do watermark do líder (delta ≤
`reclaimQuiesceThreshold`), o incumbente **pausa a produção** (`LeaderSyncingException`, retry do
cliente), o watermark **congela**, o candidato emparelha **exato** e o gate B abre — o handoff
completa de forma coordenada. O `FOLLOWER_PROGRESS` do emparelhamento é repassado ao coordinator na
hora (`noteFollowerWatermark`) para não esperar o próximo heartbeat. A pausa é **bounded por
construção**: cobre só o trecho final (nunca o catch-up inteiro), expira em
`reclaimQuiesceMaxDuration` **retendo a liderança** (disponibilidade primeiro) e o
`reclaimQuiesceCooldown` impede loop de re-engage. Join-quiesce ativo tem precedência.

### (c) Detecção + resolução determinística de dual-leader (D10c, sempre ativa)

O heartbeat ganhou a **flag de líder** (1 byte opcional ao final do frame binário, retro-compatível
nos dois sentidos — decoder antigo ignora o byte extra; frame antigo decodifica `false`; caminho
JSON coberto pelo default Jackson). Um líder que observa **3 heartbeats consecutivos** de outro nó
com a flag (debounce — a janela de overlap de um handoff legítimo é sub-heartbeat) resolve pela
**mesma ordem total da eleição**: o de menor afinidade cede, adota o rival e **ressinca via a
maquinaria do D8** (pending bootstrap → snapshot → cutover com SET dos contadores + truncate do
binlog local — a cauda produzida na janela dual é **descartada**, o mesmo desfecho da contenção
operacional, agora automático e bounded); o de maior afinidade retém. A escada de epochs morre na
primeira observação: o lado perdedor **adota** o termo observado em vez de re-stampar acima. O F2 do
D9 segue intocado — a resolução é um caminho novo, exclusivo de dois líderes se observando.

### Configuração

```java
.leaderPauseOnReclaim(true)                         // opt-in (default false) — recomendado em pares HA com prioridades
.reclaimQuiesceThreshold(1024L)                     // "perto" em sequências
.reclaimQuiesceMaxDuration(Duration.ofSeconds(5))   // pausa bounded
.reclaimQuiesceCooldown(Duration.ofSeconds(60))     // anti-loop
// (c) é sempre ativo; a flag de líder no heartbeat é retro-compatível (rolling upgrade seguro)
```

### Validação

- `JoinQuiesceReleaseGateTest` (3): reproduz o release de 1,5s (falhava antes do fix) e os
  endurecimentos H2/H3.
- `ReclaimQuiesceTest` (5): engage só na janela de aproximação de candidato de maior afinidade,
  pausa em `replicate()`, handoff por emparelhamento exato, timeout+cooldown, precedência do
  join-quiesce.
- `DualLeaderResolutionTest` (6): perdedor cede após o debounce (hook 1×), vencedor nunca cede,
  flap não resolve, escada de epoch suprimida, tie-break por NodeId, afinidade desconhecida não
  resolve.
- `DualLeaderYieldResyncTest` (2): yield arma pending para todos os tópicos, sobrevive ao REQUEST,
  cutover re-ancora na linhagem do vencedor; re-promoção supersede o yield.
- `DualLeaderLivelockE2ETest`: **produção contínua durante o handoff** (padrão novo na suíte) —
  líder único no deadline, nunca dois líderes além do overlap transitório, epoch estável, zero
  duplicatas e zero perda de itens ackados.

### Limitação conhecida (follow-up epoch-aware)

Os contadores de progresso são **escalares e cegos a linhagem**: se um nó alguma vez aplicou ops de
um ramo descartado (ex.: janela dual ou churn de eleição no boot **com produção ativa**), seu
contador fica inflado em relação à linhagem vencedora e o emparelhamento numérico exato fica
inalcançável — o (b) expira e o (c) resolve com descarte. O endurecimento estrutural (ordenação
epoch-aware de linhagem no protocolo de heartbeat) é o follow-up já registrado da PR #142.
