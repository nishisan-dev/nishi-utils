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
