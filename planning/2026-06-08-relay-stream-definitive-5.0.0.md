# Plano — Migração definitiva da Cardinal para RELAY_STREAM + nishi-utils 5.0.0

## Context (por que esta mudança)

O cluster HA da Cardinal (tevent-cardinal, 2 nós, topic `cardinal-state`, ~2.5–3.4k ops/s) replica
estado sobre o `ngrid`/`ReplicationManager` da **nishi-utils**. No modelo anterior (push broadcast +
recuperação por NAK com reordenação em buffer de memória), sob firehose o follower não acompanha o
líder: trava pedindo `SEQUENCE_RESEND_REQUEST` em ranges antigos enquanto ops live chegam à frente,
`Gaps Detected` cresce aos milhares, cai em snapshot-storm. O objetivo do dono é o modelo
**MySQL master/slave (binlog)**: o líder gera um log ordenado, **grava em disco** e o follower
**puxa por cursor durável, persiste em ordem e aplica no seu ritmo** — atrasando se preciso, mas
sempre convergindo, sem gap, sem re-pedido de sequência, sem snapshot-storm.

**Descoberta-chave desta sessão:** o trabalho pesado **já está implementado**. A investigação
(git log + leitura direta de código, não memória) mostrou que o RELAY_STREAM e as knobs de retenção
do binlog que o usuário pediu **já existem e estão commitados**. O que falta é: tornar o RELAY_STREAM
o caminho **definitivo** (remover o legado — decisão do usuário: 5.0.0), **migrar a Cardinal** para
consumi-lo, **validar em pré-prod 218/219** sob pressão real, e **publicar**.

---

## Estado atual verificado (fatos, não memória)

### nishi-utils — `/home/lucas/Projects/nishisan/nishi-utils`
- Branch atual `feature/resend-log-retention` (PR **#136** OPEN); `main` já contém o RELAY_STREAM
  (PR **#135** mergeado). Versão parent/módulos = **4.6.1**. Publica via `publish.yml` (GitHub
  Release → GitHub Packages; espelhado no Nexus Vivo `dev-nexus.tdigital-vivo.com.br/.../nishisan-dev/nishi-utils/`).
- **RELAY_STREAM 100% implementado** em `ngrid/replication/ReplicationManager.java`:
  `handleRelayStreamFetch` (líder serve o op-log durável), `handleRelayStreamBatch` (follower persiste
  em ordem), `relayFetchLoop`/`ensureRelayFetchLoop` (pull por cursor durável, 1 fetch em voo,
  idempotente), `isRelayStreamMode()`, commit **ASYNC** (binlog = fonte da verdade, sem push),
  cutover de snapshot re-ancorando o cursor. Protocolo: `MessageType.RELAY_STREAM_FETCH/BATCH` +
  `RelayStreamFetchPayload`/`RelayStreamBatchPayload`.
- **Fase 0 (saneamento de epoch) implementada:** epoch como termo de cluster monotônico
  (`fix(coordination): make leader epoch a monotonic cluster term`), fencing por identidade do líder
  acordado (`fix(replication): fence by agreed-leader identity, not epoch magnitude`), e **eleição por
  afinidade de prioridade + janela de descoberta no boot** (`feat(coordination): elect by leadership
  affinity with boot discovery window`).
- **Knobs de retenção do binlog do líder já existem** (branch atual, espelham `max_binlog_size`):
  `ReplicationConfig.resendLogSegmentMaxBytes` (long, default 0=off), `resendLogMaxSegments` (int,
  default 0=off), `relayExpireAfterWrite` (Duration, default ZERO=off). `ResendLog` rotaciona por bytes
  (`shouldRollBySize`) e retém por nº de segmentos. Já há `relayStreamFetchBatch`(512),
  `relayStreamPollInterval`(50ms), `relayStreamFetchTimeout`(2s), `relayMaxBacklog`(200k) no builder.
- **Legado ainda presente (alvo da Fase 8):** enum `FollowerIngestMode { INLINE(default), RELAY_LOG,
  RELAY_STREAM }`; `SEQUENCE_RESEND_REQUEST/RESPONSE`; reorder buffer (`relayReorderByTopic`,
  `RELAY_REORDER_CAP`, `drainReorderContiguous`); push em `replicateToFollowers`; drop policy do
  `OutboundChannel` (`isCountable`).

### Cardinal — `/home/lucas/Projects/vivo/tems/tevent-cardinal`
- Parent TEMS `/home/lucas/Projects/vivo/tems/pom.xml` pina `<nishi-utils-core.version>4.6.1</…>` (linha 43).
  Empacotado como jar fat (shade). Roda como `java -jar /app/tevent-cardinal-<v>.jar /app` (base dir
  `/app`: `config/` JSON + `var/`). Config externa (não há yml/json no repo de produção; exemplo em
  `deploy/docker/dev/config/cardinal-ha/card-a.json`). Branch `feature/cardinal-ha-deterministic-oplog`.
- Config HA em **tevent-lib** (`com.linuxrouter.cardinal.config`): `CardinalHAConfiguration`
  (mode/clusterName/nodes/election/replication + `validate()`), `HAReplicationConfig`
  (`followerIngestMode` "INLINE"/"RELAY_LOG", `leaderPauseOnJoin`=true, `persistentResendLog`=true,
  `logRetentionCount`=100k, `logRetention`="PT30M", `dataDir`, `drainGateBackstopMs`, …),
  `HAElectionConfig` (heartbeat/timeout/lease/minClusterSize), `HANodeConfig` (id/host/port/**priority**).
- `CardinalClusterManager` monta `ReplicationConfig.builder(1)` a partir de `ha.getReplication()`
  (linhas ~160-171). **Ponto que quebra na Fase 8:** linhas **172** e **280**
  `boolean relayMode = resolveFollowerIngestMode() == FollowerIngestMode.RELAY_LOG;` — só reconhece
  RELAY_LOG, e RELAY_LOG **deixará de existir**. `relayMode` governa o gate (`decideGateAction`) e o
  `CardinalReplicator`.
- **`leaderPauseOnJoin` (#129) É o `pauseOnJoin` do item 11:** a lib pausa as escritas do líder enquanto
  um follower que entra alcança; a Cardinal **respeita** via `ConsumerCardinalThread` →
  `cm.shouldHoldConsumerForFollowerSync()` (= `replicationManager.isJoinQuiescing()`) e
  `CardinalReplicator.active()` (guard `!isJoinQuiescing()`).
- **Kafka/offset (itens 8-9):** group.id = `clusterName` (compartilhado), `AUTO_OFFSET_RESET=earliest`,
  `MAX_POLL_INTERVAL=900s`, `commitAsync` após enfileirar; só o líder faz `subscribe` (follower
  `unsubscribe`+`commitSync`). No failover, o promovido `subscribe` e o **consumer group** retoma do
  último offset commitado → continuidade sem gap. `seedSequence(max(global, applied))` ancora a seq.

---

## Decisões desta sessão (do usuário)
1. **Lib 5.0.0 DEFINITIVA** — executar a **Fase 8** (remover INLINE/RELAY_LOG + push/NAK/reorder/drop),
   RELAY_STREAM vira default. Alinha ao princípio "sempre o definitivo, sem fallback para legado".
2. **Validação só em pré-prod 218/219** — sem teste de integração JVM in-repo. A suíte de
   **resiliência da própria nishi-utils** (`-Presilience`) permanece como **gate obrigatório de build**
   antes de publicar.
3. **Líder preferido = node-1 (10.200.20.218)** — maior prioridade; node-2 (219) assume no failover e
   devolve a liderança por afinidade quando node-1 volta e sincroniza (item 10).
4. Base de desenvolvimento local da lib = **5.0.0** (não 4.7.0) — evita a colisão com o jar 4.6.1 local
   que já foi reconstruído com RELAY_STREAM.

---

## Frente A — nishi-utils 5.0.0 (Fase 8 + retenção + build local)

> Reusa e finaliza o plano já aprovado `nishi-utils/planning/2026-06-08-relay-stream-replication.md`
> (Fase 8) e `…/2026-06-08-resend-log-retention.md` (já implementado). Branch:
> `feature/resend-log-retention` → após Fase 8, fechar PR #136 e abrir o cutover 5.0.0.

### A.1 — Fase 8: remoção definitiva do legado
Arquivos críticos: `ngrid/replication/ReplicationManager.java`, `…/FollowerIngestMode.java`,
`…/ReplicationConfig.java`, `ngrid/common/MessageType.java`, `ngrid/structures/NGridConfig.java`,
`…/NGridNodeBuilder.java`, `cluster/transport/OutboundChannel.java`.
- Remover do enum `FollowerIngestMode` os valores `INLINE` e `RELAY_LOG`; manter só `RELAY_STREAM`.
- Default do builder → `RELAY_STREAM` em **3 pontos**: `ReplicationConfig.Builder:455`,
  `NGridNodeBuilder:57`, `NGridConfig:540`. Atualizar Javadocs (refs a INLINE/RELAY_LOG em
  `ReplicationConfig:211-212,596`, `NGridConfig:259,314,830`, `NGridNodeBuilder:203`).
- Remover o protocolo NAK: `MessageType.SEQUENCE_RESEND_REQUEST/RESPONSE` + handlers + payloads.
- Remover o reorder buffer (`relayReorderByTopic`, `RELAY_REORDER_CAP`, `drainReorderContiguous`,
  `bufferRelayResendOperations`) e o ramo de gap em `drainRelayOnce` (em stream o relay nunca tem
  buraco → caminho morto).
- Remover o broadcast push em `replicateToFollowers` (manter `leaderEmissionLock` por tópico /
  sequência monotônica + o espelhamento no `resendLogStore`) e a drop policy do `OutboundChannel`
  (`isCountable` — sem push, sem drop).
- Testes: reescrever/remover `RelayLogReplicationTest`, `SequenceResendProtocolTest`,
  `OutboundChannelTest` (drop); ajustar `NGridConfigLoaderTest`, `NGridConfigValidationTest`,
  `ReplicationConfigTest`, `PersistentResendLogRegressionTest`, `ProactiveColdJoinWatermarkTest`,
  `ReplicationLogTimeRetentionTest`, `FollowerSnapshotCutoverTest` onde citam modos legados.

### A.2 — Ergonomia para o consumidor
- Adicionar `FollowerIngestMode.isRelay()`? Desnecessário pós-Fase 8 (só há RELAY_STREAM). A Cardinal
  deixa de ter lógica de modo (ver Frente B).

### A.3 — Versão 5.0.0 + build local
- Bump 4.6.1 → **5.0.0**: `mvn versions:set -DnewVersion=5.0.0 -DprocessAllModules=true -DgenerateBackupPoms=false`.
- Build/instala no `~/.m2` para integração local (sem Nexus/GitHub):
  `mvn -q -pl nishi-utils-core -am clean install -DskipTests` (gera `dev.nishisan:nishi-utils-core:5.0.0`).
- **Gate obrigatório antes de qualquer publish** (CI não roda a suíte ngrid):
  `mvn -pl nishi-utils-core test` (unit) **e** `mvn -pl nishi-utils-core test -Presilience -Dsurefire.rerunFailingTestsCount=1`
  (RELAY_STREAM: `streamContiguousUnderFirehoseNoNak`, `slowFollowerLagsButNeverLoses`,
  `restartResumesFromCursorNoSnapshot`, `failoverContinuesBinlogAcrossPromotion`, e os de epoch/afinidade).
- Atualizar `doc/` (pt-BR) + `doc/CHANGELOG.md` (breaking: RELAY_STREAM default, remoção de INLINE/RELAY_LOG/NAK).

---

## Frente B — Cardinal: migração para RELAY_STREAM (compila contra 5.0.0)

> A remoção de INLINE/RELAY_LOG **quebra a compilação** da Cardinal (refs em
> `CardinalClusterManager:172,280`). A migração é obrigatória, não opcional.

### B.1 — Config (tevent-lib `HAReplicationConfig` + `CardinalHAConfiguration`)
- `HAReplicationConfig.followerIngestMode` default → `"RELAY_STREAM"`.
- **⚠ Allowlist no `CardinalHAConfiguration.validate()`:** há hoje uma allowlist explícita
  (`!"INLINE".equalsIgnoreCase(t) && !"RELAY_LOG".equalsIgnoreCase(t)`) que **rejeita RELAY_STREAM** —
  o boot falharia com `HAConfigurationException` **antes** de `resolveFollowerIngestMode()`. No 5.0.0,
  trocar para aceitar **só RELAY_STREAM** (mensagem clara rejeitando INLINE/RELAY_LOG, que não existem
  mais no enum). `resolveFollowerIngestMode()` idem.
- Novos campos em `HAReplicationConfig` (defaults generosos, já com relay log — pedido do usuário), com
  getters/setters de bean (Jackson/Gson): `long resendLogSegmentMaxBytes = 10_737_418_240L` (10GB/seg),
  `int resendLogMaxSegments = 10`, `String relayExpireAfterWrite = ""` (vazio=ZERO=off) +
  `@JsonIgnore Duration resolveRelayExpireAfterWrite()` (espelha `resolveLogRetention()`).
- Guardrails em `validate()` (RELAY_STREAM): `resendLogSegmentMaxBytes` = 0 **ou** `>= 1MiB`;
  `resendLogMaxSegments` = 0 **ou** `>= 2`; **não** ambos 0 (binlog sem teto cresceria sem bound);
  `relayExpireAfterWrite` se não-vazio → `Duration.parse` válido e `> 0`.
- **`persistentResendLog`:** em stream o `resendLogStore` é inicializado **incondicionalmente** na lib
  (`ReplicationManager` ~355-364, independe da flag) — então exigir `true` é coerência de config, não
  requisito funcional. Não bloquear o boot; o manager pode forçar `true` + log (ver B.2).

### B.2 — Wiring (`CardinalClusterManager`)
- Encadear no `ReplicationConfig.builder` (bloco ~160-171): `.resendLogSegmentMaxBytes(...)`,
  `.resendLogMaxSegments(...)`, `.relayExpireAfterWrite(ha.getReplication().resolveRelayExpireAfterWrite())`.
  Em stream, passar `.persistentResendLog(true)` com `logger.info` (coerência — ver B.1).
- **Remover `relayMode`** (confirmado em **exatamente 2 pontos**: linhas **172** e **280**; sem terceiro)
  e a lógica dual INLINE-vs-RELAY de `decideGateAction`: com RELAY_LOG/INLINE removidos, relay é o único
  mundo → o gate é sempre "nunca ativa enquanto `isLeaderSyncing()`" (o ramo INLINE grace-force vira
  código morto → remover). `CardinalReplicator` deixa de receber o flag no construtor; seu
  `active()` guard `!isLeaderSyncing()` continua válido (sempre relay).
- Reescrever os testes que assumiam dois modos: `CardinalClusterManagerTest` (casos INLINE vs RELAY_LOG
  do `decideGateAction` — agora caso único), `CardinalReplicationHandlerTest`.
- **Teste unitário novo (mandatório, roda em `mvn test`, NÃO é o IT pré-prod):** validação de
  `HAReplicationConfig`/`CardinalHAConfiguration` para RELAY_STREAM — aceitação + guardrails
  (`segmentMaxBytes < 1MiB` rejeitado, `maxSegments==1` rejeitado, ambos 0 rejeitado). Em
  `tevent-lib/src/test` (ex.: `HAReplicationConfigValidationTest`).

### B.3 — Dependência + rebuild (Java 21 obrigatório)
- `tems/pom.xml` linha 43 → `<nishi-utils-core.version>5.0.0</nishi-utils-core.version>` (resolve do `~/.m2` local).
- Build/instala a lib local: `JAVA_HOME=<java-21> mvn -f nishi-utils/pom.xml -pl nishi-utils-core -am -DskipTests install`.
- Rebuild do fat-jar da Cardinal (+ tevent-lib): `JAVA_HOME=<java-21> mvn -f tems/pom.xml -pl tevent-cardinal -am -DskipTests package`.
  Artefato de deploy: `dist/tevent-cardinal/tevent-cardinal-3.1.0.jar` (gate `dist.skip=false`).
- Suíte: `mvn -pl tevent-lib,tevent-cardinal test` (o pom da Cardinal não tem failsafe/profile de IT;
  `*IT` rodam por `-Dtest=NomeIT` — convenção do `OpLogReplicationIT` existente).

---

## Frente C — Validação em pré-produção (218/219) — a "integração" oficial

Servidores: **10.200.20.218** `/app/node-1` (root, **líder preferido**, Xmx48g) e **10.200.20.219**
`/app/node-2` (root, Xmx16g). Cardinais paradas; `minClusterSize:1` (pairMode). Config JSON `ha` de cada
nó (node-1 mostrado; node-2 muda `dataDir` e a `priority` do próprio nó):
```json
"ha": {
  "mode": "cluster", "clusterName": "cardinal-preprod",
  "nodes": [
    { "id": "card-a", "host": "10.200.20.218", "port": 9090, "priority": 100 },
    { "id": "card-b", "host": "10.200.20.219", "port": 9090, "priority": 50 }
  ],
  "election": { "heartbeatIntervalMs": 2000, "heartbeatTimeoutMs": 6000, "leaseTimeoutMs": 18000, "minClusterSize": 1 },
  "replication": {
    "topic": "cardinal-state",
    "dataDir": "/app/node-1/var/cardinal/replication",   // ⚠ ABSOLUTO (regressão de epoch veio de var/ relativo)
    "logRetention": "PT30M", "logRetentionCount": 100000,
    "followerIngestMode": "RELAY_STREAM",
    "persistentResendLog": true, "leaderPauseOnJoin": true,
    "resendLogSegmentMaxBytes": 10737418240,             // 10GB/segmento (max_binlog_size)
    "resendLogMaxSegments": 10                           // ≤10 segmentos → até ~100GB binlog/topic (checar disco)
  }
}
```
Afinidade: **node-1 priority 100 > node-2 priority 50** (node-1 preferido). Confirmar que o start script
**não faz `cd`** para diretório diferente do que ancora o `dataDir` absoluto.

Layout do `var/.../replication`: `leader-epoch.dat`, `sequence-state.dat`, `relay/<topic>/`,
`resend-log/<topic>/seg-NNNNNNNNNN.dat`.

### C.1 — Inventário read-only (antes de tudo)
- Layout de `/app/node-{1,2}`: jar atual, mecanismo de start (systemd/nohup/script — e seu CWD),
  `config/` JSON atual (**guardar cópia p/ rollback**), `var/`, e `java -version` (Java 21).
- **Espaço em disco** na partição do `var/`: `df -h`. Piso recomendado **~120GB livres/nó** (100GB binlog
  + relay do follower + snapshots + folga). **Abort se não houver folga.**

### C.2 — Deploy (guardar bundle antigo: jar 4.6.1 + config — ver rollback)
- Subir o fat-jar reconstruído (contra nishi-utils-core **5.0.0** local) nos dois nós.
- Aplicar a nova config (RELAY_STREAM + knobs generosas + `dataDir` absoluto + prioridades).
- **Limpar `var/` por completo** em cada nó **antes de cada (re)teste** (ênfase do usuário): remover
  `relay/`, `resend-log/`, `leader-epoch.dat`, `sequence-state.dat` e filas/snapshots antigos. Epoch
  persistido de uma topologia anterior desbalanceia eleição/cutover; formato de binlog/cursor antigo é
  incompatível. (Operação destrutiva — executar só no deploy, com confirmação.)

### C.3 — Sequência de validação (mapeada nos 11 passos)
1. **Eleição:** subir os 2 nós → node-1 (preferido) eleito líder; node-2 follower. Conferir `Leader
   Epoch` **igual** nos dois (saneamento de epoch).
2-4. **Op-log no líder:** ligar firehose Kafka; node-1 consome, gera deltas, grava no binlog (`var/.../resend-log/<topic>/seg-*.dat`).
5. **Follower ingere/aplica:** node-2 puxa por `RELAY_STREAM_FETCH`, persiste no relay em ordem, aplica;
   `Applied` de node-2 acompanha node-1; **`Gaps Detected == 0`**, **sem `SEQUENCE_RESEND_REQUEST`** nos logs.
6. **Atraso controlado:** induzir lentidão no apply do follower (ou aumentar firehose) → o lag cresce,
   `relayMaxBacklog` segura o fetch, e ao cessar a pressão o follower **converge sem snapshot** nem perda.
7-9. **Failover (queda do líder):** matar node-1 → node-2 aplica o backlog pendente do relay, é promovido
   (gate só ativa quando `isLeaderSyncing()==false`), `subscribe` no Kafka e **retoma do offset do
   consumer group** (sem reprocessar/pular; dedup por `identifier`). Conferir seq monotônica
   (`seedSequence`) e binlog contínuo na promoção.
10. **Retorno do líder original:** religar node-1 → ele entra como follower, **sincroniza** (pauseOnJoin/
    quiesce: node-2 pausa escritas enquanto node-1 alcança), e **reassume por afinidade** (maior
    prioridade) com handoff limpo (epoch re-carimbado, fencing por identidade). Continuidade de offset
    do Kafka preservada no segundo handoff.
11. **pauseOnJoin:** durante C.3-10, confirmar que node-1 recém-entrado **não processa** (consumer
    segurado por `shouldHoldConsumerForFollowerSync`) antes de estar sincronizado.

### C.4 — Métricas/observação (accessors da lib confirmados)
`coordinator.getLeaderEpoch()` **igual nos 2 nós** (estável); `getGapsDetected()==0`;
`getSnapshotFallbackCount()==0` após o cold-join (1 snapshot inicial é aceitável; storm = falha);
no follower `getRelayStreamCursor(topic)` converge a `getLeaderHighWatermark(topic)`,
`getReplicationLag(topic)`~0, `isStreaming(topic)=true`, `getStreamBytesIn(topic)` crescente;
`getGlobalSequence()`/`getLastAppliedSequence()` monotônicos no failover; segmentos
`resend-log/<topic>/seg-*.dat` respeitando ~10GB e **≤10 arquivos**; nenhum `SEQUENCE_RESEND_REQUEST`/NAK
nos logs (no 5.0.0 esse protocolo nem existe mais).

### C.5 — Critérios de abort/rollback
Abort (parar e diagnosticar — não avançar sobre estado inconsistente): epoch divergente persistente,
gate em `ALARM_AND_WAIT` estourando o backstop (STANDBY com alarme), `getGapsDetected()` crescente,
`getSnapshotFallbackCount()` em loop, binlog excedendo o teto de disco. **Rollback é binário** (no 5.0.0
não há fallback de config: INLINE/RELAY_LOG não existem mais): parar os 2 nós, restaurar o **bundle
antigo** (jar nishi-utils 4.6.1 + config anterior), **limpar `var/`** (formato incompatível) e subir smoke.

---

## Frente D — Release 5.0.0 (após validação verde)
1. `nishi-utils`: fechar/mergear a branch (Fase 8 + retenção) → `main`. `gh release create v5.0.0`
   (título + changelog rico: breaking RELAY_STREAM default, remoção de legado/NAK, knobs de binlog) →
   `publish.yml` publica core+oss no Packages/Nexus.
2. `tems`: bump `nishi-utils-core.version` 5.0.0 (do Nexus, não mais `~/.m2`), rebuild da Cardinal contra
   o artefato publicado, e release TEMS (bump de minor) para congelar a integração.

---

## Mapa fluxo do usuário (11 passos) → mecanismo
| # | Passo | Onde já é atendido |
|---|---|---|
| 1 | Líder eleito | Fase 0.4 afinidade por prioridade (lib) + `encodeNodeId(priority,id)` (Cardinal) |
| 2-3 | Líder gera e grava op-log | `ResendLog/ResendLogStore` (binlog segmentado em disco) |
| 4 | Envia aos followers | `handleRelayStreamFetch` (líder serve por cursor) |
| 5 | Follower ingere/aplica incremental | `relayFetchLoop` + `handleRelayStreamBatch` + apply loop |
| 6 | Follower atrasa mas aplica backlog | cursor durável + `relayMaxBacklog` (flow-control) |
| 7 | Queda do líder → aplica backlog | relay durável drenado antes da ativação (gate `isLeaderSyncing`) |
| 8 | Continua do offset Kafka correto | `subscribe` + consumer group retoma offset commitado |
| 9 | Consumer group garante continuidade | group.id = `clusterName`, `commitAsync`/`commitSync` |
| 10 | Líder original reassume após sincronizar | afinidade (prioridade) + epoch term + fencing por identidade |
| 11 | pauseOnJoin antes de processar | `leaderPauseOnJoin`(#129) + `shouldHoldConsumerForFollowerSync` |

---

## Riscos e armadilhas
- **dataDir relativo:** a regressão de epoch 7→1 em pré-prod veio de `dataDir` relativo + `var/` recriado
  no cutover. Usar **caminho absoluto** e validar.
- **Disco do binlog:** 10×10GB/topic ⇒ até ~100GB por tópico no líder. Confirmar espaço em `/app/node-1`.
- **Quiesce vs `max.poll.interval` (900s):** quiesce repetido em failover múltiplo pode aproximar o
  timeout do consumer group; o código já re-`subscribe` após retomar — observar nos drills.
- **Compile-break em cascata:** remover INLINE/RELAY_LOG quebra a Cardinal **e** quaisquer testes/usos
  na lib — a Frente A e B andam juntas; o build TEMS só fecha após a Cardinal migrar.
- **`commitAsync` + crash entre poll e processamento:** janela de reprocesso mitigada por dedup
  (`identifier`); documentar.
- **Fase 8 é breaking de API** (enum/menos default): nada de fallback de runtime — é o "definitivo".

---

## Verificação end-to-end
- **Lib:** `mvn -pl nishi-utils-core test` + `mvn -pl nishi-utils-core test -Presilience -Dsurefire.rerunFailingTestsCount=1`;
  build `mvn -B -DskipTests -pl nishi-utils-core,nishi-utils-oss -am package`.
- **Cardinal:** `mvn -pl tevent-lib,tevent-cardinal -am clean package` + suíte de testes.
- **Pré-prod (read-only após deploy):** stats com `Leader Epoch` igual, `Gaps Detected==0`, `Applied`
  acompanhando, lag estável, `seg-*.dat` ≤10 e ≤10GB, sem `SEQUENCE_RESEND_REQUEST`; drills de
  failover/retorno conforme C.3.

---

## Arquivos críticos
**nishi-utils:** `ngrid/replication/{ReplicationManager,ReplicationConfig,FollowerIngestMode}.java`,
`ngrid/common/MessageType.java`, `ngrid/structures/{NGridConfig,NGridNodeBuilder}.java`,
`cluster/transport/OutboundChannel.java`, `pom.xml` (versão 5.0.0), `doc/`+`CHANGELOG.md`,
testes do pacote `ngrid/replication`.
**Cardinal/TEMS:** `tems/pom.xml:43` (versão da dep), `tevent-lib/.../config/HAReplicationConfig.java`
(+`CardinalHAConfiguration.validate`), `tevent-cardinal/.../cluster/CardinalClusterManager.java`
(linhas 160-172, 280; remoção do `relayMode`), `…/CardinalReplicator.java`, `…/CardinalReplicationHandler.java`,
testes `CardinalClusterManagerTest`/`CardinalReplicationHandlerTest`. Config de deploy de cada nó (JSON).

---

## Sequência de commits (atômicos; cada um compila e passa)
**nishi-utils (branch `feature/resend-log-retention` → cutover 5.0.0):**
1. `refactor(replication): remove push+NAK+reorder; RELAY_STREAM default` (Fase 8)
2. `test(replication): reescreve suites legadas para o modelo de stream`
3. `chore(release): bump 5.0.0 + CHANGELOG (breaking) + doc`

**Cardinal/TEMS (branch `feature/cardinal-relay-stream`):**
4. `feat(ha): knobs de retenção do binlog (resendLogSegmentMaxBytes/MaxSegments) em HAReplicationConfig`
5. `refactor(ha): migra Cardinal para RELAY_STREAM único; remove relayMode/gate dual`
6. `chore(deps): nishi-utils-core 5.0.0 + rebuild`

**Release (após validação pré-prod):** 7. `gh release create v5.0.0` (lib) → 8. bump Nexus na TEMS + release TEMS.

---

## Orquestração por time de agentes (execução)
O usuário quer conduzir com um time de agentes. Sugestão de fan-out por workflow, respeitando a ordem:
- **Fase Lib (A):** 1 agente executor da Fase 8 (worktree isolado) + 1 agente que reescreve/roda a suíte
  de testes; gate de resiliência como barreira antes de prosseguir.
- **Fase Cardinal (B):** 1 agente para o plumbing de config (tevent-lib) + 1 para o wiring/remoção de
  `relayMode` (tevent-cardinal), em paralelo, com build TEMS como barreira.
- **Fase Pré-prod (C):** condução sequencial (deploy → drills) — não paralelizar ações nos servidores.
A execução das Frentes A e B é acoplada (o build TEMS só fecha com a lib 5.0.0 instalada localmente).
