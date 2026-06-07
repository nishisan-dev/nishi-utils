# Plano — Issues #122 (GAP A) e #123 (GAP B)

## Context

Ambas as issues nascem do caso de uso **`tevent-cardinal`** (HA active-standby por op-log,
construído sobre a `nishi-utils` 4.2.0) e endereçam lacunas no `ReplicationManager`/`ReplicationConfig`
do módulo **nishi-utils-core**. São independentes entre si, mas tocam os mesmos arquivos e o mesmo
fluxo de failover/recovery, então serão entregues juntas (em commits atômicos separados).

- **#122 [GAP A] — Retenção temporal do resend log.** O resend log do op-log
  (`ReplicationManager.replicationLogBySequence`) é trimado **apenas por contagem**
  (`replicationLogRetention`, default 1000). Não há como configurar **por quanto tempo** o backlog
  fica disponível para um follower que reingressa antes de cair em snapshot. O cardinal já tem o
  campo `ha.replication.logRetention` (ISO-8601) validado do lado dele; falta o wiring na lib.

- **#123 [GAP B] — Preempção de liderança não gateada por sync.** Ao reassumir liderança, o novo
  líder marca `leaderSyncing=true` e dispara `attemptLeaderSync` assíncrono, mas:
  1. `replicate()` **não checa `isLeaderSyncing()`** → aceita escrita com estado velho durante o
     catch-up (janela de divergência), e `MapClusterService` sequer tem o pre-check que o
     `QueueClusterService` tem.
  2. `attemptLeaderSync` deixa `leaderSyncing=true` preso quando `resolveSyncSource()==null`
     (nó sozinho / primeiro líder de cluster novo) → consumidor que gateia em `isLeaderSyncing()`
     fica travado para sempre.

**Resultado esperado:** janela de backlog configurável por tempo (complementar ao teto de contagem),
e fechamento robusto da janela de divergência no failover, eliminando o *grace* que o cardinal hoje
usa como salvaguarda.

**Decisões confirmadas com o usuário:**
- #122: expor o setter de retenção temporal na `ReplicationConfig.Builder` **e** na `NGridConfig`
  (facade), com passthrough no `NGridNode`.
- #123: o gate é **fail-fast** — `replicate()` **lança** `LeaderSyncingException` (não bloqueia).

---

## Issue #122 — Retenção temporal do resend log

Abordagem **definitiva**: trim temporal nativo do `replicationLogBySequence` (NQueue **não** está
nesse caminho — o resend log é um `NavigableMap` em-memória; introduzir NQueue aqui seria reescrita
desnecessária). O timestamp é **leader-local** (instante do commit/index), **sem** alterar o
`ReplicationPayload` (que é tipo de wire serializado por Jackson e enviado a followers).

### Mudanças

**1. `ReplicationConfig` / `ReplicationConfig.Builder`**
(`nishi-utils-core/.../ngrid/replication/ReplicationConfig.java`)
- Novo campo `Duration replicationLogRetentionTime` (default **desabilitado** = `Duration.ZERO`,
  seguindo a convenção `0 = desabilitado` da NQueue, `NQueue.java:1357`).
- Setter `Builder.replicationLogRetentionTime(Duration)` no mesmo padrão fluent dos demais
  (validação: `Objects.requireNonNull` + rejeitar negativo; zero = desabilitado).
- Getter `replicationLogRetentionTime()`. Atualizar construtor privado e `build()`.

**2. `ReplicationManager`** (`.../ngrid/replication/ReplicationManager.java`)
- Trocar o tipo de valor do resend log (linha 141) de `ReplicationPayload` para um record
  privado `TimedPayload(ReplicationPayload payload, long indexedAtMillis)`.
- `indexReplicationPayload()` (1097-1107): gravar `new TimedPayload(payload, System.currentTimeMillis())`;
  manter o trim **por contagem** existente; **adicionar** eviction temporal oportunística do head
  (enquanto `now - head.indexedAtMillis > retentionTimeMillis`, `pollFirstEntry()`) — espelha o
  prefixo contíguo de `NQueue.skipExpiredRecordsLocked()` (`NQueue.java:1065-1101`). Ambos os passes
  rodam → **o que evictar primeiro vence** (contagem = teto de memória; tempo = janela de backlog).
- Serve-resend (1177-1196): `TimedPayload tp = topicLog.get(seq)` → usar `tp.payload()`; `tp==null`
  continua indo para `missingSequences` (caminho de gap → snapshot fallback **já existente**).
- Eviction temporal para tópicos **ociosos** (sem novos commits): estender o passe agendado
  `trimLog()` (1785-1798, já agendado a cada `max(5s, 5×timeout)`) para também varrer
  `replicationLogBySequence` e evictar heads expirados por tempo, quando a retenção temporal estiver
  habilitada. Sem isso, um tópico que parou de escrever nunca liberaria o backlog dentro da janela.
- Observabilidade: contador `AtomicLong replicationLogTimeEvictedCount` + getter
  `getReplicationLogTimeEvictedCount()` (mesmo estilo de `getGapsDetected()` etc.), e um getter de
  tamanho do resend log por tópico para asserts de teste.

**3. Wiring na facade** (`.../ngrid/structures/NGridConfig.java` + `NGridNode.java`)
- `NGridConfig.Builder`: adicionar o par coeso `replicationLogRetention(int)` **e**
  `replicationLogRetentionTime(Duration)` (campos + getters + setters), resolvendo a inconsistência
  apontada (não expor só o tempo sem o teto de contagem companheiro).
- `NGridNode` (bloco 351-369): repassar ambos para o `replicationBuilder` quando não-nulos (mesmo
  padrão do `replicationOperationTimeout`, linhas 352-354).

> Nota: a camada de config YAML (`${VAR}` interpolation) não mapeia os knobs finos de replicação hoje;
> manter fora do escopo desta entrega (programático via Builder apenas), salvo pedido em contrário.

---

## Issue #123 — Gate de sync + clear correto

### Parte 1 — Gate fail-fast em `replicate()`

**1. Nova exceção** `LeaderSyncingException`
(`.../ngrid/replication/LeaderSyncingException.java`)
- `public final class LeaderSyncingException extends IllegalStateException` — estende
  `IllegalStateException` (não `RuntimeException`) **de propósito**: o pre-check atual já lança
  `IllegalStateException("Leader sync in progress")` (`QueueClusterService.java:597`) e o próprio
  `replicate()` lança `IllegalStateException` para "not leader" (`ReplicationManager.java:382`).
  Estender mantém compatibilidade com chamadores/testes que capturam `IllegalStateException`,
  refinando o tipo. Construtores `(String)` e `(String, Throwable)`.

**2. Gate em `replicate()`** (`ReplicationManager.java:379-402`)
- Inserir, logo após o check `isLeader()` (382) e antes do `hasValidLease()` (384):
  `if (isLeaderSyncing()) { throw new LeaderSyncingException("Leader is syncing, write rejected to prevent stale-state divergence"); }`.
  Cobre **todos** os backends (Map e Queue), defesa em profundidade.

**3. Alinhar o pre-check do Queue** (`QueueClusterService.java:595-599`)
- `ensureLeaderReady()` passa a lançar `LeaderSyncingException` (ainda é `IllegalStateException` →
  sem quebra). Mantido como fail-fast antecipado (evita montar o command à toa); a fonte de verdade
  do gate passa a ser o `replicate()`.

### Parte 2 — Clear de `leaderSyncing` sem syncSource

**`attemptLeaderSync()`** (`ReplicationManager.java:1601-1612`)
- No early-return de `syncSource == null` (1606-1607): antes de retornar, **limpar o estado** —
  `leaderSyncTopics.clear(); leaderSyncing.set(false);` + log informativo.
  Justificativa de segurança: `resolveSyncSource()` retorna `null` **somente** quando não há nenhum
  outro membro ativo alcançável (`ReplicationManager.java:1588-1598`); logo este nó é
  necessariamente a réplica mais avançada alcançável e pode liderar. No cenário real de divergência
  da issue (L1 reingressa com L2 ativo), L2 está em `activeMembers()` e `resolveSyncSource` retorna
  L2 — o sync prossegue normalmente, não cai neste ramo.
- Isso destrava `NGrid.local(1)` e o barrier de readiness em `NGridLocalBuilder.java:285-287`.

---

## Arquivos críticos

| Arquivo | Mudança |
|---|---|
| `.../ngrid/replication/ReplicationConfig.java` | #122: campo/getter/setter `replicationLogRetentionTime` |
| `.../ngrid/replication/ReplicationManager.java` | #122: `TimedPayload`, index/serve/trim temporal, métrica · #123: gate em `replicate()`, clear em `attemptLeaderSync` |
| `.../ngrid/replication/LeaderSyncingException.java` | #123: **novo** (extends `IllegalStateException`) |
| `.../ngrid/queue/QueueClusterService.java` | #123: `ensureLeaderReady()` lança `LeaderSyncingException` |
| `.../ngrid/structures/NGridConfig.java` | #122: facade `replicationLogRetention` + `replicationLogRetentionTime` |
| `.../ngrid/structures/NGridNode.java` | #122: passthrough p/ `replicationBuilder` (bloco 351-369) |

**Reuso:** padrão de eviction por prefixo contíguo de `NQueue.skipExpiredRecordsLocked()`
(`NQueue.java:1065-1101`); padrão de métrica `AtomicLong` + getter (`getGapsDetected()`,
`ReplicationManager.java:148-157`); padrão de exceção `LeaseExpiredException`; caminho de fallback
`missingSequences` → snapshot já existente (`ReplicationManager.java:1191-1205` + `checkForMissingSequences` 1031-1087).

---

## Testes

Convenção: `*Test.java` (in-process), profile `-Presilience`; timing determinístico via
`Thread.sleep` com `Duration` curta (padrão de `NQueueExpireTest`) e polling/`awaitClusterConsensus`.

**#122** — novo `ReplicationLogTimeRetentionTest` (espelha `ReplicationMemoryEvictionTest`):
- `retentionTime=100ms`: indexar payloads, `sleep(200ms)`, indexar um novo (dispara eviction
  oportunística) → asserir que os antigos saíram e `getReplicationLogTimeEvictedCount()>0`.
- Resend de sequência já expirada por tempo → resposta com `missingSequences` (assert via
  follower caindo em `getSnapshotFallbackCount()>0`).
- Retenção temporal desabilitada (default `ZERO`) → comportamento count-only inalterado (regressão).
- Convivência tempo×contagem (o que evictar primeiro vence).

**#123** — novo `LeaderSyncGateTest`:
- Com `leaderSyncing=true`, `replicate()` lança `LeaderSyncingException`; com `false`, prossegue.
- Cluster single-node `NGrid.local(1)`: após eleição, `isLeaderSyncing()` converge para `false`
  (valida o clear-on-null) e o primeiro `offer/put` **não** lança "Leader sync in progress".
- Regressão de failover: `QueueNodeFailoverIntegrationTest` / `SequenceGapRecoveryIntegrationTest`
  continuam verdes (gate não introduz deadlock no caminho de promoção real).

**Comandos:**
```bash
mvn -pl nishi-utils-core clean install -DskipTests   # rebuild do módulo após mudança de API
mvn -pl nishi-utils-core test -Dtest=ReplicationLogTimeRetentionTest
mvn -pl nishi-utils-core test -Dtest=LeaderSyncGateTest
mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1   # suite de resiliência completa
```

---

## Documentação (Definition of Done)

- Atualizar `doc/` (pt-BR) cobrindo `replicationLogRetentionTime` (semântica tempo×contagem,
  fallback para snapshot) e o gate de `LeaderSyncingException` no failover.
- Atualizar Javadoc dos novos métodos (`mvn verify -Pvalidate-javadoc` deve passar).
- Diagrama de sequência do failover com o gate, se houver `docs/diagrams/`.

## Execução

- Branch única `feature/replication-retention-and-sync-gate` (ou duas: `feature/replication-log-retention-time` + `fix/leader-sync-gate`), commits atômicos por responsabilidade lógica:
  1. #122 config (ReplicationConfig) · 2. #122 manager (trim temporal + métrica) · 3. #122 facade
     (NGridConfig/NGridNode) · 4. #123 exceção + gate · 5. #123 clear-on-null · 6. testes · 7. docs.
- `mvn clean install` no módulo ao mudar a API (CLAUDE.md §5.1) antes de rodar a suíte completa.
