# Plano — Hardening RELAY_LOG (#127, #128, #129) + Broadcast inter-nós

## Context

Durante a validação do `tevent-cardinal` (TEMS) em **HA de 2 nós** com o `ReplicationManager` em modo **RELAY_LOG** (#124), em pré-prod (218/219) sob carga real de produção, três falhas distintas porém interligadas foram observadas no bootstrap/convergência do follower sob alta produção. As três issues nasceram dessa medição real e se referenciam mutuamente:

- **#127** — o op-log de resend do líder vive em **heap** (`NavigableMap<Long,TimedPayload>`/TreeMap sincronizado). Sob alta produção a retenção por **contagem** vence a janela **temporal** (cobrir 30min a ~5x estouraria o heap), a janela vira segundos, o follower instala o snapshot em `S`, pede `S+1..` e o líder **já evictou** → "missing sequences → snapshot fallback" em loop; o relay do follower cresce sem limite (18 GB observados).
- **#128** — o **apply do follower não acompanha** a produção do líder sob burst (~1.850 vs ~2.600+ ops/s); o lag cresce monotonicamente. O drain do relay é 1-a-1 (`peek→apply→poll`). Falta também **métrica pública** de tamanho/idade do relay.
- **#129** — em RELAY_LOG o follower só sincroniza **reativamente** (ao ver gap no tráfego); contra um líder **quiescente**, um follower **novo** nunca sincroniza. Além disso a **auto-eleição transitória no boot** (pairMode/minClusterSize=1) marca o nó como "caught-up no vazio" e polui o bootstrap subsequente.

Em paralelo, queremos uma primitiva de **mensageria leve inter-nós** sobre o `ReplicationManager` (`broadcastMessage(String)` + listener `onMsgBroadcasted` com a identidade do produtor) para que quem usa o `ReplicationManager` coordene melhor as ações.

**Resultado pretendido:** convergência determinística do follower sob carga (sem espiral de re-snapshot, sem crescimento ilimitado do relay), throughput de apply acima da produção típica, observabilidade de lag, e uma API simples de coordenação entre nós.

## Decisões aprovadas (usuário)

| Tema | Decisão |
|---|---|
| **#127 storage** | **Híbrido**: cache quente em heap (janela recente) + **formato próprio em disco** (nem NQueue nem NMap) — append rápido estilo NQueue, **auto-compactação**, governado por tempo. Reuso de codecs onde fizer sentido. |
| **#128 apply** | **Apply em lote (batch)** — consumidor single-thread por tópico mantido, commit em lote. Sem paralelismo por chave (risco de ordem/fencing). |
| **Broadcast** | **Com loopback** — o produtor também recebe a própria mensagem em `onMsgBroadcasted`. |
| **Entrega** | **Branch única** `feature/relay-log-hardening-127-128-129` + commits atômicos, ordem #127→#128→#129→broadcast, **release 4.5.0** (fixes + feature). |

---

# Work Item 1 — #127: op-log de resend persistente (formato próprio, híbrido)

## Abordagem

Construir um **store sequencial próprio, segmentado e indexado por sequência** (`ResendLog`), desenhado para o padrão de acesso exato do resend:
- **append** por sequência contígua/crescente (ordem de commit);
- **leitura aleatória** por sequência `get(seq)` e por faixa `[from..to]` (o read path atual em `handleSequenceResendRequest` é exatamente `for(seq=from..to) get(seq)` — `ReplicationManager.java:1629`, **sem** operações navegáveis);
- **retenção** por tempo **e** por contagem;
- **auto-compactação barata** descartando **segmentos inteiros** antigos (sem reescrita).

**Por que formato próprio e não NQueue/NMap:** NQueue é FIFO (sem `get(seq)` aleatório); NMap é keyed mas não tem eviction composta (watermark de sequência + janela temporal) nem ordenação de cauda nativa. Um log segmentado dá `get(seq)` O(1) por offset implícito (sequências densas) **e** compactação por rotação/descarte de segmento — exatamente "velocidade de NQueue + auto-compactação" pedido.

### Componentes novos (package `dev.nishisan.utils.ngrid.replication`, package-private)

- **`ResendLog`** (por tópico):
  - **Segmentos**: cada segmento é um arquivo com um run contíguo de sequências; rola ao atingir `segmentMaxEntries` ou `segmentMaxAge`.
  - **Frame por registro**: `[seq][timestamp][len][bytes]`, onde `bytes` reusa **`RelayEntryCodec`** (mesma tupla `epoch, seq, topic, opId, payloadBytes` já usada pelo relay — `RelayEntry`/`RelayEntryCodec` são package-private no mesmo pacote → **reuso, não reinvenção**).
  - **Índice por segmento**: como as sequências dentro do segmento são densas, o índice é um array de offsets `offset[seq - baseSequence]` → `get(seq)` O(1). Metadados do segmento: `baseSequence`, `lastSequence`, `minTimestamp`, `maxTimestamp`.
  - **Diretório em memória**: `NavigableMap<Long, Segment>` por `baseSequence`; `get(seq)` faz `floorEntry(seq)` + offset; range = caminha segmentos.
  - **Auto-compactação/eviction**: contagem total > cap → descarta segmentos mais antigos inteiros; segmento com `maxTimestamp` além da janela temporal → descarta. Descartar = apagar arquivo + remover do diretório (sem reescrita). O low-watermark sobe para o `baseSequence` do novo segmento mais antigo.
  - **Crash-safety**: append sequencial; no `open()` re-escaneia segmentos, reconstrói o diretório e **trunca registro parcial** na cauda. Offsets reconstruídos do arquivo de dados (sem fsync de índice). `fsync` segue a política do relay (OS-managed por padrão; janela de perda recuperada por re-snapshot).
- **`ResendLogStore`** (por nó): mapa `topic → ResendLog`, espelhando o papel de `RelayStore.java` (template). Lifecycle em `start()/stop()/close()`.

### Modificações em `ReplicationManager.java`

- Manter `replicationLogBySequence` (TreeMap em heap) como **cache quente** (janela recente, cap por contagem pequeno) — caminho rápido para resend dos deltas mais frescos.
- `indexReplicationPayload` (`:1509`): **dual-write** — `put` no heap (com cap pequeno) **e** `resendLogStore.append(topic, seq, frame)`. Falha de append do op-log **não** falha o commit (degrada para snapshot fallback). Encoding via `handlers.get(topic).encodePayload(...)` (mesma chamada do relay em `:1118`).
- `handleSequenceResendRequest` (`:1589`): ler **heap primeiro**, depois `resendLogStore.read(topic, from, to)` para a faixa residual; sequências ausentes (evictadas por tempo) entram em `missingSequences` → caminho de snapshot-fallback **existente** (sem mudança lá).
- `getReplicationLogSize(topic)` (`:1927`): somar heap + `ResendLog`.
- `start()`: inicializar `ResendLogStore` em `dataDirectory/resend-log` quando habilitado; `stop()/close()`: fechar.

### Config (`ReplicationConfig` + Builder + `NGridNodeBuilder`/`NGridConfig`)
- `persistentResendLog` (boolean, default `false` — preserva comportamento atual; `true` no cardinal).
- `resendLogSegmentMaxEntries` (int) e `resendLogSegmentMaxAge` (Duration) — rotação de segmento.
- Reuso de `replicationLogRetentionTime` (janela temporal, já existe) como governo do disco; `replicationLogRetention` (já existe) passa a reger **só** o cache quente em heap.
- Expor `persistentResendLog` e a janela temporal via `NGridNodeBuilder` (plumbing já existe para `followerIngestMode`/`relayDurability` em `NGridNode`).

### Commits atômicos (#127)
1. `feat(replication): ResendLog — log segmentado próprio indexado por sequência (+ testes isolados)`
2. `feat(replication): ResendLogStore (por tópico) espelhando RelayStore`
3. `feat(replication): ReplicationConfig.persistentResendLog + segment knobs`
4. `feat(replication): op-log de resend híbrido (cache heap + ResendLog em disco)`
5. `feat(config): expor persistentResendLog/retention via NGridNodeBuilder`
6. `test(replication): regressão da espiral de re-snapshot (contagem-vs-tempo sob carga)`

> Nota de design: `ResendLog` nasce package-private e focado; fica como **candidato a utilitário público** (ao lado de NQueue/NMap) numa evolução futura, sem comprometer superfície de API agora.

---

# Work Item 2 — #128: throughput de apply em lote + métrica de relay

## Abordagem: **batch-apply** (consumidor single-thread mantido, ordem estrita preservada)

`OFFER` é não-idempotente e `nextExpectedSequenceByTopic` deve avançar **estritamente em ordem** no frontier per-topic. O lote **não introduz paralelismo**: amortiza o overhead (lock/flush/contadores/round-trips de peek-poll) ao longo de N entradas. Ganho de throughput com **zero risco de ordem/fencing** — paralelismo por chave foi **descartado** (exigiria extração de chave + reordenação no frontier, exatamente a classe de bug #124).

### Modificações em `ReplicationManager.java`
- `drainRelayOnce` (`:1193`): substituir o loop 1-a-1 (`:1220-1262`) por batch:
  1. Após `drainReorderContiguous`, **batch-peek** via `relay.readRange(0, batchSize)` (leitura **não-consumidora** já existente em `NQueue.readRange`); `batchSize = config.relayApplyBatchSize()`.
  2. Caminhar o lote em ordem: stale/dup (`epoch < lastSeen || seq < nextExpected`) → contar para `poll` em bloco; `seq == nextExpected` → aplicar (handler.apply em ordem), incrementar `nextExpected` local; `seq > nextExpected` → **parar no gap** (caminho de buffer/resend existente, inalterado).
  3. **Commit do lote**: adquirir `sequenceBufferLock` **uma vez**, avançar `nextExpectedSequenceByTopic` para `aplicado+1`, adicionar opIds ao `applied`, `recordApplied`, marcar `sequenceStateDirty`, então `relay.poll()` no prefixo processado.
  - **Invariante**: nunca `poll()` antes do `apply` retornar **e** do commit do frontier. Se `apply` lançar no meio, commit/poll só do prefixo bem-sucedido; cabeça falha re-tentada na próxima iteração (igual à recuperação por-op atual em `:1176`).
- Refatorar o corpo de `applyRelayEntry` (`:1286`) em `applyDecoded(topic, RelayEntry, deferCommit)` compartilhado entre o lote, o tail single-entry e o re-inject de resend.

### Métrica pública de relay (#128 item 2)
- `public long getRelaySize(String topic)` → `relayStore.relayFor(topic).size(true)`.
- `public long getRelayHeadAgeMillis(String topic)` → de `peekRecord().meta().getTimestamp()` (extrair helper `relayHeadTimestamp(topic)` e reusar em `checkRelayHeadAgeAndBootstrap:480`).
- `public Map<String,Long> getRelaySizes()` sobre `handlers.keySet()`.
- Surface em `NGridNode.operationalSnapshot()` (`:656`) — tamanho por tópico + idade máxima da cabeça.

### Config
- `relayApplyBatchSize` (int, default `256`, `>=1`) + exposição via `NGridNodeBuilder`.

### Commits atômicos (#128)
1. `refactor(replication): extrair applyDecoded + relayHeadTimestamp (sem mudança de comportamento)`
2. `feat(replication): apply do relay em lote (relayApplyBatchSize)`
3. `feat(replication): métricas públicas de tamanho/idade do relay`
4. `feat(metrics): expor relay size/head-age em NGridOperationalSnapshot`
5. `test(replication): corretude do lote + ordem sob burst`

---

# Work Item 3 — #129: sync proativo no join + leader-pause-on-join + sem caught-up transitório

Os três sub-fixes entram **juntos** ((1)+(2) são obrigatórios juntos; (3) protege (1) de ser poluído pelo boot).

### 3a. Sync proativo no join (lado follower)
`checkLagAndSync` (`:418`) retorna cedo em relay mode e só chama `checkRelayHeadAgeAndBootstrap` (que exige relay **não-vazio**). Follower novo (relay vazio) + líder quiescente = nunca sincroniza.
- Em `onLeaderChanged` (`:2066`), quando o local vira **follower** com líder conhecido não-self, marcar tópicos para catch-up proativo (set `proactiveSyncRequested`, limpo a cada troca de líder — dispara **uma vez** por join).
- Novo branch no caminho relay-mode (ou `checkProactiveJoinSync` agendado): para tópico em **progresso zero** com `coordinator.getTrackedLeaderHighWatermark() > lastAppliedSequence` **e** relay vazio (`getRelaySize==0 && getRelayHeadAgeMillis==0` — usa a métrica do #128), disparar `requestSync(topic)` **uma vez**. Guard distingue o cold-join genuíno do lag em-regime (que o relay deve absorver, sem snapshot).

### 3b. Leader-pause-on-join / quiesce (lado líder) — espelho do drain-gate de failover
O drain-gate existente (`leaderSyncing`/`leaderSyncTopics`/`maybeReleaseRelayDrainGate:2157`) gateia o backlog **local** na promoção. #129 quer o líder **pausar produção** enquanto um **follower remoto** não-caught-up faz join.

**Lacuna achada:** o líder **não tem** sinal de applied-watermark do follower (`HeartbeatPayload` só carrega `leaderHighWatermark`, líder→follower; e acks são por-op, ausentes quando o líder está quiescente). É preciso um canal dedicado.
- **Nova mensagem** `MessageType.FOLLOWER_PROGRESS` + `record FollowerProgressPayload(String topic, long appliedSequence, long epoch)` (JSON via caminho polimórfico existente — **sem mudança de codec**). Follower envia no join e periodicamente enquanto faz catch-up.
- Líder rastreia `Map<NodeId, Map<String,Long>> followerAppliedByTopic` (atualizado em `handleFollowerProgress`).
- **Join-quiesce gate** paralelo ao `leaderSyncing`: `AtomicBoolean joinQuiescing` + conjunto de nós/tópicos aguardados. Join detectado via `coordinator.addMembershipListener` (`:435`). O write-gate em `replicate()` (`:565`, hoje `if (isLeaderSyncing())`) ganha cláusula `|| isJoinQuiescing()` com razão própria. **Release** quando todo follower aguardado reporta applied dentro de `joinSyncLagThreshold` do `getGlobalSequence()`, **ou** por timeout `joinQuiesceMaxDuration`, **ou** em `onPeerDisconnected` (ReplicationManager já é `TransportListener`) — para um follower que morre no join **não** congelar o líder.
- (3a)+(3b) compõem: líder pausa → follower drena/snapshot até a cabeça → reporta → líder libera. Como o relay absorve lag, a janela de quiesce é curta.

### 3c. Evitar caught-up transitório no boot
pairMode/minClusterSize=1: o nó se auto-elege no boot antes de ver o peer (`requiredActiveMembersForLeadership:603` retorna `minClusterSize`; `recomputeLeader:590` pega max NodeId), depois stepa para follower (~0,4s). Ao se eleger vazio, o relay vazio libera o drain-gate (`:2090`) e ele carrega a crença falsa de "sincronizado".
- **Guard RM-local** (menor raio de impacto): `startedAtMillis` + `joinPeerDiscoveryWindow`. Em `onLeaderChanged` ao virar líder em relay-mode com `activeMembers().size() <= minClusterSize` dentro da janela de boot, **suprimir** a liberação do drain-gate por relay-vazio até a janela expirar **ou** um peer aparecer/ser reconciliado. Assim o nó não se anuncia como líder caught-up prematuramente. (Alternativa em `ClusterCoordinator` com atraso de eleição foi **descartada** — mexe na temporização de eleição amplamente, risco maior.)

### Config
- `joinQuiesceMaxDuration` (Duration, default `10s`), `followerProgressInterval` (Duration, default `500ms`), `joinPeerDiscoveryWindow` (Duration, default `2s`), `joinSyncLagThreshold` (long, default pequeno). Expor `joinQuiesceMaxDuration`/`joinPeerDiscoveryWindow` via `NGridNodeBuilder`.

### Commits atômicos (#129)
1. `feat(common): FOLLOWER_PROGRESS + FollowerProgressPayload`
2. `feat(replication): report periódico de progresso do follower`
3. `feat(replication): sync proativo no join (cold-follower + líder quiescente)`
4. `feat(replication): leader-pause-on-join (gate de quiesce; bounded + release-on-disconnect)`
5. `feat(replication): suprimir caught-up transitório no boot (janela de descoberta de peer)`
6. `feat(config): joinQuiesce/followerProgress/peerDiscovery + exposição NGrid`
7. `test(replication): convergência cold-join + quiesce do líder + sem-synced-transitório`

---

# Work Item 4 — Broadcast inter-nós (`broadcastMessage` + `onMsgBroadcasted`, com loopback)

## Abordagem
Construir sobre o **`transport.broadcast`** existente (`TcpTransport:173`, fan-out per-peer em virtual threads), com `MessageType.USER_BROADCAST` + payload novo + listener delegado por `ReplicationManager` e `NGridNode`. **Best-effort** (fire-and-forget), **com loopback**.

### Arquivos
- `common/MessageType.java`: adicionar `USER_BROADCAST`.
- `common/BroadcastMessagePayload.java`: `record BroadcastMessagePayload(String body)` — a identidade do produtor viaja em `ClusterMessage.source()` (reuso do envelope, sem duplicar).
- `replication/BroadcastListener.java`: `@FunctionalInterface void onMsgBroadcasted(NodeId producer, String message)`.
- `ReplicationManager`:
  - `Set<BroadcastListener> broadcastListeners` (`CopyOnWriteArraySet`, idioma do projeto).
  - `broadcastMessage(String str)`: `transport.broadcast(ClusterMessage.request(USER_BROADCAST, "broadcast", local, null, payload))` **e**, por loopback, despachar aos listeners locais com `producer = local` (despacho num worker/virtual-thread, **não** inline na thread chamadora, para espelhar o caminho remoto e evitar reentrância).
  - `addBroadcastListener`/`removeBroadcastListener`.
  - `onMessage` (`:784`): rotear `USER_BROADCAST` → `broadcastListeners.forEach(l -> l.onMsgBroadcasted(message.source(), payload.body()))` (já roda no worker pool do transport — manter listeners não-bloqueantes; documentar).
- `NGridNode`: delegar `broadcastMessage(String)` e `addBroadcastListener(...)`; opcionalmente expor no facade `NGrid`/`NGridCluster`.

### Semântica (para a documentação de fechamento)
- **Loopback ON** (decisão do usuário): produtor recebe a própria msg — coordenação uniforme em todos os nós.
- **Best-effort**, não-ordenado, não-durável. Para entrega garantida/ordenada, usar fila replicada. Documentar claramente para fixar expectativa e evitar scope creep.

### Commits atômicos (broadcast)
1. `feat(common): USER_BROADCAST + BroadcastMessagePayload (+ teste de codec)`
2. `feat(replication): broadcastMessage + BroadcastListener (com loopback)`
3. `feat(api): expor broadcastMessage/addBroadcastListener em NGridNode`
4. `test: entrega ponta-a-ponta + identidade do produtor + loopback`

---

# Reuso (não reconstruir)
- **#127**: `RelayEntry`/`RelayEntryCodec` (mesmo pacote) p/ o frame; caminho `missingSequences → snapshot-fallback` já trata "evictado por tempo".
- **#128**: `NQueue.readRange` (já existe); extrair `relayHeadTimestamp` do `checkRelayHeadAgeAndBootstrap`; corpo de `applyRelayEntry` via `applyDecoded`.
- **#129**: padrão `leaderSyncing`/`maybeReleaseRelayDrainGate` p/ o join-gate; `requestSync`/`syncingTopics`; `coordinator.getTrackedLeaderHighWatermark()`; `addMembershipListener`; padrão janitor `checkStuckSyncs` p/ o timeout de quiesce; `TransportListener.onPeerDisconnected` p/ release.
- **Broadcast**: `transport.broadcast`, `ClusterMessage.request`/`source()`, idioma `CopyOnWriteArraySet`+add/remove, caminho polimórfico Jackson (sem registro de codec).

# Config — resumo (novas chaves)
| Chave | Default | Item |
|---|---|---|
| `persistentResendLog` | `false` | #127 |
| `resendLogSegmentMaxEntries` / `resendLogSegmentMaxAge` | TBD / TBD | #127 |
| `relayApplyBatchSize` | `256` | #128 |
| `joinQuiesceMaxDuration` | `10s` | #129 |
| `followerProgressInterval` | `500ms` | #129 |
| `joinPeerDiscoveryWindow` | `2s` | #129 |
| `joinSyncLagThreshold` | pequeno | #129 |

# Documentação (CLAUDE.md §7 — Markdown + PlantUML)
- `doc/ngrid/oplog-ha-hardening.md`: novas seções p/ #127 (ResendLog), #128 (batch + métricas), #129 (cold-join/quiesce/boot).
- `doc/ngrid/broadcast-messaging.md` (novo): API, semântica best-effort/loopback, e **nota de fechamento** — não havia pub/sub público; `transport.broadcast` era interno; a API fina sobre ele é o encaixe mínimo correto.
- `doc/diagrams/`: `ngrid_resend_oplog_segmented.puml`, `ngrid_relay_batch_apply.puml`, `ngrid_join_quiesce_proactive_sync.puml`, `ngrid_broadcast_messaging.puml` (incorporar via `uml.nishisan.dev`).
- `doc/CHANGELOG.md`: entrada **4.5.0** (fixes #127/#128/#129 + feature broadcast). `pom.xml` 4.4.0 → 4.5.0.
- Copiar este plano aprovado p/ `/planning`.

# Sequência & verificação
1. Branch `feature/relay-log-hardening-127-128-129`. Ordem **#127 → #128 → #129 → broadcast**; dentro do #128, a métrica pública entra **antes** dos commits do #129 que a leem.
2. `mvn -pl nishi-utils-core clean install` a cada bloco; testes unitários `mvn test`; resiliência `mvn test -Presilience`.
3. **Reprodução determinística (testes novos)**:
   - #127: sob `replicationLogRetention` baixo + janela temporal alta + produção rápida, asserir que resend **não** cai em snapshot-fallback (sem disco) e **cai** com disco habilitado → fim da espiral.
   - #128: lote preserva ordem/fencing; throughput de apply > produção em burst sintético.
   - #129: `newFollowerSyncsAgainstQuiescentLeader` (converge sem tráfego novo); `leaderPausesUntilJoiningFollowerCatchesUp`; quiesce libera por timeout/disconnect; sem "synced" transitório no boot solo.
   - broadcast: A `broadcastMessage("x")` → B e **A** (loopback) recebem `onMsgBroadcasted(A, "x")`.
4. CI **não** roda a suíte de resiliência ngrid — validar mudanças ngrid **localmente** (`-Presilience`).
5. Release 4.5.0 via `gh release create` antes da tag (CLAUDE.md §8), changelog rico.
