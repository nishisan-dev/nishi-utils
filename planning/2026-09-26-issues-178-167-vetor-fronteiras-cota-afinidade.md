# Plano — #178 (vetor de fronteiras por tópico), #167 item 3 (cota e afinidade) e revisão do NGrid — release 8.8.0

## Contexto

**#178 (NGrid, `nishi-utils-core/.../replication/ReplicationManager.java`):** o odômetro global `appliedSequence`/`lastAppliedSequence` mistura escalas incompatíveis:
- Seed no restart (`seedAppliedSequenceFromFrontier`, `:640-691`): `max(Σ_t (nextExpected_t − 1), globalSequence)` → **soma** das fronteiras.
- Commit do seguidor (`commitRelayBatch`, `:2013`): `max(applied, seq_tópico)` → **máximo** por tópico.
- Commit do líder no caminho padrão (`leaderLocalApply=true`, `recordApplied` `:3513`): `incrementAndGet()` → **contagem** por sessão; no caminho `leaderLocalApply=false` (`:1019`): máximo.
- `globalSequence` (`:896`) conta toda op de qualquer tópico; o líder anuncia `max(globalSequence, lastApplied)` (`:550-557`, `leaderQuiesceTarget` `:2822`) mas os gates comparam com `lastApplied`.
- `completeSnapshotCutover` (`:1278-1280`) e `reanchorAsDemotedIncumbent` (`:3185-3194`) fazem **SET** do odômetro global com o watermark de **um** tópico (`primaryTopic()` = `handlers.keySet().stream().findFirst()`, arbitrário).
- `resetSequenceState` (`:720-723`) zera `lastAppliedSequence` mas não `appliedSequence`.

Consequências em produção (TEMS 8.6/8.7): `HIGH_REPLICATION_LAG` de milhões sem backlog real; `peer watermark above its own applied` após restart de seguidor; e (comentário da issue) o gate de eleição agregado não distingue um seguidor que perdeu a última op de `map:ngrrd.catalog` quando `map:ngrrd.nodes` (escrito a cada tick do `NodeStatusReporter`) domina o valor → novo líder eleito atrás de um seguidor elegível, `resumeInFlight` não encontra a migração (`LeaderFailoverDuringMigrationClusterTest`, ~1/8 em 2 CPUs). Com `RELAY_STREAM` (quórum 1) a op confirmada é perdida.

**#167 item 3 (ngrrd-cluster):** itens 1 e 2 entregues na 8.4.0. Falta cota por nó (`maxSeries`/`maxBytes`: nó na cota não recebe placement nem migração) e regras de afinidade por definition/prefixo de chave respeitadas por placement, rebalance e drain. O catálogo (`SeriesPlacement`) hoje não guarda a definition da série; `PlaceRequest.definitionHashHex` chega ao líder mas não é lido.

**Revisão do NGrid:** o cluster ngrrd ainda não roda "maduro". Achados relevantes em eleição/replicação/transport recebem teste de reprodução e correção no mesmo PR (decisão do usuário).

**Decisões do usuário:** vetor por tópico no heartbeat (fallback escalar para nós antigos); corrigir tudo que a revisão encontrar, inclusive os itens estruturais B8/B9/C9 (por último, na onda C); um PR, release **8.8.0**.

## Execução (regras gerais)

- Branch `claude/issues-178-167-ngrrd-ygtv07` (já criado a partir de `main` = 8.7.0). Commits atômicos por fatia; TDD (teste vermelho primeiro). Maven com JDK 21.
- Idioma: identificadores em inglês; comentários/Javadoc em inglês nos arquivos do core que já são em inglês (`ReplicationManager`, `ClusterCoordinator`, `TcpTransport`, codecs); PT-BR nos arquivos do ngrrd, nos nomes de `@Test` do ngrrd, no CHANGELOG, docs e PR. Sem atribuição a agente/modelo em commits ou código.
- Ordem: Parte 1 (core #178) → Parte 2 ondas A e B (core) → Parte 3 (ngrrd #167 item 3 + fence) → Parte 2 ondas C e D → Parte 4 (versão/docs). As fatias do ngrrd 1–9 (cota/regras) são independentes do core e podem correr em paralelo por subagente em worktree desde o início; a fatia 10 (fence) e a Parte 2 onda B (transport) dependem da Parte 1 instalada (`mvn -pl nishi-utils-core install -DskipTests`).
- Antes de qualquer correção da Parte 2: rodar na base `RelayStreamReplicationTest`, `JoinQuiesceReleaseGateTest` e 8× `LeaderFailoverDuringMigrationClusterTest` (2 CPUs) para registrar as assinaturas de falha reais e comparar no fim.
- Cada onda termina com o gate completo da seção Verificação antes de começar a próxima (evita acumular vermelhos difíceis de atribuir).
- Compatibilidade: mudanças aditivas no fio (trailing bytes no heartbeat, campos novos anuláveis em JSON e records serializáveis com construtores de compatibilidade e fixtures `.ser` legadas).

## Parte 1 — Core NGrid: #178, vetor de fronteiras por tópico

### 1.1 `feat(ngrid): fronteira aplicada por tópico e odômetro derivado` (ReplicationManager)
- Novo tipo `replication/TopicFrontiers` (record imutável: `Map<String, Long> byTopic`, `total()` = Σ, `get(topic)`, `dominates(other, threshold)`, `isBehind(other, threshold)`, `compare(other, threshold)` → `AHEAD | BEHIND | EQUAL | INCOMPARABLE`, e `resolveIncomparable` por `total()`; chaves sintéticas (`_global`, `_topic:*`) excluídas; parse/format para logs).
- `ReplicationManager.appliedFrontiers()`: para cada tópico com handler ou fronteira persistida, `frontier_t = max(sequenceByTopic[t], nextExpected[t] − 1)` (mesma fórmula de `currentLeaderTopicSequence`, `:2197`, válida para líder e seguidor). Leitura lock-free (mapa concorrente). Junto com a correção A1 (Parte 2: o líder passa a avançar `nextExpected[t]` e o cursor de fetch a cada commit), as duas parcelas convergem e a fronteira vira simplesmente `nextExpected_t − 1` em ambos os papéis.
- **Odômetro escalar derivado**: `getLastAppliedSequence()` passa a devolver `appliedFrontiers().total()` (soma das fronteiras, escala única para líder e seguidor). Remover os writes de `appliedSequence`/`lastAppliedSequence` em `seedAppliedSequenceFromFrontier` (`:686-690`, mantém só o re-anchor D9), `:1019`, `recordApplied` (`:3513`), `commitRelayBatch` (`:2013`), `completeSnapshotCutover` (`:1278-1279`), `reanchorAsDemotedIncumbent` (`:3185-3186`) e `resetSequenceState` (`:721`). `globalSequence` continua existindo (métrica de produção, persistência `_global`, `getGlobalSequence()` usado por testes) mas **sai** de `advertisedLeaderHighWatermark` e de `leaderQuiesceTarget`, que passam a usar `total()` (mesma escala do seguidor). Manter o campo `appliedSequence` apenas se algum teste depender; caso contrário remover.
- `advertisedLeaderHighWatermark()` (`:550`): `-1` com bootstrap gate; senão `total()`. `localAppliedForReclaim()` idem.
- `TopicReplicationStatus` ganha `maxPeerFrontier` (fronteira máxima observada nos peers elegíveis para o tópico, `-1` se desconhecida) — preenchido a partir do coordinator (1.3) para o fence do ngrrd (Parte 3, fatia 10).
- Teste `ReplicationManagerSequenceStateTest`: novo caso com 3 tópicos (`map:ngrrd.catalog`, `map:ngrrd.nodes`, `map:ngrrd.geometries`) e `_global` maior que a soma → `getLastAppliedSequence()` == soma e `appliedFrontiers()` por tópico; caso de ex-líder (`_topic:*` acima da fronteira) mantém o re-anchor D9. Teste em `RelayStreamReplicationTest`: 200 offers + 200 puts → `getLastAppliedSequence()` do seguidor converge em 400 (hoje trava em 200 — artefato da escala `max`: com 200 ops em cada um de dois tópicos o máximo por tópico nunca passa de 200; a asserção do teste compara com `globalSequence` = 400; ver Onda D).

### 1.2 `feat(ngrid): heartbeat carrega o vetor de fronteiras por tópico` (codec + payload)
- `HeartbeatPayload` ganha `Map<String, Long> topicFrontiers` (imutável, vazio = ausente/nó antigo), construtor de compatibilidade, `@JsonProperty("topicFrontiers")` anulável.
- `BinaryFrameCodec`: após o flag `leader`, seção final OPCIONAL: `u16 count` + `count × (u16 len, UTF-8 topic, i64 frontier)`. Decoder: `remaining() >= 2` → lê; senão mapa vazio. Encoder só emite a seção quando o mapa não é vazio (PING nunca). Atualizar a tabela do Javadoc. Compatível nos dois sentidos (decoder antigo ignora trailing; decoder novo trata ausência como vazio).
- Limite prático: até 255 tópicos por heartbeat (documentar; ngrrd usa 3–4). Se exceder, envia os `N` maiores por nome ordenado e loga WARNING uma vez.
- `ClusterCoordinator.setTopicFrontiersSupplier(Supplier<Map<String,Long>>)` alimentado por `ReplicationManager.start()`; `HeartbeatPayload.now(...)` passa o mapa (`ClusterCoordinator.java:769`).
- Testes: `BinaryFrameCodecTest` (round-trip com vetor, frame antigo sem seção → vazio, frame novo lido por decoder "antigo" simulado = ignora trailing), `JacksonMessageCodecTest` (JSON sem campo → vazio).

### 1.3 `fix(ngrid): gates de eleição comparam a fronteira por tópico` (ClusterCoordinator)
- Novo `Map<NodeId, Map<String,Long>> peerTopicFrontiers` ao lado de `peerHighWatermark` (`:98`), atualizado no recebimento do heartbeat (`:1914-1919`); limpo nos mesmos pontos (`:836`, `:1826`, LEAVE). `noteFollowerWatermark` (`:1142`) ganha overload com vetor (vem do `FOLLOWER_PROGRESS`, 1.4).
- Helper privado `FrontierComparison compareWithPeer(NodeId peer)`: se local e peer têm vetor → `TopicFrontiers.compare` com `syncReclaimLagThreshold` por tópico; incomparável → decide por `total()`; senão (peer antigo, sem vetor) → comparação escalar atual.
- `isCaughtUpToCluster()` (`:1379`): "caught up" = nenhum peer elegível ativo está AHEAD (vetor) — substitui `localApplied >= maxPeer − threshold`; a trava `reclaimCaughtUpLatch` e a janela de boot mantêm a semântica. A trava é limpa quando um peer reporta um vetor AHEAD (hoje `:1917`).
- `candidateIsBehindLocalWatermark` (`:1407`): candidato BEHIND por vetor quando ambos têm vetor.
- `highestWatermarkActivePeer` (`:290`) e `deferralFollowTarget`: escolhe por vetor (dominância; incomparável → `total()`; empate → afinidade), mantendo o escalar para peers antigos.
- Escape D9 (`:1052-1092`): `localApplied > electedWatermark + threshold` vira "local AHEAD do eleito por vetor".
- WARN "peer watermark above its own applied" (`:956-966`): imprime o tópico divergente.
- `maxActivePeerHighWatermark()` continua (escalar `total()` para observabilidade e para o `nudgeLeadershipOnCatchUp`, que passa a usar a comparação vetorial); novo `maxActivePeerTopicFrontier(topic)` público para o `TopicReplicationStatus.maxPeerFrontier`.
- Testes determinísticos com `LoopbackTransport`/`Harness` (mesmo padrão de `LeaderSyncBeforeReclaimTest`, `IneligibleMemberWatermarkStalemateTest`): (a) reprodução do comentário da #178: A (afinidade maior) com `{catalog: 9, nodes: 20}` e B com `{catalog: 10, nodes: 19}`, mesma soma → B lidera, A defere e adota B; (b) peer antigo (sem vetor) → gate escalar; (c) vetores incomparáveis com somas diferentes → maior soma lidera; (d) líder nunca abdica por vetor de seguidor (F2 preservado); (e) escape D9 por vetor.

### 1.4 `feat(ngrid): FOLLOWER_PROGRESS e quiesce por tópico`
- `FollowerProgressPayload` ganha `Map<String,Long> topicFrontiers` (anulável). `handleFollowerProgress` (`:2727`) e `leaderQuiesceTarget` usam vetor quando presente (joiner liberado do quiesce quando ≥ fronteira do líder em cada tópico dentro do threshold); escalar `total()` para seguidores antigos.
- Handback D11: `HandbackRequestPayload`/`Grant` continuam com um watermark escalar (`total()`), mas `reanchorAsDemotedIncumbent` deixa de fazer SET global de um tópico (1.1). Registrar em `doc/ngrid/oplog-ha-hardening.md` que o congelamento é por `total()` e que o handback multi-tópico fica como limitação (`primaryTopic()` arbitrário) — ou, se a revisão 2.x mostrar risco real, congelar produção em todos os tópicos.
- Testes: `JoinQuiesceReleaseGateTest` (hoje vermelho na base, PR #180) deve passar com a escala unificada; adicionar caso multi-tópico.

### 1.5 Observabilidade
- `NGridNode.operationalSnapshot` (`:748-768`): lag global = Σ lag por tópico (`getReplicationLag(topic)` quando `leaderHwmByTopic` conhecido); `leaderHighWatermark`/`lastApplied` na escala `total()`. `DistributedMap.getOptional` (`:454-475`, `Consistency.bounded`) usa o lag por tópico do próprio mapa (`getReplicationLag("map:"+name)`) em vez do escalar global.
- `NGridDashboardReporter`/`NGRRD_NODE_STATUS`: expor `appliedByTopic` no snapshot (record `NGridOperationalSnapshot` com campo novo e construtor de compatibilidade).
- Testes: `NGridOperationalSnapshotTest`, `NGridAlertEngineTest` (sem lag fantasma com 3 tópicos).

### 1.6 E2E (`-Presilience`)
- `MultiTopicRestartWatermarkE2ETest` (novo, 2–3 nós reais, 3 tópicos, ≥1 restart de seguidor): após sincronizar, `getLastAppliedSequence()` igual em todos, lag 0, nenhum log "peer watermark above its own applied", líder estável.
- `LeaderFailoverDuringMigrationClusterTest` (ngrrd, `-Pngrrd-cluster`) rodado ≥ 8× com `-Djdk.virtualThreadScheduler.parallelism=2` como critério de aceite da issue.

## Parte 2 — Revisão do NGrid (achados → teste de reprodução + correção)

Regra: cada item começa por um teste que falha na base; correção mínima; commit próprio `fix(ngrid): …`. Se a reprodução não confirmar o achado, registrar no CHANGELOG (“verificado, não reproduz”) e seguir. Ordem = severidade. RM = `replication/ReplicationManager.java`, CC = `cluster/coordination/ClusterCoordinator.java`, TT = `cluster/transport/TcpTransport.java`, NR = `cluster/transport/NetworkRouter.java` (todos em `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/`).

### Onda A — perda de dados / dois líderes / sem líder
- **A1 Líder rebaixado ao vivo re-aplica as próprias escritas (ALTO, RM).** O líder nunca avança `nextExpectedSequenceByTopic` (só `:672`, `:1287`, `:1891`, `:2002`, `:3194`) e o cursor de fetch (`relayStreamCursor` `:1526-1530`) não é re-ancorado na demoção (`onLeaderChanged` `:2594-2618`). Após step-down/reclaim/preferred-leader, `maybeSendFetch` puxa do cursor obsoleto, o novo líder serve (espelhou como seguidor, `:2027`) e o laço de apply (`:1797/1807`) re-aplica — OFFER de fila duplica. Correção: no commit do líder (`completeOperation`/`checkCompletion`), `nextExpected[t] = max(nextExpected[t], seq+1)` e cursor `= max(cursor, seq)`; na demoção, re-ancorar cursor de todos os tópicos em `nextExpected−1` (mesma regra do D9 no restart). Isso também unifica `frontier_t = nextExpected_t − 1` para ambos os papéis (Parte 1.1). Teste E2E `LiveDemotionNoReplayE2ETest` (2 nós reais; líder A produz N na fila, B com afinidade maior entra e reclama; A não duplica itens; conteúdo por prova).
- **A2 Escape D9 pode promover dois nós (ALTO, CC `:1052-1093`).** Não checa `assertingLeaderPeer(localId)` nem se o nó local é o melhor entre os não-eleitos; roda antes do bloco “seguir o líder que serve” (`:1107`). Cenário: restart sujo total, E (afinidade maior) anuncia −1, B e C recebem a recusa de E e ambos assumem → dual-leader D10c com descarte de cauda. Correção: não escapar se algum peer elegível afirma liderança; só escapar se o local é o topo (vetor de fronteiras, depois afinidade) entre os elegíveis não-eleitos. Teste com dois `ClusterCoordinator` num barramento loopback compartilhado (evoluir o `Harness` de `LeaderlessStalemateEscapeTest`).
- **A3 Heartbeats do mesmo peer aplicados fora de ordem (MÉDIO, CC `:1915/:1973/:1985`; TT `:1106` despacha cada mensagem em thread virtual própria).** `announceLeadershipChange` (`leader=false`) pode ser superado por um heartbeat periódico anterior (`leader=true`) por até um intervalo: readoção de nó rebaixado, apagamento de recusas D9, regressão de `trackedLeaderEpoch` (descarta `FOLLOWER_PROGRESS`, RM `:2736`). Correção: guardar o último `epochMilli` por origem e descartar heartbeats mais antigos (o campo já existe no payload). Teste unitário com `Harness` de `DualLeaderResolutionTest`.
- **A4 LEAVE de votante desfeito por heartbeat atrasado (MÉDIO, CC `:1807-1814`, `:1873`, `:1937`).** `touch()` reativa o membro e re-registra `leader=true`; failover espera evicção (9 s + tick) em vez de imediato. Correção: após `onPeerLeaving`, ignorar heartbeats do peer com `epochMilli` anterior ao LEAVE (janela = `heartbeatTimeout`) — depende de A3. Teste em `LeaveMembershipTest`.
- **A5 `reclaimCaughtUpLatch` nunca é limpa na demoção/step-down (MÉDIO, CC `:1649-1654`, `:1694-1708`, `:1381-1383`).** Ex-líder que cedeu por D10c e ressincronizou pode reclamar atrás de um peer. Correção: limpar a trava na demoção, em `stepDown` e quando o vetor local regride (cutover); implementar o reset “nenhum peer à frente” prometido no Javadoc (`:162-163`). Teste unitário.
- **A6 Fence do catálogo no ngrrd** → Parte 3, fatia 10.

### Onda B — transport: failover lento e requests perdidos
- **B1 Relay sem link vivo ao destino (ALTO, NR `:96-100`, `:226-251`; TT `:110`).** `reachabilityMap[L]` mantém o líder morto “alcançável via proxy” para sempre (todos gossipam L), a rota vira PROXY e nunca volta (`promoteToDirect` só em conexão bem-sucedida); `isProxied(L)` dá graça de evicção (CC `:825-832`, fator 2) e `isReachable(L)` fica true; cada request via relay volta UNDELIVERABLE. É a assinatura 1 de `LeaderFailoverDuringMigrationClusterTest`. Correção: (i) candidato a relay só se ele reporta link **conectado** ao destino (o `PEER_UPDATE`/handshake passam a carregar `connectedPeers` além de `knownPeers`; peer antigo sem o campo = comportamento atual); (ii) UNDELIVERABLE do relay marca `reachabilityMap[target]` sem esse relay e, sem candidatos, rota volta a DIRECT; (iii) `isProxied` só conta rota PROXY validada por entrega/ping nos últimos `2×heartbeatTimeout`. Testes: `ProxyRoutingIntegrationTest` (relay sem link ao destino não é escolhido; morte do destino faz rota voltar a DIRECT e evicção não ganha graça), `NetworkRouterTest`.
- **B2 Cliente inelegível como relay de tráfego storage↔storage (ALTO, NR/TT `:1058-1095`).** Correção: candidato a relay exige `isLeaderEligible()` (ou flag `relayCapable` no handshake, default = elegível). Teste em `ProxyRoutingIntegrationTest`.
- **B3 Nó escolhido como proxy de si mesmo (MÉDIO, NR `:104-108`, TT `:851-853`, `:1740`).** `updateReachability` pode gravar `PROXY(via = target)`; após a morte do alvo a rota nunca cura. Correção: excluir o próprio destino em `findBestProxy` (não só em `send()`); `markDirectFailure` com rota PROXY cujo `via == target` volta a DIRECT. Teste `NetworkRouterTest`.
- **B4 Sem backoff/cache negativo de discagem (MÉDIO, TT `:515-580`, `:478-503`).** 2 dials/s do reconnect loop + heartbeats + probe + RTT, sem memória de falha; caller enfileirado no lock por peer espera k×`connectTimeout` num black-hole. Correção: `DialFailureCache` por peer (`failedAt`, backoff exponencial 500 ms → `max(5 s, heartbeatTimeout)`, com jitter); `send`/`sendAndAwait`/`probeLoop`/`RttMonitor` consultam antes de discar; reconnect loop respeita o backoff. Teste `TcpTransportConcurrentMeshTest` (contagem de dials via `beforeDialHook` após matar um peer).
- **B5 `sendAndAwait` perde requests na troca de conexão (ALTO, TT `:1204-1218`, `:1539`, `:1613-1615`, `:711`).** Frames na `OutboundChannel`/`heldBeforeHandshake` da conexão perdedora do desempate são descartados em silêncio; o futuro espera `requestTimeout` (20 s+). Correção: ao fechar a conexão perdedora, **redrenar** os frames pendentes para a conexão sobrevivente (ou falhar os `pendingResponses` cujo frame não saiu). Teste `PendingResponseDisconnectGraceTest` (request enfileirado na conexão fechada é entregue ou falha rápido).
- **B6 `pendingResponses` indexado por destino final, não por próximo salto (ALTO, TT `:1229`).** Queda do relay não falha requests roteados por ele. Correção: registrar `via` no pendente; `failPendingResponsesTo(via)` também. Teste `UndeliverableRequestIntegrationTest`.
- **B7 `sendAndAwait` disca sincronamente na thread do chamador (MÉDIO, TT `:348/:358`) e `ngrid-metrics` é uma thread única (RttMonitor + LeaderReelectionService).** Correção: `sendAndAwait` faz o dial no `workerPool` e devolve o futuro imediatamente (timeout passa a cobrir o dial); `TransportClusterRpc` do ngrrd (`rpc/TransportClusterRpc.java:151`) fica correto por consequência. Teste `BroadcastNonBlockingTest`-like para `sendAndAwait`.
- **B8 Backpressure (#113) regrediu (MÉDIO, `OutboundChannel.java:49-100`).** Fila ilimitada; `depth`/`dropped` sempre 0; sem `SO_TIMEOUT`/keepalive contra peer half-open. Correção: restaurar capacidade limitada com política já documentada em `planning/113-backpressure-outbound-tcptransport.md`, `setKeepAlive(true)`, e `SO_TIMEOUT` = `2×heartbeatTimeout` no leitor (fecha half-open). Teste `OutboundBackpressureConvergenceTest` + teste de half-open com socket que para de ler.
- **B9 Votantes nunca esquecidos (MÉDIO, TT `:1349-1372`, CC `:1594-1600`).** Storage substituído sob novo id eleva a maioria para sempre. Correção mínima: LEAVE de votante com `reason=decommission` (novo `LeavePayload.reason`) é honrado em primeira mão e esquece o peer (tombstone longo); comando administrativo no ngrrd (`ngrrd_admin forget-node <id>`) e API `NGridNode.forgetPeer(id)`. Teste `TcpTransportLeaveTest` + `DepartedMemberQuorumElectionTest`.
- **B10 Baixos:** votante com porta 0 conta no numerador e não no denominador (CC `:1544/:1596`); evicção pode sobrescrever heartbeat fresco (`:817-835`); reativação por `touch()` não notifica membership (`:1937`); `stop()` mantém `leader` (`:532-538`); UNDELIVERABLE de resposta ignorado (TT `:1158-1160`); vazamento em `pendingResponses` com `requestTimeout ≤ 0` (`:378`). Correções pontuais com teste unitário cada.

### Onda C — replicação e coordinator
- **C1 Snapshot sem validação de origem/sessão (MÉDIO, RM `:1183-1265`).** Chunks de líderes diferentes costurados; cutover pode rodar num nó já promovido (reusa sequências, trunca op-log vivo). Correção: `syncSessionId` no `SYNC_REQUEST`/`SYNC_RESPONSE` (aditivo), descartar chunk de origem ≠ líder atual ou sessão ≠ ativa; re-checar `!isLeader()` dentro do executor antes de `completeSnapshotCutover`. Teste unitário com `StubTransport`.
- **C2 Handback D11 multi-tópico (ALTO se `affinityHandbackMode`, RM `:3330`, `:3188-3219`).** `primaryTopic()` arbitrário; W global aplicado a um tópico. Correção: `HandbackGrantPayload` passa a carregar `Map<String,Long> frozenByTopic` (campo novo, compat); congelar/re-ancorar todos os tópicos; `reanchorAsDemotedIncumbent(Map)`. Teste `AutomaticLeaderHandbackE2ETest` com 2 tópicos. (O ngrrd usa `affinityHandbackMode=true` por padrão — `StorageNodeConfig:718-719` —, portanto é relevante.)
- **C3 NPE no step-down servindo handback (MÉDIO, RM `:2601`).** `newLeader.equals(handbackPeer)` com `newLeader == null`. Correção: `Objects.equals`; isolar exceções de listeners em CC (`:1661`, `:1702`) com try/catch + log, para os demais listeners e o `announceLeadershipChange` rodarem. Teste unitário.
- **C4 Append no op-log falha no shutdown e infla `globalSequence` (MÉDIO, RM `:908-913`, `:896`, NGridNode `:850/:891`).** `replicate()` não checa `running`; `NGridNode.close()` fecha o RM antes do coordinator; `executor.submit` pós-shutdown deixa op em `pending` para sempre. Correção: `replicate()` rejeita com `IllegalStateException("shutting down")` quando `!running`; inverter a ordem de close (coordinator step-down → RM); rollback de `globalSequence` junto com o de tópico. Teste unitário.
- **C5 Entrada de relay inválida bloqueia o tópico para sempre (MÉDIO, RM `:1795`, `:1810-1814`).** Correção: decode dentro do try; após N falhas consecutivas da mesma sequência, mover para `relay/<topic>.dead` e avançar (log SEVERE + métrica `deadLettered`). Teste unitário com frame corrompido.
- **C6 Fetch loop: sem backoff na recusa (20 recomputes/s por tópico, RM `:1606-1608`), deadline não limpa na troca de líder (`:1518`), tempestade de snapshots (`:1263`).** Correção: backoff exponencial na recusa (50 ms → 1 s) e `reevaluateLeadership` no máximo 1×/intervalo; limpar `relayFetchPendingUntilByTopic` em `onLeaderChanged`; cooldown de 5 s entre `needSnapshot` consecutivos. Teste `RelayStreamObservabilityTest`.
- **C7 `resetSequenceState()` inconsistente e sem chamadores (BAIXO, RM `:719-732`).** Corrigir (zera tudo, inclusive cursores) ou remover.
- **C8 Epoch nunca persistido (MÉDIO, NGridNode `:338-343`, CC `:392/:415`).** `dataDirectory=null` → `epochPath` nulo; o Javadoc de `withPairMode` promete fencing por epoch que não existe. Correção: passar `config.dataDirectory()`; persistir; após restart total o epoch continua monotônico. Teste `ClusterCoordinatorEpochPersistenceTest`.
- **C9 Lease (MÉDIO, CC `:850-852`, `:817-833`).** Lease renovado no mesmo sinal que já rebaixa; líder isolado segue líder até evicção (até ~15 s); `stepDown()` não agenda reeleição (flap com o nudge de 200 ms). Correção mínima: líder isolado (nenhum heartbeat de votante em `heartbeatTimeout`) faz step-down proativo e rejeita escritas (`LeaseExpiredException`); `stepDown` agenda `recomputeLeader` após `heartbeatInterval`. Teste `LeaderLeaseRearmOnFailoverTest` estendido.
- **C10 Listeners rodam sob `leaderComputationLock` com I/O de disco (BAIXO, CC `:874`, RM `:3188`).** Documentar como limitação; sem mudança nesta rodada (risco de reordenar locks), salvo se C3 exigir.

> B8, B9 e C9 são os itens estruturais: implementados por último, depois de todo o resto verde, cada um em commit próprio com teste, para poderem ser revertidos isoladamente se a validação no TEMS apontar regressão.

### Onda D — testes e docs
- `RelayStreamReplicationTest` (`:152`): asserção por tópico (`getRelayStreamCursor`/`nextExpected`) e `getLastAppliedSequence() == 400` após a Parte 1; portas fixas 9861/9862 → `allocateFreeLocalPort`. Remover a entrada de `doc/testes-vermelhos-conhecidos.md`.
- `JoinQuiesceReleaseGateTest`: rodar na base para capturar a asserção que falha; corrigir a causa (a análise estática não a encontrou — provável quórum com 2 votantes sem pair mode, `CC:1594-1599`).
- `LeaderFailoverDuringMigrationClusterTest`: ≥ 8 execuções verdes com 2 CPUs após A1–A6 e B1–B7; remover da lista de vermelhos conhecidos.
- Docs desatualizadas: `AGENTS.md` (cita `resilience.yml` inexistente; só core publicado), `doc/oss/ngrrd-cluster.md` ~L707 (`pr-validation` roda dois `*ClusterTest`), `doc/oss/ngrrd-cluster.md` §13 (itens corrigidos saem da lista de issues abertas do core).

## Parte 3 — ngrrd-cluster: #167 item 3 (cota e regras de placement) + fence do catálogo

Módulo `nishi-utils-ngrrd-cluster`, pacote `dev.nishisan.utils.oss.cluster` (caminhos relativos a ele). Nome de config **`ngrrd.placement.rules`** (não "afinidade", que já significa afinidade de liderança no NGrid).

### Decisões de desenho
- **Regras no YAML de todos os storages** (uniforme, como `distribution.mode` na 8.4.0); o líder aplica a própria cópia; cada nó publica um fingerprint (SHA-256 canônico, 16 hex) em `StorageNodeStatus.placementRulesHash`; divergência gera `NGRRD_PLACEMENT_RULES divergent …` (WARNING, com throttle como `DistributionWeights.LAST_WARNING`) e coluna `RULES` com `!` na CLI. Regras nunca são descartadas por divergência.
- **Semântica das regras:** `name` (único), critérios `definition` (== `metadata.name`) e/ou `keyPrefix` (AND quando ambos); ação exatamente uma de `pin: [nós]` / `exclude: [nós]`; primeira que casa vence; sem match → livre. Série com `definitionName == null` (legado) só casa regras sem critério `definition`. `pin` nunca transborda: sem nó elegível → `NO_STORAGE_NODE_AVAILABLE` / drain pendente.
- **Cota:** `ngrrd.quota.maxSeries` / `maxBytes`, limite duro (sem fator 95%), `0` = ilimitado; só barra **destinos** (placement, rebalance, drain, `MIGRATE_PREPARE`). Nó acima da cota (adoção, dono preferido, cota reduzida) vira fonte preferencial no rebalance.
- **Dono preferido** (adoção/abort) ignora cota e regras (os bytes já estão lá), log FINE; o rebalance corrige depois (fase 0).
- **Definition até o líder:** `PlaceRequest.definitionName` (cliente preenche a partir do YAML já parseado em `client/DefaultNgrrdClusterClient` ~L455) → `PlacementContext.seriesKey/definitionName/placementRules` → `SeriesPlacement.definitionName` (campo final anulável). Backfill oportunista no PLACE idempotente da reabertura (`PlacementRequestHandler.handlePlace` ~L162: `ACTIVE` sem nome + request com nome → regrava). Sem backfill em lote (limitação documentada: série legada nunca reaberta só casa `keyPrefix`).

### YAML
```yaml
ngrrd:
  quota: { maxSeries: 200000, maxBytes: 68719476736 }   # opcional; 0/omitido = sem limite
  placement:
    rules:
      - { name: tems-core, definition: ifaceStats, keyPrefix: "br-sp/", pin: [storage-1, storage-2] }
      - { name: no-lab-on-3, keyPrefix: "lab/", exclude: [storage-3] }
```

### Fatias (commits, nesta ordem)
1. `test(ngrrd): fixtures legados 8.7.0 de StorageNodeStatus e SeriesPlacement` — gerar `src/test/resources/legacy-catalog/{node-8.7.0.ser,placement-8.7.0.ser}` ANTES de mudar os records; casos em `catalog/StorageNodeStatusTest` e `catalog/SeriesPlacementTest`.
2. `feat(ngrrd): regras de placement e elegibilidade de destino` — novos `placement/PlacementRule` (record + `matches(seriesKey, definitionName)` + `exclusionReason(nodeId)` → `rule_pinned_elsewhere(nome)`/`rule_excluded(nome)`), `placement/PlacementRules` (`NONE`, `of(list)` rejeita nome duplicado, `fingerprint`, `match`, `exclusionReason`), `placement/DestinationEligibility` (`quotaReason(status, pendingSeries, pendingBytes, requestedBytes)` → `quota_series(n/max)`/`quota_bytes(...)`, `ruleReason`, `reason`, `warnIfRulesDiverge`). Testes unitários puros.
3. `feat(ngrrd): cota e hash de regras no status e na configuração do nó` — `catalog/StorageNodeStatus` (+`quotaMaxSeries`, `quotaMaxBytes`, `placementRulesHash` no fim; ctor de 11 args vira compat; `withLoad`/`withState` propagam), `node/NodeStatusReporter.quota(...)`/`placementRulesHash(...)`, `node/StorageNodeConfig` (DTOs `QuotaSection`, `PlacementSection`, `RuleSection` em `NgrrdSection`; `toConfig` valida e falha no boot; builder; ctor compat), `node/NgrrdStorageNode` (~L276 wiring; log `NGRRD_PLACEMENT_RULES loaded count= hash=`). Testes: `StorageNodeStatusTest`, `NodeStatusReporterTest`, `StorageNodeConfigYamlTest`, `DistributionConfigTest`.
4. `feat(ngrrd): definitionName no PlaceRequest, no contexto de placement e no catálogo` — `protocol/PlaceRequest` (5º campo, ctors compat), `catalog/SeriesPlacement` (campo final, `withDefinitionName`, propagado por `withGeometry`/`migrating`/`completed`/`aborted`), `placement/PlacementContext` (3 campos finais, ctors compat), `node/PlacementRequestHandler` (contexto + backfill), `client/DefaultNgrrdClusterClient`, `client/RemoteSeriesHandle`, `client/PlacementResolver`; `node/LocalReconciler` L433 segue sem nome. Testes: `ProtocolCodecTest`, `SeriesPlacementTest`, `PlacementRequestHandlerTest`, `PlacementResolverTest`.
5. `feat(ngrrd): placement respeita cota e regras` — `placement/LeastLoadedPlacementPolicy.choose()` (filtro `DestinationEligibility.reason` após `CapacityBudget.fits`, com bypass do dono preferido; log `NGRRD_PLACEMENT_NO_CANDIDATE series= excluded=`), `PlacementRequestHandler` recebe `PlacementRules` (ctor compat) e chama `warnIfRulesDiverge`. Testes: `LeastLoadedPlacementPolicyTest` (cota de séries, cota de bytes com reserva, pin restringe, pin não transborda, exclude, dono preferido ignora, legado só keyPrefix), `WeightedDistributionTest`.
6. `feat(ngrrd): rebalance e drain respeitam cota e regras` — `rebalance/CapacityAwarePlanner` (ctor +`rules`, +`definitionNameBySeries`; `destinations()` exclui `loads ≥ quotaMaxSeries`; `fits()` inclui `quotaMaxBytes`; `eligible(key, target)` antes de `fits` no drain e no laço; **fase 0** move séries cujo dono viola regra, limitada por `maxMovesPerCycle`; alvos ponderados por *water-filling* com cap na cota e redistribuição por peso; em COUNT, fonte acima da cota sempre cede), `rebalance/RebalancePlanner` (overload novo, antigos delegam), `rebalance/Rebalancer` (`excludedDestinations()` inclui `quota_*`; `planMoves()` constrói `definitionNameBySeries`; `NGRRD_DRAIN_PENDING` com motivo `…_or_quota_or_rules rulesSkipped=N`), `rebalance/MigrationCoordinator.runMigration()` (após o gate de lag: `SKIPPED "destino <dst> inelegível: <motivo>"`; ctor +`rules` compat), `catalog/CatalogView` (expor `pendingBytesByNode`/`pendingMigrationSeriesByNode` se faltar). Testes: `RebalancePlannerTest`, `RebalancerTest`, `MigrationCoordinatorTest`.
7. `feat(ngrrd): destino recusa MIGRATE_PREPARE além da cota local` — `rebalance/MigrationExecutor.handlePrepare()` (config local: `catalogEntryCount + alvos abertos ≥ maxSeries` ou `used + reserved + totalBytes > maxBytes` → `failTarget`), `protocol/MigrateStatus.QUOTA_EXCEEDED` (novo; documentar que fonte 8.7.0 trata enum desconhecido como falha do RPC → abort, aceitável pois versões mistas não são suportadas), wiring em `NgrrdStorageNode`. Testes: `MigrationExecutorTest`, `ProtocolCodecTest`.
8. `feat(ngrrd): status admin e CLI exibem cota e regras` — `protocol/AdminStatusResponse.placementRulesHash` (líder; ctor compat), `node/AdminRequestHandler.handleStatus()`, `admin/NgrrdClusterAdminCli.printStatus()` (linha `REGRAS: <hash|-> (<n>)`, colunas `QUOTA` `maxSeries/maxBytes` e `RULES` com `!` quando diverge do líder). Testes: `NgrrdClusterAdminCliTest`, `AdminRequestHandlerTest`, `AdminStatusClusterTest`.
9. `test(ngrrd): testes de cluster de cota e de regras` (`-Pngrrd-cluster`, via `NgrrdClusterTestHarness.start(base, 3, builder -> …)`) — `QuotaClusterTest` (nó 1 com `maxSeries=2`; 12 séries → nó 1 ≤ 2; drain do nó 2 não sobe o nó 1; `status` mostra QUOTA; `rebalance` lista `storage-1(quota_series…)`) e `PlacementRulesClusterTest` (pin de definition A → {2,3}; exclude prefixo `lab/` de 3; drain de nó com pin exclusivo fica pendente com `NGRRD_DRAIN_PENDING`; nó com regras diferentes → `RULES !` e log de divergência).
10. `fix(ngrrd): novo líder aguarda a fronteira do catálogo antes de retomar migrações` — **fence** em `rebalance/MigrationCoordinator.onLeaderChanged/resumeInFlight` (~L656-727): antes de varrer `placementsLocal()`, aguardar (até `resumeFenceTimeout`, padrão 30 s) `TopicReplicationStatus("map:ngrrd.catalog")` com `relaySize == 0`, sem `syncing`/`pendingBootstrap` e `nextExpected − 1 ≥ maxPeerFrontier` (campo novo da Parte 1.1); timeout → WARNING `NGRRD_RESUME_FENCE_TIMEOUT` e varredura mesmo assim. A varredura de 20×500 ms passa a contar **a partir** do fence. Teste unitário com `FakeCatalog`/status simulado; `LeaderFailoverDuringMigrationClusterTest` como E2E.
11. `docs(ngrrd): cotas por nó e regras de placement (issue #167, item 3)` — `doc/oss/ngrrd-cluster.md` (§6.1 YAML, §7 ordem de elegibilidade ACTIVE → alcançável → capacidade 95% → cota → regras → frescor → dono preferido, §8 alvos com cap e fase 0, §9 drain pendente, §11 CLI, §13 limites), `doc/oss/ngrrd-cluster-operacao.md` (substituir a frase da L572 por seção "Cotas e regras de placement (issue #167, item 3)": dimensionar cota por RAM/page cache, trocar regras com restart coordenado, ler `RULES !`).

### Riscos
- Troca de regras exige restart coordenado (janela com líder aplicando conjunto antigo) — visível pelo aviso e pela coluna `RULES !`.
- 17 arquivos de teste constroem `StorageNodeStatus`: manter todos os construtores existentes.
- `seriesCount` pode já incluir imagens reservadas de migração e `pendingSeries` conta de novo → conservador, nunca inseguro.

## Parte 4 — Versão, docs e fechamento
- `8.8.0` nos 5 POMs (raiz, core, oss, ngrrd-cluster, ngrid-test) e `README.md:585`.
- `doc/CHANGELOG.md`: entrada `## 2026-MM-DD — <título> — release 8.8.0` com seções por módulo, "Limitações conhecidas", compatibilidade e ordem de upgrade (storages → clientes; heartbeat aditivo; durante a janela mista os gates caem no escalar `total()`).
- `doc/ngrid/arquitetura.md` (heartbeat + eleição por vetor), `doc/ngrid/oplog-ha-hardening.md` (seção nova "D12 — fronteiras por tópico"), `doc/oss/ngrrd-cluster.md` e `ngrrd-cluster-operacao.md` (cota/regras, fence, colunas novas), `doc/testes-vermelhos-conhecidos.md` (remover entradas corrigidas), `AGENTS.md`.
- Copiar este plano para `planning/2026-09-26-issues-178-167-vetor-fronteiras-cota-afinidade.md`.
- Comentar nas issues #178 e #167 após o merge (pedir validação no TEMS). PR em PT-BR.

## Verificação
- `mvn -pl nishi-utils-core test` (baseline, sem `-DexcludeNgrid`) → 0 falhas, incluindo `RelayStreamReplicationTest` e `JoinQuiesceReleaseGateTest`.
- `mvn -pl nishi-utils-core test -Presilience -Dsurefire.rerunFailingTestsCount=1`.
- `mvn -pl nishi-utils-core clean install -DskipTests && mvn -pl nishi-utils-ngrrd-cluster verify` e `... verify -Pngrrd-cluster` (≥ 3 rodadas de `LeaderFailoverDuringMigrationClusterTest` e dos `*ClusterTest` novos com `-Djdk.virtualThreadScheduler.parallelism=2`).
- `mvn verify -Pvalidate-javadoc`; o comando do `pr-validation.yml` (`mvn verify -pl core,oss,ngrrd-cluster -am -DexcludeNgrid=true -Dsurefire.rerunFailingTestsCount=1`).
- Compatibilidade: teste de codec com frame 8.7.0 (sem seção) e JSON legado; fixtures `.ser` legadas do ngrrd.

---

## Checkpoint de execução — 2026-09-26 (branch `claude/issues-178-167-ngrrd-ygtv07`)

Snapshot do que já está feito, do que foi encontrado e do que ainda falta, para o caso de o
ambiente cair. Tudo abaixo está commitado e no remoto (último commit desta seção incluído).

### Status por parte

| Parte | Status | Observações |
|---|---|---|
| 1.1–1.6 core #178 (vetor de fronteiras, odômetro derivado, gates por vetor, quiesce por tópico, observabilidade, E2E) | **feito** | `TopicFrontiers`, `ReplicationManager.appliedFrontiers()`, heartbeat binário com seção final, `priorityTopics`. `MultiTopicRestartWatermarkE2ETest` verde. |
| 2 Onda A (A1–A5) | **feito** | A1 `LiveDemotionNoReplayE2ETest` (240→120 na 8.7.0), A2 `StalemateEscapeSingleWinnerTest`, A3/A4/A5 em `TopicFrontierElectionGateTest`/`LeaveMembershipTest`. |
| 2 Onda B (B1–B8, B10) | **feito, integrado** | Branch do subagente (9 commits) merged sem conflito. |
| 2 Onda C (C1–C9) | **feito** | `RelayStreamRobustnessTest`, `CoordinatorHardeningTest` (C9 = step-down por isolamento). |
| B9 (esquecer votante) | **feito** | `Transport/NGridNode.decommissionPeer`, `TcpTransport` (tombstone 24 h), `ngrrd.admin.forget` + CLI `forget-node`, `ForgetNodeClusterTest` verde. |
| 3 ngrrd cota + regras (fatias 1–9, 11) | **feito, integrado** | 10 commits do subagente; `QuotaClusterTest`/`PlacementRulesClusterTest` verdes. |
| 3 fatia 10 (fence do catálogo) | **feito (código+unitários)** | `MigrationCoordinator.awaitResumeFence`; só loga quando precisou esperar — nas rodadas verdes o fence foi satisfeito de imediato. |
| Onda D (testes/docs) | **feito** | `RelayStreamReplicationTest` por tópico e portas dinâmicas; `doc/testes-vermelhos-conhecidos.md` sem entradas; AGENTS.md. |
| 4 versão/docs | **feito** | 8.8.0 nos 5 POMs, README, quickstart; CHANGELOG 8.8.0 completo. |
| Verificação final | **pendente** | ver abaixo. |

### Achados relevantes durante a execução (além do plano)

- **Yield do líder recém-eleito** (commit `439631c`): a eleição corre com o heartbeat (3 s); o
  sobrevivente de maior afinidade era eleito com vetores desatualizados e, ao ver o outro à frente
  no catálogo, retinha (F2) — a op confirmada se perdia (`LeaderFailoverDuringMigrationClusterTest`,
  ~1 em 6). Líder que nada produziu desde a eleição cede ao peer que o domina; perda de líder remoto
  e LEAVE de votante disparam heartbeat imediato. Regressão E2E do core verde depois disso.
- **Rodada do `LeaderFailoverDuringMigrationClusterTest` sem os fixes de transporte**: falhou
  "sem líder" porque storage-1 só recebia os heartbeats de storage-0 via relay por storage-2
  (achado B1: rota PROXY nunca voltava a DIRECT); ao matar storage-2 o líder ficou isolado e o C9
  rebaixou corretamente. Motivo de integrar a Onda B antes de repetir o critério de aceite 8×.
- `RelayStreamConcurrentIngestTest.gapRepull…` falha como root (pré-existente na base) → ignorado
  por `Assumptions` quando root. `NGridIntegrationTest` era flaky na base (1/2); verde nas rodadas
  após os fixes.
- Suíte do core do subagente de transporte (base = main 8.7.0 + B1–B10, sem as partes 1–3) reproduz
  os vermelhos conhecidos da base (`RelayStreamReplicationTest` 200/400, ingest como root) — não
  representam o branch integrado.

### Verificação pendente (ordem)

1. `mvn -pl nishi-utils-ngrrd-cluster verify` (unitários do ngrrd contra core 8.8.0) — em execução.
2. 8× `LeaderFailoverDuringMigrationClusterTest` com `-Pngrrd-cluster -Djdk.virtualThreadScheduler.parallelism=2`
   no branch integrado (critério de aceite da #178); + 2× dos `*ClusterTest` novos
   (`QuotaClusterTest`, `PlacementRulesClusterTest`, `ForgetNodeClusterTest`, `IdleCatalogFailoverClusterTest`).
3. `mvn -pl nishi-utils-core test` completo e `-Presilience -Dsurefire.rerunFailingTestsCount=1`.
4. `mvn verify -Pvalidate-javadoc` e o comando do `pr-validation.yml`
   (`mvn verify -pl nishi-utils-core,nishi-utils-oss,nishi-utils-ngrrd-cluster -am -DexcludeNgrid=true -Dsurefire.rerunFailingTestsCount=1`).
5. Limpar worktrees dos subagentes (`.claude/worktrees/agent-*`), PR (só quando pedido) e
   comentar nas issues #178/#167 após o merge.

### Como retomar num ambiente novo

```bash
git fetch origin claude/issues-178-167-ngrrd-ygtv07 && git checkout claude/issues-178-167-ngrrd-ygtv07
export JAVA_TOOL_OPTIONS=
mvn -B -q install -DskipTests -Djacoco.skip=true -pl .,nishi-utils-core,nishi-utils-oss   # core/oss 8.8.0 no ~/.m2
mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster -Dtest=LeaderFailoverDuringMigrationClusterTest \
    -DfailIfNoSpecifiedTests=false -Djdk.virtualThreadScheduler.parallelism=2
```
