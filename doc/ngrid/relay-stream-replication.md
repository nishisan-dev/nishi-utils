# Replicação RELAY_STREAM — relay-log sequencial estilo MySQL

## Contexto

A replicação do NGrid era **push-uma-vez + recuperação por NAK**: o líder fazia
broadcast de cada operação (`REPLICATION_REQUEST`) e o follower reordenava num
buffer **em memória limitado** (cap 50k), pedindo `SEQUENCE_RESEND_REQUEST` ao
detectar buracos. Sob _firehose_ (milhões de eventos), qualquer gap — race de
handler, blip de epoch, watermark de snapshot — virava **tempestade de NAK** e
ocasional _snapshot fallback_, com o follower perseguindo um alvo móvel.

O `RELAY_STREAM` substitui esse modelo pelo de **relay-log do MySQL** (master/slave):

> O produtor (líder) escreve seu **op-log durável** (binlog); o follower **puxa**
> esse log como um **stream estritamente sequencial dirigido por um cursor
> durável**, persiste em ordem no seu relay e **aplica no seu ritmo**. Em regime
> permanente **não há gap, NAK nem snapshot** — o pull é contíguo por construção.

## Fluxo

![RELAY_STREAM — follower puxa o op-log do líder](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_relay_stream_pull.puml)

- **Líder serve o op-log** (`handleRelayStreamFetch`): responde a um
  `RELAY_STREAM_FETCH(topic, from, maxBatch)` com um **run contíguo** lido do
  op-log a partir de `from` (corta no primeiro buraco), ou sinaliza
  `needSnapshot` quando `from` está abaixo do piso de retenção (`oldestSequence`).
- **Follower puxa** (`relayFetchLoop` + `handleRelayStreamBatch`): uma _IO thread_
  por tópico busca a partir do cursor durável (a cauda do relay), persiste os
  frames **em ordem** (`relayFor(topic).offer`), avança o cursor **somente após o
  append durável** e acorda a _apply thread_. Re-fetch do mesmo `from` é
  **idempotente** (duplicatas descartadas).
- **Apply** (`relayApplyLoop`/`drainRelayOnce`): drena o relay em lotes,
  estritamente em ordem; como o stream é contíguo, **o ramo de gap nunca dispara**.

## Op-log do líder (binlog) × relay do follower — retenção

Os dois arquivos têm políticas de limpeza diferentes, espelhando o MySQL:

| | nishi-utils | MySQL |
|---|---|---|
| **Op-log do líder** (`ResendLog`) | retenção FIXA por **contagem** (`resendLogMaxEntries`, default 10M) + **tempo** opcional (`replicationLogRetentionTime`), drop de segmento inteiro; independe do consumo | binlog: `max_binlog_size` + `binlog_expire_logs_seconds`, independe do slave |
| **Relay do follower** (`NQueue`) | `TIME_BASED` + **`retentionClampToConsumer=true`** + compactação 30s → guarda só o backlog não-aplicado | relay log purgado pela SQL thread (`relay_log_purge`) |

O op-log é a fonte da verdade do stream e, no modo stream, é inicializado
**incondicionalmente** (independente de `persistentResendLog`). O piso
(`oldestSequence`) define até onde um follower atrasado pode puxar antes de
precisar de snapshot — tunável por `resendLogMaxEntries` (exposto no
`NGridConfig.Builder`).

## Semântica de durabilidade — ASYNC (default)

O `RELAY_STREAM` é **ASYNC**: o líder commita no próprio op-log (quorum efetivo 1)
e **não bloqueia** nos followers, que puxam no seu ritmo. Throughput máximo; a
janela de perda da cauda não-puxada na falha do líder é a mesma do MySQL async
(no Cardinal, recuperável re-drivando do Kafka). Um **semi-sync opt-in** (líder
espera ≥1 follower persistir o receipt antes do ack) está previsto para fechar
essa janela quando a durabilidade ≥2 nós for requisito.

## Cutover, restart e failover

- **Snapshot cutover** (abaixo do piso): `completeSnapshotCutover` ancora
  `nextExpected = watermark+1` **e re-ancora o cursor do stream no watermark**,
  retomando o pull de `watermark+1`.
- **Restart limpo**: o cursor inicializa da **cauda do relay** (maior sequência
  persistida) → retoma de onde parou, sem re-puxar nem snapshot. _Pressupõe que o
  estado aplicado do backend é durável até a fronteira — fila (NQueue) sempre;
  mapa só com `mapPersistenceMode` ≠ DISABLED._
- **Failover**: o follower **espelha cada run aplicado no próprio op-log**, então
  um nó promovido serve o histórico por stream em vez de forçar snapshot.

## Coordenação — epoch como termo de cluster + afinidade

O `RELAY_STREAM` depende de uma coordenação sã: o frame do stream carrega `epoch`,
então um fencing insalubre congelaria o stream igual ao push. A Fase 0 corrige:

![Epoch monotônico + fencing por identidade](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_epoch_cluster_term.puml)

- **Termo monotônico convergido**: todo nó adota o maior `epoch` visto; o líder
  re-carimba acima de qualquer termo fantasma (`observeEpoch`, guarda `>` estrito
  evita escalada por eco). Corrige a regressão (líder voltou em epoch menor após
  perder o `leader-epoch.dat`).
- **Fencing por identidade**: o follower aceita o líder **acordado** (`max(NodeId)`)
  e adota seu epoch mesmo que pareça menor — nunca congela; rejeita só um ex-líder
  stale (identidade diferente, epoch menor).
- **Eleição por afinidade de prioridade**: ordena por `(priority, NodeId)`; o
  não-preferido **defere** a auto-eleição numa janela de descoberta no boot
  (`bootDiscoveryWindow`) até confirmar isolamento (lead-while-alone / AP);
  descoberta do preferido via gossip dispara reeleição limpa. Reduz a "reeleição
  curiosa".

## Configuração

Modo selecionado por `followerIngestMode: RELAY_STREAM` (YAML mapeia por nome) ou
`NGridConfig.Builder.followerIngestMode(FollowerIngestMode.RELAY_STREAM)`.

| Knob (`ReplicationConfig`) | Default | Efeito |
|---|---|---|
| `relayStreamFetchBatch` | 512 | máx. entradas por fetch |
| `relayStreamPollInterval` | 50ms | long-poll quando em dia |
| `relayStreamFetchTimeout` | 2s | re-emissão idempotente de fetch perdido |
| `relayMaxBacklog` | 200000 | flow-control: pausa o fetch quando o relay enche |
| `resendLogMaxEntries` | 10M | janela do binlog (exposto em `NGridConfig.Builder`) |

Eleição (`ClusterCoordinatorConfig`): `bootDiscoveryWindow` (default ZERO =
desligado) habilita a deferência do não-preferido; prioridade vem da
`NodeInfo.priority()` (config YAML `node.priority` / `seedNodes[].priority`).

## Observabilidade (por tópico)

`ReplicationManager` expõe, e o `TopicReplicationStatus` carrega:
`getRelayStreamCursor`, `getLeaderHighWatermark`, `getLeaderOldestSequence`,
`getReplicationLag`, `getStreamBytesIn`, `isStreaming`. O contador de progresso
aplicado (`getLastAppliedSequence`) é re-semeado da fronteira durável no restart.

## Migração

`RELAY_STREAM` é o modo definitivo (sem fallback de runtime para push+NAK). Os
modos `INLINE`/`RELAY_LOG` permanecem para compatibilidade e serão removidos junto
com o push+NAK+reorder quando `RELAY_STREAM` virar default. A troca de
`followerIngestMode` exige restart coordenado do cluster (config estática).
