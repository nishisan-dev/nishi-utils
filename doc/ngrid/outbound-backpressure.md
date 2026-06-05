# Backpressure na fila outbound do NGrid (drop + catch-up)

> Referência: issue #113. Relacionada à #112 (HA active-standby com `quorum=1`).

## Contexto

Sob `quorum=1` + `strictConsistency=false`, a escrita do líder é **best-effort**:
o `put` completa após o *ack* local, **sem esperar RTT** dos followers. Esse é o
modo de HA active-standby (RPO best-effort).

Nesse modo, os followers não exercem contrapressão sobre o líder. A fila de saída
(*outbound*) **por conexão** do `TcpTransport` era ilimitada
(`LinkedBlockingQueue` sem capacidade). Num cenário de *fault management* — um
*trap storm* gera rajadas de milhares de eventos/s, com payloads de vários KB —
o escritor (writer) pode não acompanhar o produtor e a fila cresce sem limite,
causando **pressão de heap / risco de OOM no líder**.

## Política: drop + catch-up

A capacidade da fila outbound passa a ser **configurável por conexão**. A política
ao encher é **descartar a mensagem de replicação** excedente e deixar o follower
atrasado se recuperar pelo **mecanismo de gap/snapshot já existente**.

Princípios:

- **Somente `REPLICATION_REQUEST` é limitado/descartado.** É a origem do consumo de
  memória (payloads gordos) e o único tráfego 100% recuperável via catch-up.
- **Controle nunca é represado nem descartado** (heartbeat, ping, sync, acks,
  client request/response, sequence resend). Assim o ciclo de heartbeat **nunca é
  atrasado** — eliminando o risco de reeleição espúria que a política de *block*
  traria.
- **O líder permanece assíncrono.** O `put` não bloqueia: ao encher, descarta e
  segue.
- **Recuperação automática e periódica.** O *high-watermark* do líder viaja no
  heartbeat; o follower roda `checkLagAndSync()` a cada ~2s e, ao detectar lag,
  dispara `requestSync` (snapshot). Um `REPLICATION_REQUEST` descartado é
  reconciliado **mesmo que a rajada pare**.

### Semântica do limite

- O limite é **por enlace físico (conexão)**, não por destino lógico. Sob
  roteamento por proxy, um `REPLICATION_REQUEST` destinado a *D* pode trafegar
  pela conexão de um proxy *P* e ser descartado lá — é seguro (alívio de memória
  em *P*; o catch-up de *D* recupera).
- O limite é **soft**: sob produtores concorrentes, a profundidade pode exceder a
  capacidade por, no máximo, o número de produtores simultâneos — nunca cresce sem
  limite.
- A métrica de descartes contabiliza **descartes por capacidade** (backpressure),
  não perdas por ausência de rota.

## Fluxo

![Backpressure outbound (drop + catch-up)](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_outbound_backpressure.puml)

## Configuração

### YAML

```yaml
cluster:
  transport:
    workers: 4
    # 0 = ilimitada (default, compatível). Ex.: limitar a 10.000 mensagens de
    # replicação pendentes por conexão:
    outboundQueueCapacity: ${OUTBOUND_QUEUE_CAPACITY:0}
```

### Builder (programático)

```java
NGridConfig config = NGridConfig.builder(local)
        .replicationFactor(1)          // quorum=1 → best-effort
        .strictConsistency(false)
        .outboundQueueCapacity(10_000) // 0 = ilimitada (default)
        .build();
```

## Observabilidade (RF3)

A ocupação e os descartes da fila outbound, **por nó**, são expostos no
`NGridNode.operationalSnapshot()` e serializados pelo `NGridDashboardReporter`
(seção `transport` do `dashboard.yaml`):

```yaml
transport:
  outboundQueueDepthByNode:
    follower-1: 0      # mensagens de replicação pendentes na conexão
  outboundDroppedByNode:
    follower-1: 1234   # replicações descartadas por backpressure (cumulativo)
```

A profundidade é medida **mesmo no modo ilimitado** (capacidade 0), permitindo
diagnosticar a pressão *antes* de definir um limite.

## Critério de aceitação e testes

- **Prova formal do bound (memória estável):** teste unitário determinístico
  `OutboundChannelTest` — produtor rápido + consumidor lento: a profundidade de
  replicação nunca excede a capacidade, há descartes, e o controle sempre passa.
- **Integração em cluster:** `OutboundBackpressureConvergenceTest` — com a
  capacidade configurada e `quorum=1`, o cluster converge (todas as chaves chegam
  ao follower via replicação + catch-up) e a métrica por nó é exposta no snapshot.

## Referências de código

- `cluster/transport/OutboundChannel.java` — fila + política + contadores.
- `cluster/transport/TcpTransport.java` — `Connection` delega ao canal; acessores
  `outboundQueueDepths()` / `outboundDropped()`.
- `cluster/transport/TcpTransportConfig.java`, `structures/NGridConfig.java` —
  `outboundQueueCapacity` (default 0).
- `config/ClusterPolicyConfig.java`, `config/NGridConfigLoader.java` — binding YAML.
- `replication/ReplicationManager.java` — `checkLagAndSync()` (catch-up periódico).
- `metrics/NGridOperationalSnapshot.java`, `metrics/NGridDashboardReporter.java` —
  exposição da métrica (RF3).
