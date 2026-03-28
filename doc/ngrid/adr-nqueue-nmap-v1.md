# ADR — Semântica V1 do NQUEUE e NMAP no NGrid

## Status
- Aceito para a linha atual de hardening do core.

## Contexto
- O `NQUEUE` já possui base de log distribuído com `TIME_BASED`, key/headers, replay por offset, catch-up por snapshot e reenvio de sequência.
- O `NMAP` já possui núcleo de KV distribuído com `put/get/remove`, persistência opcional em WAL+snapshot e leituras `STRONG`, `BOUNDED` e `EVENTUAL`.
- O principal risco atual não é “ausência total de funcionalidade”, e sim mistura de semânticas concorrentes e expansão precoce de escopo.

## Decisão
### NQUEUE
- O produto passa a ser tratado como **stream/log distribuído inspirado em Kafka**.
- O caminho principal é `queues:` com retenção `TIME_BASED`.
- O consumo lógico usa identidade explícita `groupId` + `consumerId` via `DistributedQueue.openConsumer(...)`.
- `DistributedQueue.poll()` continua existindo como compatibilidade do modelo antigo baseado em `NodeId`.
- `DELETE_ON_CONSUME` continua existindo apenas por legado e não orienta o roadmap principal.

### NMAP
- O produto passa a ser tratado como **KV distribuído resiliente inspirado em Hazelcast**.
- O foco desta fase é consistência, persistência, failover e contrato de leitura.
- A API pública central permanece: `put/get/remove/containsKey/size/keySet/putAll`.

## Não objetivos do v1
### NQUEUE
- Sem partições estilo Kafka.
- Sem transactions.
- Sem broker-side ack/redelivery.
- Sem rebalanceamento sofisticado de consumer group.

### NMAP
- Sem entry processor.
- Sem listeners distribuídos.
- Sem near-cache.
- Sem compute grid.

## Consequências
- O contrato do `NQUEUE` fica mais claro para replay e consumidores independentes.
- O caminho legado continua compatível, mas deixa de ser o modelo recomendado.
- O `NMAP` segue pequeno na API e forte na operação, em vez de tentar competir em breadth com Hazelcast nesta fase.
