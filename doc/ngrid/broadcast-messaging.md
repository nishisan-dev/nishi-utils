# Broadcast inter-nós (`broadcastMessage` / `onMsgBroadcasted`)

> Disponível desde a **4.5.0**. Primitiva de **coordenação leve** entre nós do NGrid.

## Visão geral

Permite que código que usa o `ReplicationManager`/`NGridNode` troque **mensagens pequenas e
fire-and-forget** entre os nós do cluster, para coordenar ações (ex.: sinalizar um evento, pedir um
recálculo, anunciar um estado local). A identidade do **produtor** acompanha cada mensagem.

```java
// Em qualquer nó:
node.addBroadcastListener((produtor, msg) -> {
    // executado numa worker thread — mantenha não-bloqueante
    log.info("{} disse: {}", produtor, msg);
});

// Em qualquer nó (inclusive o produtor recebe — loopback):
node.broadcastMessage("recalcular-particao-7");
```

Também disponível diretamente no `ReplicationManager`
(`replicationManager.broadcastMessage(...)` / `addBroadcastListener(...)`).

## Semântica

| Propriedade | Comportamento |
|---|---|
| **Entrega** | **Best-effort.** Vai para todos os membros alcançáveis no momento do envio (via `transport.broadcast`). Um peer offline simplesmente perde a mensagem. |
| **Ordem** | **Não ordenada.** Sem garantia de ordem entre mensagens ou entre nós. |
| **Durabilidade** | **Não durável.** Não persiste; não é re-entregue após restart. |
| **Loopback** | **Ligado.** O produtor também recebe a própria mensagem (com `produtor` = id local), numa worker thread (espelha o caminho remoto). |
| **Threading** | O listener roda no worker pool. Mantenha-o curto e não-bloqueante. |

> **Quando NÃO usar:** se você precisa de entrega garantida, ordenada ou durável (ex.: comandos que
> não podem se perder), use uma **fila replicada** (`DistributedQueue`) — ela é leader-mediated,
> replicada por quórum e persistida. O broadcast é deliberadamente o oposto: barato e descartável.

## Implementação

- Novo `MessageType.USER_BROADCAST` + `BroadcastMessagePayload(String body)`. A identidade do produtor
  viaja no `source` do `ClusterMessage` (não é duplicada no payload).
- `broadcastMessage` faz fan-out via o `transport.broadcast` existente e despacha em **loopback** aos
  listeners locais numa worker thread.
- Recepção: `onMessage` roteia `USER_BROADCAST` → `dispatchBroadcast(source, body)`.

## Nota de fechamento (alternativas avaliadas)

A sugestão original era um método no `ReplicationManager`. Ao revisar o código:

- **Não existia** pub/sub público no NGrid. `DistributedQueue`/`DistributedMap` são estruturas
  **replicadas e duráveis** (pesadas, leader-mediated) — ferramenta errada para sinais
  fire-and-forget de coordenação.
- **Já existia** `transport.broadcast(ClusterMessage)` (fan-out per-peer em virtual threads), mas é
  **interno** (lida com `ClusterMessage` cru) e não tem contrato de produtor/listener.

Por isso a escolha foi uma **API fina** sobre `transport.broadcast` (novo `MessageType` + payload +
`BroadcastListener`), exposta tanto no `ReplicationManager` (objeto de coordenação que o usuário já
detém) quanto no `NGridNode` (superfície pública). É o encaixe mínimo correto: reusa o primitivo de
transporte existente e o idioma de listener do projeto (`CopyOnWriteArraySet` + `add/remove`), sem
acoplar a mensageria de coordenação ao caminho de replicação (sequência/quórum), que não tem nada a ver
com isto.

## Diagrama

![Broadcast inter-nós com loopback](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_broadcast_messaging.puml)
