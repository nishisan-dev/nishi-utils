# NGrid — Conexão direta entre peers concorrentes (issue #117)

## Contexto

Em um cluster usando `TcpTransport` + `ClusterCoordinator`, peers iniciados
**concorrentemente** em configuração full-mesh sofriam um *simultaneous open*
TCP: cada par abria **dois sockets** (um outbound de cada lado). O handshake
substituía a conexão anterior de forma **incondicional e sem desempate**,
fazendo cada lado fechar o próprio outbound. O resultado era *flapping* e, no
limite, um par acessível **somente via proxy** (hub). Quando o hub saía, os
peers restantes perdiam o caminho um para o outro, caíam abaixo do quórum e o
cluster ficava **sem líder** — failover quebrado, com o primeiro nó virando um
SPOF.

## Causa raiz

Três defeitos compunham o problema, todos em
`dev.nishisan.utils.ngrid.cluster.transport.TcpTransport`:

1. **Registro de conexão sem reconciliação.** O registro outbound publicava a
   conexão em `connections` de forma incondicional, em uma corrida com o
   handshake inbound (locks distintos) — um dial podia sobrescrever/orfanar a
   conexão canônica.
2. **Handshake com *blind replace*.** Ao receber um handshake, a conexão
   anterior para o peer era fechada sem critério; em *simultaneous open*, ambos
   os lados fechavam o próprio outbound.
3. **Probe de recuperação inócuo.** O probe abria um socket descartável **sem
   handshake** e promovia a rota a `DIRECT` sem reconectar de fato — a próxima
   mensagem recolapsava para `PROXY` (flapping), prendendo o par no hub.

## Solução

Ponto único de reconciliação, `registerLiveConnection`, sob o lock por-peer,
para onde convergem **tanto o caminho outbound quanto o handshake inbound**:

- **Desempate determinístico por NodeId.** Quando há duas conexões vivas para o
  mesmo peer, vence a iniciada pelo **menor NodeId**. Ambos os endpoints
  computam o mesmo vencedor (`(local < remote) == conn.outboundInitiated`) e
  convergem para o **mesmo socket físico**. Não há mais blind-replace.
- **Bootstrap preservado.** O dial dos `initialPeers` (seeds) permanece
  **incondicional** — um seed não conhece o nó que entra e não pode discar de
  volta. A colisão resultante é resolvida pela reconciliação acima.
- **Auto-promoção de rota.** Ao concluir o handshake de uma conexão direta, a
  rota é promovida a `DIRECT` (self-heal de rota proxy remanescente).
- **Probe correto.** O probe passa a reestabelecer uma conexão **real** (com
  handshake) antes de promover; qualquer colisão é reconciliada pela mesma
  regra.

![Reconciliação determinística de simultaneous-open](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrid_simultaneous_open.puml)

## Hardening de quórum (ClusterCoordinator)

Dois ajustes complementares no `ClusterCoordinator`:

- **Denominador robusto.** `requiredActiveMembersForLeadership()` conta apenas
  peers com porta real (`port > 0`), evitando que clientes de descoberta e
  placeholders de gossip (porta 0) inflem a maioria exigida e estagnem a
  eleição.
- **Grace via reachability de proxy.** `evictDeadMembers()` concede um *grace*
  **limitado** (`2 × heartbeatTimeout`) a um membro com heartbeat atrasado que
  ainda seja `transport.isReachable` (inclui caminho via proxy), evitando perda
  espúria de quórum durante um flap de link direto.

> **Trade-off documentado.** Considerar reachability somente-via-proxy mantém o
> hub relator como um SPOF parcial — por isso o grace é **limitado no tempo**:
> um peer genuinamente morto (sem rota, ou atrasado além da janela) continua
> sendo despejado. Com a malha direta restabelecida pela correção do
> transporte, esse caminho atua apenas em estados degradados transitórios.

## Evidência e testes

- **`TcpTransportConcurrentMeshTest`** (core) — guarda do invariante: três
  transports full-mesh iniciados concorrentemente convergem para uma malha
  direta **estável** (sem proxy, sem flapping).
- **`NGridConcurrentMeshFailoverIT`** (ngrid-test, profile `docker-resilience`)
  — cenário em rede real: full-mesh + start concorrente + netem + failover;
  cada nó reporta `DIRECT_PEERS_COUNT == 2` e o cluster reelege ao perder um nó.

> **Nota de reprodutibilidade.** A corrida original **não é reproduzível sob
> demanda** em ambiente de teste (loopback ou bridge Docker, mesmo com perda de
> pacotes): numa rede funcional o TCP retransmite o `FIN` e o código não
> corrigido se recupera via reconexão. O estado "preso em proxy" reportado
> exigiu uma falha de caminho real. A correção é garantida pelo **invariante
> determinístico** de `registerLiveConnection`; os testes acima validam o
> comportamento corrigido e guardam contra regressões.
