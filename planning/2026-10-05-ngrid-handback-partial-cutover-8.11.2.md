# 8.11.2: handback sem cutover parcial e sem reancoragem de tópicos não instalados

Issue #199. Incidente no CTP em 2026-10-05, às 17:23:36: o candidato instalou só
`_ngrid-queue-offsets`, declarou o cutover, e o incumbente renumerou `map:ngrrd.nodes` para baixo.
A investigação (só leitura, em `dcd8154`) confirmou a causa. O defeito existe desde a 8.8.0.
Escopo aprovado pelo usuário em 2026-10-05.

## Causa
- `NGridNode.start()` inicia o `replicationManager` e o `coordinator` antes de registrar filas e mapas
  configurados. O nó anuncia fronteiras de disco sem handler e pode pedir handback.
- O candidato arma bootstrap só para `handlers.keySet()`. O `frozenByTopic` do GRANT nunca é lido.
  Ele conclui no último tópico pendente e envia um `cutoverByTopic` com fronteiras de disco de tópicos
  que não instalou.
- O incumbente faz SET incondicional do vetor (`reanchorAsDemotedIncumbent`), sem comparar com
  `handoverFrozenByTopic`.
- O papel do candidato é zerado antes de `assumeLeadershipForHandback`. O tick de 200 ms reenvia o
  REQUEST, recebe ABORT e entra em cooldown espúrio de 60 s.
- `aheadEligiblePeer` devolve o primeiro peer da iteração, não o melhor.

## Entregas
- **A. O contrato do handback é o `byTopic` do GRANT** (fallback para `handlers.keySet()` com líder
  antigo, sem vetor).
  - O candidato conclui só quando instalou **todos** os tópicos exigidos. Um tópico exigido sem
    handler é armado ao ser registrado, com limite de `handoverSnapshotTimeout`; se o limite estoura,
    aborta.
  - `cutoverByTopic` contém só os tópicos instalados.
  - Compatível com o fencing e o rollback da 8.11.1.
- **B. O incumbente valida o vetor.** Só reancora quando o cutover é igual à fronteira congelada do
  tópico; nunca para baixo. Em caso de divergência: não reancora, mantém op-log e relay e loga SEVERE
  `NGRID_HANDBACK_VECTOR_MISMATCH`.
- **C. Gate de prontidão.** Filas e mapas configurados ficam registrados antes de o nó poder pedir ou
  aceitar handback ou ser elegível a líder. O mecanismo fica a critério da implementação (reordenar o
  `start()` ou uma flag `handlersReady`). O gate também fecha o bootstrap parcial em restart sujo
  (o gate D8 soltava no primeiro tópico).
- **D.** O papel do candidato não é zerado antes da promoção; no lugar, um estado transitório. Só um
  REQUEST por tentativa.
- **E.** `aheadEligiblePeer` escolhe o melhor peer via `compareAdvertisedState`.

Fora do escopo (#200):
- `tieBreak` comparando o total antes dos tópicos prioritários;
- SYNC sem handler descartado em silêncio;
- HWM anunciado sem handler;
- backstop truncando tópicos não instalados.

## Testes determinísticos
- **T1:** candidato com só parte dos handlers registrada no GRANT.
  - Nenhum COMPLETE nem promoção até instalar todos os tópicos.
  - `cutoverByTopic == frozenByTopic`.
  - Variante em que o timeout estoura: aborta.
- **T2:** COMPLETE com tópico abaixo do congelado. O incumbente não rebaixa, não trunca o op-log nem
  purga o relay, e emite o marcador.
- **T3:** cluster de 3 nós com o registro de um mapa atrasado (seam de teste) e escrita contínua no
  mapa durante o handback.
  - Nenhuma fronteira do incumbente diminui.
  - O terceiro nó não fica à frente.
  - Todo put confirmado está presente nos 3 nós.
  - Um único REQUEST por tentativa.
- **T4:** mapa de carga lenta. O nó não fica elegível nem pede handback até o último mapa registrar.
- Mutações provando cada correção.

## Validação
- JDK 21, Maven serializado com `flock`.
- Suítes: core (`mvn test`), resiliência (`-Presilience`), cluster (`mvn verify` e `-Pngrrd-cluster`)
  e Javadoc.
- 8.11.2 é patch: sem API pública nova, salvo justificativa.
- Documentação:
  - CHANGELOG;
  - `doc/oss/ngrrd-cluster-operacao.md`, com o handback seguro e a retirada da mitigação de
    `priority`.
