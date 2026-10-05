# 8.10.1: rótulo do snapshot, seguidor à frente do líder e drenagem do mapa no shutdown

Incidente do CTP (tems): a réplica de `map:ngrrd.catalog` na storage-209 ficou 3 dias descartando
operações do líder, com lag 0, e terminou com 40 chaves ausentes e possíveis valores desatualizados.
Escopo aprovado pelo usuário em 2026-10-04. A análise está registrada no canal
`/tmp/tems-nishiutils-purga-cluster-chat.md`.

## Causa raiz (core NGrid; o código da v8.8.0 é igual ao da main)

1. No líder, `ReplicationManager.getSyncSequenceForTopic` rotula o snapshot do `SYNC_REQUEST` com o
   contador de produção **cru** (`sequenceByTopic`). Um líder recém-promovido que ainda não produziu
   no tópico usa o valor de um mandato antigo.
   - Um handback D11 servido por esse líder faz SET da fronteira **para baixo** nos participantes.
   - Os nós de fora continuam na numeração anterior, ficando "à frente".
   - O #177 corrigiu o mesmo erro só no HWM (`currentLeaderTopicSequence`).
2. O seguidor à frente busca a partir de `cursor+1` e recebe lote vazio, sem `needSnapshot`.
   - O lag é truncado em 0.
   - Depois, a deduplicação por seq descarta como duplicadas as operações do líder até ele alcançar o
     cursor.
   - O aviso "peer watermark above its own applied ... retaining" (`ClusterCoordinator`) é só sintoma;
     não há caminho corretivo.
3. Bug independente (M1): o `NGridNode.close` fecha os `DistributedMap`, cujo callback remove o
   `MapClusterService` de `mapServices`. O laço seguinte nunca fecha os serviços, e o writer do NMap
   não é drenado.

## Entregas

- **A. Rótulo correto.**
  - `getSyncSequenceForTopic` no líder passa a usar `max(produzido, nextExpected-1)`, igual a
    `currentLeaderTopicSequence`.
  - Na promoção a líder, normaliza `sequenceByTopic[t] = max(contador, nextExpected-1)` para todos
    os tópicos.
  - Revisar os outros pontos que leem `sequenceByTopic` cru como fronteira (watermark de handback,
    `byTopic`, status).
- **B. Autocura do seguidor à frente.**
  - Gatilho: o `RELAY_STREAM_BATCH`/fetch traz `leaderHighWatermark` válido abaixo do cursor do
    seguidor de forma persistente (K respostas seguidas **e** T segundos), com líder estável e sem
    `leaderSyncing`.
  - Ação: arma `relayPendingBootstrap` do tópico e loga SEVERE com um marcador estável.
  - Histerese e cooldown contra falso positivo.
  - Custo aceito: operações que só existiam no seguidor se perdem, mas a réplica converge em vez de
    divergir em silêncio.
- **M1. Drenagem no shutdown.** O `NGridNode.close` fecha e drena todos os `MapClusterService`,
  inclusive os que o callback de destroy removeu.
- **Reancoramento escalar legado:** avaliar a remoção. Ele usa um `primaryTopic` arbitrário, e na
  8.8.0+ o `byTopic` está sempre presente. Remover só se todo caminho de upgrade suportado (≥ 8.8.0)
  enviar `byTopic`.

Fora do escopo:
- anti-entropia de conteúdo;
- o bug do watermark do snapshot lido antes do apply assíncrono (M2);
- a persistência do snapshot instalado no NMap.

Esses itens viram issues.

## Testes determinísticos

1. **Unitário A:** líder promovido com `_topic:T` desatualizado e fronteira maior, sem produzir; o
   `SYNC_REQUEST` deve responder com a fronteira.
2. **Unitário B:** lotes com HWM do líder abaixo do cursor por K vezes e T segundos devem armar o
   bootstrap e emitir `SYNC_REQUEST`. Um caso isolado ou dentro da histerese não arma.
3. **Cluster (resiliência) com handback:**
   - o nó A lidera e produz em T;
   - B é promovido sem escrever em T;
   - A volta com mais afinidade e faz handback;
   - A escreve chaves novas;
   - C deve contê-las. Antes da correção, C fica sem nenhuma, com lag 0.
4. **Unitário M1:** escritas enfileiradas no NMap são persistidas após `NGridNode.close`.

## Versão e validação

- 8.10.1 (patch): sem mudança de API pública.
- JDK 21, Maven serializado com `flock`. Rodar o `mvn test` do core e do módulo cluster, o
  `-Presilience` do core (a CI não roda) e o `-Pngrrd-cluster`.
- Documentação: CHANGELOG; runbook `doc/runbooks/post_incident_consistency.md`, que afirma que
  "reiniciar o follower força snapshot" (falso no RELAY_STREAM); operação do ngrrd-cluster com
  rolling upgrade e resync.
