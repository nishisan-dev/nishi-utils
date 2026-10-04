# 8.10.0: `PLACEMENT_UNAVAILABLE` e gate de criação fora do lock de stripe

Pedido do tems/ngrrd-server, discutido no canal `/tmp/tems-nishiutils-purga-cluster-chat.md`.
Escopo aprovado pelo usuário em 2026-10-04.

## Problema (8.9.0)

- `SeriesDeleteHandler.creationGate` consulta os storages um por vez, com `SERIES_INSPECT` e o
  `requestTimeout` cheio. Não checa alcançabilidade antes de chamar.
- O gate roda dentro de `catalog.placementLock(key)`, um dos 256 stripes. Um storage pendurado
  bloqueia também o PLACE de outras séries novas do mesmo stripe.
- A falha do gate sobe como exceção e chega ao cliente como `REMOTE_ERROR` genérico. O tems
  distingue o caso pela string `ngrrd.series.inspect`. Com o nó pendurado, o cliente estoura junto
  com o líder e recebe `TIMEOUT`; o `TransportRetry` trata `TIMEOUT` como falha de transporte e
  retenta até o prazo, o que provoca o laço no coordenador.

## Entregas

1. **Alcançabilidade:** participante fora de `leaderView.reachableNodeIds()` gera
   `PLACEMENT_UNAVAILABLE` imediato, sem RPC.
2. **Inspeção paralela com timeout curto próprio:** nova configuração `placementInspectTimeout`,
   padrão de 2 s, validada como positiva. Timeout ou falha de transporte geram `PLACEMENT_UNAVAILABLE`.
   Precedência do resultado: `QUARANTINED` > `MIGRATING` (remoção em curso) > `PLACEMENT_UNAVAILABLE`.
3. **RPCs fora do lock de stripe** (prioridade do tems):
   - sob o lock, faz a pré-checagem (liderança, placement ausente, carência pós-liderança);
   - solta o lock e roda o gate;
   - readquire o lock e revalida liderança e ausência de placement antes de escolher e gravar.
     Se surgiu placement, segue o caminho de placement existente (`OK`, ou `MIGRATING` se houver remoção).
   - Justificativa: as mutações do lado do líder que interessam ao gate (PLACE, adoção, remoção,
     migração) exigem placement presente ou o criam. Logo, "ausente → ausente" só ocorre com uma
     remoção já finalizada, porque o placement só sai depois de todos os participantes aplicarem,
     e criar depois dela é legítimo. O lock nunca protegeu o estado remoto, como a quarentena feita
     pelo reconciliador.
   - Teste determinístico com gate bloqueado por latch:
     - outra chave do mesmo stripe progride;
     - PLACE concorrente da mesma chave não duplica o placement;
     - remoção em curso aparece → `MIGRATING`;
     - perda de liderança → `NOT_LEADER`.
4. **Contrato:**
   - `SeriesStatus.PLACEMENT_UNAVAILABLE` e `ErrorCode.PLACEMENT_UNAVAILABLE`;
   - `PlaceResponse.unavailableNodeIds`;
   - `NgrrdClusterException.unavailableNodeIds()`, que devolve uma lista vazia nos demais casos.
5. **Cliente:** `PlacementResolver` lança na hora, sem retry interno. Status desconhecido (`null`
   pelo `READ_UNKNOWN_ENUM_VALUES_AS_NULL`) vira `REMOTE_ERROR` em vez de NPE.
6. **Compatibilidade:**
   - `PlaceRequest.acceptsPlacementUnavailable` (falso no JSON antigo).
   - O líder só emite o status novo para quem anunciar a flag. Para os demais, devolve erro de
     aplicação com uma mensagem que contém `ngrrd.series.inspect`, preservando a discriminação
     atual do tems.
   - A ordem de deploy continua: storages primeiro.

Fora do escopo: criação degradada (dispensada de comum acordo).

## Versão

8.10.0 (minor). Há constante nova em enums públicos (`ErrorCode`), um acessor novo e campos novos
em records do protocolo. Um `switch` exaustivo do consumidor sobre `ErrorCode` precisa de ajuste.

## Validação

- JDK 21 (`JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`), com Maven serializado por `flock`.
- `mvn test` completo, `-Pngrrd-cluster` (inclui `SeriesDeleteClusterTest`) e `-Pvalidate-javadoc`.
- Documentação: `doc/oss/ngrrd-cluster-purga.md`, `doc/oss/ngrrd-cluster.md` e `doc/CHANGELOG.md`.
