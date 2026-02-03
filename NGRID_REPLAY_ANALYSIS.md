# NGrid replay/duplicidade - analise do cenario

## Contexto

Durante o teste `scenario-1` do `ngrid-test`, foi observado replay/duplicidade de mensagens no epoch 1 (INDEX-1-*). O objetivo do teste era validar o comportamento de consumo com queda e retorno do seed.

O cenario executa:
- 1 seed + 2 clients.
- Producao de 60 mensagens (1s por mensagem).
- O seed e interrompido e reiniciado durante o teste.
- Os clients continuam em execucao e consomem via `pollWhenAvailable`.

## Resultado observado (resumo)

Ao analisar `/tmp/ngrid-scenario.log`:
- O epoch 1 produz INDEX-1-0..INDEX-1-9.
- Apos a queda do seed, o client passa a agir como lider e ocorrem duplicidades de INDEX-1-*.
- Depois do restart do seed, o epoch 2 (INDEX-2-*) e consumido de forma limpa.

Exemplo real:
- `client-2 recebeu-duplicado: INDEX-1-0` ate `INDEX-1-9`.
- `client-1 recebeu-duplicado: INDEX-1-0` ate `INDEX-1-9`.

## Evidencias no log

As duplicidades aparecem junto de `POLL` replicado pelo lider temporario:
- `QueueClusterService apply: Applied replication command POLL`
- Em seguida: `client-2 recebeu-duplicado: INDEX-1-x`
- Logo apos: `Replication request opId=... seq=... topic=queue:global-events from=client-2`

Isso indica que o lider temporario esta processando polls com offsets desatualizados, causando reentrega das mensagens.

## Hipotese principal

A duplicidade nao vem de mensagens duplicadas na fila, mas de regressao de offsets quando o lider muda:
1) Seed cai.
2) Client assume lideranca com offsets locais atrasados (ou nao sincronizados).
3) Ao servir polls, o cliente entrega mensagens ja consumidas.

Quando o seed retorna, ele volta com estado antigo e nao sincroniza offsets com quem estava lider. Isso gera replay, mas o efeito observado no log se concentra antes do restart (epoca 1), durante a lideranca provisoria dos clients.

## Tentativa de correcao (passo 2)

Foi implementado um bootstrap do lider:
- No evento `onLeaderChanged`, o novo lider solicita `SYNC_REQUEST` dos topicos para o lider anterior (ou outro membro).
- Enquanto o sync nao termina, o lider bloqueia `offer/poll/peek` para evitar operar com estado incompleto.

Arquivos tocados:
- `src/main/java/dev/nishisan/utils/ngrid/common/SyncRequestPayload.java`
- `src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java`
- `src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java`

Resultado apos o ajuste:
- Duplicidades do epoch 1 continuam ocorrendo.
- A melhoria nao eliminou o replay no cenario atual.

## Conclusao atual

O comportamento indesejado persiste mesmo com a sincronizacao de lider implementada. Isso sugere que o problema principal e a regressao de offsets durante a lideranca provisoria (antes do restart do seed), e nao apenas quando o seed volta.

## Solucao Implementada

A duplicidade foi resolvida implementando um mecanismo híbrido de sincronização de offset:

1. **Local Offset Cache**: Os clientes agora mantêm um cache local autoritativo do seu offset (`LocalOffsetStore`), atualizado a cada consumo.
2. **Offset Hint**: Ao fazer poll, o cliente envia seu offset local como "dica" para o líder.
3. **QueueRecord**: O protocolo de resposta foi enriquecido para incluir o offset exato da mensagem consumida, permitindo que o cliente atualize seu cache local com precisão.
4. **Resiliência a Reset**: Mesmo que o `DistributedOffsetStore` no node líder seja resetado (ex: restart sem persistência), o líder usa `max(stored, hint)` para determinar o próximo índice, respeitando o progresso do cliente.

## Resultado Final

Após a correção, o `scenario-1` foi reexecutado e observou-se:
- Transição limpa do epoch 1 (`INDEX-1-*`) para epoch 2 (`INDEX-2-*`).
- Ausência de mensagens de "recebeu-duplicado".
- Logs confirmando o uso do hint quando necessário (se o atraso do distributed store for detectado).

O problema de regressão de offset por volatilidade do armazenamento distribuído foi mitigado.


