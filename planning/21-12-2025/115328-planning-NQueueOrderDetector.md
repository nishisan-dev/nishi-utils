# Planning - NQueueOrderDetector

Data: 21-12-2025 11:53:28
Branch: `feature/nqueue-order-detector`

## Objetivo
Implementar um controle **autocontido** na `NQueue` para detectar **eventos fora de ordem** de forma eficaz nos cenários previstos (disco, memória e handoff), **sem impactar a performance atual**.

## Contrato de “ordem”
- Definição de ordem: a fila atribui um número monotônico global `seq` (`globalSequence.incrementAndGet()`) para cada evento aceito em `offer(...)`.
- A ordem correta de entrega é: todo `poll(...)` deve observar `seq` **estritamente crescente**.
- O detector deve acusar apenas **regressão/duplicata** (`seq <= lastDeliveredSeq`).
  - “Gaps” (pulo de valores) não são problema por si só (ex.: crashes, resets, descartes, ou simplesmente comparação não exige continuidade).

## Estado atual (baseline)
- `globalSequence` é usado no append direto, no memory buffer e no handoff.
- A verificação de fora-de-ordem hoje é espalhada (em blocos no `poll()` e em `consumeNextRecordLocked()`), usando `lastDeliveredIndex`.

## Design proposto
### 1) Detector centralizado (single-point)
Criar uma rotina interna única para validação e contabilização:
- `private void recordDeliveryIndex(long seq)`
  - No-op quando desabilitado.
  - Quando habilitado:
    - se `deliveryLastIndex != -1 && seq <= deliveryLastIndex` então incrementa métrica `OUT_OF_ORDER` e contador interno.
    - atualiza `deliveryLastIndex = seq`.

**Fontes cobertas** (todas chamam `recordDeliveryIndex(seq)` no momento de devolver o item ao usuário):
- DISK: `consumeNextRecordLocked()` (via `NQueueRecordMetaData.getIndex()`).
- HANDOFF: blocos que devolvem `handoffItem` dentro de `poll()`/`poll(timeout, ...)`.
- (Opcional / se houver entrega direta do buffer): caso exista caminho que devolva item do `drainingQueue`/`memoryBuffer` sem passar por disco, também deve chamar o detector com o `seq` do `MemoryBufferEntry`.

### 2) “Zero overhead” quando desligado
- Adicionar flag em `NQueue.Options` (default `false`) para habilitar/desabilitar o detector.
- Implementação deve evitar alocações e logging no hot path.
- Um `boolean` final cacheado em `NQueue` (ex.: `orderDetectionEnabled`) controla early-return do detector.

### 3) Métricas e observabilidade
- Reutilizar `NQueueMetrics.OUT_OF_ORDER` (sem mudar semântica pública).
- (Opcional) Expor getters leves:
  - `getLastDeliveredIndex()` e `getOutOfOrderCount()` somente para debug/testes.

## Escopo de mudanças
Arquivos previstos:
- `src/main/java/dev/nishisan/utils/queue/NQueue.java`
- `src/main/java/dev/nishisan/utils/queue/NQueue$Options` (se estiver na mesma classe, alterado ali)
- `src/test/java/dev/nishisan/utils/queue/*` (adicionar/ajustar testes cobrindo handoff/memory/compaction)

## Plano de testes
1. Testes existentes devem continuar verdes (especialmente os que já verificam `nqueue.out_of_order`).
2. Novos testes (JUnit 5) focados em transições:
   - Handoff: consumidor bloqueado + `offer` com `allowShortCircuit=true`, garantindo que o contador não cresce e itens saem em ordem.
   - Memory buffer + drain: habilitar `enableMemoryBuffer`, produzir em burst com concorrência e consumir em paralelo; assert de `out_of_order == 0`.
   - Compaction + memory mode: forçar compaction e manter produção/consumo concorrentes, assert de `out_of_order == 0`.

## Critérios de aceite
- Detector cobre disco + handoff (e memória caso exista entrega direta sem persistência).
- Sem regressões de performance percebíveis: quando flag desabilitada, o código vira efetivamente no-op.
- Suite de testes passa.

## Riscos / pontos de atenção
- Evitar false-positives por “reset” após restart: `deliveryLastIndex` é runtime-only e deve iniciar em `-1` em cada instância.
- Caso exista algum caminho que entregue do buffer sem passar por `consumeNextRecordLocked()`/handoff, ele precisa ser instrumentado também.

