# Feature Report - NQueueOrderDetector

Branch: `feature/nqueue-order-detector`

## Arquivos criados/modificados
- Criado: `planning/21-12-2025/115328-planning-NQueueOrderDetector.md`
- Modificado: `src/main/java/dev/nishisan/utils/queue/NQueue.java`
- Criado: `src/test/java/dev/nishisan/utils/queue/NQueueOrderDetectorTest.java`

## Resumo da implementação
- `NQueue.Options.withOrderDetection(boolean)`:
  - Flag para habilitar o detector (default: `false`).
- `NQueue`:
  - Campo cacheado `orderDetectionEnabled` para garantir early-return no hot path.
  - Método centralizado `recordDeliveryIndex(long seq)`:
    - Registra monotonicidade do `seq` entregue.
    - Incrementa `outOfOrderCount` e notifica `statsUtils` com `NQueueMetrics.OUT_OF_ORDER` quando `seq <= lastDeliveredIndex`.
  - Substituição de checks espalhados:
    - Caminho HANDOFF (`handoffItem`) em `poll()` e `poll(timeout, ...)`.
    - Caminho DISK em `consumeNextRecordLocked()`.
  - Getters de diagnóstico (thread-safe via lock):
    - `getLastDeliveredIndex()`
    - `getOutOfOrderCount()`

## Matriz de rastreabilidade (Plano -> Código)
- Contrato: detector baseado em `seq` monotônico (`globalSequence`) e valida `seq <= lastDeliveredSeq`
  - Implementado em `src/main/java/dev/nishisan/utils/queue/NQueue.java` (`recordDeliveryIndex`)
- Centralizar detector (single-point) e cobrir caminhos de entrega:
  - DISK: `consumeNextRecordLocked()` -> `recordDeliveryIndex(currentIndex)`
  - HANDOFF: `poll()` / `poll(timeout, ...)` -> `recordDeliveryIndex(handoffItem.index())`
- Zero overhead quando desligado:
  - `Options.enableOrderDetection` default `false`, cacheado em `orderDetectionEnabled` e early-return no `recordDeliveryIndex`.
- Testes cobrindo handoff/memória/compaction:
  - Implementado em `src/test/java/dev/nishisan/utils/queue/NQueueOrderDetectorTest.java`

## Quality gates
- Build: PASS (`mvn test` exit code 0)
- Unit/Integration tests: PASS (inclui `NQueueOrderDetectorTest`)

## Observações
- O detector é runtime-only (reinicia o baseline em cada instância) para evitar falso-positivo após restart.
- O detector monitora a ordem **na entrega** (poll). Ele não valida “ordem de append” no log (pode ser um próximo passo se desejado).

