# NMap Lock Striping para Offloading

Este plano detalha a refatoração das estratégias de offloading em disco do `NMap` (`DiskOffloadStrategy` e `HybridOffloadStrategy`), substituindo o gargalo do Lock Global (único `ReentrantReadWriteLock`) por uma arquitetura multithread baseada em **Lock Striping**. A camada de persistência (serialização) se manterá inalterada conforme combinado.

## User Review Required

> [!WARNING]
> **Alterações no comportamento do LRU (Hybrid):**
> O LRU Cache (Lista `LinkedHashMap` da `HybridOffloadStrategy`) passará de **estritamente global** para local (por segmento). Na prática, significa que `maxInMemoryEntries` de 10.000 com `16` stripes será particionado em 16 mini-caches de ~`625` itens. A política funcionará eficientemente, mas é levemente "aproximada" em comparação com a fila global anterior. Favor confirmar se esse trade-off atende, visto que é o padrão da indústria para caches concorrentes de alta performance.

## Proposed Changes

### NMap Offloading Module

#### [MODIFY] [DiskOffloadStrategy.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/map/DiskOffloadStrategy.java)
- Introduzir a variável estática de configuração de concorrência: `private static final int CONCURRENCY_LEVEL = 16;`.
- Substituir o lock único global `private final ReentrantReadWriteLock lock` por um array tipado dinâmico `private final ReentrantReadWriteLock[] locks`.
- Criar a função auxiliar `private ReentrantReadWriteLock getStripeLock(Object key)` calculando o hash modular.
- Alterar os métodos granulares (`get`, `put`, `remove`, `containsKey`) para adquirir apenas o Lock pertencente à "fatia" computada pela chave.
- Otimizar `size()` e `isEmpty()` removendo bloqueios e transpondo a chamada puramente para os métodos thread-safe do `ConcurrentHashMap`.
- Para o método de massa `clear()`, aplicar mecanismo de _lock lock order_ fixada (adquirir todos os Write Locks sequencialmente do 0 ao N para não dar deadlock) durante a limpeza da lixeira de arquivos físicos.

#### [MODIFY] [HybridOffloadStrategy.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/map/HybridOffloadStrategy.java)
- Substituir o lock único pelo mesmo array: `private final ReentrantReadWriteLock[] locks`.
- Modularizar do Cache em Memória (`LinkedHashMap`):
  - Ao invés de `private final LinkedHashMap<K, V> hotCache`, usar um `List<LinkedHashMap<K, V>> hotCaches`.
  - Criar o cálculo do limite fracionado `maxInMemoryEntriesPerStripe = Math.max(1, maxInMemoryEntries / CONCURRENCY_LEVEL);`.
- Reescrita do `evictIfNeeded(int stripeIndex)` na thread atrelada: processará evicção sincrona limitada **somente e isoladamente** à fatia bloqueada momentaneamente pelo `put` da vez.
- Modificar o `close()` e o `clear()` para realizarem _Iterating Global Locks_ para não haver condição de corrida (Deadlock prevention) na escrita sequencial.

## Open Questions

> [!CAUTION]
> Para o método abstrato `size()` das coleções, e especialmente pro caso do `Hybrid` por conta das coletas JMX Metrics (`hotSize`, `coldSize`), como ele reflete as estatísticas para o prometheus, está de acordo em o método iterar por todos os segmentos para obter o `size()`? Ele terá consistência total no `cold` (Pois é `ConcurrentHashMap`), porém os de `hotCache` exigirão adquirir ReadLocks globais rapidamente se seguirmos o _strict math_. Se for para fins de monitoria em grafana, podemos apenas contabilizar sem ReadLocks sabendo do delay de ~milissegundos?

## Verification Plan

### Automated Tests
- Rodar a suite local via comandos definidos.
- Testes específicos impactados: `NMapOffloadTest.java`, `HybridOffloadStrategyTest.java`.
- Validar se o maven task relata tudo passando no build local.

### Manual Verification
- Reavaliação no contexto de pipeline para garantia que nenhum teste distribuído nos NGrids falhou silenciosamente antes do release final.
