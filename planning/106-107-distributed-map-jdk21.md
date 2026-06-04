# Plano — Issues #106 e #107 (nishi-utils 4.x)

## Context

Ambas as issues nascem do mesmo objetivo: adotar `ngrid`/`nmap` 4.x como **store de
estado replicado** do TEvent Cardinal (TEMS) para HA active-standby, com requisito-mestre
de **0 refactor** nas ~300 regras Groovy de produção.

- **#107** destrava o *consumo*: o TEMS roda em **JDK 21** (Groovy 4.0.24 não decodifica
  bytecode Java 25 em runtime). O 4.0.0 fixou `maven.compiler.source/target=25`, mas a
  varredura confirmou que **não há nenhuma API exclusiva de JDK > 21** no código
  (virtual threads são GA desde 21; zero `StructuredTaskScope`/`ScopedValue`/`foreign`/
  `--enable-preview`). Decisão: **baseline global `release=21`** (definitivo; o artefato
  roda em 21+ e segue compilável em 25).
- **#106** destrava o *modelo de uso*: as regras tocam o estado como `java.util.Map`
  (`get`, `put`, `.each`, `.values()`, `.containsKey`, `.size()`, `.clear()`). Para trocar
  o backing dos caches do Cardinal sem alterar regra alguma, `DistributedMap<K,V>` precisa
  ser **drop-in de `java.util.Map<K,V>`**.

Ordem de execução: **#107 primeiro** (mecânico, valida o build em JDK 21), depois **#106**.

Branch sugerida: `feature/distributed-map-jdk21` (ou duas: `build/baseline-jdk21` e
`feature/distributed-map-as-map`). Commits atômicos por responsabilidade.

---

## Parte A — #107: baseline JDK 21 (definitivo)

Mudança puramente de configuração — sem alteração de código de produção.

1. **`pom.xml` (raiz, l.87-88)** — substituir as duas properties por uma só:
   ```xml
   <maven.compiler.release>21</maven.compiler.release>
   ```
   (remover `maven.compiler.source`/`maven.compiler.target` — `release` é o canônico e
   evita conflito; impede uso acidental de API > 21).

2. **`ngrid-test/pom.xml` (l.168-169)** — remover o override redundante de `source/target=25`
   (passa a herdar `release=21` do parent).

3. **`ngrid-test/Dockerfile` (l.1, l.12)** — `maven:3.9-eclipse-temurin-25` → `-21`;
   `eclipse-temurin:25-jre` → `eclipse-temurin:21-jre`.

4. **GitHub Actions** — `java-version: '25'` → `'21'`:
   - `.github/workflows/publish.yml` (l.20)
   - `.github/workflows/security-scan.yml` (l.27)
   - `.github/workflows/resilience.yml` (l.38, 66, 86, 117 — 4 jobs)

5. **Docs** — `CLAUDE.md` (l.40) e `AGENTS.md` (~l.120): atualizar a nota de toolchain
   para "POMs e Dockerfile compilam com Java 21".

**Validação A:** `mvn -version` (garantir JDK 21 ativo) → `mvn clean verify` →
`mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1`. Build do container:
`docker build -f ngrid-test/Dockerfile .`.

---

## Parte B — #106: `DistributedMap implements java.util.Map<K,V>`

Abordagem definitiva (sem adapter/fallback): a API pública passa a retornar `V`; as
variantes `Optional` são **preservadas** com sufixo `Optional`. Decisão de design para o
`clear()`: **novo opcode `CLEAR` reutilizável** (esvazia mantendo a persistência viva),
distinto do `DESTROY` (que apaga os arquivos).

### B1. Novo opcode CLEAR no pipeline de replicação/persistência

- **`map/NMapOperationType.java`** — adicionar `CLEAR` **no fim do enum** (após `DESTROY`).
  Crítico: o WAL serializa o opcode pelo **ordinal** (`NMapPersistence.applyDecoded`,
  l.515-519). Preservar `PUT=0, REMOVE=1, DESTROY=2, CLEAR=3` mantém compat com WALs já
  gravados.
- **`map/NMapPersistence.java`** —
  - `applyDecoded` switch (l.539-541): adicionar `case CLEAR -> data.clear();`.
  - `appendSync`/`appendAsync` (l.203-245): garantir que o encode do registro tolera
    `key=null`/`value=null` para o `CLEAR` (validar a serialização do frame; ajustar se a
    chave nula não for suportada hoje).
- **`ngrid/map/MapReplicationCommand.java`** — factory `clear()` (type=`CLEAR`, key/value
  nulos), análoga a `destroy()`.
- **`ngrid/map/MapReplicationCodec.java`** — confirmar encode/decode de comando sem
  key/value (ajustar se necessário).

### B2. `MapClusterService` (`ngrid/map/MapClusterService.java`)

- **`apply()`** (switch l.278-308): `case CLEAR -> { data.clear(); if (persistence != null)
  persistence.appendAsync(NMapOperationType.CLEAR, null, null); return; }`
  — **não** chama `persistence.destroy()`; a engine permanece viva e reutilizável.
- **`clearReplicated()`** (novo, espelha `destroyReplicated()` l.166-169): encode `CLEAR` +
  `waitForReplication(replicationManager.replicate(topic, encoded))`.
- **Leituras locais novas** (snapshot imutável do `data`, eventually-consistent — RF7/8/11):
  - `Collection<V> values()` → cópia defensiva de `data.values()`.
  - `Set<Map.Entry<K,V>> entrySet()` → cópia defensiva de `data.entrySet()` (evita
    `ConcurrentModificationException` sob escrita concorrente — RF11).
  - `boolean containsValue(Object)` → scan local O(n).

### B3. `DistributedMap` (`ngrid/structures/DistributedMap.java`)

- Assinatura: `implements java.util.Map<K,V>, TransportListener, Closeable`.
- **Retornos `V` + variantes `Optional` preservadas** (erasure impede dois retornos na
  mesma assinatura):
  - `V put(K,V)` ← delega a `putOptional(K,V): Optional<V>` (lógica atual l.177-194).
  - `V remove(Object)` ← `removeOptional(K): Optional<V>` (cast interno p/ K).
  - `V get(Object)` ← `getOptional(K): Optional<V>` e `getOptional(K, Consistency)`
    (preserva a lógica de consistência atual l.310-348).
  - `boolean containsKey(Object)` — ajustar de `K` para `Object` (contrato Map).
  - `boolean containsValue(Object)` — delega `mapService.containsValue`.
  - `Collection<V> values()` / `Set<Map.Entry<K,V>> entrySet()` — delegam ao mapService.
  - `keySet()`, `size()`, `isEmpty()`, `putAll(Map)` — já compatíveis (manter).
  - `void clear()` — leader: `mapService.clearReplicated()`; follower: `invokeLeader(
    mapClearCommand, new EncodedCommand(MapReplicationCodec.encode(...clear())))`.
- **Roteamento do comando clear**: adicionar prefixo `map.clear:` + `mapClearCommand`,
  e tratar em `executeLocal()` (l.406-451) e no `Set.of(...)` de `onMessage()` (l.475).
- RF10 (read-after-write local no líder) **já é garantido** pelo fluxo atual
  (`put`→`mapService.put`→replicate aplica `data.put` localmente antes de retornar);
  preservar.
- Métodos `default` de `Map` (`getOrDefault`, `forEach`, `compute`, `merge`, …) — a
  implementação default da interface basta para o "0 refactor"; só sobrescrever se algum
  teste RF demonstrar necessidade.

### B4. Migrar consumidor interno do contrato `Optional`

- **`ngrid/queue/DistributedOffsetStore.java`** (l.34, 48, 59, 70) — hoje faz
  `(Optional<?>) offsets.get(key)`. Migrar para `offsets.getOptional(key)`; `put` continua
  (retorno `V` descartado). Único ponto interno afetado pelo breaking change.

### B5. Testes (RF1–RF12)

- **`DistributedMapApiTest.java`** (estender) — usar `NGrid.local(n)`:
  - RF1-RF5: `get/put/remove` retornando `V`/`null`; `containsKey(Object)`, `size()`.
  - RF6: `clear()` replicado em cluster + **reuso** (put após clear funciona; persistência
    viva). Verificar propagação aos followers.
  - RF7/RF8: `values()` e `entrySet()` + iteração estilo `.each`.
  - RF10: read-after-write local no líder na mesma passada.
  - RF11: iteração sobre `entrySet()`/`values()` sob escrita concorrente sem
    `ConcurrentModificationException`.
  - RF12: `assertTrue(map instanceof java.util.Map)`.
- **Teste drop-in**: bloco que substitui `ConcurrentHashMap` por `DistributedMap`
  exercitando RF1–RF12 (prova do "0 refactor").
- **Volume** (opcional/perfil): ~370k entradas validando `get`/iteração local — marcar
  como teste pesado para não onerar a suíte baseline.

**Validação B:** `mvn -pl nishi-utils-core test` (baseline) →
`mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1` → `mvn clean install`
(multi-módulo, propaga ao ngrid-test).

---

## Pontos de atenção / riscos

- **Ordinal do enum**: `CLEAR` **deve** entrar no fim (compat de WAL).
- **WAL com chave nula** no `CLEAR`: validar/ajustar o encode de `NMapPersistence`.
- **Breaking change de API** (`Optional<V>`→`V`): aceitável na linha 4.x (a issue o assume);
  documentar no changelog/release notes. Variantes `*Optional` preservam o acesso anterior.
- **Custo de memória** do snapshot em `values()`/`entrySet()` a 370k entradas — cópia
  defensiva é o trade-off por RF11 (sem CME).
- **Persistência viva pós-`CLEAR`**: confirmar que `NMapPersistence` continua operando
  (sem `destroy()`), aceitando puts subsequentes.

## Documentação (DoD)

- Atualizar `doc/` (seção ngrid map): `DistributedMap` agora é `java.util.Map`, semântica
  do `clear()` replicado (CLEAR vs DESTROY) e das views locais eventually-consistent.
- Após aprovação, copiar este plano para `/planning` (convenção do projeto).

## Verificação end-to-end (resumo)

```bash
# Pré: JDK 21 ativo
mvn -version
# A (#107)
mvn clean verify
mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1
docker build -f ngrid-test/Dockerfile .
# B (#106)
mvn -pl nishi-utils-core test
mvn test -Dtest=DistributedMapApiTest
mvn clean install
```
