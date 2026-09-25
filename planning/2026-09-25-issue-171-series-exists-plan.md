# Issue #171 — Existência de série e abertura sem criar: plano de implementação

> **Para agentes:** executar tarefa a tarefa (subagent-driven). Passos em checkbox (`- [ ]`).

**Objetivo:** permitir perguntar se uma série existe sem criá-la e abrir uma série só se ela existir,
nos modos local (`nishi-utils-oss`) e cluster (`nishi-utils-ngrrd-cluster`).

**Arquitetura:** flag `createIfMissing` em `Ngrrd.OpenOptions` propagada até o `NgrrdWriter` (local) e
até o `OPEN` do storage (cluster); exceção única `SeriesNotFoundException`; no cluster, consultas em
lote respondidas pela réplica local do catálogo, com misses confirmados no líder via comando novo
paginado (`ngrrd.catalog.lookup`) e verificação física opcional por nó (`ngrrd.series.exists.batch`).

**Stack:** Java 21, Maven multi-módulo, JUnit 5, Jackson (codec do NGrid).

**Spec:** `planning/2026-09-25-issue-171-series-exists-design.md` (ler antes de cada tarefa).

## Restrições globais

- Worktree: `/home/lucas/Projects/nishisan/nishi-utils-171`, branch `feature/171-series-exists`.
- Identificadores em inglês; Javadoc, comentários, mensagens de exceção, docs e commits em PT-BR.
  Nomes de métodos `@Test` em PT-BR (padrão do módulo, ex.: `placeRequestComPreferredOwnerSobreviveAoRoundTrip`).
- Commits atômicos, mensagem em PT-BR no formato `tipo(escopo): descrição` (ex.:
  `feat(ngrrd): ...`), **sem** qualquer menção a IA/agente/Co-Authored-By. Nunca `git add -A`/`git add .`
  — adicionar arquivos explicitamente.
- Imports: sem imports não usados, sem FQN inline em código novo (o código existente tem alguns FQN
  inline; não replicar).
- `open` sem a opção nova mantém comportamento idêntico (cria).
- Falha ao consultar (sem líder, timeout, transporte, comando desconhecido) **nunca** vira `false`/
  `NOT_FOUND`/`NOT_PLACED`.
- Lotes de `ngrrd.catalog.lookup` e `ngrrd.series.exists.batch`: no máximo `catalogLookupBatchSize`
  chaves por chamada (default 2000, válido 1..10000), páginas **sequenciais**. Servidor recusa
  (status `ERROR`) pedido com mais de 10000 chaves.
- Versão final: **8.6.0**.
- Testes:
  - oss: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn -pl nishi-utils-oss test`
  - cluster unit: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn -pl nishi-utils-ngrrd-cluster -am verify -DexcludeNgrid=true` (NUNCA `mvn install`: ~/.m2 compartilhado)
  - cluster in-process: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn -pl nishi-utils-ngrrd-cluster -am verify -Pngrrd-cluster -DexcludeNgrid=true -Dtest=<Classe> -Dsurefire.failIfNoSpecifiedTests=false`
  - Relatar contagem real de testes (Tests run/Failures/Errors), não só BUILD SUCCESS.

---

### Tarefa 1: modo local — `createIfMissing`, `SeriesNotFoundException`, `Ngrrd.exists`

**Arquivos:**
- Criar: `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/api/SeriesNotFoundException.java`
- Modificar: `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/Ngrrd.java` (`OpenOptions`,
  `buildHandle`, novos `exists`)
- Modificar: `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/writer/NgrrdWriter.java`
  (construtor completo)
- Teste: `nishi-utils-oss/src/test/java/dev/nishisan/utils/oss/NgrrdCreateIfMissingTest.java`
- Doc: `doc/oss/ngrrd.md` (seção de abertura)

**Produz (usado pelas tarefas seguintes):**
```java
package dev.nishisan.utils.oss.api;
/** Série inexistente aberta com {@code createIfMissing=false}. Não é falha de transporte. */
public class SeriesNotFoundException extends RuntimeException {
    public SeriesNotFoundException(String seriesKey) { ... }          // mensagem PT-BR com a chave
    public SeriesNotFoundException(String seriesKey, String message) { ... }
    public String seriesKey() { ... }
}

// Ngrrd.OpenOptions
public record OpenOptions(Durability durability, OnGeometryChange onGeometryChange, boolean createIfMissing) {
    public OpenOptions(Durability durability, OnGeometryChange onGeometryChange) { this(durability, onGeometryChange, true); }
    public OpenOptions withCreateIfMissing(boolean createIfMissing) { return new OpenOptions(durability, onGeometryChange, createIfMissing); }
    // defaults/durability/onGeometryChange/of: inalterados (createIfMissing=true)
}

// Ngrrd
public static boolean exists(BlobVolumeRegistry registry, NgrrdUri locator, String yamlContent)
public static boolean exists(BlobVolume volume, NgrrdUri locator, String yamlContent)
public static boolean exists(String yamlContent, StorageFactory.StorageBindings bindings, Map<String, String> tags)
```

- [ ] **Passo 1: testes que falham** em `NgrrdCreateIfMissingTest` (usar `@TempDir`; reaproveitar o
  YAML/fixtures de `NgrrdBlobFacadeTest` para sharded blob e de um teste existente de localDisk —
  procurar `StorageBindings` em `nishi-utils-oss/src/test`). Casos:
  - `abrirSemCriarSerieInexistenteNoBlobLancaENadaCria`: `Ngrrd.open(volume, locator, yaml,
    OpenOptions.defaults().withCreateIfMissing(false))` lança `SeriesNotFoundException` com
    `seriesKey()` == `locator.seriesPath()`; depois `volume.storage().exists(objectKey)` é `false` e
    `Ngrrd.exists(volume, locator, yaml)` é `false`.
  - `abrirSemCriarSerieExistenteNoBlobAbre`: abre com default, escreve 1 amostra, `checkpoint`,
    fecha; reabre com `createIfMissing=false` → sucesso; `Ngrrd.exists(...)` é `true`.
  - `openPadraoContinuaCriandoNoBlob`: `OpenOptions.defaults().createIfMissing()` é `true` e o open
    padrão cria (`exists` vira `true`).
  - Os mesmos três para localDisk via `Ngrrd.fromYaml(yaml, bindings, tags, null, options)` e
    `Ngrrd.exists(yaml, bindings, tags)`; na inexistente, conferir que nenhum arquivo `.ngrr` foi
    criado no diretório (`Files.walk` sem nenhum `*.ngrr`).
  - `construtorDeDoisArgumentosMantemCriacao`: `new OpenOptions(null, null).createIfMissing()` é `true`.
- [ ] **Passo 2:** `mvn -pl nishi-utils-oss test -Dtest=NgrrdCreateIfMissingTest` → falha de compilação.
- [ ] **Passo 3: implementar.**
  - `NgrrdWriter`: novo construtor completo com `boolean createIfMissing` ao final (após
    `LongSupplier nowEpochMs`); o atual de 8 args delega com `true`. Logo após montar `storageKey` e
    **antes** de `GeometryReconciler.reconcile`/`openSeries`:
    ```java
    if (!createIfMissing && !provider.seriesExists(storageKey)) {
        throw new SeriesNotFoundException(seriesKey);
    }
    ```
    Conferir a semântica de `seriesExists` em `LocalDiskStorage` e `S3Storage` (tem que ser "objeto
    existe", não "tamanho > 0"); se divergir, corrigir no provider e cobrir no teste.
  - `Ngrrd.buildHandle`: passa `options.createIfMissing()` ao writer. Checar que nada antes do writer
    (ex.: `StorageFactory.from`) cria o objeto da série.
  - `Ngrrd.exists`: parse + validate do YAML (igual a `open`), `storageKey =
    StorageKey.series(def.spec().storage().objectNaming(), seriesKey)`; storage do volume
    (`volume.bindings()` + `StorageFactory.from`) ou dos `bindings`; responde via
    `SeriesChannelProvider.seriesExists` se o storage implementar, senão `storage.exists`. Se o
    storage for próprio (backend ≠ `SHARDED_BLOB`), fechá-lo como `DefaultHandle.close` faz quando
    `ownsStorage`. `objectNaming` nulo segue o mesmo default que o writer usa.
  - Javadoc de `OpenOptions.createIfMissing` e de `exists` (contrato: sem I/O de criação; no blob é
    lookup no catálogo em memória).
- [ ] **Passo 4:** `mvn -pl nishi-utils-oss test` → tudo verde; relatar contagem.
- [ ] **Passo 5:** doc em `doc/oss/ngrrd.md`: subseção "Abrir sem criar e consultar existência" com
  exemplo de `withCreateIfMissing(false)` + `catch (SeriesNotFoundException e)` e `Ngrrd.exists`.
- [ ] **Passo 6: commit** `feat(ngrrd): abertura sem criar e consulta de existência no modo local (#171)`.
- [ ] **Passo 7:** `mvn -pl nishi-utils-oss -am install -DskipTests -q` (disponibiliza o oss novo ao cluster).

---

### Tarefa 2: protocolo do cluster

**Arquivos:**
- Modificar: `.../cluster/protocol/SeriesStatus.java` (+ `NOT_FOUND`, com Javadoc: "série inexistente
  e abertura sem criar")
- Modificar: `.../cluster/protocol/OpenRequest.java` (+ componente final `Boolean createIfMissing`;
  construtor secundário com a assinatura atual de 6 args delegando `null`; método
  `boolean createIfMissingOrDefault()` → `createIfMissing == null || createIfMissing`)
- Criar: `.../cluster/protocol/CatalogLookupRequest.java`, `CatalogLookupResponse.java`,
  `SeriesExistsBatchRequest.java`, `SeriesExistsBatchResponse.java`
- Modificar: `.../cluster/protocol/Commands.java`
- Teste: `nishi-utils-ngrrd-cluster/src/test/java/.../protocol/ProtocolCodecTest.java`

(`...` = `nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster`)

**Produz:**
```java
public record CatalogLookupRequest(List<String> seriesKeys) {            // compacto: null → List.of(), List.copyOf
    public static final int MAX_KEYS = 10_000; }
public record CatalogLookupResponse(SeriesStatus status, String leaderNodeId,
        Map<String, SeriesPlacement> found, String message) {             // compacto: found null → Map.of(), Map.copyOf
    public static CatalogLookupResponse ok(Map<String, SeriesPlacement> found)
    public static CatalogLookupResponse notLeader(String leaderNodeId)
    public static CatalogLookupResponse error(String message) }
public record SeriesExistsBatchRequest(List<String> seriesKeys) { public static final int MAX_KEYS = 10_000; }
public record SeriesExistsBatchResponse(SeriesStatus status, Set<String> present, String message) {
    public static SeriesExistsBatchResponse ok(Set<String> present)
    public static SeriesExistsBatchResponse error(String message) }
// Commands
public static final String CATALOG_LOOKUP = "ngrrd.catalog.lookup";        // entra em LEADER_COMMANDS
public static final String SERIES_EXISTS_BATCH = "ngrrd.series.exists.batch";
```

- [ ] **Passo 1: testes que falham** em `ProtocolCodecTest`, seguindo o helper de round-trip já usado
  no arquivo:
  - `catalogLookupRequestSobreviveAoRoundTrip`, `catalogLookupRequestComListaNulaVemVazia`
  - `catalogLookupResponseComPlacementsAtivoEMigrandoSobreviveAoRoundTrip` (um `ACTIVE` e um
    `SeriesPlacement.migrating(...)`)
  - `catalogLookupResponseNotLeaderComHintSobreviveAoRoundTrip`
  - `seriesExistsBatchRequestEResponseSobrevivemAoRoundTrip`
  - `openRequestComCreateIfMissingFalseSobreviveAoRoundTrip` e
    `openRequestSemCreateIfMissingDesserializaComoCriar` (JSON sem o campo → `createIfMissingOrDefault()`
    `true` — simula cliente antigo)
- [ ] **Passo 2:** rodar `-Dtest=ProtocolCodecTest` → falha de compilação.
- [ ] **Passo 3:** implementar os records e constantes. Atualizar todos os `new OpenRequest(...)` do
  módulo (usar grep) — o construtor de 6 args continua válido, então só mudar onde for necessário.
  Conferir `switch` exaustivos sobre `SeriesStatus` no módulo (`grep -rn "switch (.*status" main`) e
  tratar `NOT_FOUND` onde o compilador exigir (sem `default` novo que engula o caso).
- [ ] **Passo 4:** `mvn -pl nishi-utils-ngrrd-cluster verify` → verde; relatar contagem.
- [ ] **Passo 5: commit** `feat(ngrrd-cluster): protocolo de consulta de catálogo e existência em lote (#171)`.

---

### Tarefa 3: líder responde `ngrrd.catalog.lookup`

**Arquivos:**
- Modificar: `.../cluster/node/PlacementRequestHandler.java`
- Teste: `nishi-utils-ngrrd-cluster/src/test/java/.../node/PlacementRequestHandlerTest.java`

**Consome:** `CatalogLookupRequest/Response`, `Commands.CATALOG_LOOKUP` (tarefa 2).

**Comportamento:**
```java
// super(transport, Set.of(Commands.PLACE, Commands.CATALOG_LOOKUP))
// handle(): CATALOG_LOOKUP NÃO pega admissionLock (é leitura); PLACE continua igual.
private CatalogLookupResponse handleCatalogLookup(CatalogLookupRequest request) {
    if (!leaderView.isLeader()) return CatalogLookupResponse.notLeader(leaderView.leaderId().orElse(null));
    if (request.seriesKeys().size() > CatalogLookupRequest.MAX_KEYS) return CatalogLookupResponse.error("...");
    Map<String, SeriesPlacement> found = new HashMap<>();
    boolean anyMiss = false;
    for (String key : request.seriesKeys()) {
        Optional<SeriesPlacement> p = catalog.placementStrong(key);   // no líder: leitura local autoritativa
        if (p.isPresent()) found.put(key, p.get()); else anyMiss = true;
    }
    // Mesma janela de graça do PLACE: réplica do líder recém-eleito pode não ter convergido;
    // um miss nessa janela não pode virar "não existe".
    if (anyMiss && clock.millis() - becameLeaderAtMs < placementGraceAfterLeadership.toMillis())
        return CatalogLookupResponse.notLeader(leaderView.leaderId().orElse(null));
    if (!leaderView.isLeader()) return CatalogLookupResponse.notLeader(...);  // perdeu liderança no meio
    return CatalogLookupResponse.ok(found);
}
```
Reusar o `notLeaderResponse()`/hint existente para extrair o id do líder, se aplicável.

- [ ] **Passo 1: testes que falham** (mesmo setup/fakes do `PlacementRequestHandlerTest`):
  - `catalogLookupNoLiderDevolvePresentesEOmiteAusentes`
  - `catalogLookupForaDoLiderRespondeNotLeaderComHint`
  - `catalogLookupNaJanelaDeGracaComMissRespondeNotLeader` (liderança recém-assumida via
    `onLeaderChanged`, relógio dentro da graça, uma chave ausente)
  - `catalogLookupNaJanelaDeGracaSemMissRespondeOk`
  - `catalogLookupNaoCriaPlacement` (catálogo inalterado após consulta de chave ausente)
  - `catalogLookupAcimaDoLimiteRespondeErro` (10001 chaves)
- [ ] **Passo 2:** rodar → falha.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `mvn -pl nishi-utils-ngrrd-cluster verify` → verde; contagem.
- [ ] **Passo 5: commit** `feat(ngrrd-cluster): líder responde consulta de catálogo em lote (#171)`.

---

### Tarefa 4: storage — `OPEN` sem criar e `SERIES_EXISTS_BATCH`

**Arquivos:**
- Modificar: `.../cluster/node/StorageRequestHandler.java`
- Teste: `nishi-utils-ngrrd-cluster/src/test/java/.../node/StorageRequestHandlerTest.java`

**Comportamento:**
- `HANDLED_COMMANDS` inclui `SERIES_EXISTS_BATCH` (sem checagem de ownership, como `SERIES_EXISTS`).
- `handleOpen`: após ownership `OK` e validação de prefixo, **antes** de `geometryService.beforeOpen` e
  `registry.open`:
  ```java
  if (!request.createIfMissingOrDefault() && !registry.isOpen(request.seriesKey())
          && !volume.storage().exists(SeriesObjectKeys.objectKey(seriesObjectPrefix, request.seriesKey()))) {
      recordError(SeriesStatus.NOT_FOUND);
      return new SeriesStatusResponse(SeriesStatus.NOT_FOUND, self.value(), "série inexistente: " + key);
  }
  ```
  e `registry.open(...)` recebe `OpenOptions.of(durability, onGeometryChange)
  .withCreateIfMissing(request.createIfMissingOrDefault())` — defesa em profundidade: se o objeto
  sumir entre o check e o open, o writer lança `SeriesNotFoundException`; capturá-la antes do
  `catch (RuntimeException)` genérico e responder `NOT_FOUND`. Conferir se
  `SeriesHandleRegistry.open` repassa `OpenOptions` inteiras ao `Ngrrd.open`; se reconstruir as
  opções, preservar `createIfMissing`.
- `handleSeriesExistsBatch`: acima de `MAX_KEYS` → `error(...)`; senão
  `present = {k | volume.storage().exists(objectKey(prefix, k))}` → `ok(present)`.

- [ ] **Passo 1: testes que falham** (setup do `StorageRequestHandlerTest`):
  - `openSemCriarComObjetoAusenteRespondeNotFoundENadaCria` (placement `ACTIVE(self)`; depois
    `volume.storage().exists(objectKey)` é `false` e `registry.isOpen` é `false`)
  - `openSemCriarComObjetoPresenteAbre`
  - `openSemFlagContinuaCriando` (request com `createIfMissing=null`)
  - `openSemCriarComSerieJaAbertaRespondeOk`
  - `openSemCriarDeNaoDonoContinuaRespondendoWrongOwner` (ownership vem antes do NOT_FOUND)
  - `seriesExistsBatchDevolveSoAsPresentes` e `seriesExistsBatchAcimaDoLimiteRespondeErro`
- [ ] **Passo 2:** rodar → falha.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `mvn -pl nishi-utils-ngrrd-cluster verify` → verde; contagem.
- [ ] **Passo 5: commit** `feat(ngrrd-cluster): storage recusa abrir série inexistente sem criar (#171)`.

---

### Tarefa 5: cliente — consulta paginada ao líder e resolução sem `PLACE`

**Arquivos:**
- Criar: `.../cluster/client/CatalogLookupClient.java`
- Modificar: `.../cluster/client/PlacementLookup.java`, `.../cluster/client/PlacementResolver.java`
- Teste: `.../client/CatalogLookupClientTest.java`, `.../client/PlacementResolverTest.java`
  (usar `RecordingClusterRpc`)

**Produz:**
```java
/** Consulta o catálogo no líder em páginas sequenciais; nunca cria placement. */
public final class CatalogLookupClient {
    public CatalogLookupClient(ClusterRpc rpc, RetryPolicy retry, Clock clock, int batchSize)
    /** Placements presentes no líder; chaves ausentes ficam fora do mapa.
     *  @throws NgrrdClusterException em qualquer falha (NO_LEADER, TIMEOUT, transporte, REMOTE_ERROR) —
     *          nunca resposta parcial. */
    public Map<String, SeriesPlacement> lookup(Collection<String> seriesKeys, Duration maxWait)
}
// PlacementLookup
/** Placement existente, sem nunca fazer PLACE.
 *  @throws SeriesNotFoundException se o líder confirmar que não há placement
 *  @throws NgrrdClusterException se não foi possível confirmar */
SeriesPlacement resolveExisting(String seriesKey, Duration maxWait);
/** Placement da réplica local/override, o mais recente (sem RPC). */
Optional<SeriesPlacement> placementCached(String seriesKey);
```

**Comportamento do `CatalogLookupClient.lookup`:**
- Chaves distintas (preservar ordem), fatiadas em páginas de `batchSize`; um prazo único
  `deadline = now + min(retry.timeout(), maxWait)` para todas as páginas.
- Por página, loop igual ao `PlacementResolver.placeAtLeader`: aguardar líder (`rpc.leaderId()` com
  poll de 50 ms até o prazo → `NO_LEADER`); `rpc.call(leader, CATALOG_LOOKUP, request,
  CatalogLookupResponse.class, remaining)`; falha de transporte (`TransportRetry.isTransportFailure`) →
  `TransportRetry.awaitConnectionOrBackoff` e repete; `NOT_LEADER` → usa o hint na próxima tentativa +
  backoff; `OK` → acumula `found`; `ERROR`/outros → `NgrrdClusterException(REMOTE_ERROR, message)`.
  Falha não-transporte do `rpc.call` (ex.: líder antigo sem handler) propaga como `NgrrdClusterException`.
- Extrair para helper privado compartilhado (ou classe package-private `LeaderCalls`) a lógica de
  prazo/backoff/aguardar líder hoje duplicada em `PlacementResolver`, e fazer `placeAtLeader` usá-la —
  sem mudar o comportamento do `PLACE`.

**`PlacementResolver.resolveExisting`:** o construtor passa a receber `CatalogLookupClient` (sem
manter o antigo); atualizar o call site em `DefaultNgrrdClusterClient` e os testes. `resolveExisting`
e `placementCached` são abstratos em `PlacementLookup`; atualizar todas as implementações (inclusive
fakes de teste — `grep -rn "implements PlacementLookup\|new PlacementLookup" nishi-utils-ngrrd-cluster/src`). `freshest(override, local)` `ACTIVE` → devolve. Senão (ausente
ou `MIGRATING`) → `lookup(List.of(key), maxWait)`; presente → se `ACTIVE`, grava override; devolve;
ausente → `SeriesNotFoundException(seriesKey)`.

- [ ] **Passo 1: testes que falham:**
  - `CatalogLookupClientTest`: `paginaEmLotesDoTamanhoConfigurado` (5000 chaves, batch 2000 → 3
    chamadas `CATALOG_LOOKUP` com 2000/2000/1000 chaves, em sequência); `deduplicaChaves`;
    `seguePistaDeNotLeader`; `retentaFalhaDeTransporteDentroDoPrazo`; `semLiderLancaNoLeader`;
    `erroDoLiderLancaRemoteError`; `falhaNumaPaginaNaoDevolveResultadoParcial`; `listaVaziaNaoFazRpc`.
  - `PlacementResolverTest`: `resolveExistingComPlacementLocalAtivoNaoFazRpc`;
    `resolveExistingComMissConsultaLiderENuncaFazPlace` (nenhum `Commands.PLACE` gravado);
    `resolveExistingAusenteNoLiderLancaSeriesNotFound`; `resolveExistingMigrandoConsultaLider`;
    `resolveExistingSemLiderLancaNgrrdClusterExceptionENaoSeriesNotFound`;
    testes existentes do `PLACE` continuam verdes.
- [ ] **Passo 2:** rodar → falha.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `mvn -pl nishi-utils-ngrrd-cluster verify` → verde; contagem.
- [ ] **Passo 5: commit** `feat(ngrrd-cluster): consulta paginada ao catálogo do líder sem posicionar (#171)`.

---

### Tarefa 6: API do cliente — `exists`/`find`, config e open sem criar

**Arquivos:**
- Criar: `.../cluster/api/SeriesInfo.java`
- Modificar: `.../cluster/api/NgrrdClusterConfig.java` (+ `catalogLookupBatchSize`, builder e YAML
  `client.catalogLookupBatchSize`)
- Modificar: `.../cluster/api/NgrrdClusterClient.java`, `.../cluster/client/DefaultNgrrdClusterClient.java`,
  `.../cluster/client/RemoteSeriesHandle.java`
- Teste: `.../api/NgrrdClusterConfigYamlTest.java`, `.../client/RemoteSeriesHandleTest.java`,
  novo `.../client/ClientExistenceTest.java` (se o cliente for difícil de montar em unit, extrair a
  lógica para `client/SeriesExistence` package-private testável com `CatalogService` fake/
  `PlacementLookup` fake + `CatalogLookupClient` com `RecordingClusterRpc`)

**Produz:**
```java
public record SeriesInfo(String seriesKey, String ownerNodeId, PlacementState state,
        String targetNodeId, long updatedAtEpochMs) {
    static SeriesInfo of(String seriesKey, SeriesPlacement placement) }
// NgrrdClusterClient
boolean exists(String seriesKey);
Map<String, Boolean> exists(Collection<String> seriesKeys);   // mapa com todas as chaves pedidas
Optional<SeriesInfo> find(String seriesKey);
```

**Comportamento:**
- `exists(Collection)`: `ensureOpen`; hits via `resolver.placementCached(key)` → `true`; misses →
  `lookupClient.lookup(misses, config.retryTimeout())` → presentes `true`, demais `false`.
  Exceção propaga. `exists(String)` delega. `find`: cache hit → `SeriesInfo.of`; senão lookup de 1
  chave → `Optional`.
- `open(..., options)` com `createIfMissing=false`: handle em cache → devolve; senão
  `RemoteSeriesHandle.open()`.
- `RemoteSeriesHandle`:
  - método privado `resolvePlacement(Duration maxWait)`: `options.createIfMissing()` ?
    `resolver.resolve(seriesKey, hash, geometry, maxWait)` : `resolver.resolveExisting(seriesKey, maxWait)`;
    usar em `open(OperationRetry)`, `noteWrongOwner` e demais chamadas a `resolver.resolve` do handle.
  - `OpenRequest` com `options.createIfMissing()` (enviar `Boolean.FALSE` só quando falso; `null`
    quando verdadeiro, para máxima compatibilidade).
  - `open(OperationRetry)`: status `NOT_FOUND` → `throw new SeriesNotFoundException(seriesKey)`.
  - `reopen()` continua absorvendo e logando (falha em `NOT_FOUND` = `false`, sem recriar).
    `handleRetryableStatus` (`NOT_OPEN` → `open(retry)`) propaga `SeriesNotFoundException` ao chamador.
- `NgrrdClusterConfig`: `catalogLookupBatchSize` default 2000, validação 1..10000
  (`IllegalArgumentException` PT-BR); YAML opcional.
- Javadoc de `exists`/`find` com o contrato de consistência da spec (seção "Contrato de consistência").

- [ ] **Passo 1: testes que falham:**
  - config: default 2000; YAML lê valor; 0 e 10001 rejeitados.
  - existência: `existsComHitLocalNaoFazRpc`; `existsComMissesConsultaLiderEmLote` (1 chamada p/ 3
    misses); `existsAusenteNoLiderDevolveFalse`; `existsSemLiderLancaExcecaoENuncaFalse`;
    `existsDevolveTodasAsChavesPedidas`; `existsMigrandoEhTrue`; `findDevolveDonoEstadoEAlvo`;
    `findAusenteDevolveVazio`.
  - `RemoteSeriesHandleTest`: `openSemCriarNaoFazPlaceEEnviaFlag`; `openSemCriarComNotFoundDoStorageLancaSeriesNotFound`;
    `openSemCriarSemPlacementLancaSeriesNotFound`; `reopenSemCriarComNotFoundDevolveFalse`;
    `openPadraoEnviaFlagNula`.
- [ ] **Passo 2:** rodar → falha.
- [ ] **Passo 3:** implementar (wiring: `DefaultNgrrdClusterClient` cria o `CatalogLookupClient` com
  `rpc`, `leaderRetry`, `Clock.systemUTC()`, `config.catalogLookupBatchSize()` e injeta no resolver).
- [ ] **Passo 4:** `mvn -pl nishi-utils-ngrrd-cluster verify` → verde; contagem.
- [ ] **Passo 5: commit** `feat(ngrrd-cluster): cliente consulta existência e abre série sem criar (#171)`.

---

### Tarefa 7: cliente — `verify` em lote por nó

**Arquivos:**
- Criar: `.../cluster/api/SeriesVerification.java`, `.../cluster/client/SeriesVerifier.java`
- Modificar: `NgrrdClusterClient`, `DefaultNgrrdClusterClient`
- Teste: `.../client/SeriesVerifierTest.java`

**Produz:**
```java
public enum SeriesVerification { PRESENT, MISSING_ON_OWNER, NOT_PLACED, UNVERIFIED }
// NgrrdClusterClient
/** Confirma no dono se o objeto existe; lotes por nó limitados a catalogLookupBatchSize. */
Map<String, SeriesVerification> verify(Collection<String> seriesKeys);
final class SeriesVerifier {
    SeriesVerifier(PlacementLookup resolver, CatalogLookupClient lookup, ClusterRpc rpc,
            Duration requestTimeout, Duration retryTimeout, int batchSize)
    Map<String, SeriesVerification> verify(Collection<String> seriesKeys)
}
```

**Comportamento:**
1. Placements: cache (`placementCached`) + `lookup(misses)`; sem placement → `NOT_PLACED`.
   Falha do lookup → exceção (nada presumido).
2. Agrupar por `ownerNodeId` (inclusive `MIGRATING`: o dono é a origem). Por nó, páginas de
   `batchSize`, sequenciais: `SERIES_EXISTS_BATCH` com `requestTimeout`; `present` → `PRESENT`.
   Falha (exceção ou status ≠ OK) numa página → as chaves daquela página ficam `UNVERIFIED`; as
   demais páginas/nós seguem.
3. Ausentes: `lookup(ausentes)` no líder (fresco). Sem placement → `NOT_PLACED`; dono igual →
   `MISSING_ON_OWNER`; dono diferente → reperguntar **uma vez** ao novo dono (mesma paginação):
   presente → `PRESENT`, ausente → `MISSING_ON_OWNER`, falha → `UNVERIFIED`.
4. O mapa devolvido contém todas as chaves pedidas.

- [ ] **Passo 1: testes que falham:** `agrupaPorDonoEPagina` (2 nós, batch 2, 5 chaves num nó → 3
  chamadas para ele); `semPlacementEhNotPlaced`; `ausenteNoDonoEhMissingOnOwner`;
  `donoMudouDuranteVerificacaoReperguntaAoNovoDono`; `falhaNumNoMarcaSoSuasChavesComoUnverified`;
  `falhaNoLookupDoLiderLancaExcecao`; `devolveTodasAsChaves`.
- [ ] **Passo 2:** rodar → falha.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `mvn -pl nishi-utils-ngrrd-cluster verify` → verde; contagem.
- [ ] **Passo 5: commit** `feat(ngrrd-cluster): verificação física de séries em lote por nó (#171)`.

---

### Tarefa 8: testes in-process do cluster

**Arquivos:**
- Criar: `nishi-utils-ngrrd-cluster/src/test/java/dev/nishisan/utils/oss/cluster/SeriesExistenceClusterTest.java`
  (usar `NgrrdClusterTestHarness`; ver `DistributedWriteReadClusterTest` e
  `CheckpointAfterMigrationClusterTest` como modelo de setup, YAML e espera por consenso)

**Casos (2 ou 3 storages + 1 cliente):**
- `serieExistenteExisteEAbreSemCriar`: open padrão + escrita + checkpoint; `exists`=true; `find`
  traz dono; open sem criar num **segundo cliente** (réplica fria) → handle funcional (lê o valor).
- `serieInexistenteNaoCriaNada`: `exists(k)`=false; open sem criar → `SeriesNotFoundException`;
  depois, em **todos** os storages, nenhuma entrada em `CatalogService.placementLocal(k)` e
  `volume.storage().exists(objectKey)` falso; `verify(k)`=`NOT_PLACED`.
- `serieEmMigracaoContaComoExistente`: série aberta/escrita; gravar no catálogo (via
  `CatalogService` de um nó) `SeriesPlacement.migrating(atual, outroNo, "m-test", now)`; aguardar a
  réplica do cliente; `exists`=true e `find().state()`=`MIGRATING`; iniciar open sem criar num
  segundo cliente em background; após ~1 s regravar `ACTIVE` (placement original); o open conclui
  sem `SeriesNotFoundException`.
- `placementSemArquivo`: gravar `SeriesPlacement.active(storageX, now)` para chave nunca aberta;
  `exists`=true; `verify`=`MISSING_ON_OWNER`; open sem criar → `SeriesNotFoundException`; objeto
  continua ausente no storageX.
- `existsEmLoteGrandePagina`: 5000 chaves inexistentes com `catalogLookupBatchSize=2000` → todas
  `false`, sem erro.

- [ ] **Passo 1:** escrever os testes.
- [ ] **Passo 2:** `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster -Dtest=SeriesExistenceClusterTest`
  → verde (rodar 3 vezes para checar estabilidade; relatar as 3).
- [ ] **Passo 3:** suíte completa `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster` → relatar
  contagem; falhas pré-existentes devem ser comparadas com `main` antes de concluir.
- [ ] **Passo 4: commit** `test(ngrrd-cluster): cenários in-process de existência e abertura sem criar (#171)`.

---

### Tarefa 9: documentação, CHANGELOG e versão 8.6.0

**Arquivos:**
- `doc/oss/ngrrd-cluster.md`: seção "Consultar existência e abrir sem criar" — `exists`/`find`/
  `verify`, `createIfMissing`, `SeriesNotFoundException` vs `NgrrdClusterException`, contrato de
  consistência (copiar da spec), `catalogLookupBatchSize`, exemplo de conciliação (remover do catálogo
  externo só com `false`/`SeriesNotFoundException`, nunca em exceção de cluster).
- `doc/oss/ngrrd-cluster-operacao.md`: ordem de atualização (storages antes dos clientes que usam
  as APIs novas) e o comportamento de líder antigo/storage antigo.
- `doc/CHANGELOG.md`: entrada 8.6.0 no padrão das anteriores.
- Versão 8.5.0 → 8.6.0 em todos os `pom.xml` e `README.md` (usar `grep -rn "8\.5\.0"` e seguir o que
  a PR #172 alterou: `ngrid-test/pom.xml`, `nishi-utils-core/pom.xml`, `nishi-utils-ngrrd-cluster/pom.xml`,
  raiz, oss, README, quickstart).
- [ ] **Passo 1:** editar.
- [ ] **Passo 2:** `mvn clean install -DskipTests -q` e `mvn verify -Pvalidate-javadoc -DexcludeNgrid=true
  -pl nishi-utils-oss,nishi-utils-ngrrd-cluster -am` → verde.
- [ ] **Passo 3: commits** separados: `docs(ngrrd): consulta de existência e abertura sem criar (#171)` e
  `chore: versão 8.6.0`.
