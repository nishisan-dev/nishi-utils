# Plano: Durabilidade configurável no ngrrd (FSYNC | OS_CACHE)

## Context

Hoje o `NgrrdWriter` materializa **e** força (`fsync` no disco / `PUT` no S3) em **todo**
`checkpoint()`/`flush()` — ver `checkpointAndForce()` em
`nishi-utils-oss/.../writer/NgrrdWriter.java:523-544`, que chama `channel.force()`
incondicionalmente. Para cargas de ingestão de alta frequência (ex.: backfill, séries
voláteis cuja perda em crash é tolerável), o `fsync` por checkpoint é o gargalo.

Queremos expor **durabilidade como knob**: `FSYNC` (atual, default) vs `OS_CACHE`
(checkpoint materializa os bytes — leitores no mesmo processo continuam enxergando o CDP
parcial via page cache do SO — mas **pula** o `force()`; o SO decide quando descarregar).
A janela de perda passa a ser apenas um **crash abrupto**: um `close()` limpo ainda
descarrega tudo, porque `LocalSeriesChannel.close()` (`LocalDiskStorage.java:238-248`) e
`S3SeriesChannel.close()` (`S3Storage.java:222-224`) chamam `force()` no fechamento.

Precedente direto no monorepo: o enum `RelayDurability` do core
(`nishi-utils-core/.../ngrid/replication/RelayDurability.java`: `ALWAYS | GROUP_COMMIT |
OS_MANAGED`). Reaproveitamos o mesmo modelo conceitual; `GROUP_COMMIT` fica para um
follow-up.

**Caveat central (decisão: fail-fast):** no S3 o `force()` **é** o `PUT` — desligá-lo
significaria nunca publicar a série. Portanto `OBJECT_STORAGE` + `OS_CACHE` é rejeitado na
abertura. O knob é, na prática, do storage local.

### Decisões aprovadas
1. **Superfície:** ambos — `storage.durability` no YAML define o default; um `OpenOptions`
   passado a `Ngrrd.fromYaml(...)` sobrescreve por abertura (mesma definição abre `FSYNC`
   em produção e `OS_CACHE` num job de backfill).
2. **S3 + OS_CACHE:** fail-fast com `IllegalArgumentException` clara na abertura.
3. **Group-commit:** adiado para follow-up (não entra nesta entrega; enum fica com 2
   valores, sem placeholder não-implementado).
4. **Consumer:** o ngrrd não tem consumer (readers são stateless). Esta entrega expõe só
   o enum/opção; o mapeamento `commit.fsync: true|false → FSYNC|OS_CACHE` é responsabilidade
   de uma camada externa que consome a lib — apenas documentado aqui.

## Approach

### 1. Novo enum `Durability` (api)
Arquivo novo: `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/api/Durability.java`.
Seguir o padrão de `WriteMode.java`/`StorageBackendType.java` (mesma pasta): enum +
`@JsonCreator from(String)` aceitando `fsync`/`os_cache`/`FSYNC`/`OS_CACHE`.

```java
public enum Durability {
    /** fsync por checkpoint (disco) / PUT por checkpoint (S3). Default. */
    FSYNC,
    /** Disco local: checkpoint materializa mas pula o fsync; SO descarrega no seu ritmo.
        Não suportado em OBJECT_STORAGE. */
    OS_CACHE;

    @JsonCreator
    public static Durability from(String value) { /* trim + valueOf(upper), null→null */ }
}
```
Default (quando ausente no YAML e sem override) resolvido como `FSYNC` no ponto de consumo,
não no enum.

### 2. Superfície YAML — `StorageSpec`
Arquivo: `nishi-utils-oss/.../definition/StorageSpec.java`. Adicionar 4º campo opcional ao
record:
```java
public record StorageSpec(
    @JsonProperty("backend") StorageBackendType backend,
    @JsonProperty("objectNaming") ObjectNaming objectNaming,
    @JsonProperty("writePolicy") WritePolicy writePolicy,
    @JsonProperty("durability") Durability durability   // novo; null = default (FSYNC)
) {}
```
Campo ausente no YAML desserializa como `null` (compatível com YAMLs existentes).
**Atenção:** todo `new StorageSpec(...)` direto (fixtures/builders de teste) precisa do 4º
arg — fazer `grep -rn "new StorageSpec(" nishi-utils-oss/src` e atualizar (passar `null`).

### 3. Superfície de abertura — `Ngrrd.OpenOptions` + overloads
Arquivo: `nishi-utils-oss/.../Ngrrd.java`. Record aninhado público:
```java
public record OpenOptions(Durability durability) {        // null = usar default do YAML
    public static OpenOptions defaults()              { return new OpenOptions(null); }
    public static OpenOptions durability(Durability d){ return new OpenOptions(d); }
}
```
Adicionar variantes `fromYaml(..., OpenOptions options)` para `Path`, `InputStream` e
`String`. Os overloads atuais delegam com `OpenOptions.defaults()` — **sem mudança de
comportamento** para chamadores existentes (resolve para `FSYNC`).

No overload central, **após** `NgrrdYamlLoader.parse` + `NgrrdDefinitionValidator.validate`:
```java
Durability effective = resolveDurability(options, def.spec().storage()); // open > YAML > FSYNC
// fail-fast ANTES de StorageFactory.from / abrir o writer (evita GET no S3)
if (def.spec().storage().backend() == StorageBackendType.OBJECT_STORAGE
        && effective == Durability.OS_CACHE) {
    throw new IllegalArgumentException(
        "durability OS_CACHE não é suportada com backend OBJECT_STORAGE: no S3 o force() "
      + "é o próprio PUT (publicação); desligá-lo nunca publicaria a série. "
      + "Use FSYNC ou backend localDisk.");
}
...
NgrrdWriter writer = new NgrrdWriter(def, storage, seriesKey, metrics, seriesLock, effective);
```
A checagem fica **só** no open (sobre a durabilidade *efetiva*), não em
`NgrrdDefinitionValidator` — assim um YAML com `objectStorage` + `os_cache` ainda pode ser
recuperado por um override `FSYNC` na abertura, em vez de falhar no parse.

### 4. Gating do force — `NgrrdWriter`
Arquivo: `nishi-utils-oss/.../writer/NgrrdWriter.java`.
- Novo campo `private final Durability durability;`
- Novo construtor completo `NgrrdWriter(def, storage, seriesKey, metrics, seriesLock,
  durability)`. Os 3 construtores existentes (usados direto em testes) delegam com
  `Durability.FSYNC` — preservam o comportamento atual.
- Em `checkpointAndForce()` (linha 543), trocar `channel.force();` por:
  ```java
  if (durability == Durability.FSYNC) {
      channel.force();
  }
  // OS_CACHE: pula o fsync por checkpoint; SO descarrega no seu ritmo.
  // Um close() limpo ainda descarrega o pendente (janela de perda = crash abrupto).
  ```
- **Mantido inalterado** (decisão explícita):
  - `createFresh()` linha 172 `channel.force()` — integridade estrutural única na criação
    (header válido + pré-alocação antes de qualquer escrita); barato, fora do hot path.
  - Caminho `Shutdown` → `checkpointAndForce()` + `closeChannelQuietly()` →
    `channel.close()` → `force()`: close limpo continua descarregando uma vez.

### 5. Testes
- **Spy de contagem de force** (test double): `ForceCountingStorage implements NgrrdStorage,
  SeriesChannelProvider` delegando a um `LocalDiskStorage` e envolvendo o `SeriesChannel`
  para contar chamadas a `force()`. Modelar em `FailingStorage` de
  `NgrrdWriterFailureTest.java` (precedente de storage de teste).
- Em `nishi-utils-oss/src/test/.../writer/` (novo `NgrrdWriterDurabilityTest` ou estender
  `NgrrdWriterIncrementalTest`):
  - `osCacheNaoForcaPorCheckpoint`: write + checkpoint(OS_CACHE) → contador de `force()`
    permanece no baseline do `createFresh` (1); CDP parcial continua **legível** (page cache).
  - `fsyncForcaPorCheckpoint`: FSYNC → `force()` chamado no checkpoint.
  - `closeDescarregaMesmoEmOsCache`: OS_CACHE + `close()` → contador de `force()` incrementa.
- Em `nishi-utils-oss/src/test/.../oss/` (façade `Ngrrd`):
  - `openOptionSobrescreveYamlDurability`: YAML `fsync` + `OpenOptions.durability(OS_CACHE)`
    → efetivo OS_CACHE (e o caso inverso).
  - `s3ComOsCacheRejeitaNaAbertura`: YAML `backend: objectStorage` + efetivo OS_CACHE →
    `IllegalArgumentException` (lançada antes de qualquer I/O S3; usa `S3Settings` dummy).

### 6. Documentação
Arquivo: `doc/oss/ngrrd.md` (pt-BR, padrão do projeto).
- Nova subseção **Durabilidade**: `FSYNC` (default) vs `OS_CACHE`; gating do fsync por
  checkpoint; garantia de descarga em `close()` limpo (perda só em crash abrupto); caveat
  S3 (`force()` = `PUT`, daí o fail-fast); como uma camada externa mapeia
  `commit.fsync: true|false → Durability`; nota de `GROUP_COMMIT` como follow-up planejado.
- Referência YAML: documentar `storage.durability: fsync|os_cache` (default `fsync`).
- Atualizar o bullet do fluxo de escrita que afirma que checkpoint "torna durável" para
  refletir a dependência do modo de durabilidade.

## Critical files
- `nishi-utils-oss/.../api/Durability.java` (novo) — espelha padrão de `WriteMode.java`
- `nishi-utils-oss/.../definition/StorageSpec.java` — 4º campo `durability`
- `nishi-utils-oss/.../Ngrrd.java` — `OpenOptions`, overloads, resolução + fail-fast
- `nishi-utils-oss/.../writer/NgrrdWriter.java` — campo + construtor + gating em `checkpointAndForce()`
- Testes em `nishi-utils-oss/src/test/.../writer/` e `.../oss/`
- `doc/oss/ngrrd.md`

## Commits sugeridos (atômicos)
1. `feat(ngrrd): adiciona enum Durability (FSYNC|OS_CACHE)`
2. `feat(ngrrd): expõe durability em StorageSpec (YAML) e OpenOptions (abertura)`
3. `feat(ngrrd): aplica gating de fsync por checkpoint conforme Durability`
4. `feat(ngrrd): rejeita OS_CACHE com backend OBJECT_STORAGE na abertura`
5. `test(ngrrd): cobre durabilidade FSYNC/OS_CACHE e fail-fast S3`
6. `docs(ngrrd): documenta durabilidade configurável e caveat S3`

(Após aprovação, copiar este plano para `/planning` na raiz do repo, conforme convenção.)

## Verification
```bash
# Build + unit tests do módulo oss (independente do core/ngrid)
mvn -pl nishi-utils-oss clean install

# Foco nos novos testes
mvn -pl nishi-utils-oss test -Dtest=NgrrdWriterDurabilityTest
mvn -pl nishi-utils-oss test -Dtest=NgrrdDurabilityFacadeTest   # nome a definir

# Integração S3/LocalStack — garante que o caminho FSYNC+S3 segue intacto
mvn -pl nishi-utils-oss verify -Pngrrd-integration
```
Checagens manuais de aceite:
- OS_CACHE em disco: após `checkpoint()`, o CDP parcial é legível (page cache) **e** o
  contador de `force()` não sobe; após `close()`, sobe (descarga limpa).
- FSYNC (default): comportamento idêntico ao atual (regressão zero).
- `objectStorage` + OS_CACHE efetivo: `IllegalArgumentException` na abertura, sem tocar o S3.
