# Plano — ngrrd: `onGeometryChange` + paridade RRD (DS sem `derive`)

## Contexto

Hoje, quando a geometria de uma definition muda (ex.: `+1 DS`, novo RRA, mudança de
`stepSec`/`rows`), o `NgrrdWriter.openOrCreate()` (`writer/NgrrdWriter.java:161-183`)
detecta a divergência do `geometryHash` e faz **clean cut silencioso**: recria a
série do zero, **descartando toda a história**, emitindo apenas um
`System.err.println`. Na prática, um simples restart do coletor após editar o YAML
apaga 30 dias de dados. Esse é o incidente que originou esta melhoria.

Além disso, a modelagem de coluna está acoplada ao bloco `derive`: em
`SeriesGeometry` (`format/SeriesGeometry.java:66-76`) um DS só vira coluna se tiver
`derive.output`. Logo, **um GAUGE puro (temperatura, %CPU) não é persistido** — é
preciso declarar um `derive.output` passthrough artificial. Isso quebra a paridade
conceitual com o RRDtool, onde o **tipo** do DS define a transformação e o **nome**
do DS é o nome da coluna.

**Resultado pretendido:**
1. Restart nunca destrói história por omissão — o default passa a ser **falhar com
   relatório**; recriação e migração viram escolhas explícitas e versionadas.
2. DS sem bloco `derive` passam a ser persistidos com paridade RRD real
   (GAUGE as-is; COUNTER/DERIVE/ABSOLUTE derivando por tipo).

## Decisões já tomadas (com o usuário)

- **Entrega única** cobrindo enum `onGeometryChange` + relatório + MIGRATE + CLI +
  `schemaRevision`, mais a paridade RRD (DS sem derive).
- **`schemaRevision` reusa o campo `flags`** do header (offset 6, `u16`, hoje `0`) —
  **sem** bump de versão de formato; zero risco a arquivos `.ngrr` existentes
  (limite 65535 revisões).
- **MIGRATE estrutural exato**: remapeia colunas/archives **por nome**, copiando
  células; coluna nova = `NaN`. Mudança que altere `baseStep`/`stepSec`/`rows`
  (reamostragem) **não** é migrável → cai em FAIL com mensagem clara.
- **Paridade RRD completa**: GAUGE as-is + COUNTER/DERIVE/ABSOLUTE sem `derive`
  derivando com defaults; coluna sempre = `ds.name()` quando não houver `derive.output`.

## Semântica de configuração (template a seguir)

Replicar **exatamente** o padrão do enum `Durability` recém-adicionado:
`api/Durability.java` (enum + `@JsonCreator from(String)` tolerante a `null`),
campo opcional em `StorageSpec` (`definition/StorageSpec.java:21`), resolução com
precedência em `Ngrrd.resolveDurability` (`Ngrrd.java:148-156`) e override via
`Ngrrd.OpenOptions` (`Ngrrd.java:169-178`).

### Precedência de `onGeometryChange`
`OpenOptions` (abertura) > `spec.storage.onGeometryChange` (YAML) > **`FAIL`** (default).

### Trava por `schemaRevision` (gate anti-acidente)
Na reconciliação, comparar `geometryHash` gravado vs esperado:
- **hash igual** → geometria idêntica: abre/reidrata normal. (Se a revisão da
  definition for maior, regrava só o campo `flags` do header para selar a revisão —
  operação barata, sem rewrite.)
- **hash diferente** → geometria mudou:
  - `revisão_definition > revisão_arquivo` → mudança **intencional** → aplica
    `onGeometryChange` (FAIL/RECREATE/MIGRATE).
  - `revisão_definition <= revisão_arquivo` → mudança **não versionada** →
    **FAIL sempre** (ignora `onGeometryChange`), com mensagem pedindo bump explícito
    de `schemaRevision`. É isso que impede que uma edição acidental de YAML vire
    rewrite de 90 GB no boot.

## Arquitetura da mudança

### A. Paridade RRD — DS sem `derive` (fundação; afeta geometria/hash)

1. **`SeriesGeometry` (`format/SeriesGeometry.java:66-76`)** — generalizar a extração
   de colunas: **todo** DS vira coluna. `derivedName = derive.output.name` quando
   houver, senão `ds.name()`; `rawName = ds.name()`.
   - **Invariante crítica:** **não alterar a serialização do `geometryHash`**
     (`SeriesGeometry.java:205-219`). Para definitions que já usam `derive.output`,
     o conjunto de colunas e o hash permanecem idênticos → séries existentes **não**
     disparam migração no upgrade. Só definitions que adicionarem GAUGE/colunas novas
     mudam o hash (e aí o gate de revisão atua corretamente).

2. **Novo `engine/SampleDeriver`** (generaliza `engine/CounterDeriver.java`) —
   função pura que, dado `DataSourceDef` + tipo + estado prev + amostra, devolve o
   valor a persistir, com **defaults por tipo** quando `derive` ausente:
   - `GAUGE` → valor as-is, com clamp `min`/`max` (campos já existem em
     `DataSourceDef`, hoje não aplicados).
   - `COUNTER` → taxa via lógica atual do `CounterDeriver`; sem `derive`, usa
     defaults: fórmula implícita `delta/deltaT`, `clampNegativeToZero=false`,
     `onReset=UNKNOWN`, `onWrap=AUTO`. Mantém wrap (32/64) e reset detection.
   - `DERIVE` → `delta/deltaT` permitindo negativos, **sem** wrap/reset.
   - `ABSOLUTE` → `currValue/deltaT` (reset a cada leitura; usa só o ts anterior).
   - `derive.output.formula`, quando presente, permanece como pós-processamento
     (counter/derive). Manter `CounterDeriver` como caminho interno do COUNTER ou
     absorvê-lo no `SampleDeriver` — preferir absorver para um único ponto de verdade.
   - **Atenção/breaking:** hoje DERIVE/ABSOLUTE **com** `derive.output` são
     passthrough (`NgrrdWriter.deriveValue:374-376`). A paridade real passa a derivar
     por delta — muda comportamento desses tipos. Alinhado a "aplicar o definitivo".

3. **`NgrrdWriter.deriveValue` (`writer/NgrrdWriter.java:354-378`)** — substituir o
   `return Double.NaN` (DS sem `derive.output`) e o passthrough por chamada ao
   `SampleDeriver`, despachando por tipo. `state.counterPrev*` passa a ser usado por
   qualquer tipo derivado (não só COUNTER).

4. **`NgrrdDefinitionValidator` (`config/NgrrdDefinitionValidator.java`)** —
   - Permitir DS sem `derive.output` (hoje GAUGE puro é aceito no parse mas vira
     "buraco"); garantir que **todo** DS gere um nome de coluna válido.
   - `collectDataSourceNames:89-109` já barra colisão entre `derive.output.name` e
     outro DS — estender para garantir unicidade do nome de coluna efetivo
     (`ds.name()` quando não há derive), evitando que um `derivedName` colida com o
     `ds.name()` de um GAUGE.
   - Manter `counterBits` obrigatório para COUNTER.

5. **Reader** — nenhuma mudança estrutural: `NgrrdReader.read` usa
   `geo.columnIndex(dsName)` por `derivedName`, que para GAUGE será `ds.name()`.

### B. `schemaRevision` no header

6. **`definition/NgrrdMetadata.java`** — adicionar `@JsonProperty("schemaRevision")
   Integer schemaRevision` (null → 0). Validar `0..65535` no validator.

7. **`format/SeriesFileCodec.java`** —
   - `encodeStaticSection`/`buildInitialImage` passam a receber a revisão e gravá-la
     em `flags` (`buf.putShort` no offset 6) em vez de `0` (linha 91). CRC já cobre
     `[0..92)`, recalculado normalmente.
   - `SeriesHeader` (`format/SeriesHeader.java`) — renomear/expor `flags` como
     `schemaRevision()` (mantendo o slot binário). `decodeFixedHeader` já lê o `u16`.

8. **`format/SeriesGeometry.java` + codec** — para diff/migração:
   - Refatorar `SeriesGeometry` separando "extrair colunas+archives" do "cálculo de
     offsets". Construtor canônico privado `(baseStepSec, columns, archivesLite)` +
     factories `fromDefinition(def)` (mantém o construtor público atual) e
     **`fromPersisted(SeriesHeader, byte[] staticBytes)`**.
   - Novo **`SeriesFileCodec.decodeStaticSection(byte[], SeriesHeader)`** — espelho
     de `encodeStaticSection`: lê `DS_DICT[D]` (derivedName, rawName, dsType) e
     `ARCH_TABLE[A]` (rraName, cf, stepSec, rows, xff), reconstruindo a geometria
     persistida **sem** a definition.

### C. Reconciliação, relatório e migração (novo pacote `oss.migration`)

9. **`api/OnGeometryChange.java`** — enum `FAIL | MIGRATE | RECREATE` + `@JsonCreator
   from(String)` (padrão `Durability`).

10. **`StorageSpec` + `Ngrrd.OpenOptions` + resolução** — adicionar
    `onGeometryChange`; `Ngrrd.resolveGeometryChange(options, storageSpec)` espelhando
    `resolveDurability` (`Ngrrd.java:148-156`). Propagar para o `NgrrdWriter`
    (novo parâmetro de construtor, como foi feito com `Durability`).

11. **`migration/GeometryDiff` + `migration/GeometryChangeReport`** — compara
    geometria persistida vs nova e classifica: DS adicionado/removido/reordenado,
    archive adicionado/removido, archive com `stepSec`/`rows` alterado, `baseStep`
    alterado. Decide `migrávelEstruturalmente` (true só se `baseStep` igual e todo
    archive **comum** mantém `stepSec`+`rows`). Produz o **relatório legível**
    (o "relatório que faltou"): o que mudou + se é migrável + bytes da série.

12. **`migration/GeometryReconciler`** — executado **antes** de abrir o channel de
    escrita (no construtor do `NgrrdWriter`, antes de `provider.openSeries`). Usa
    `storage.get`/`storage.atomicReplace` (idempotente, tudo-ou-nada):
    - objeto inexistente/menor que header → retorna (writer fará `createFresh`).
    - hash igual → no-op (ou sela revisão no `flags`).
    - hash diferente → aplica o gate de revisão e então:
      - `FAIL` → lança `NgrrdGeometryChangeException` com o relatório.
      - `RECREATE` → `atomicReplace(buildInitialImage(geoNova, hashNovo, revNova))`.
      - `MIGRATE` → `GeometryMigrator.migrate(...)` (abaixo) → `atomicReplace`.
    - Após reconciliar, o `openOrCreate` do writer encontra geometria já compatível.
      Manter no `openOrCreate` um ramo incompatível **defensivo** (lança
      `IllegalStateException`) em vez do clean-cut atual.

13. **`migration/GeometryMigrator`** — migração estrutural exata:
    - Mapas por nome: archive novo→antigo (`rraName`+`cf`, exige `stepSec`+`rows`
      iguais), coluna nova→antiga (`derivedName`).
    - Monta imagem nova (`buildInitialImage`), depois sobrescreve:
      - **rings**: para cada archive comum e cada `row∈[0,rows)`, copia célula
        `(archAntigo,row,colAntiga)`→`(archNovo,row,colNova)`; coluna nova sem
        correspondente = `NaN`; archive novo sem correspondente = `NaN`.
      - **live-state**: preserva `lastUpEpochMs`; ponteiros `curRow`/`curRowEpochSec`
        por archive comum (mesmo `rows` → posição física idêntica); `counterPrev`,
        `pdp` remapeados por coluna; `cdp_prep` remapeado por `(archive,coluna)`.
      Preserva história **e** continuidade da derivação. Reescreve `flags=revNova` e
      `definitionHash=hashNovo`.

### D. CLI `ngrrd-migrate`

14. **`cli/NgrrdMigrateCli`** (classe `main`, sem libs externas — estilo do
    `ngrid-test/.../Main.java`): args = caminho do YAML, bindings de storage
    (`rootDir` p/ localDisk ou config S3), `--dry-run`, `--parallelism N`,
    `--on-geometry-change MIGRATE|RECREATE` (default `MIGRATE`).
    - Carrega/valida a definition, instancia storage, `storage.list(seriesPrefix)`
      (via `ObjectNaming.seriesPrefixOrDefault()`), itera as `*.ngrr`.
    - `--dry-run`: lê só o header de cada série, classifica via `GeometryDiff`,
      imprime **relatório agregado** ("N séries divergentes, M migráveis, ~X GB") +
      resumo por série. **Não escreve nada.**
    - Execução real: pool com paralelismo limitado, cada worker chama o
      `GeometryReconciler` (reusa a lógica per-série) com a política escolhida.
    - Empacotamento: documentar `java -cp <jar> dev.nishisan.utils.oss.cli.NgrrdMigrateCli`
      (avaliar `exec-maven-plugin`/manifest no `nishi-utils-oss/pom.xml`).
    - **Distinção registrada:** migração *eager no boot* = caminho per-série na
      abertura de cada handle (paralelizado pela app); migração em *janela* /
      datasets grandes (45k séries ≈ 90 GB) = CLI offline com `--dry-run` primeiro.

## Arquivos

**Novos**
- `api/OnGeometryChange.java`
- `engine/SampleDeriver.java` (generaliza `CounterDeriver`)
- `migration/GeometryDiff.java`, `migration/GeometryChangeReport.java`,
  `migration/GeometryReconciler.java`, `migration/GeometryMigrator.java`
- `config/NgrrdGeometryChangeException.java` (ou em `migration/`)
- `cli/NgrrdMigrateCli.java`

**Modificados**
- `format/SeriesGeometry.java` (coluna p/ todo DS; `fromPersisted`)
- `format/SeriesFileCodec.java` (revisão no `flags`; `decodeStaticSection`)
- `format/SeriesHeader.java` (`flags` → `schemaRevision()`)
- `writer/NgrrdWriter.java` (`deriveValue` via `SampleDeriver`; reconciliação;
  novo parâmetro `onGeometryChange`; ramo incompatível defensivo)
- `definition/NgrrdMetadata.java` (`schemaRevision`), `definition/StorageSpec.java`
  (`onGeometryChange`)
- `Ngrrd.java` (`OpenOptions` + `resolveGeometryChange` + fail-fast + propagação)
- `config/NgrrdDefinitionValidator.java` (DS sem derive; unicidade de coluna;
  `schemaRevision` 0..65535)

## Riscos e decisões

- **Breaking → major (7.0.0).** Mudanças: default `FAIL` (antes clean-cut silencioso);
  semântica real de DERIVE/ABSOLUTE (antes passthrough); GAUGE puro passa a ocupar
  ring. Anunciar no changelog/release. Garantir que séries sem mudança de YAML **não**
  rebotem (invariante do `geometryHash`).
- **Primeira mudança de geometria pós-upgrade trava** pedindo `schemaRevision`
  (revisão de arquivos antigos = 0). É o comportamento desejado — documentar bem.
- **`storage.get` lê o objeto inteiro** na reconciliação; só ocorre quando há
  divergência (raro) e a série típica é pequena (≈ MB). Aceitável.
- **Cap de revisão 65535** (`u16`); validar e mensagem clara se exceder.

## Testes (`mvn test`, espelhar `NgrrdWriterDurabilityTest`/`NgrrdDurabilityFacadeTest`)

- **Paridade RRD:** GAUGE sem derive persiste e lê as-is; COUNTER/DERIVE/ABSOLUTE
  sem derive derivam conforme tipo; clamp `min`/`max`.
- **`schemaRevision`/codec:** revisão round-trip no `flags`; `decodeStaticSection`
  reconstrói geometria; CRC intacto; arquivos v1 (flags=0) abrem sem migração.
- **`onGeometryChange`:** FAIL lança `NgrrdGeometryChangeException` com relatório e
  **não** escreve; RECREATE recria; gate de revisão trava sem bump; precedência
  OpenOptions>YAML>FAIL.
- **MIGRATE estrutural:** `+1 DS` preserva história dos DS antigos (nova coluna NaN)
  e ponteiros/continuidade; remoção/reordenação de DS; mudança de `rows`/`stepSec` →
  FAIL "requer reamostragem".
- **CLI:** `--dry-run` produz relatório agregado sem efeitos; execução migra em lote.

## Verificação ponta a ponta

1. `mvn -pl nishi-utils-oss test` (unitários).
2. `mvn verify -Pngrrd-integration` (LocalStack/Testcontainers) — migração também em
   backend S3.
3. Manual: criar série local com GAUGE sem derive, escrever pontos, ler; depois
   `+1 DS` com `schemaRevision` bumpado e `onGeometryChange: MIGRATE`, reabrir,
   confirmar história preservada; rodar `NgrrdMigrateCli --dry-run` e conferir o
   relatório.

## Documentação (DoD)

- Atualizar `doc/oss/ngrrd.md`: layout do header (campo `flags`=`schemaRevision`),
  `onGeometryChange`, `schemaRevision`, DS sem derive/paridade RRD, CLI `ngrrd-migrate`.
- Diagrama PlantUML em `doc/diagrams/` do fluxo de reconciliação/migração
  (incorporado via `uml.nishisan.dev`).
- Copiar este plano aprovado para `/planning` na raiz do projeto.
- Commits atômicos por peça (A→B→C→D), na ordem de dependência.
