# Plano — Tornar a documentação do ngrrd (nishi-utils-oss) mais prática

## Context

A documentação atual em `doc/oss/ngrrd.md` é forte em **conceitos** (termos RRD,
arquitetura C4, fluxos de escrita/leitura, layout binário) mas tem lacunas
**práticas** que dificultam quem quer realmente usar o formato:

1. **Multi-métrica não fica evidente.** No mundo real um RRD modela uma interface
   inteira (traffic in/out, errors, discards…). A doc mostra `write`/`read` de
   **uma** métrica por vez e não há um walkthrough end-to-end de uma interface
   com várias métricas simultâneas.
2. **Tamanho do arquivo não é explicado.** O usuário perguntou explicitamente se
   o arquivo gerado é fixo ou dinâmico. A doc cita "header de 30 bytes" e "8 bytes
   por datapoint" mas não fecha a conta nem explica o modelo de crescimento.

Objetivo: enriquecer `doc/oss/ngrrd.md` (somente esse arquivo, em pt-BR) com uma
seção de **exemplo prático multi-métrica** e uma seção de **tamanho do arquivo
(fixo vs dinâmico)** com fórmula e tabela de footprint — sem alterar código.

## Decisões (alinhadas com o usuário)

- **Exemplo:** interface de rede realista com **6 DataSources** —
  `in_octets→in_bps`, `out_octets→out_bps`, `in_errors→in_eps`,
  `out_errors→out_eps`, `in_discards→in_dps`, `out_discards→out_dps`. Marcado
  claramente como exemplo ilustrativo (estende o padrão do
  `iface-traffic-errors-v1.yaml`, que tem 4 DS).
- **Local:** todo o conteúdo novo vai em `doc/oss/ngrrd.md`. Nenhum arquivo novo.
- **Idioma:** pt-BR (convenção do projeto, ver CLAUDE.md).

## Fatos técnicos verificados (base para a seção de footprint)

Confirmados lendo `NgrrdWriter.java`, `BlockCodec`/`BlockHeader`, `StorageKey.java`
e `TimeSpec`:

- **Cada bloco `.ngrrd` tem tamanho determinístico e fixo** depois de fechado:
  `30 bytes (header) + rowsPorBloco × 8 bytes (payload double IEEE-754)`.
  - `rowsPorBloco = blockSizeSec / rra.stepSec` (ver `NgrrdWriter.persistRraBlock`,
    `cdpCount = pdps.length / groupSize`, `nishi-utils-oss/.../writer/NgrrdWriter.java:353-354`).
  - Ex. (`blockSizeSec=21600`): RRA 5m (step 300) → 72 linhas → **606 B/bloco**;
    RRA 1h (step 3600) → 6 linhas → **78 B/bloco**; RRA 2h (step 7200) → 3 linhas
    → **54 B/bloco**.
- **O conjunto total por série é limitado (ring buffer no nível de bloco)**, NÃO
  cresce indefinidamente: `NgrrdWriter.expireOutOfWindowBlocks` (`:312-346`) apaga
  blocos cujo fim caiu fora de `retentionSec = rra.rows × rra.stepSec`.
- **Difere do RRDtool clássico:** RRDtool pré-aloca **um único arquivo** com o
  tamanho final desde o dia 1. O ngrrd grava **muitos arquivos pequenos** (1 por
  janela `blockSizeSec`, por `(RRA, DS, CF)`), crescendo de zero até um teto e
  então mantendo-se ~constante por expiração.
- **Só blocos agregados são materializados hoje.** `StorageKey.rawBlock` existe
  mas **não é chamado** em código de produção (writer só usa `aggBlock`). O
  prefixo `raw` faz parte da convenção de nomes, porém não é populado pelo writer
  atual — o footprint é 100% definido pelos RRAs. (Documentar como nota, sem mudar
  comportamento.)
- **Identidade elegante do footprint** (derivada e conferida):
  `footprint(RRA,DS,CF) = rows×8 (payload) + blocos×30 (headers)`, onde
  `blocos = ceil(rows×stepSec / blockSizeSec)`. Equivalente a
  `rows × (8 + 30 × stepSec / blockSizeSec)`. O payload total bate **exatamente**
  `rows × 8` (o ring de blocos guarda exatamente `rows` CDPs).

## Mudanças em `doc/oss/ngrrd.md`

### 1. Nova seção: "Exemplo prático — uma interface de rede completa"
Inserir após "Definição YAML — exemplo mínimo" (após a linha ~173).

- **Introdução curta:** explicar que uma interface real tem várias métricas e que
  cada métrica = 1 DataSource (raw COUNTER) → 1 saída derivada (`derive.output`).
- **YAML ilustrativo com 6 DataSources** seguindo o padrão exato já validado em
  `nishi-utils-oss/src/test/resources/iface-traffic-errors-v1.yaml`:
  - `in_octets/out_octets` (COUNTER 64-bit) → `in_bps/out_bps`, `formula: "delta * 8 / deltaT"`, unit `bit/s`.
  - `in_errors/out_errors` (COUNTER 32-bit) → `in_eps/out_eps`, `formula: "delta / deltaT"`, unit `errors/s`.
  - `in_discards/out_discards` (COUNTER 32-bit) → `in_dps/out_dps`, `formula: "delta / deltaT"`, unit `discards/s`. ← métricas novas, mesmo padrão.
  - `archives.appliesTo.include`: as 6 saídas derivadas.
  - Reusar os 3 RRAs do exemplo real: `rra_5m_30d`, `rra_1h_6mo`, `rra_2h_1y`.
  - Presets `daily` / `weekly` / `monthly` listando as 6 séries + um preset de
    `errors_daily` com `cf: MAX`.
  - Nota: "exemplo ilustrativo; a versão de 4 DS testada no repo está em
    `nishi-utils-oss/src/test/resources/iface-traffic-errors-v1.yaml`".

### 2. Substituir/expandir "Uso programático" (linhas ~177-205)
Trocar o exemplo de uma única métrica por um fluxo realista, espelhando o que os
testes fazem (`NgrrdFacadeTest`, `IfaceTrafficSmokeIT`):

- **Escrita multi-métrica em loop** (um `tsMs` por coleta SNMP, 6 `write`):
  ```java
  long ts = ...; // epoch ms da coleta (alinhe ao baseStepSec)
  handle.write("in_octets",   new Sample(ts, inOctets));
  handle.write("out_octets",  new Sample(ts, outOctets));
  handle.write("in_errors",   new Sample(ts, inErrors));
  handle.write("out_errors",  new Sample(ts, outErrors));
  handle.write("in_discards", new Sample(ts, inDiscards));
  handle.write("out_discards",new Sample(ts, outDiscards));
  handle.flush(); // bloqueia até fechar/persistir o bloco corrente
  ```
- **Leitura por preset retornando várias séries de uma vez** (`Map<String,SeriesResult>`):
  ```java
  Map<String, SeriesResult> daily = handle.read("daily");
  SeriesResult inBps  = daily.get("in_bps");
  SeriesResult outBps = daily.get("out_bps");
  for (DataPoint p : inBps.points()) { /* p.tsEpochMs(), p.value() (NaN = gap) */ }
  ```
- **Leitura ad-hoc com `ViewQuery`** (janela/step/cf/maxPoints explícitos), citando
  que `BestFitSelector` escolhe o RRA adequado e que `maxPoints` faz downsample.
- Notas curtas: `write` é assíncrono (worker single-thread, fila); `flush()` força
  o fechamento do bloco; `NaN` representa gap/unknown (reset → `onReset: unknown`);
  manter o exemplo S3/MinIO já existente.

### 3. Nova seção: "Tamanho do arquivo — fixo ou dinâmico?"
Inserir após "Layout físico (storage)" (após a linha ~111). Responde diretamente
a pergunta do usuário:

- **Por bloco (fixo):** `tamanhoBloco = 30 + (blockSizeSec / rra.stepSec) × 8` bytes,
  fixo e determinístico assim que o bloco fecha. Tabela por RRA do exemplo:

  | RRA | stepSec | linhas/bloco (`blockSizeSec/step`) | bytes/bloco |
  |-----|---------|-----|-----|
  | rra_5m_30d | 300 | 72 | 606 B |
  | rra_1h_6mo | 3600 | 6 | 78 B |
  | rra_2h_1y | 7200 | 3 | 54 B |

- **Por série (limitado, não infinito):** explicar a expiração por retenção
  (`rows × stepSec`) → ring buffer no nível de bloco. Fórmula:
  `footprint(RRA,DS,CF) = rows × (8 + 30 × stepSec / blockSizeSec)` bytes, com
  `blocos vivos = ceil(rows × stepSec / blockSizeSec)`. Tabela de footprint por
  `(RRA,DS,CF)` para o exemplo (5m≈71 KiB, 1h≈55 KiB, 2h≈77 KiB) e o total da
  série de 6 DS × 3 RRA × 2 CF ≈ **2,4 MiB** em retenção plena, com ≈ **27,6 mil
  arquivos** (1 por janela de 6 h por combinação).
- **Comparação com RRDtool:** RRDtool = 1 arquivo pré-alocado no tamanho final;
  ngrrd = muitos arquivos pequenos, crescendo de zero até o teto e estabilizando.
- **Nota de tuning + overhead de header:** para RRAs grossos cada bloco guarda
  poucas linhas, então o header de 30 B pesa (RRA 2h: 54 B/bloco, ~56% overhead).
  Regra prática: dimensionar `blockSizeSec` grande o bastante frente ao `stepSec`
  do RRA mais grosso para amortizar o header e reduzir contagem de arquivos.
- **Nota de implementação atual:** apenas blocos agregados (RRAs) são gravados; o
  prefixo `raw` é parte da convenção de nomes mas não é populado pelo writer atual.

### 4. Ajuste menor na seção "Conceitos"
Acrescentar uma linha na tabela explicando que **uma série modela várias métricas
(DS)** — ancorar a ideia logo no início para o leitor que vem do RRDtool.

## Arquivos
- **Editar:** `doc/oss/ngrrd.md` (único arquivo alterado).
- **Referência (somente leitura, não alterar):**
  `nishi-utils-oss/src/test/resources/iface-traffic-errors-v1.yaml` (padrão do YAML),
  `nishi-utils-oss/src/test/java/.../NgrrdFacadeTest.java` e `IfaceTrafficSmokeIT.java`
  (padrão de uso write/read), `nishi-utils-oss/.../writer/NgrrdWriter.java` (footprint).

## Verificação
1. **Coerência do YAML do exemplo:** conferir campo a campo contra
   `iface-traffic-errors-v1.yaml` (nomes de chaves, `type`, `counterBits`,
   `derive.output`, `archives.appliesTo`, `presets`) — as 6 DS devem seguir o
   mesmo schema já desserializado por `NgrrdYamlLoader`/`NgrrdDefinition`.
2. **Coerência da API Java:** assinaturas dos exemplos batem com `NgrrdHandle`
   (`write(String, Sample)`, `read(String)→Map`, `read(String, ViewQuery)`) e
   `Sample(long,double)` / `DataPoint(long,double)` / `SeriesResult.points()`.
3. **Conferir a aritmética do footprint** recomputando a tabela:
   `30 + (21600/step)×8` por bloco e `rows × (8 + 30×step/21600)` por combinação.
4. **Render dos diagramas/markdown:** garantir que as tabelas renderizam e que
   nenhum link PlantUML existente foi quebrado (não adicionamos diagramas novos).
5. (Opcional) `mvn -pl nishi-utils-oss verify` continua passando — mudança é só de
   documentação, então a suíte não deve ser afetada; rodar apenas para sanidade.
