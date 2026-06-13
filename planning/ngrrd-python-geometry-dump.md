# Plano — Dump de geometria na lib Python do ngrrd

## Contexto

A lib Python `ngrrd-python` (em `python/ngrrd-python/`) já lê toda a geometria de
um arquivo `.ngrr` ao abrir (`NgrrdReader.open()` popula `self.header`,
`self.columns`, `self.archives` e `self._live_state`), mas **não** expõe um dump
de geometria pura de forma direta:

- `get_metadata()` devolve só um resumo (nomes de colunas e `"rra/CF"`), sem
  tipos de DS, sem `step_sec`/`rows`/`xff` por archive, sem offsets do header.
- O CLI `ngrrd-dump` **exige `--archive`** e sempre materializa dados do ring —
  não dá para rodar `ngrrd-dump arquivo.ngrr` só para inspecionar a estrutura.
- Não há `to_dict()`/`__repr__` legível nas dataclasses; para ver a geometria
  completa o usuário precisa iterar `reader.columns`/`reader.archives` na mão.

**Objetivo:** adicionar um dump de geometria dedicado — método na API
(`describe_geometry()` + `to_dict()` nas dataclasses) e modo no CLI
(`--geometry`, sem exigir `--archive`) — que emita a estrutura completa do
arquivo (campos, tipos, RRAs, step/rows/xff, offsets do header) **sem ler os
rings**. A geometria já está toda parseada em memória após `open()`; o trabalho
é só serializá-la de forma estável.

## Mudanças

### 1. `models.py` — `to_dict()` nas dataclasses

Adicionar um método `to_dict()` (serializável p/ JSON) em cada dataclass,
mantendo-as `frozen`. Enums viram `name` + `ordinal` (legível e completo):

- `SeriesColumn.to_dict()` → `{"derived_name", "raw_name", "raw_type", "raw_type_ordinal"}`
  (`raw_type` = `self.raw_type.name`).
- `SeriesArchive.to_dict()` → `{"rra_name", "cf", "cf_ordinal", "step_sec", "rows", "xff", "group_size", "ring_base_offset", "ring_bytes"}`
  (`cf` = `self.cf.name`; `ring_bytes` = `rows * <column_count> * 8` — calculado no reader,
  então `ring_bytes` fica fora do `to_dict` do archive ou recebe `column_count` como arg).
  **Decisão:** `SeriesArchive.to_dict()` não conhece `column_count`; o `ring_bytes`
  é adicionado pelo `describe_geometry()` do reader. `to_dict()` do archive cobre
  os campos próprios da dataclass.
- `SeriesHeader.to_dict()` → todos os 11 campos; `definition_hash` em `.hex()`.

### 2. `reader.py` — `describe_geometry()`

Novo método público que compõe a geometria completa a partir do estado já
parseado (reusa `self.header`, `self.columns`, `self.archives`, `self._live_state`;
chama `self._require_header()`; **não** chama `_archive_rows`):

```python
def describe_geometry(self) -> dict[str, Any]:
    """Return the full structural geometry without materializing ring data."""
    header = self._require_header()
    return {
        "file": str(self.file_path),
        "header": header.to_dict(),
        "last_update_ms": self._live_state["last_up_ms"],
        "columns": [c.to_dict() for c in self.columns],
        "archives": [
            {**a.to_dict(), "ring_bytes": a.rows * header.column_count * 8}
            for a in self.archives
        ],
    }
```

Manter `get_metadata()` como está (resumo barato); `describe_geometry()` é a
visão completa. Sem fallback/duplicação: `get_metadata` continua sendo o resumo.

### 3. `cli.py` — modo geometria

- `build_parser()`: trocar `--archive` para `required=False`; adicionar
  `--geometry` (`action="store_true"`, help: "Dump geometry only, no ring data").
- `main()`:
  - Modo geometria quando `args.geometry` **ou** `args.archive is None`.
    Nesse modo: `payload = reader.describe_geometry()`; serializa em JSON
    (`json.dumps(payload, indent=2)`) ou XML.
  - Modo dados (atual) quando há `--archive` e não há `--geometry`: inalterado
    (`{"metadata": metadata, "data": data}`).
  - Se `--geometry` **e** `--archive` forem passados juntos: geometria tem
    precedência (documentar no help) — evita erro chato; alternativa seria
    `parser.error(...)`. **Decisão:** geometria tem precedência.
- XML: adicionar `geometry_to_xml(geometry: dict) -> str` produzindo um doc
  `<ngrrd-geometry>` limpo (`<header>`, `<columns><column .../></columns>`,
  `<archives><archive .../></archives>`), no mesmo estilo `minidom` já usado em
  `to_xml`. Não reaproveitar `to_xml` (formato data-cêntrico não casa com geometria).

Resultado: `ngrrd-dump arquivo.ngrr` (sem `--archive`) e
`ngrrd-dump arquivo.ngrr --geometry --format xml` passam a funcionar.

### 4. `__init__.py`

Sem novos símbolos obrigatórios (os métodos ficam nas classes já exportadas).
Nenhuma mudança necessária, salvo se quisermos exportar helpers — não é o caso.

## Arquivos

- `python/ngrrd-python/src/ngrrd_python/models.py` — `to_dict()` nas 3 dataclasses.
- `python/ngrrd-python/src/ngrrd_python/reader.py` — `describe_geometry()`.
- `python/ngrrd-python/src/ngrrd_python/cli.py` — `--geometry`, `--archive` opcional, `geometry_to_xml()`.
- `python/ngrrd-python/tests/test_reader.py` — novos testes (reusa `build_ngrr_image`/`create_mock_ngrr`).
- `doc/oss/ngrrd.md` — seção "Uso programático": exemplo de `describe_geometry()` e do `ngrrd-dump --geometry`.
- `python/ngrrd-python/README.md` — nota curta do modo geometria do CLI.
- Cópia do plano aprovado para `planning/` na raiz do projeto (diretriz global).

## Testes

Adicionar em `tests/test_reader.py` (reaproveitando os helpers existentes
`build_ngrr_image` / `create_mock_ngrr` com 2 colunas + 2 archives):

- `test_describe_geometry`: valida `header` (version, base_step_sec,
  definition_hash hex, counts, offsets), `columns` (derived_name, raw_name,
  `raw_type == "COUNTER"`), `archives` (rra_name, `cf == "AVERAGE"/"MAX"`,
  step_sec, rows, xff, group_size, ring_base_offset, ring_bytes) e
  `last_update_ms`. Garantir que **não** depende de ring values (passar arquivo
  sem `ring_values`).
- `test_to_dict_models`: checa as chaves de `SeriesColumn.to_dict()` /
  `SeriesArchive.to_dict()` / `SeriesHeader.to_dict()`.
- `test_cli_geometry_json` e `test_cli_geometry_xml`: chamam `cli.main([...])`
  com `--geometry` (e o caso sem `--archive`), capturam stdout (`capsys`) e
  verificam que o JSON/XML contém a geometria e **não** contém bloco `data`.

Rodar:

```bash
cd python/ngrrd-python
python -m pytest            # ou: pytest
```

(Subprojeto Python é independente do build Maven; não afeta a suíte ngrid.)

## Verificação end-to-end

1. `cd python/ngrrd-python && python -m pytest` — toda a suíte verde, incluindo os novos testes.
2. Gerar/usar um `.ngrr` real (ou o mock dos testes) e rodar:
   - `python -m ngrrd_python.cli <arquivo>.ngrr --geometry` → JSON com header/columns/archives, sem `data`.
   - `python -m ngrrd_python.cli <arquivo>.ngrr --geometry --format xml` → `<ngrrd-geometry>` renderiza sem erro.
   - `python -m ngrrd_python.cli <arquivo>.ngrr --archive daily` → comportamento atual intacto (regressão).
3. Conferir doc `doc/oss/ngrrd.md` renderizando o exemplo novo coerente com a API.
