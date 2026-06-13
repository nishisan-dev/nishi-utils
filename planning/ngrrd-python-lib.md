# Implementação da Biblioteca Python: ngrrd-python

## 1. Background & Motivation
O formato `ngrrd` (Nishi Grid Round-Robin Database) foi desenvolvido em Java como parte do ecossistema `@nishi-utils-oss` para armazenamento ultra-eficiente de séries temporais. Existe a necessidade de consumir esses dados em ecossistemas focados em dados, especificamente Python, para permitir integrações com APIs baseadas em JSON e ferramentas de Machine Learning/Analytics no futuro.

## 2. Scope & Impact
Será criada uma biblioteca Python stand-alone chamada `ngrrd-python`. O objetivo desta primeira versão é permitir a leitura dos arquivos binários `.ngrr` utilizando uma abordagem de *Lazy Evaluation* via mapeamento de memória (`mmap`) e exportação simplificada para dicionários e JSON nativo.

## 3. Proposed Solution
A arquitetura se baseará no módulo nativo `struct` do Python combinado com `mmap`.
A biblioteca será estruturada da seguinte forma:

* **Scaffolding (`pyproject.toml`)**: Setup inicial utilizando *Src Layout* (`src/ngrrd_python/`).
* **Parser de Seção Estática**: Decodificará os 96 bytes iniciais (Fixed Header) usando estruturação `big-endian` (`>`) e os dicionários dinâmicos de Data Sources (Columns) e Archives.
* **Parser de Live State**: Mapeamento dos metadados de ring-pointers (`curRow`, `curRowEpochSec`) essenciais para reconstruir cronologicamente o Ring Buffer.
* **Acesso Lazy aos Ring Buffers**: O acesso aos dados efetivos (Double IEEE-754) nos Ring Buffers ocorrerá estritamente sob demanda.
* **Interface do Usuário**: Uma classe `NgrrdReader` atuando como API principal expondo métodos como `.get_metadata()` e `.read_archive_as_dict(archive_name)`.

## 4. Alternatives Considered
* **Eager In-Memory Parsing**: Ler todos os ring-buffers para a memória na abertura. Rejeitado devido ao risco de estouro de RAM com arquivos `.ngrr` muito grandes com longa política de retenção.

## 5. Implementation Plan
### Fase 1: Setup e Estrutura Básica
- Criação do pacote `ngrrd-python` com layout recomendado e empacotamento gerenciado via ferramentas modernas (ou `setuptools` limpo).
- Setup da suíte de testes (com `pytest`).

### Fase 2: Mapeamento de Byte-Format (`struct`)
- Implementar as classes internas de dataclass para `SeriesHeader`, `SeriesColumn` e `SeriesArchive`.
- Codificar as funções de extração binária iterando corretamente sobre a string de bytes (considerando pad 8-align até o LiveStateOffset e RingDataOffset).

### Fase 3: Lógica de Ring Buffer
- Reconstruir a lógica Java presente em `NgrrdReader.java` no que diz respeito ao cálculo cronológico. O arquivo em disco é escrito em `row-major`, e precisa ser lido do slot mais antigo até o `anchor` (mais recente).

### Fase 4: Exportação JSON
- Criar a camada de abstração que converte a leitura raw em uma lista de dicionários do tipo: `[{"ts": 1234567000, "in_bps": 120.5, "out_bps": 50.0}, ...]`.

## 6. Verification
- Escrita de testes unitários mockando sequências de bytes estáticas que reproduzem os outputs do `SeriesFileCodec.java` atual para garantir total compatibilidade entre os formatos.

## 7. Migration & Rollback
Não se aplica diretamente ao repositório Java atual. A lib viverá preferencialmente no mesmo repositório sob uma pasta `python/` ou como um submódulo, não quebrando a compatibilidade com a implementação base.
