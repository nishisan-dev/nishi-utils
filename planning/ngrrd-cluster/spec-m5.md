# Spec M5 — documentação, diagramas, versão 8.3.0 e changelog

Repositório: /home/lucas/Projects/nishisan/nishi-utils, branch `feature/ngrrd-cluster`. Pré-requisito: M4 commitado.
Desenho: `planning/ngrrd-cluster.md`. Histórico: `planning/ngrrd-cluster/checkpoint.md` (contém as decisões e os trade-offs que a documentação deve registrar).

## Regras
- Documentação em PT-BR, na voz do time, sem menção a agentes. Código/identificadores em inglês.
- Sem commit, sem `git add -A`, sem `git stash`. Maven: o bump de versão exige `mvn clean install -DskipTests` na RAIZ (única exceção autorizada), depois `mvn -pl nishi-utils-ngrrd-cluster verify` e `mvn -pl nishi-utils-oss test -Dtest=NgrrdBlobFacadeTest`.
- Diagramas PlantUML em `doc/oss/diagrams/` com nomes `snake_case`, embutidos via `![título](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/<arquivo>.puml)` (mesmo padrão de `doc/oss/ngrrd-blob-volume.md`). Valide a sintaxe renderizando cada `.puml` pelo proxy (curl) ou por `plantuml` local se existir; sem erro de sintaxe.

## 1. `doc/oss/ngrrd-cluster.md` (novo)
Seções: visão geral e motivação (gargalo de disco não medido → métricas por nó); modelo (storage nodes, líder = coordenador, cliente membro inelegível, sem réplica — consequências explícitas de queda de nó); catálogo (dois mapas persistentes, `SeriesPlacement`/`StorageNodeStatus`, consistência eventual no cliente e forte no líder); protocolo (tabela de comandos e statuses, `NOT_OPEN`, `WRONG_OWNER`, `MIGRATING`); cliente (`NgrrdCluster.connect`, mesmo `NgrrdHandle`, buffer por nó, BLOCK/FAIL, orçamento de close, retentativas); storage node (config YAML completa com defaults, `NgrrdStorageNodeMain`, registry de handles, auto-cura, reconciliação e adoção de volume single-node como caminho de migração); placement (regra total: candidatos, preferido, carga efetiva, fillRatio, nodeId; frescor e fallback); rebalanceamento e migração (máquina de estados, chunks, SHA-256, resolução por novo líder, limites); drenagem e manutenção (drain/activate/status, movimentação de nó com mesmo id); métricas (`NodeMetricsSnapshot`, `ClientMetricsSnapshot`, marker `NGRRD_NODE_STATUS`, `NGRRD_REBALANCE`, `NGRRD_RECONCILE`); CLI admin; testes (profile `ngrrd-cluster`, por que fica fora do CI hospedado, como rodar); limites conhecidos (JSON+Base64 nos chunks, churn de bootstrap do NGrid, perda de amostras no close com orçamento esgotado) e trabalhos futuros (os três chips de core: ressincronização de mapa não persistente, higiene do TcpTransport, NMapPersistence falhar alto).

## 2. Diagramas (`doc/oss/diagrams/`)
- `ngrrd_cluster_c4_container.puml` — C4 container: consumer (cliente), storage nodes, catálogo replicado, volume blob por nó.
- `ngrrd_cluster_sequence_write.puml` — open → place (líder) → open (dono) → writeBatch/checkpoint → read, incluindo WRONG_OWNER/NOT_OPEN.
- `ngrrd_cluster_sequence_migration.puml` — coordenador, origem, destino, catálogo: start, chunks, commit, flip, finish; ramos abort e resolução por novo líder.
- `ngrrd_cluster_state_migration.puml` — estados de `SeriesPlacement`/`MigratePhase`.
Embutir os quatro em `ngrrd-cluster.md`.

## 3. Outros documentos
- `README.md` (raiz): seção do módulo `nishi-utils-ngrrd-cluster` (o que é, coordenada Maven, link para o doc).
- `doc/oss/ngrrd.md`: parágrafo curto na introdução apontando o modo distribuído.
- `CLAUDE.md` (raiz): "Repository Structure" com o módulo novo; "Build & Test Commands" com `mvn -pl nishi-utils-ngrrd-cluster verify` e `-Pngrrd-cluster`; nota em "Testing Conventions" sobre `*ClusterTest` e os markers `NGRRD_NODE_STATUS`/`NGRRD_STORAGE_NODE_STARTED`.
- `doc/testes-vermelhos-conhecidos.md`: registrar a sensibilidade à carga dos `*ClusterTest` do módulo (não são vermelhos conhecidos, mas ficam fora do CI hospedado por desenho, como a suíte ngrid).
- Publicação: `.github/workflows/publish.yml` lista módulos explicitamente — `mvn ... -pl nishi-utils-core,nishi-utils-oss -am deploy` (linha ~42) e os assets da release (linhas ~52-57: jar, sources, javadoc de core e oss). Inclua `nishi-utils-ngrrd-cluster` nos dois lugares (mesmos três artefatos). Confira também `pr-validation.yml` (`-DexcludeNgrid=true` na raiz roda `mvn test`; o módulo novo roda seus unitários lá — os `*ClusterTest` ficam fora por padrão).

## 4. Versão 8.3.0 e diário de bordo
- Bump `8.2.0` → `8.3.0` em TODOS os poms do reactor (raiz, core, oss, ngrrd-cluster, ngrid-test) e em qualquer referência de versão em docs que precise (grep por `8.2.0`).
- `doc/CHANGELOG.md` ("Diário de Bordo"): entrada datada no topo, `## <data> — 🟢 Feature: ngrrd cluster (armazenamento distribuído) — release 8.3.0`, no mesmo tom das entradas existentes: motivação, o que entrou (por marco), decisões (sem réplica, líder eleito, cliente membro), defeitos encontrados no caminho (core: role inelegível, mapa persistente no builder; módulo: os bloqueantes que os Refuters pegaram, em uma linha cada), trade-offs e limites, como operar.
- Depois do bump: `mvn clean install -DskipTests` na raiz; `mvn -pl nishi-utils-ngrrd-cluster verify`; `mvn -pl nishi-utils-oss test -Dtest=NgrrdBlobFacadeTest`; `mvn verify -Pvalidate-javadoc -pl nishi-utils-ngrrd-cluster` (corrija Javadoc faltante nos tipos públicos do módulo se o profile reclamar).

## 5. Relatório
Arquivos criados/alterados; diagramas renderizados (evidência do curl com HTTP 200 e content-type de imagem); comandos e resultados; NÃO VERIFICADO.
