# NGrid Map – Testes Docker de Simulação Distribuída

## Objetivo

Validar o comportamento do `DistributedMap` sob cenários de mundo real utilizando **Testcontainers**. O cluster de **5 nós** Docker reproduz falhas catastróficas, transições de liderança e carga sustentada de leitura e escrita concorrentes.

> **Baseline TDD** — Estes testes são independentes da maturidade atual do NMap. É esperado que alguns falhem inicialmente; conforme a implementação evolui, os testes passam progressivamente.

---

## Topologia do Cluster

```text
┌───────────────────────────────────────────────────────────────┐
│                      Docker Network (bridge)                  │
│                                                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐              │
│  │  seed-1    │  │  node-2    │  │  node-3    │              │
│  │  server    │  │ map-stress │  │ map-reader │              │
│  │ SEED+COORD │  │  Producer  │  │  Consumer  │              │
│  └────────────┘  └────────────┘  └────────────┘              │
│                                                               │
│  ┌────────────┐  ┌────────────┐                               │
│  │  node-4    │  │  node-5    │                               │
│  │  server    │  │ map-reader │                               │
│  │  (réplica) │  │  Consumer  │                               │
│  └────────────┘  └────────────┘                               │
│                                                               │
│  Writes  → roteados via invokeLeader()                        │
│  Reads   → STRONG: roteado ao leader                          │
│            EVENTUAL: lê réplica local                         │
└───────────────────────────────────────────────────────────────┘
```

### Papéis

| Modo          | Papel              | Descrição                                                                                        |
|---------------|--------------------|-------------------------------------------------------------------------------------------------|
| `server`      | Nó genérico        | Armazena réplicas. O `seed-1` acumula o papel de Seed + Coordinator.                            |
| `map-stress`  | **Producer**       | Executa `map.put()` contínuo (~50-100 ops/s, configurável via env `MAP_RATE_PER_SEC`). Loga cada operação com padrão `MAP-PUT:epoch-index=key:value`. Trata falhas com retry sem morrer.                                     |
| `map-reader`  | **Consumer/Reader**| Lê ciclicamente alternando `STRONG` e `EVENTUAL`. Executa `keySet()` periodicamente. Loga resultados (`MAP-READ-STRONG`, `MAP-READ-EVENTUAL`, `MAP-KEYSET`, `MAP-STALE`). Continua operando durante failovers.                  |

---

## Cenários de Teste

### 1. `NGridMapLeaderCrashIT` — Queda de Líder

#### `shouldMaintainMapConsistencyDuringLeaderCrash`
Simula queda abrupta (**SIGKILL**) do líder durante produção ativa.

| Etapa | Ação |
|-------|------|
| 1     | Producer injeta ~50 puts/s, readers fazem gets contínuos |
| 2     | Após 3s de estabilidade → **SIGKILL no líder** |
| 3     | Cluster deve eleger novo líder em < 15s |

**Validações:**
- Novo líder eleito sem split-brain (exatamente 1 líder)
- Producer retoma produção após reeleição
- Readers STRONG retomam leituras sem hang permanente
- Dados pré-crash acessíveis no novo líder

#### `shouldSurviveDoubleCrash`
Derruba **2 nós** (incluindo o líder), forçando o cluster ao limite do quórum (3/5).

**Validações:**
- Cluster mantém operações com quórum mínimo
- Producer sobrevive à dupla queda
- Reads EVENTUAL continuam via réplica local

---

### 2. `NGridMapReelectionIT` — Reeleição e Split-Brain

#### `shouldMaintainOperationsDuringGracefulReelection`
Simula reeleição limpa via **SIGTERM** (graceful stop) sob carga de 100 puts/s.

**Validações:**
- Nova liderança em < 10s
- Perda de puts durante transição < 20%
- Reads EVENTUAL **nunca** falham (réplica local)
- Após estabilização, reads STRONG retornam 100%

#### `shouldPreventSplitBrainWrites`
Isola o líder desligando 3 seguidores, criando partição de rede.

**Validações:**
- Líder isolado **rejeita puts** (lease expirado, sem quórum)
- Reader no líder isolado falha em gets STRONG
- Quórum restante elege novo líder funcional

---

### 3. `NGridMapHighThroughputIT` — Volume e Recuperação

#### `shouldAchieveSustainedThroughputAndConsistency`
Producer injeta ~1.000 chaves (100 ops/s × 10s) com 2 readers simultâneos.

**Validações:**
- Throughput sustentado ≥ 40 puts/s
- Readers veem **todas** as keys (convergência eventual)
- `keySet().size()` converge para 1.000 em todos os nós
- Zero stale reads (valor anterior divergente)

#### `shouldCatchupAfterNodeCrashAndRecovery`
Crash e restart limpo de um nó no meio de 1.000 puts contínuos.

| Etapa | Ação |
|-------|------|
| 1     | Producer injeta 50 puts/s continuamente |
| 2     | Após 500 puts → **crash de 1 nó** (não producer/reader) |
| 3     | Após 1.000 puts → **restart do nó crashado** |

**Validações:**
- Nó reiniciado faz catch-up via snapshot/log sync
- Reader no nó reiniciado retorna dados corretos
- Dados convergem em todo o cluster

---

## Como Executar

### Pré-requisitos

- **Docker Engine** rodando (≥ API 1.40)
- **Testcontainers** ≥ 2.0.3 (já configurado no `pom.xml`)
- Projeto multi-module: o `nishi-utils-core` é compilado automaticamente pelo reactor Maven

### Flag obrigatória

Todos os testes exigem `-Dngrid.test.docker=true`. Sem ela, os testes são **ignorados automaticamente** (`@EnabledIfSystemProperty`).

### Comandos

**Suite completa da raiz do projeto (recomendado):**
```bash
cd nishi-utils
TESTCONTAINERS_RYUK_DISABLED=true \
  mvn verify -pl ngrid-test -Dngrid.test.docker=true \
  -Dit.test=NGridMap*IT -DfailIfNoTests=false
```

> O `-pl ngrid-test` compila apenas o módulo de teste. O reactor Maven resolve a dependência do `nishi-utils-core` automaticamente.

**De dentro do `ngrid-test/` (requer `mvn install` prévio na raiz):**
```bash
cd nishi-utils/ngrid-test
TESTCONTAINERS_RYUK_DISABLED=true \
  mvn verify -Dngrid.test.docker=true \
  -Dit.test=NGridMap*IT -DfailIfNoTests=false
```

**Teste específico:**
```bash
TESTCONTAINERS_RYUK_DISABLED=true \
  mvn verify -pl ngrid-test \
  -Dtest=NGridMapLeaderCrashIT \
  -Dngrid.test.docker=true
```

**Apenas build da imagem (sem executar testes):**
```bash
mvn package -DskipTests
```

> **Nota:** A env `TESTCONTAINERS_RYUK_DISABLED=true` é recomendada em ambientes com Docker rootless ou certas distribuições Linux para evitar timeouts do Ryuk container cleaner.

---

## Estrutura do Projeto (Multi-Module)

```
nishi-utils/                      ← parent (packaging: pom)
├── pom.xml                       ← nishi-utils-parent
├── nishi-utils-core/             ← código da biblioteca (jar)
│   ├── pom.xml
│   └── src/
└── ngrid-test/                   ← testes Docker (jar)
    ├── pom.xml                   ← depende de nishi-utils-core
    ├── Dockerfile
    ├── config/
    └── src/
```

## Arquivos Relacionados

| Arquivo | Descrição |
|---------|-----------|
| `ngrid-test/src/main/java/.../Main.java` | Ponto de entrada da app Docker (modos `server`, `map-stress`, `map-reader`) |
| `ngrid-test/config/map-stress-config.yml` | Config do producer de alta carga |
| `ngrid-test/config/map-reader-config.yml` | Config do consumer/reader |
| `ngrid-test/src/test/.../NGridMapNodeContainer.java` | Wrapper Testcontainers para nós NGrid |
| `ngrid-test/src/test/.../AbstractNGridMapClusterIT.java` | Classe base do cluster de 5 nós |
| `ngrid-test/src/test/.../NGridMapLeaderCrashIT.java` | Testes de queda de líder |
| `ngrid-test/src/test/.../NGridMapReelectionIT.java` | Testes de reeleição e split-brain |
| `ngrid-test/src/test/.../NGridMapHighThroughputIT.java` | Testes de throughput e recuperação |
