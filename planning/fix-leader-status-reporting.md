# Corrigir Reeleição Graceful de Líder — Testes Docker NGrid

## Contexto

Os testes Docker de integração (`NGridMapReelectionIT`, `NGridMapLeaderCrashIT`) falham com timeout de 30s ao esperar por `countLeaders() == 1` após parar/matar o líder.

## Causa Raiz Identificada

**O problema NÃO é no `ClusterCoordinator` — a eleição funciona corretamente.**

O problema é que os nós de teste **não reportam status de liderança continuamente nos logs**, e o mecanismo de detecção (`NGridMapNodeContainer.isLeader()`) depende inteiramente do parsing de logs.

### Evidência

- `Main.startServer()` (role `server`) → imprime `CURRENT_LEADER_STATUS:true/false` a cada 2s ✅
- `Main.startMapStress()` (role `map-stress`) → imprime `Is Leader:false` apenas **UMA VEZ** no startup ❌
- `Main.startMapReader()` (role `map-reader`) → imprime `Is Leader:false` apenas **UMA VEZ** no startup ❌

O `NGridMapNodeContainer.isLeader()` parseia os logs buscando a **última ocorrência** de `CURRENT_LEADER_STATUS:` ou `Is Leader:`. Quando o `seed-1` (role `server`) morre e o `node-5` (role `map-reader`) é eleito como novo líder pelo `ClusterCoordinator`, o `node-5` **nunca imprime que virou líder** → o teste nunca detecta.

## Proposed Changes

### Entrypoint Docker (`Main.java`)

#### [MODIFY] [Main.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/main/java/dev/nishisan/utils/test/Main.java)

Adicionar reporting periódico de `CURRENT_LEADER_STATUS:` em **todos** os roles (`map-stress` e `map-reader`), não apenas no `server`.

**Em `startMapStress()`** (a partir da linha 378): Adicionar log `CURRENT_LEADER_STATUS:` a cada iteração do loop principal, usando `node.coordinator().isLeader()`.

**Em `startMapReader()`** (a partir da linha 422): Adicionar log `CURRENT_LEADER_STATUS:` periodicamente no loop de leitura (a cada N iterações para não poluir logs).

---

### Correção Secundária: `NGridNode.startServices` — `minClusterSize`

#### [MODIFY] [NGridNode.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java)

O `minClusterSize` é calculado como `Math.max(1, Math.min(config.replicationQuorum(), config.peers().size() + 1))`.

No cenário Docker com 5 nós (cada um conhece apenas o seed como peer), isso resulta em `Math.min(3, 2)` = **2**. Quando o `seed-1` morre e cada nó sobrevivente tem o coordinator com membros descobertos via mesh gossip (5 nós), o `minClusterSize=2` é conservador mas funcional. Nenhuma alteração necessária aqui — o coordinator funciona corretamente.

> [!NOTE]
> O cálculo de `minClusterSize` está correto para o cenário em questão. Os membros são descobertos via mesh gossip do transport, e o coordinator acumula todos os membros vistos. Com 4 nós ativos após a queda do seed, `activeCount=4 >= minClusterSize=2` → eleição prossegue normalmente.

## Verification Plan

### Automated Tests

1. **Testes unitários existentes (core):**
   ```bash
   cd /home/lucas/Projects/nishisan/nishi-utils
   mvn test -pl nishi-utils-core
   ```
   Estes devem continuar passando sem regressão.

2. **Rebuild Docker + testes de integração Docker:**
   ```bash
   cd /home/lucas/Projects/nishisan/nishi-utils
   DOCKER_BUILDKIT=0 docker build -t ngrid-test:latest ngrid-test/
   mvn verify -pl ngrid-test -Dngrid.test.docker=true
   ```
   Os testes `shouldMaintainOperationsDuringGracefulReelection` e `shouldPreventSplitBrainWrites` devem passar.
