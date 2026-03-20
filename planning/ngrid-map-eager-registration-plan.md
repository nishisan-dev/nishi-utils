# Fix NGrid Map: Eager Registration + Resposta Defensiva + Config

Corrigir 3 problemas que causam deadlock funcional nos testes Docker do `DistributedMap`: (1) mapas configurados no YAML não são instanciados eagerly no `NGridNode`, (2) `DistributedMap.onMessage()` ignora silenciosamente requests para mapas desconhecidos, (3) `server-config.yml` não declara `stress-map`.

## Proposed Changes

### NGridConfig — Propagação da lista de mapas

O `NGridConfig` hoje propaga a lista de filas via `queues()`, mas não tem nenhum campo análogo para mapas. Precisamos adicionar um `MapConfig` e a lista correspondente.

#### [NEW] [MapConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/MapConfig.java)

Record ou classe imutável para configuração de mapa no domain model, análogo a `QueueConfig`. Campos: `name`, `persistenceMode`.

#### [MODIFY] [NGridConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridConfig.java)

- Adicionar campo `List<MapConfig> maps` (imutável)
- Adicionar accessor `maps()`
- Adicionar `addMap(MapConfig)` no Builder
- Manter backward compat: se nenhum mapa for configurado, lista vazia

---

### NGridConfigLoader — Converter mapas do YAML para domain

#### [MODIFY] [NGridConfigLoader.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/config/NGridConfigLoader.java)

No `convertToDomain()`, iterar sobre `yamlConfig.getMaps()` e chamar `builder.addMap(...)` para cada `MapPolicyConfig`, convertendo `MapPolicyConfig` → `MapConfig`.

---

### NGridNode — Eager registration de mapas + persistence per-map

#### [MODIFY] [NGridNode.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java)

No `startServices()`, após a criação do mapa padrão (linha 386), adicionar loop de eager registration análogo ao de filas:

```java
if (config.maps() != null && !config.maps().isEmpty()) {
    for (MapConfig mapConfig : config.maps()) {
        maps.computeIfAbsent(mapConfig.name(), n -> createDistributedMap(n, mapConfig));
    }
}
```

Também adaptar `createDistributedMap` para receber opcionalmente um `MapConfig` com o persistence mode específico, em vez de usar sempre o global `config.mapPersistenceMode()`.

---

### DistributedMap — Resposta defensiva no `onMessage()`

#### [MODIFY] [DistributedMap.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/DistributedMap.java)

> [!IMPORTANT]
> Hoje, quando um `CLIENT_REQUEST` chega para um mapa que o nó não instanciou, **nenhum listener reconhece o comando** (o `return` silencioso na linha 406). O `CompletableFuture` no follower fica pendurado indefinidamente.

**Solução**: Não alterar o `DistributedMap.onMessage()` em si (já que cada instância só ouve os seus comandos), mas adicionar um handler genérico no `NGridNode` que detecte `CLIENT_REQUEST` sem resposta e retorne erro explícito.

Na verdade, a abordagem mais simples e correta: o **problema é resolvido pelo Fix 1** (eager registration). Se todos os mapas configurados são instanciados eagerly, cada `DistributedMap` que roda como listener no líder reconhecerá os comandos do seu mapa. Ainda assim, como proteção defensiva, adicionaremos no `NGridNode` um `TransportListener` catch-all para `CLIENT_REQUEST` de mapas não reconhecidos.

#### [MODIFY] [NGridNode.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java)

Adicionar um `TransportListener` defensivo que intercepta `CLIENT_REQUEST` com qualifier `map.put:*`, `map.remove:*` ou `map.get:*` que não tenha um `DistributedMap` correspondente registrado, e responde com `ClientResponsePayload(success=false, error="Map not found: <name>")`.

---

### Configuração do `stress-map` no server

#### [MODIFY] [server-config.yml](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/config/server-config.yml)

Adicionar `stress-map` na seção `maps` para que o servidor (quando eleito líder) tenha o mapa registrado.

---

## Verification Plan

### Automated Tests

1. Build completo do multi-modulo:
   ```bash
   cd /home/lucas/Projects/nishisan/nishi-utils && mvn clean install -DskipTests
   ```

2. Testes unitários do `nishi-utils-core`:
   ```bash
   cd /home/lucas/Projects/nishisan/nishi-utils && mvn test -pl nishi-utils-core
   ```
   Inclui [DistributedMapApiTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/map/DistributedMapApiTest.java), [MapClusterServiceConcurrencyTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/map/MapClusterServiceConcurrencyTest.java), e [NGridConfigLoaderTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/config/NGridConfigLoaderTest.java).

### Manual Verification

Os testes Docker (`NGridMapLeaderCrashIT`, `NGridMapReelectionIT`, `NGridMapHighThroughputIT`) requerem ambiente Docker privilegiado para rodar. A verificação manual desses testes fica a cargo do usuário rodar localmente com:
```bash
DOCKER_BUILDKIT=0 docker build -t ngrid-test:latest ngrid-test/
mvn verify -pl ngrid-test -Dngrid.test.docker=true
```
