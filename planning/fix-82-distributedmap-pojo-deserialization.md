# Fix #82 — DistributedMap POJO Deserialization (ClassCastException → LinkedHashMap)

## Diagnóstico Técnico

### Cadeia de serialização completa

```
map.put("key", new TunnelRegistryEntry()) — líder
  → MapReplicationCommand.put(key, value)
      ↓ field: Object key @JsonTypeInfo(CLASS)
      ↓ field: Object value @JsonTypeInfo(CLASS)
  → ReplicationPayload(data = MapReplicationCommand)
      ↓ field: Object data @JsonTypeInfo(CLASS)  ← PONTO CRÍTICO
  → ClusterMessage(payload = ReplicationPayload)
      ↓ field: Object payload @JsonTypeInfo(CLASS) ← funciona para ReplicationPayload
  → JacksonMessageCodec.encode() → bytes → rede → decode()
```

### Onde o tipo é perdido

O `@JsonTypeInfo(use = CLASS)` em um campo `Object` funciona **apenas um nível** de profundidade. No `ReplicationPayload.data`, o Jackson inclui o `@class` do `MapReplicationCommand` — isso funciona. Mas dentro do `MapReplicationCommand`, os campos `key` e `value` **também** são `Object`, e o Jackson precisa fazer uma segunda resolução de tipo polimórfico em nested objects.

O problema é que **o mapper do `JacksonMessageCodec` não tem `activateDefaultTyping`** habilitado. O `@JsonTypeInfo` em campos individuais funciona ao serializar, pois o Jackson inclui o `@class` no JSON. Mas ao **deserializar** o `MapReplicationCommand` (que chegou como payload interno), o campo `value` é um POJO sem anotações Jackson próprias. O Jackson, sem typing global, fallback para `LinkedHashMap`.

**Reprodução do JSON gerado:**
```json
{
  "@class": "...MapReplicationCommand",
  "type": "PUT",
  "key": { "@class": "java.lang.String", "..." },
  "value": {
    "@class": "dev.nishisan.ishin.gateway.tunnel.TunnelRegistryEntry",
    "field1": "...",
    "field2": "..."
  }
}
```

O `@class` **está presente** no JSON de saída. O problema ocorre na **desserialização**: o Jackson, ao reconstruir `MapReplicationCommand` via `@JsonCreator`, recebe o campo `value` como um `JsonNode` e tenta instanciar com o `@class` hint — mas **sem `MAPPERS` configurados ou `defaultTyping` ativo**, o mecanismo de `@JsonTypeInfo` em campos `Object` requer que o `ObjectMapper` que processa o `MapReplicationCommand` também tenha `visibilidade ALL nos campos` E que o tipo alvo seja **conhecido/visível** no classpath.

A causa real é mais sutil: o `MapReplicationCommand` é deserializado **antes** de ser entregue ao `MapClusterService.apply()`. Ele é deserializado como parte do `ClusterMessage → ReplicationPayload.data`. Nesse ponto, o `ObjectMapper` do `JacksonMessageCodec` precisa instanciar o `TunnelRegistryEntry` — e **consegue** (o `@class` está lá). O problema ocorre no caso de **Sync (snapshot)**, onde o payload viaja como `SyncResponsePayload.data` que é um `Map<K,V>`. Nesse fluxo, não há `@JsonTypeInfo`!

### Dois fluxos afetados

| Fluxo | Classe | Tem @JsonTypeInfo? | Resultado |
|-------|--------|---------------------|-----------|
| PUT replication (líder → follower) | `ReplicationPayload.data` → `MapReplicationCommand.value` | ✅ Sim (aninhado 2 níveis) | Funciona se o mapper tiver field visibility |
| Sync/Snapshot (follower lag recovery) | `SyncResponsePayload.data` → `Map<K,V>` diretamente | ❌ Não | **LinkedHashMap** garantido |

### Confirmação do fluxo de snapshot

```java
// MapClusterService.getSnapshotChunk()
Map<K, V> chunk = new HashMap<>(); // valores são TunnelRegistryEntry
return new SnapshotChunk(chunk, ...); // entregue como SyncResponsePayload.data
```

O `SyncResponsePayload` **tem** `@JsonTypeInfo(CLASS)` no campo `data`. O Jackson, ao serializar, inclui:
```json
{
  "data": {
    "@class": "java.util.HashMap",
    "key1": { ... }
  }
}
```

O `@class` captura o **container** (`HashMap`), não os **valores** dentro dele. O Jackson infere os valores como `LinkedHashMap` porque:
1. O tipo genérico `V` não está disponível em runtime (type erasure)
2. O `ObjectMapper` do `JacksonMessageCodec` não tem `activateDefaultTyping` — sem ele, os valores do Map não recebem `@class` próprio

Portanto o `data` chega ao `installSnapshot()` como `Map<String, LinkedHashMap>` em vez de `Map<String, TunnelRegistryEntry>`.

---

## Solução Escolhida

**Opção híbrida entre 1 e 4 da issue**: Usar `activateDefaultTyping` com `NON_FINAL` em um `ObjectMapper` **dedicado ao pipeline de replicação**, sem impactar o codec de transporte geral. 

Isso garante:
- Sem breaking changes para outros payloads do `ClusterMessage`
- Fidelidade de tipo para qualquer POJO dentro do `MapReplicationCommand` e do snapshot
- Sem necessidade de anotar POJOs do consumer (zero impacto no ishin-gateway)

### Estratégia

1. **`MapReplicationCodec`** — novo helper que usa um `ObjectMapper` com `defaultTyping` apenas para serializar/deserializar `MapReplicationCommand` de/para `byte[]`.
2. **`MapClusterService`** passa a armazenar o command já serializado como `byte[]` no `ReplicationPayload`, em vez do objeto em memória.
3. Ao `apply()`, o `MapClusterService` deserializa o `byte[]` de volta com o `MapReplicationCodec`.
4. No snapshot (sync), o `MapClusterService` também usa `MapReplicationCodec` para serializar/deserializar o `Map<K,V>`.

> **Alternativa mais simples sem breaking change na estrutura:** Adicionar `@JsonTypeInfo` no nível de classe do `SyncResponsePayload.data` e habilitar `activateDefaultTyping` global.

### Decisão Final — Abordagem Cirúrgica

A abordagem mais limpa e com menor risco é:

**Serializar `MapReplicationCommand` para `byte[]` no ponto de criação, e transportar esses bytes opacos dentro do `ReplicationPayload.data`.**

Isso resolve:
- Replication flow: o `byte[]` viaja sem perda de tipo
- Sync flow: o snapshot também serializa `Map<K,V>` para `byte[]` com o mapper tipado

E mantém o `JacksonMessageCodec` sem alteração.

---

## Proposed Changes

### `MapReplicationCodec` [NEW]

#### [NEW] `MapReplicationCodec.java`
**Path:** `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/map/MapReplicationCodec.java`

Novo utilitário estático com `ObjectMapper` próprio configurado com:
```java
mapper.activateDefaultTyping(
    LaissezFaireSubTypeValidator.instance,
    ObjectMapper.DefaultTyping.NON_FINAL,
    JsonTypeInfo.As.PROPERTY
);
```

Expõe:
- `byte[] encode(MapReplicationCommand cmd)`
- `MapReplicationCommand decode(byte[] bytes)`
- `byte[] encodeSnapshot(Map<?,?> map)`
- `Map<?,?> decodeSnapshot(byte[] bytes)`

---

### `MapClusterService` [MODIFY]

#### [MODIFY] [MapClusterService.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/map/MapClusterService.java)

- `put()` e `remove()`: serializar o `MapReplicationCommand` para `byte[]` via `MapReplicationCodec` antes de chamar `replicationManager.replicate(topic, bytes)`
- `apply()`: receber `byte[]` e deserializar via `MapReplicationCodec` antes do switch
- `getSnapshotChunk()`: serializar o `Map<K,V>` chunk via `MapReplicationCodec.encodeSnapshot()`
- `installSnapshot()`: receber `byte[]` e deserializar via `MapReplicationCodec.decodeSnapshot()`

---

### `SyncResponsePayload` [VERIFY]

#### [VERIFY] [SyncResponsePayload.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/SyncResponsePayload.java)

Verificar se o campo `data` já tem `@JsonTypeInfo`. Se for `byte[]`, o Jackson serializa como Base64 e desserializa corretamente — **nenhuma alteração necessária**.

---

### Testes [NEW/MODIFY]

#### [NEW] `DistributedMapPojoReplicationTest.java`
**Path:** `nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/map/DistributedMapPojoReplicationTest.java`

Teste de integração end-to-end com cluster de 3 nós:
- Define `TunnelEntryStub` (POJO sem anotações Jackson)
- Líder faz `put("k", new TunnelEntryStub(...))`
- Aguarda replicação
- Follower chama `get("k")` e afirma que o resultado é instância de `TunnelEntryStub` (não `LinkedHashMap`)
- Testa também o caminho de sync: mata o follower, reinicia, aguarda o sync e valida o tipo

#### [MODIFY] `MapReplicationCommandTest.java` (se existir) ou novo unit test para `MapReplicationCodec`

---

## Verification Plan

### Automated Tests
```bash
mvn -pl nishi-utils-core test -Dtest=DistributedMapPojoReplicationTest -Dsurefire.useFile=false
mvn -pl nishi-utils-core test -Dtest=DistributedMapApiTest -Dsurefire.useFile=false
mvn -pl nishi-utils-core test -Dsurefire.useFile=false
```

### Manual Verification
- Rebuild `nishi-utils-core` e publicar snapshot localmente (`mvn clean install`)
- Atualizar dependência no `ishin-gateway` e testar o ambiente Docker com `docker compose up`
- Validar que o `tunnel-1` consegue ler as entradas publicadas pelos proxies sem `ClassCastException`

---

## Open Questions

> [!IMPORTANT]
> **A abordagem de `byte[]` pode ser um breaking change de serialização** se existirem nodes em versões diferentes no mesmo cluster durante um rolling upgrade. Confirmar se o ambiente de deploy suporta downtime ou se é necessário uma estratégia de migração de protocolo.

> [!NOTE]
> A versão atual do pom.xml é `3.6.0`, não `3.6.1`. O fix deverá ser lançado como `3.6.1` (patch), pois não há breaking changes na API pública do `DistributedMap`.
