# nishi-utils-oss

Módulo independente que implementa o formato **ngrrd** (Nishi Grid Round-Robin
Database) — séries temporais com paridade conceitual ao RRDtool, persistência
plugável (disco local ou S3-compatível) e definição declarativa em YAML.

## Quickstart

```xml
<dependency>
  <groupId>dev.nishisan</groupId>
  <artifactId>nishi-utils-oss</artifactId>
  <version>3.7.1</version>
</dependency>
```

```java
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.storage.StorageFactory;

try (NgrrdHandle handle = Ngrrd.fromYaml(
        Path.of("series.yaml"),
        StorageFactory.StorageBindings.forLocalDisk(Path.of("/var/ngrrd")),
        Map.of("deviceId", "r1", "interfaceId", "eth0"))) {
    handle.write("in_octets", new Sample(System.currentTimeMillis(), 12345L));
    var daily = handle.read("daily").get("in_bps");
}
```

Documentação completa em [`doc/oss/ngrrd.md`](../doc/oss/ngrrd.md).

## Persistência incremental (rrdtool-like)

Por padrão (`writePolicy.persistenceMode: blockRollover`) a janela do bloco vive só em
memória e é materializada no rollover (ao cruzar `blockSizeSec`) ou em `flush()`. O modo
**`incremental`** dá semântica estilo rrdtool:

```yaml
spec:
  storage:
    objectNaming:
      statePrefix: "state"          # onde o estado durável é gravado
    writePolicy:
      persistenceMode: incremental
      persistLastValue: true
```

- o estado da série (último valor por DS raw + acumuladores da janela) é persistido em
  `state/<seriesKey>/writer.state` e **reidratado no `open`** — o handle fica stateless na
  reabertura (o counter sobrevive a eviction/restart);
- `handle.checkpoint()` materializa o bloco aberto como **parcial** (`FLAG_PARTIAL`) sem
  fechar a janela, deixando o dado legível antes do rollover (sem perda por overwrite);
- ideal para consumidores com working set grande e cache de handles pequeno (ex.: Kafka
  consumer de métricas), em que `flush()` por bloco reiniciaria a derivação de counter.

## Testes

```bash
mvn -pl nishi-utils-oss verify                       # unitário + JaCoCo
mvn -pl nishi-utils-oss verify -Pngrrd-integration   # inclui S3 IT (LocalStack)
```

## Licença

GNU GPL v3 (or later).
