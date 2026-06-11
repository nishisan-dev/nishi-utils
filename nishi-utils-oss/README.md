# nishi-utils-oss

Módulo independente que implementa o formato **ngrrd** (Nishi Grid Round-Robin
Database) — séries temporais com paridade conceitual ao RRDtool, persistência
plugável (disco local ou S3-compatível) e definição declarativa em YAML.

## Quickstart

```xml
<dependency>
  <groupId>dev.nishisan</groupId>
  <artifactId>nishi-utils-oss</artifactId>
  <version>6.0.0</version>
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
    handle.checkpoint();
    var daily = handle.read("daily").get("in_bps");
}
```

Documentação completa em [`doc/oss/ngrrd.md`](../doc/oss/ngrrd.md).

## Objeto único (paridade com o RRDtool)

A partir da 6.0.0 cada série é um **único objeto binário** (`<seriesPrefix>/<seriesKey>.ngrr`),
de tamanho fixo e pré-alocado, com ring buffers atualizados in-place — em paridade com o
arquivo `.rrd`. Não há mais blocos por `(rra,ds,cf)` nem manifesto YAML (resolve a explosão
de arquivos em disco local da [#144](https://github.com/nishisan-dev/nishi-utils/issues/144)).

A persistência é sempre **incremental (rrdtool-like)**:

- o estado vivo (último valor por DS raw + acumuladores + ponteiros do ring) mora no próprio
  objeto e é **reidratado no `open`** — o handle fica stateless na reabertura (o counter
  sobrevive a eviction/restart);
- `handle.checkpoint()` (ou `flush()`) materializa o CDP em progresso como **parcial** e
  torna o objeto durável (`fsync` no disco / PUT no S3), deixando o dado legível antes do
  passo do RRA fechar;
- a retenção é o ring buffer de cada RRA (`rows × stepSec`): o CDP mais antigo é sobrescrito
  in-place, sem deletar arquivos.

Em disco o objeto é gravado por região via `FileChannel` (só o que mudou); em S3 o objeto
inteiro é regravado por PUT no `checkpoint()`/`close()`. **Invariante:** um writer por série.

## Testes

```bash
mvn -pl nishi-utils-oss verify                       # unitário + JaCoCo
mvn -pl nishi-utils-oss verify -Pngrrd-integration   # inclui S3 IT (LocalStack)
```

## Licença

GNU GPL v3 (or later).
