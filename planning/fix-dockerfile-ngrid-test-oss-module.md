# Corrigir build do `ngrid-test/Dockerfile` quebrado pela introdução do módulo `nishi-utils-oss`

## Contexto

O pipeline de PR está falhando no job `docker-resilience-gate` (workflow `.github/workflows/resilience.yml:93`) ao executar `docker build -t ngrid-test:latest -f ngrid-test/Dockerfile .`. O erro:

```
The project dev.nishisan:nishi-utils-parent:3.7.1 (/workspace/pom.xml) has 1 error
Child module /workspace/nishi-utils-oss of /workspace/pom.xml does not exist
```

**Causa-raiz:** o aggregator `pom.xml` declara três módulos (`nishi-utils-core`, `nishi-utils-oss`, `ngrid-test` — `pom.xml:11-15`), mas o `ngrid-test/Dockerfile` só copia dois deles (`Dockerfile:4-6`). Quando o Maven lê o aggregator pom, valida a existência de todos os diretórios listados em `<modules>` e aborta porque `nishi-utils-oss/` está ausente do build context.

O Dockerfile foi escrito antes do scaffolding do oss (commit `a9acc0b`) e não foi atualizado. O `ngrid-test` em si **não depende** de `nishi-utils-oss` (somente de `nishi-utils-core`, conforme `ngrid-test/pom.xml:20`), portanto o build do oss dentro da imagem é desnecessário.

**Objetivo:** desbloquear o pipeline corrigindo de forma definitiva, sem deixar o débito antigo para trás.

## Arquivo a modificar

- `/home/lucas/Projects/nishisan/nishi-utils/ngrid-test/Dockerfile`

## Mudança

Alterar o estágio `build` do Dockerfile para:

1. Copiar também o diretório `nishi-utils-oss` (necessário para que o Maven consiga **ler** o aggregator pom, mesmo quando o build do oss for omitido).
2. Restringir o `install` aos módulos efetivamente necessários para o `ngrid-test` usando `-pl nishi-utils-core -am`, espelhando o que o próprio CI já faz no step `Install parent and core modules` (`resilience.yml:96`). Isso evita compilar o oss (que arrasta AWS SDK + LocalStack) dentro do build da imagem, mantendo a build do container enxuta.

### Diff final esperado

```dockerfile
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace

COPY pom.xml /workspace/pom.xml
COPY nishi-utils-core /workspace/nishi-utils-core
COPY nishi-utils-oss /workspace/nishi-utils-oss
COPY ngrid-test /workspace/ngrid-test

RUN mvn -pl nishi-utils-core -am -DskipTests -Dmaven.javadoc.skip=true -Dskip.docker.build=true install
RUN mvn -f ngrid-test/pom.xml -DskipTests -Dskip.docker.build=true package

FROM eclipse-temurin:25-jre
WORKDIR /app

COPY --from=build /workspace/ngrid-test/target/ngrid-test-jar-with-dependencies.jar /app/ngrid-test.jar
COPY --from=build /workspace/ngrid-test/config /app/config

ENTRYPOINT ["java", "-jar", "/app/ngrid-test.jar"]
```

Pontos-chave:
- A linha `COPY nishi-utils-oss …` é **obrigatória**: ainda que `-pl` restrinja o build set, o Maven valida o módulo no momento de ler o aggregator pom.
- `-pl nishi-utils-core -am` instala apenas o parent (implícito) + `nishi-utils-core` no repo local. O segundo `mvn -f ngrid-test/pom.xml package` resolve o `nishi-utils-core` a partir do repo local; o parent é localizado via `<relativePath>`.
- O estágio runtime continua igual — a imagem final só carrega o jar do `ngrid-test`.

## Verificação

O pipeline oficial roda em **GitHub Actions** — workflow `.github/workflows/resilience.yml`, job `docker-resilience-gate`, step "Build Docker test image" (linha 93). A verificação definitiva é vê-lo passar.

1. **Reprodução local do build** (espelha exatamente o comando do CI, antes do push):
   ```bash
   docker build -t ngrid-test:latest -f ngrid-test/Dockerfile .
   ```
   Critério de sucesso: build conclui sem o erro `Child module /workspace/nishi-utils-oss … does not exist`. Se Docker não estiver disponível localmente, pular este passo e ir direto ao item 3.

2. **Smoke do jar gerado** (opcional, confirma que o entrypoint da imagem ainda funciona):
   ```bash
   docker run --rm ngrid-test:latest --help 2>&1 | head
   ```

3. **Validação no GitHub Actions** (definitivo):
   - Após commitar na branch `feature/ngrrd-smoke`, acompanhar o run de `PR Validation & Resilience Pipeline` no PR correspondente.
   - O job `Docker Resilience Gate` precisa avançar do step **"Build Docker test image"** sem o erro reportado, e idealmente concluir o step **"Run resilience integration tests"** com sucesso.
   - Comando útil de monitoramento via CLI:
     ```bash
     gh run watch
     ```

## Notas de commit

- Branch atual já é `feature/ngrrd-smoke` (mesma branch do agente paralelo).
- Mensagem sugerida (atômica): `fix(ci): inclui nishi-utils-oss no contexto do Dockerfile do ngrid-test`.
- Sem co-autor / sem referências a agente (regra global).
