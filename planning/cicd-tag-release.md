# CI/CD: Release somente por Tag `vy.x.z`

## Contexto

Atualmente o workflow `publish.yml` é acionado a cada push na `main` (e `feature/ngrid`). Ele faz bump automático de versão, build, deploy no GitHub Packages, commit de volta e criação de tag automática.

O objetivo é **simplificar**: a release deve ser acionada **somente** quando uma tag no padrão `vy.x.z` for pushada manualmente. A versão do artefato será extraída da tag.

## Proposed Changes

### CI/CD Workflow

#### [MODIFY] [publish.yml](file:///home/lucas/Projects/nishisan/nishi-utils/.github/workflows/publish.yml)

Substituição completa do workflow. Mudanças principais:

1. **Trigger**: `push.tags` com filtro `v*.*.*` em vez de `push.branches`
2. **Sem auto-bump**: a versão é extraída da tag (`GITHUB_REF_NAME` → remove o prefixo `v`)
3. **Versão no POM**: usa `mvn versions:set` com a versão da tag antes do build
4. **Sem commit de volta**: não há mais commit automático de bump
5. **Sem criação de tag**: a tag já existe, foi criada pelo dev
6. **GitHub Release**: cria uma Release no GitHub automaticamente usando a tag

```yaml
name: Maven Release

on:
  push:
    tags:
      - 'v[0-9]+.[0-9]+.[0-9]+'

jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      packages: write

    steps:
    - uses: actions/checkout@v4

    - name: Set up JDK 21
      uses: actions/setup-java@v4
      with:
        java-version: '21'
        distribution: 'temurin'

    - name: Extract version from tag
      run: |
        VERSION="${GITHUB_REF_NAME#v}"
        echo "RELEASE_VERSION=$VERSION" >> $GITHUB_ENV
        echo "Releasing version: $VERSION"

    - name: Set POM version
      run: mvn versions:set -DnewVersion=${{ env.RELEASE_VERSION }} -DgenerateBackupPoms=false

    - name: Build with Maven
      run: mvn -B package --file pom.xml

    - name: Publish to GitHub Packages
      run: mvn deploy -s $GITHUB_WORKSPACE/settings.xml
      env:
        GITHUB_TOKEN:    ${{ secrets.GITHUB_TOKEN }}
        GITHUB_USERNAME: ${{ github.actor }}

    - name: Create GitHub Release
      uses: softprops/action-gh-release@v2
      with:
        tag_name: ${{ github.ref_name }}
        name: "Release ${{ github.ref_name }}"
        generate_release_notes: true
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

> [!IMPORTANT]
> O branch `feature/ngrid` não terá mais builds automáticos de CI no workflow de publish. Se precisarmos de CI nesse branch, o workflow `resilience.yml` já cobre testes.

## Verification Plan

### Manual Verification

1. Após o push do workflow atualizado para a `main`, pushear uma tag de teste: `git tag v3.0.4 && git push origin v3.0.4`
2. Verificar no GitHub Actions que o workflow `Maven Release` é acionado
3. Confirmar que o artefato é publicado no GitHub Packages com a versão `3.0.4`
4. Confirmar que uma Release é criada no GitHub com release notes automáticas
5. Confirmar que pushes normais na `main` **não** acionam o workflow de publish
