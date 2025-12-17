---
name: Adicionar opção resetOnRestart à NQueue
overview: Adicionar a opção `resetOnRestart` à classe `NQueue.Options` que, quando `true`, ignora dados persistentes existentes e inicializa uma fila nova na reinicialização. O valor padrão será `false` para manter compatibilidade com o comportamento atual.
todos: []
---

# Adicionar opção resetOnRestart à NQueue

## Objetivo

Adicionar uma opção `resetOnRestart` à classe `NQueue.Options` que permite ignorar dados persistentes existentes e inicializar uma fila nova na reinicialização, mesmo que arquivos de persistência existam.

## Implementação

### 1. Adicionar campo e método na classe Options

- **Arquivo**: [`src/main/java/dev/nishisan/utils/queue/NQueue.java`](src/main/java/dev/nishisan/utils/queue/NQueue.java)
- **Localização**: Classe `Options` (linhas 1760-1944)
- Adicionar campo privado `resetOnRestart` com valor padrão `false` (linha ~1768)
- Adicionar método público `withResetOnRestart(boolean resetOnRestart)` seguindo o padrão dos outros métodos setter (após linha ~1907)
- Incluir o campo no método `snapshot()` para que seja passado ao construtor da fila

### 2. Modificar o método open() para verificar resetOnRestart

- **Arquivo**: [`src/main/java/dev/nishisan/utils/queue/NQueue.java`](src/main/java/dev/nishisan/utils/queue/NQueue.java)
- **Localização**: Método `open()` (linhas 209-272)
- **Lógica**: 
  - Se `options.resetOnRestart` for `true`, ignorar a verificação de `Files.exists(metaPath)` e sempre inicializar uma fila nova
  - Isso significa tratar como se os arquivos não existissem, chamando `rebuildState()` que retornará um estado vazio e persistindo um novo metadata
  - A verificação deve ocorrer antes da linha 224 onde `Files.exists(metaPath)` é verificado

### 3. Detalhes da implementação

**No método `open()` (linha ~223-260):**

```java
QueueState state;
if (options.resetOnRestart) {
    // Ignorar arquivos existentes e inicializar fila nova
    state = rebuildState(ch);
    persistMeta(metaPath, state);
} else if (Files.exists(metaPath)) {
    // Comportamento atual: tentar carregar estado existente
    // ... código existente ...
}
```

**Na classe `Options`:**

- Adicionar campo: `private boolean resetOnRestart = false;`
- Adicionar método setter: `public Options withResetOnRestart(boolean resetOnRestart)`
- Não é necessário incluir no `Snapshot` interno, pois é usado apenas no momento da abertura da fila

## Comportamento esperado

- **`resetOnRestart = false` (padrão)**: Comportamento atual mantido - carrega dados persistentes se existirem
- **`resetOnRestart = true`**: Ignora arquivos existentes (`data.log` e `queue.meta`) e inicializa uma fila vazia, efetivamente resetando a fila a cada reinicialização

## Compatibilidade

- Valor padrão `false` garante que código existente continue funcionando sem alterações
- Não há mudanças na API pública além da adição do novo método setter