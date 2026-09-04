# Testes vermelhos conhecidos

Testes que falham na `main` **sem nenhuma alteração local**. Servem para quem
roda `mvn test` no repo inteiro não perder tempo investigando um vermelho que
não é seu, e para não usar "a suite estava vermelha" como desculpa para não
rodar a suite.

Regra: um teste só entra aqui com **prova de que falha na `main` limpa** (a
receita está no fim). Se você consertar um, remova a entrada no mesmo commit.

---

## `RelayStreamReplicationTest.streamConvergesQueueAndMapWithoutNakOrSnapshot`

- **Módulo:** `nishi-utils-core` (NGrid — replicação de cluster)
- **Constatado em:** 2026-09-04, `main` em `52c32cf`
- **Falha:** `follower did not converge via stream: applied=200 target=400`
  após ~31 s de espera (`RelayStreamReplicationTest.java:154`, chamado de `:78`)
- **Consistente**, não intermitente: falhou nas três execuções feitas — duas com
  alterações locais em `nishi-utils-oss` e uma com a `main` limpa via
  `git stash -u`.
- **Escopo:** o vermelho é do RELAY_STREAM da replicação. `nishi-utils-core` não
  depende de `nishi-utils-oss`, então mudanças no ngrrd não têm como alcançá-lo.
- **Efeito prático:** `mvn test` na raiz termina em `BUILD FAILURE` com
  `Tests run: 536, Failures: 1`. O módulo `nishi-utils-oss` fica **verde**
  (234 testes) e é o que importa para quem mexe no ngrrd.
- **Contorno enquanto não for corrigido:** valide o módulo que você tocou —
  `mvn -pl nishi-utils-oss test`. Para publicar, `mvn install -DskipTests`
  **depois** de a suite do seu módulo passar.

Convergência parcial (metade dos 400 esperados) com timeout de 30 s tem cara de
janela curta demais para a máquina, ou de o stream parar de aplicar no meio. Não
foi investigado a fundo — é trabalho para quem for mexer na replicação.

---

## Como provar que um vermelho é pré-existente

Não confie em "não toquei nesse módulo". Rode na `main` limpa:

```bash
git stash push -u -m "wip"
mvn -pl <modulo> test -Dtest=<Classe> -DfailIfNoSpecifiedTests=false
git stash pop
```

Confirme o `git stash pop` — é o passo que costuma ser esquecido quando o teste
demora e a atenção vai embora.
