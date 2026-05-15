package dev.nishisan.utils.oss.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Contrato dos backends de armazenamento usados pelo ngrrd.
 *
 * <p>As implementações são responsáveis por persistir <strong>blocos binários</strong>
 * ({@code .ngrrd}) e <strong>manifestos YAML</strong> ({@code .yaml}). Toda
 * operação que cria ou substitui um objeto deve ser observável imediatamente
 * por uma leitura subsequente.</p>
 *
 * <p>O contrato é intencionalmente síncrono e blocking — paralelismo é
 * orquestrado pelos componentes consumidores ({@code NgrrdReader} usa
 * executor próprio para baixar blocos em paralelo).</p>
 */
public interface NgrrdStorage {

    /**
     * Grava (sobrescrevendo se já existir). Não exige atomicidade — utilize
     * {@link #atomicReplace(String, byte[])} para garantia de visibilidade
     * tudo-ou-nada do objeto resultante.
     */
    void put(String key, byte[] data);

    /**
     * Lê o objeto completo. {@link Optional#empty()} se não existir.
     */
    Optional<byte[]> get(String key);

    /**
     * Indica se a chave existe no backend.
     */
    boolean exists(String key);

    /**
     * Remove a chave. No-op se não existir.
     */
    void delete(String key);

    /**
     * Lista chaves cujo nome começa por {@code prefix}. Em sistemas
     * hierárquicos (disco), o prefixo é tratado como subdiretório. A ordem
     * <strong>não</strong> é especificada; consumidores devem ordenar quando
     * necessário.
     */
    List<String> list(String prefix);

    /**
     * Substitui o conteúdo de {@code key} de forma que leitores não observem
     * estado parcial: em disco usa tmp + {@code Files.move(ATOMIC_MOVE)}; em
     * S3 usa um único PUT (já tudo-ou-nada por contrato do serviço).
     */
    void atomicReplace(String key, byte[] data);

    /**
     * Idempotência das chaves determinísticas do ngrrd: se a chave não existe,
     * grava; se existe e é byte-a-byte idêntica, não regrava; se existe e
     * difere, faz {@link #atomicReplace(String, byte[])}.
     *
     * @return {@link VerifyResult} indicando qual caminho foi tomado.
     */
    default VerifyResult verifyOrReplaceIfIdentical(String key, byte[] data) {
        Optional<byte[]> existing = get(key);
        if (existing.isEmpty()) {
            atomicReplace(key, data);
            return VerifyResult.WRITTEN;
        }
        if (Arrays.equals(existing.get(), data)) {
            return VerifyResult.IDENTICAL_SKIPPED;
        }
        atomicReplace(key, data);
        return VerifyResult.REPLACED;
    }
}
