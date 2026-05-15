package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;

import java.util.Map;

/**
 * Contrato público de operação de um pipeline ngrrd já configurado. Devolvido
 * por {@link Ngrrd#fromYaml(java.nio.file.Path,
 * dev.nishisan.utils.oss.storage.StorageFactory.StorageBindings)}.
 *
 * <p>O escopo é o da série identificada pelas {@code tags} fornecidas — o
 * handle resolve {@code seriesKey} via {@code IdentitySpec.seriesKeyTemplate}.</p>
 *
 * <p>{@link #close()} faz shutdown ordenado: flush do bloco aberto + última
 * gravação de manifesto + fechamento das threads de writer/manifest.</p>
 */
public interface NgrrdHandle extends AutoCloseable {

    /** chave da série utilizada por este handle. */
    String seriesKey();

    /** Enfileira uma amostra para o DS raw indicado. */
    void write(String dsName, Sample sample);

    /** Força o fechamento do bloco aberto e a gravação do manifesto. */
    void flush();

    /** Lê uma série materializada com base em uma {@link ViewQuery} explícita. */
    SeriesResult read(String dsName, ViewQuery query);

    /** Executa um preset declarativo, retornando uma série por DS listado. */
    Map<String, SeriesResult> read(String presetName);

    @Override
    void close();
}
