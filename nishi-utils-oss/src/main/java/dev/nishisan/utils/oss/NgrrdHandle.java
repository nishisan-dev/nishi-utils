package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;

import java.util.Map;

/**
 * Contrato público de operação de um pipeline ngrrd já configurado. Devolvido
 * por {@link Ngrrd#fromYaml(java.nio.file.Path,
 * dev.nishisan.utils.oss.storage.StorageFactory.StorageBindings, java.util.Map)}.
 *
 * <p>O escopo é o da série identificada pelas {@code tags} fornecidas — o
 * handle resolve {@code seriesKey} via {@code IdentitySpec.seriesKeyTemplate}.</p>
 *
 * <p>{@link #close()} faz shutdown ordenado: materializa o CDP em progresso,
 * torna o objeto da série durável e encerra a thread do writer.</p>
 */
public interface NgrrdHandle extends AutoCloseable {

    /** chave da série utilizada por este handle. */
    String seriesKey();

    /** Enfileira uma amostra para o DS raw indicado. */
    void write(String dsName, Sample sample);

    /** Equivalente a {@link #checkpoint()} no formato de série única. */
    void flush();

    /**
     * Materializa o CDP em progresso como parcial e torna o objeto da série
     * durável (fsync no disco / PUT no S3), mantendo o estado vivo. Semântica
     * rrdtool-like: o dado fica legível antes do passo do RRA fechar.
     */
    void checkpoint();

    /** Lê uma série materializada com base em uma {@link ViewQuery} explícita. */
    SeriesResult read(String dsName, ViewQuery query);

    /** Executa um preset declarativo, retornando uma série por DS listado. */
    Map<String, SeriesResult> read(String presetName);

    /**
     * Variante de {@link #read(String)} com {@code endExclusiveEpochMs}
     * explícito — útil para testes/replays determinísticos onde
     * {@link System#currentTimeMillis()} não reflete o range desejado.
     */
    Map<String, SeriesResult> read(String presetName, long endExclusiveEpochMs);

    /**
     * Variante de {@link #read(String, ViewQuery)} com {@code endExclusiveEpochMs}
     * explícito.
     */
    SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs);

    @Override
    void close();
}
