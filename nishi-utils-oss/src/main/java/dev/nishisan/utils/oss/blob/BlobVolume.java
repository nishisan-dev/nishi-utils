package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;

import java.nio.file.Path;

/**
 * Um blob volume aberto: um filesystem virtual de N shards, compartilhado por
 * milhares de séries. É um recurso de longa duração — abra uma vez por processo
 * (via {@link BlobVolumeRegistry}) e feche no shutdown. Ver
 * {@code doc/oss/ngrrd-blob-volume.md}.
 */
public interface BlobVolume extends AutoCloseable {

    /** Nome lógico do volume (autoridade do {@code ngrrd://<name>/...}). */
    String name();

    /** Diretório físico do volume. */
    Path directory();

    /** Número de shards do volume. */
    int shardCount();

    /** Backend de storage subjacente (compartilhado). */
    BlobStorage storage();

    /** Bindings prontos para {@code StorageFactory}/{@code Ngrrd.fromYaml}. */
    StorageFactory.StorageBindings bindings();

    /** Escreve um snapshot do catálogo e rotaciona o WAL. */
    void checkpoint();

    /**
     * Listener de qualidade default deste volume, propagado a cada handle aberto
     * via {@code Ngrrd.open(...)} (evita fiar um listener por série). {@code null}
     * se não configurado.
     */
    default NgrrdMetricsListener qualityListener() {
        return null;
    }

    /** Listener de métricas operacionais deste volume; {@code null} se não configurado. */
    default BlobVolumeMetricsListener volumeMetricsListener() {
        return null;
    }

    /** Snapshot dos gauges operacionais do volume (modelo pull). Ver {@link BlobVolumeStats}. */
    default BlobVolumeStats stats() {
        return storage().stats();
    }

    @Override
    void close();
}
