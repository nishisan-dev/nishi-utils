package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registro thread-safe de blob volumes abertos no processo. Cada volume é aberto
 * <strong>uma única vez</strong> por nome (abrir N shards é caro) e vive até o
 * {@link #close()} do registro. Ver {@code doc/oss/ngrrd-blob-volume.md}.
 */
public final class BlobVolumeRegistry implements AutoCloseable {

    private final ConcurrentHashMap<String, BlobVolume> volumes = new ConcurrentHashMap<>();

    /** Abre (ou retorna o já aberto) volume para a configuração. Idempotente por nome. */
    public BlobVolume open(BlobVolumeConfig config) {
        return open(config, null, null);
    }

    /**
     * Abre (ou retorna o já aberto) volume associando os listeners default de
     * qualidade e operacional. Idempotente por nome: se o volume já existir, os
     * listeners desta chamada são ignorados (não fazem parte da chave de
     * identidade do volume).
     */
    public BlobVolume open(BlobVolumeConfig config, NgrrdMetricsListener qualityListener,
                           BlobVolumeMetricsListener volumeMetricsListener) {
        Objects.requireNonNull(config, "config é obrigatório");
        return volumes.computeIfAbsent(config.name(),
                name -> DefaultBlobVolume.open(config, qualityListener, volumeMetricsListener));
    }

    /** Volume registrado pelo nome; lança se ausente. */
    public BlobVolume require(String name) {
        BlobVolume volume = volumes.get(name);
        if (volume == null) {
            throw new IllegalArgumentException("volume não registrado: " + name);
        }
        return volume;
    }

    public Optional<BlobVolume> lookup(String name) {
        return Optional.ofNullable(volumes.get(name));
    }

    public Collection<BlobVolume> volumes() {
        return List.copyOf(volumes.values());
    }

    @Override
    public void close() {
        RuntimeException first = null;
        for (BlobVolume volume : volumes.values()) {
            try {
                volume.close();
            } catch (RuntimeException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        volumes.clear();
        if (first != null) {
            throw first;
        }
    }
}
