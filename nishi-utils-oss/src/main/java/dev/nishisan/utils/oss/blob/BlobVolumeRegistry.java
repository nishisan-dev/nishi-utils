package dev.nishisan.utils.oss.blob;

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
        Objects.requireNonNull(config, "config é obrigatório");
        return volumes.computeIfAbsent(config.name(), name -> DefaultBlobVolume.open(config));
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
