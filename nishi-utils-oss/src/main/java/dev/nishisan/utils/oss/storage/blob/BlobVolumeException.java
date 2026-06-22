package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.NgrrdStorageException;

/**
 * Sinalizada quando um blob volume está corrompido ou em formato inválido
 * (magic/versão/CRC inconsistentes, capacidade insuficiente, etc.). Subtipo de
 * {@link NgrrdStorageException} para manter a hierarquia única de erros de storage.
 */
public class BlobVolumeException extends NgrrdStorageException {

    public BlobVolumeException(String message) {
        super(message);
    }

    public BlobVolumeException(String message, Throwable cause) {
        super(message, cause);
    }
}
