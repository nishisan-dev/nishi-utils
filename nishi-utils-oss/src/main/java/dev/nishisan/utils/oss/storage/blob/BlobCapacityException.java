package dev.nishisan.utils.oss.storage.blob;

/** An allocation was refused before changing the volume because its budget is exhausted. */
public final class BlobCapacityException extends BlobVolumeException {
    public BlobCapacityException(String message) {
        super(message);
    }
}
