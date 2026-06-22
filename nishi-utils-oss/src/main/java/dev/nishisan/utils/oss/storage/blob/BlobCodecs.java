package dev.nishisan.utils.oss.storage.blob;

import java.util.Arrays;
import java.util.zip.CRC32;

/** Utilitários comuns aos codecs binários do blob volume (CRC-32 e MAGIC). */
final class BlobCodecs {

    private BlobCodecs() {
    }

    /** CRC-32 (IEEE) sobre {@code data[from .. from+len)} como {@code u32} em int. */
    static int crc32(byte[] data, int from, int len) {
        CRC32 crc = new CRC32();
        crc.update(data, from, len);
        return (int) crc.getValue();
    }

    /** Verifica se os primeiros bytes de {@code image} batem com {@code magic}. */
    static boolean magicMatches(byte[] image, byte[] magic) {
        return image.length >= magic.length
                && Arrays.equals(image, 0, magic.length, magic, 0, magic.length);
    }
}
