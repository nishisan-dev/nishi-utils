package dev.nishisan.utils.oss.blob;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Modo de escrita dos shards de um blob volume. A <strong>leitura</strong> é sempre
 * feita pelo mmap; apenas o caminho de escrita muda. Em {@code MAP_SHARED} mmap e
 * {@code write()} compartilham o mesmo page cache, então uma leitura via mmap enxerga
 * imediatamente o que foi gravado por {@code pwrite}, sem necessidade de {@code force}.
 *
 * <p>Motivação: com folios grandes no page cache (16–128 KB em kernels recentes), uma
 * escrita via mmap suja o folio <em>inteiro</em> e o writeback grava o folio inteiro,
 * mesmo que a escrita lógica tenha sido de poucas dezenas de bytes. Na escrita
 * bufferizada, o XFS (iomap, kernel 6.6+) e o ext4 recente rastreiam a sujeira por
 * bloco de 4 KB, reduzindo a amplificação de escrita no disco. Ver
 * {@code doc/oss/ngrrd-blob-volume.md}.</p>
 */
public enum VolumeWriteMode {

    /**
     * Escrita por {@link java.nio.MappedByteBuffer#put(int, byte[], int, int)}. É o
     * comportamento histórico e o padrão.
     */
    MMAP,

    /**
     * Escrita bufferizada por {@link java.io.RandomAccessFile#seek(long)} +
     * {@link java.io.RandomAccessFile#write(byte[], int, int)}, serializada por shard (não usa
     * {@code FileChannel}, que fecha o canal quando a thread é interrompida). A leitura continua pelo
     * mmap. O ganho é em bytes gravados no disco, não em vazão (ver {@code doc/oss/ngrrd-blob-volume.md}).
     * O nome {@code pwrite} designa escrita bufferizada posicional ({@code lseek}+{@code write} via
     * {@code java.io} sob lock por shard), não o syscall {@code pwrite(2)} literalmente; o efeito no kernel
     * é o mesmo caminho bufferizado.
     */
    PWRITE;

    /**
     * Converte texto (sem diferenciar maiúsculas de minúsculas) no modo correspondente.
     *
     * @param text valor textual, ex.: {@code "pwrite"}
     * @return o modo correspondente
     * @throws IllegalArgumentException se o texto for nulo, vazio ou desconhecido; a mensagem
     *                                  lista os valores aceitos
     */
    public static VolumeWriteMode parse(String text) {
        if (text != null) {
            String normalized = text.trim().toUpperCase(Locale.ROOT);
            for (VolumeWriteMode mode : values()) {
                if (mode.name().equals(normalized)) {
                    return mode;
                }
            }
        }
        throw new IllegalArgumentException("writeMode inválido: '" + text + "'; valores aceitos: "
                + Arrays.stream(values()).map(m -> m.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(", ")));
    }
}
