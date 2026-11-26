package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.SeriesChannel;

/**
 * {@link SeriesChannel} sobre uma região de um {@link MappedShard}. Um canal
 * "unbound" ({@code size()==0}) representa uma série ainda não alocada — o
 * {@code NgrrdWriter} então chama {@link #allocate} (via {@code createFresh}),
 * que reserva a região no volume e (re)vincula o canal. As escritas vão para
 * {@code [baseOffset, baseOffset+length)} e o {@code force} sincroniza apenas
 * essa região. {@link #close()} não desmapeia (o mapping pertence ao shard).
 */
final class BlobSeriesChannel implements SeriesChannel {

    private final BlobStorage storage;
    private final String key;
    private MappedShard shard;
    private long baseOffset;
    private long length;
    // Volátil: o ticket de prepareForce() pode remarcar o canal como sujo a partir de
    // outra thread (falha do force destacado). Fora da thread do writer a marca só vai
    // de false para true; a escrita também só a liga.
    private volatile boolean dirty;

    /** Canal unbound (série inexistente). */
    BlobSeriesChannel(BlobStorage storage, String key) {
        this.storage = storage;
        this.key = key;
        this.shard = null;
        this.length = 0;
    }

    /** Canal vinculado a uma região existente. */
    BlobSeriesChannel(BlobStorage storage, String key, MappedShard shard, long baseOffset, long length) {
        this.storage = storage;
        this.key = key;
        this.shard = shard;
        this.baseOffset = baseOffset;
        this.length = length;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public void allocate(long totalBytes) {
        BlobStorage.Region region = storage.allocateRegion(key, totalBytes);
        this.shard = region.shard();
        this.baseOffset = region.offset();
        this.length = region.length();
    }

    @Override
    public byte[] readRegion(long offset, int len) {
        ensureBound();
        checkRegion(offset, len);
        return shard.readAt(baseOffset + offset, len);
    }

    @Override
    public void writeRegion(long offset, byte[] data) {
        ensureBound();
        checkRegion(offset, data.length);
        shard.writeAt(baseOffset + offset, data);
        dirty = true;
    }

    @Override
    public void force() {
        if (shard == null || !dirty) {
            return;
        }
        shard.forceRange(baseOffset, length);
        dirty = false;
    }

    /**
     * Captura a faixa da série e limpa a marca de sujo; o ticket faz o
     * {@code forceRange} da faixa capturada em qualquer thread. Uma escrita
     * posterior (thread do writer) volta a ligar a marca, e o próximo force a
     * cobre. Em falha, o ticket remarca o canal como sujo e relança. Forces
     * concorrentes da mesma faixa são seguros: o {@code msync} é idempotente.
     */
    @Override
    public ForceTicket prepareForce() {
        if (shard == null || !dirty) {
            return ForceTicket.DONE;
        }
        MappedShard target = shard;
        long offset = baseOffset;
        long len = length;
        dirty = false;
        return () -> {
            try {
                target.forceRange(offset, len);
            } catch (RuntimeException | Error e) {
                dirty = true;
                throw e;
            }
        };
    }

    @Override
    public void close() {
        force();
    }

    private void ensureBound() {
        if (shard == null) {
            throw new BlobVolumeException("série não alocada no volume: " + key);
        }
    }

    private void checkRegion(long offset, long len) {
        if (offset < 0 || len < 0 || offset + len > length) {
            throw new BlobVolumeException("acesso fora da região da série " + key
                    + " (offset=" + offset + ", len=" + len + ", length=" + length + ")");
        }
    }
}
