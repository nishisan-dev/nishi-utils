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
    private boolean dirty;

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
        if (shard != null && length == BlobStorage.alignToPage(totalBytes)) {
            return; // idempotente: já alocado no mesmo tamanho
        }
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
