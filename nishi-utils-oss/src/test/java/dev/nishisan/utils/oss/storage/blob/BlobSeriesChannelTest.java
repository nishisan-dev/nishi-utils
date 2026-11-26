package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.SeriesChannel.ForceTicket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o force destacável do {@link BlobSeriesChannel}: {@code prepareForce()} limpa a
 * marca de sujo e devolve um ticket executável em outra thread; em falha, o ticket
 * remarca o canal como sujo, e o próximo force tenta de novo.
 */
class BlobSeriesChannelTest {

    private static final long SEG = 64 * 1024;
    private static final long BASE = ShardSuperblock.HEADER_BYTES;
    private static final UUID VOLUME_UUID = new UUID(0xDEADBEEFL, 0xCAFEBABEL);
    private static final byte[] DATA = {1, 2, 3, 4, 5, 6, 7, 8};

    private static ShardSuperblock superblock(long capacity) {
        return new ShardSuperblock(1, 0, 64, ShardSuperblock.HEADER_BYTES,
                capacity, ShardSuperblock.HEADER_BYTES, VOLUME_UUID, 1L);
    }

    @Test
    void prepareForceLimpaSujoEFalhaRemarca(@TempDir Path dir) {
        try (MappedShard shard = MappedShard.create(dir.resolve("shard-0.dat"), superblock(SEG), SEG)) {
            BlobSeriesChannel unbound = new BlobSeriesChannel(null, "series/unbound.ngrr");
            assertSame(ForceTicket.DONE, unbound.prepareForce(), "canal sem região não tem o que forçar");

            BlobSeriesChannel channel = new BlobSeriesChannel(null, "series/ok.ngrr", shard, BASE, 1024);
            assertSame(ForceTicket.DONE, channel.prepareForce(), "canal limpo não tem o que forçar");

            channel.writeRegion(0, DATA);
            ForceTicket ticket = channel.prepareForce();
            assertTrue(ticket.detached(), "canal sujo devolve um ticket destacado");
            assertSame(ForceTicket.DONE, channel.prepareForce(), "a preparação já limpou a marca de sujo");

            // Escrita depois da preparação volta a ligar a marca: o próximo force a cobre.
            channel.writeRegion(8, DATA);
            CompletableFuture.runAsync(ticket::run).join();
            assertArrayEquals(DATA, shard.readAt(BASE, DATA.length));
            assertTrue(channel.prepareForce().detached(), "escrita posterior à preparação não se perde");

            // Faixa fora da capacidade do shard: o forceRange falha, mas a escrita cabe.
            BlobSeriesChannel failing = new BlobSeriesChannel(null, "series/bad.ngrr", shard, BASE, SEG);
            failing.writeRegion(0, DATA);
            ForceTicket failingTicket = failing.prepareForce();
            assertTrue(failingTicket.detached());
            assertSame(ForceTicket.DONE, failing.prepareForce());

            CompletionException failure = assertThrows(CompletionException.class,
                    () -> CompletableFuture.runAsync(failingTicket::run).join());
            assertInstanceOf(BlobVolumeException.class, failure.getCause(), "o ticket relança a falha original");
            assertTrue(failing.prepareForce().detached(), "a falha do ticket remarca o canal como sujo");
            failing.writeRegion(0, DATA);
            assertThrows(BlobVolumeException.class, failing::force,
                    "o force() do fechamento tenta de novo a faixa remarcada");
        }
    }
}
