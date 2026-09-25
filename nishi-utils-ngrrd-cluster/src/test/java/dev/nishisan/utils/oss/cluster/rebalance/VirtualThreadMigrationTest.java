package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.node.SeriesHandleRegistry;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStartRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

public class VirtualThreadMigrationTest {
    @Test
    void migrationHandlersReleaseCarriersWhileWaitingForRpc(@TempDir Path base) throws Exception {
        // A partir do JDK 24 (JEP 491), synchronized deixa de prender a carrier thread da virtual
        // thread — a regressão que este teste cobre (handlers de migração presos num monitor enquanto
        // esperam RPC) some por si só nesse JDK, então o teste passaria mesmo com a regressão de volta,
        // sem cobrir nada. O CI roda em JDK 21; pula em vez de dar falso positivo de cobertura em
        // JDK 24+.
        Assumptions.assumeTrue(Runtime.version().feature() < 24,
                "JEP 491 (JDK 24+) faz synchronized não prender mais a carrier thread — o teste ficaria inócuo");
        // Scheduler parallelism is fixed at initialization, so this regression needs a fresh JVM.
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.virtualThreadScheduler.parallelism=2", "-Djdk.virtualThreadScheduler.maxPoolSize=2",
                "-Djdk.tracePinnedThreads=full", "-cp", System.getProperty("java.class.path"),
                VirtualThreadMigrationTest.class.getName(), base.toString()).redirectErrorStream(true)
                .redirectOutput(base.resolve("probe.log").toFile()).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), "child JVM did not terminate");
            assertEquals(0, process.exitValue(), Files.readString(base.resolve("probe.log")));
        } finally {
            process.destroyForcibly();
        }
    }

    public static void main(String[] args) throws Exception {
        NodeId source = NodeId.of("source");
        Transport transport = (Transport) Proxy.newProxyInstance(Transport.class.getClassLoader(),
                new Class[]{Transport.class}, (proxy, method, parameters) -> {
                    if (method.getName().equals("local")) { return new NodeInfo(source, "127.0.0.1", 0); }
                    if (method.getReturnType() == void.class) { return null; }
                    throw new AssertionError(method.getName());
                });
        CatalogView catalog = (CatalogView) Proxy.newProxyInstance(CatalogView.class.getClassLoader(),
                new Class[]{CatalogView.class}, (proxy, method, parameters) -> { throw new AssertionError(method.getName()); });
        CountDownLatch awaitingPrepare = new CountDownLatch(2);
        CountDownLatch responseProcessed = new CountDownLatch(1);
        List<CompletableFuture<MigrateResponse>> replies = new CopyOnWriteArrayList<>();
        ClusterRpc rpc = new ClusterRpc() {
            public <R> R call(NodeId target, String command, Object body, Class<R> type) {
                assertEquals(Commands.MIGRATE_PREPARE, command);
                var reply = new CompletableFuture<MigrateResponse>();
                replies.add(reply);
                awaitingPrepare.countDown();
                try { return type.cast(reply.get(5, TimeUnit.SECONDS)); }
                catch (Exception e) { throw new NgrrdClusterException(ErrorCode.TIMEOUT, "probe timeout", e); }
            }
            public NodeId localId() { return source; }
            public Optional<NodeId> leaderId() { return Optional.empty(); }
        };
        try (var volumes = NgrrdBlob.registry().basePath(Path.of(args[0])).volume("ngrrd").build()) {
            var volume = volumes.require("ngrrd");
            try (var registry = new SeriesHandleRegistry(volume, "ngrrd", Duration.ofMinutes(1), 100, Clock.systemUTC())) {
                var executor = new MigrationExecutor(transport, registry, volume, rpc, catalog, source,
                        256 * 1024, 64 * 1024 * 1024, Clock.systemUTC());
                try {
                    // PREPARE is rejected; the image need only exist, it is never installed as a series.
                    volume.storage().put("series/probe0.ngrr", new byte[4096]);
                    volume.storage().put("series/probe1.ngrr", new byte[4096]);
                    Thread first = start(executor, 0);
                    Thread second = start(executor, 1);
                    assertTrue(awaitingPrepare.await(2, TimeUnit.SECONDS));
                    Thread reader = Thread.ofVirtual().name("probe-response-reader").start(() -> {
                        replies.forEach(f -> f.complete(MigrateResponse.of(MigrateStatus.ERROR, "probe refusal")));
                        responseProcessed.countDown();
                    });
                    try {
                        assertTrue(responseProcessed.await(1, TimeUnit.SECONDS),
                                "two handlers must not pin both carriers and starve the response reader");
                    } finally {
                        // Platform-thread watchdog lets the failed/old implementation unwind as well.
                        replies.forEach(f -> f.complete(MigrateResponse.of(MigrateStatus.ERROR, "watchdog")));
                        first.join(2000);
                        second.join(2000);
                        reader.join(2000);
                    }
                    assertFalse(first.isAlive() || second.isAlive() || reader.isAlive());
                } finally { executor.close(); }
            }
        }
    }

    private static Thread start(MigrationExecutor executor, int index) {
        return Thread.ofVirtual().name("probe-migrate-start-" + index).start(() -> executor.handleLocal(
                Commands.MIGRATE_START, new MigrateStartRequest("probe" + index, "m" + index, "target")));
    }
}
