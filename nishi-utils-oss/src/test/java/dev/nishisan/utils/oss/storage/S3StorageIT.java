package dev.nishisan.utils.oss.storage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste de integração do {@link S3Storage} contra LocalStack via Testcontainers.
 *
 * <p>Habilitado apenas no profile Maven {@code ngrrd-integration} (Failsafe);
 * fora do profile, o nome {@code *IT.java} não é coletado pelo Surefire.</p>
 */
class S3StorageIT {

    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:3.5.0");
    private static final String BUCKET = "ngrrd-it";

    private static LocalStackContainer localstack;
    private static S3Storage storage;

    @BeforeAll
    static void setUp() {
        // Docker Engine 25+ exige API >= 1.40; docker-java 3.4.2 negocia 1.32 por padrão.
        // Forçar uma versão compatível evita BadRequestException no ping inicial.
        System.setProperty("api.version", "1.43");

        localstack = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(Service.S3);
        localstack.start();

        S3Settings settings = S3Settings.forEndpoint(
                BUCKET,
                localstack.getRegion(),
                localstack.getEndpointOverride(Service.S3),
                localstack.getAccessKey(),
                localstack.getSecretKey());

        try (S3Client bootstrap = S3Storage.buildClient(settings)) {
            bootstrap.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        storage = new S3Storage(settings);
    }

    @AfterAll
    static void tearDown() {
        if (storage != null) {
            storage.close();
        }
        if (localstack != null) {
            localstack.stop();
        }
    }

    @Test
    void putEGetRoundTripContraLocalStack() {
        byte[] payload = {10, 20, 30};
        storage.put("raw/k/in/300/100.ngrrd", payload);

        Optional<byte[]> read = storage.get("raw/k/in/300/100.ngrrd");
        assertTrue(read.isPresent());
        assertArrayEquals(payload, read.get());
    }

    @Test
    void existsRefleteEstadoAposPutEDelete() {
        String key = "manifest/k/v1.yaml";
        assertFalse(storage.exists(key));
        storage.put(key, "version: 1".getBytes());
        assertTrue(storage.exists(key));
        storage.delete(key);
        assertFalse(storage.exists(key));
    }

    @Test
    void listEnumeraChavesPorPrefixoNoBucket() {
        storage.put("agg/s/r/300/1.ngrrd", new byte[]{1});
        storage.put("agg/s/r/300/2.ngrrd", new byte[]{2});
        storage.put("manifest/s/v1.yaml", new byte[]{3});

        List<String> agg = storage.list("agg/s/r/300/");
        assertEquals(2, agg.size());
        assertTrue(agg.contains("agg/s/r/300/1.ngrrd"));
        assertTrue(agg.contains("agg/s/r/300/2.ngrrd"));
    }

    @Test
    void verifyOrReplaceFazPrimeiraGravacaoEpoisEvitaReescritaIdentica() {
        String key = "raw/idem/in/300/0.ngrrd";
        byte[] v1 = {1, 1, 1};
        assertEquals(VerifyResult.WRITTEN, storage.verifyOrReplaceIfIdentical(key, v1));
        assertEquals(VerifyResult.IDENTICAL_SKIPPED, storage.verifyOrReplaceIfIdentical(key, v1));
        assertEquals(VerifyResult.REPLACED, storage.verifyOrReplaceIfIdentical(key, new byte[]{2, 2}));
    }

    @Test
    void seriesChannelFazReadModifyWriteEReabreComMudancas() {
        String key = "series/device:r1/iface:eth0.ngrr";
        assertFalse(storage.seriesExists(key));

        byte[] head = "HEADER--".getBytes(); // 8 bytes
        byte[] mid = "MIDDLE!!".getBytes();   // 8 bytes
        try (SeriesChannel ch = storage.openSeries(key)) {
            ch.allocate(64);
            ch.writeRegion(0, head);
            ch.writeRegion(40, mid);
            ch.force(); // PUT do objeto inteiro
        }

        assertTrue(storage.seriesExists(key));
        try (SeriesChannel ch = storage.openSeries(key)) {
            assertEquals(64, ch.size());
            assertArrayEquals(head, ch.readRegion(0, 8));
            assertArrayEquals(mid, ch.readRegion(40, 8));
            assertArrayEquals(new byte[8], ch.readRegion(16, 8));
        }
    }
}
