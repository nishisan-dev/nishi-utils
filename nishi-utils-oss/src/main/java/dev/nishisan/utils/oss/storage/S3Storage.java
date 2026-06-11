package dev.nishisan.utils.oss.storage;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Backend de storage S3 (AWS SDK v2) com suporte a MinIO/Ceph via
 * {@code endpointOverride} e path-style access.
 *
 * <p>Como S3 não oferece atomic rename nativo, {@link #atomicReplace(String, byte[])}
 * é equivalente a {@link #put(String, byte[])} — o serviço já garante PUT
 * tudo-ou-nada por contrato e a chave determinística do ngrrd cobre o caso de
 * uso de idempotência.</p>
 *
 * <p>O cliente subjacente é criado uma vez no construtor e fechado em
 * {@link #close()}. Esta classe é thread-safe (o {@link S3Client} é).</p>
 *
 * <p>O objeto único da série é acessado via {@link SeriesChannelProvider}: como
 * S3 não suporta escrita parcial, o canal mantém a imagem do objeto em memória
 * (read-modify-write) e regrava o objeto inteiro por PUT em {@code force()}.</p>
 */
public final class S3Storage implements NgrrdStorage, SeriesChannelProvider, AutoCloseable {

    private final S3Client client;
    private final String bucket;

    public S3Storage(S3Settings settings) {
        Objects.requireNonNull(settings, "settings é obrigatório");
        this.bucket = settings.bucket();
        this.client = buildClient(settings);
    }

    static S3Client buildClient(S3Settings settings) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(settings.region()))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(settings.pathStyleAccess())
                        .build());

        if (settings.endpointOverride() != null) {
            builder.endpointOverride(settings.endpointOverride());
        }
        if (settings.accessKeyId() != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(settings.accessKeyId(), settings.secretAccessKey())));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }
        return builder.build();
    }

    @Override
    public void put(String key, byte[] data) {
        try {
            client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromBytes(data));
        } catch (S3Exception e) {
            throw new NgrrdStorageException("Falha S3 put " + key, e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        try {
            ResponseBytes<GetObjectResponse> resp = client.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(key).build());
            return Optional.of(resp.asByteArray());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            throw new NgrrdStorageException("Falha S3 get " + key, e);
        }
    }

    @Override
    public boolean exists(String key) {
        try {
            client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw new NgrrdStorageException("Falha S3 head " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (S3Exception e) {
            throw new NgrrdStorageException("Falha S3 delete " + key, e);
        }
    }

    @Override
    public List<String> list(String prefix) {
        List<String> out = new ArrayList<>();
        try {
            ListObjectsV2Iterable pages = client.listObjectsV2Paginator(
                    ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());
            for (var page : pages) {
                for (S3Object obj : page.contents()) {
                    out.add(obj.key());
                }
            }
        } catch (S3Exception e) {
            throw new NgrrdStorageException("Falha S3 list " + prefix, e);
        }
        return out;
    }

    @Override
    public void atomicReplace(String key, byte[] data) {
        put(key, data);
    }

    @Override
    public boolean seriesExists(String key) {
        return exists(key);
    }

    @Override
    public SeriesChannel openSeries(String key) {
        byte[] image = get(key).orElseGet(() -> new byte[0]);
        return new S3SeriesChannel(key, image);
    }

    @Override
    public void close() {
        client.close();
    }

    /** Canal sobre imagem em memória: read-modify-write com PUT em {@code force()}. */
    private final class S3SeriesChannel implements SeriesChannel {

        private final String key;
        private byte[] image;
        private boolean dirty;

        S3SeriesChannel(String key, byte[] image) {
            this.key = key;
            this.image = image;
        }

        @Override
        public long size() {
            return image.length;
        }

        @Override
        public void allocate(long totalBytes) {
            if (totalBytes > Integer.MAX_VALUE) {
                throw new NgrrdStorageException("Série excede o tamanho máximo suportado: " + totalBytes);
            }
            int n = (int) totalBytes;
            if (image.length != n) {
                image = Arrays.copyOf(image, n);
                dirty = true;
            }
        }

        @Override
        public byte[] readRegion(long offset, int len) {
            int off = (int) offset;
            if (off < 0 || off + len > image.length) {
                throw new NgrrdStorageException("Leitura além do fim da série " + key
                        + " (offset=" + offset + ", len=" + len + ", size=" + image.length + ")");
            }
            return Arrays.copyOfRange(image, off, off + len);
        }

        @Override
        public void writeRegion(long offset, byte[] data) {
            int off = (int) offset;
            int end = off + data.length;
            if (end > image.length) {
                image = Arrays.copyOf(image, end);
            }
            System.arraycopy(data, 0, image, off, data.length);
            dirty = true;
        }

        @Override
        public void force() {
            if (!dirty) {
                return;
            }
            put(key, image);
            dirty = false;
        }

        @Override
        public void close() {
            force();
        }
    }
}
