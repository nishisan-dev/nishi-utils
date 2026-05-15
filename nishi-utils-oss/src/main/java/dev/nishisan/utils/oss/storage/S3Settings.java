package dev.nishisan.utils.oss.storage;

import java.net.URI;
import java.util.Objects;

/**
 * Parâmetros de conexão para {@link S3Storage}. Pensado para cobrir AWS S3
 * canônico, MinIO e Ceph (via {@link #endpointOverride()} + {@link #pathStyleAccess()}).
 *
 * <p>Credenciais nulas indicam delegação à chain padrão do SDK (env vars,
 * profile, IRSA, instance profile, etc.).</p>
 *
 * @param bucket            bucket destino
 * @param region            região AWS (ex.: {@code us-east-1}); para LocalStack/MinIO use {@code us-east-1})
 * @param endpointOverride  endpoint custom (LocalStack, MinIO, Ceph); {@code null} = AWS público
 * @param pathStyleAccess   {@code true} para path-style ({@code https://endpoint/bucket/key})
 * @param accessKeyId       opcional; {@code null} delega à chain padrão
 * @param secretAccessKey   opcional; {@code null} delega à chain padrão
 */
public record S3Settings(
        String bucket,
        String region,
        URI endpointOverride,
        boolean pathStyleAccess,
        String accessKeyId,
        String secretAccessKey) {

    public S3Settings {
        Objects.requireNonNull(bucket, "bucket é obrigatório");
        Objects.requireNonNull(region, "region é obrigatório");
        if ((accessKeyId == null) != (secretAccessKey == null)) {
            throw new IllegalArgumentException(
                    "accessKeyId e secretAccessKey devem ser ambos null ou ambos não-null");
        }
    }

    public static S3Settings forAws(String bucket, String region) {
        return new S3Settings(bucket, region, null, false, null, null);
    }

    public static S3Settings forEndpoint(String bucket, String region, URI endpoint,
                                         String accessKeyId, String secretAccessKey) {
        return new S3Settings(bucket, region, endpoint, true, accessKeyId, secretAccessKey);
    }
}
