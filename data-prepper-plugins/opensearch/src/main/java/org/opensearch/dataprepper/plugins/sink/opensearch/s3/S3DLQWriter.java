package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

import org.opensearch.dataprepper.plugins.sink.opensearch.DLQWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class S3DLQWriter implements DLQWriter {
    private static final Logger LOG = LoggerFactory.getLogger(S3DLQWriter.class);
    private final S3Client s3Client;
    private final String bucketName;
    private final String keyPrefix;

    public S3DLQWriter(final S3Client s3Client, final String bucketName, final String keyPrefix) {
        this.s3Client = Objects.requireNonNull(s3Client);
        this.keyPrefix = Objects.requireNonNull(keyPrefix);
        this.bucketName = Objects.requireNonNull(bucketName);
    }

    @Override
    public synchronized void write(final String content) throws IOException {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(String.format(
                        "%s%s/%s", keyPrefix, Instant.now(), UUID.randomUUID()))
                .build();

        try {
            final PutObjectResponse putObjectResponse = s3Client.putObject(
                    putObjectRequest, RequestBody.fromString(content));
            if (!putObjectResponse.sdkHttpResponse().isSuccessful()) {
                throw new IOException(String.format(
                        "Failed to dump content: [%s] to S3 due to status code: %d",
                        content, putObjectResponse.sdkHttpResponse().statusCode()));
            }
        } catch (Exception ex) {
            throw new IOException(String.format(
                    "Failed to dump content: [%s] to S3.",
                    content), ex);
        }
    }

    @Override
    public void close() {
        s3Client.close();
    }

    public static void main(String[] args) throws Exception {
        S3Client s3Client1 = S3Client.builder().httpClient(ApacheHttpClient.create()).build();
        String bucketName = "test-s3-dlq-bucket";
        String keyPrefix = "ab/";
        S3DLQWriter s3FileWriter = new S3DLQWriter(s3Client1, bucketName, keyPrefix);
        s3FileWriter.write("adsad");
        s3FileWriter.close();
    }
}
