/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class S3EncryptedDataKeyWriter implements EncryptedDataKeyWriter {
    static final String S3_PREFIX = "s3://";
    static final String KEY_NAME_FORMAT = "%s.key";
    private static final String FULL_KEY_FORMAT = "%s/%s";

    private final String encryptionKeyDirectory;
    private final S3BucketAndPrefix s3BucketAndPrefix;
    private final S3Client s3Client;

    public S3EncryptedDataKeyWriter(final S3Client s3Client,
                                    final String encryptionKeyDirectory) {
        this.s3Client = s3Client;
        this.encryptionKeyDirectory = encryptionKeyDirectory;
        s3BucketAndPrefix = S3BucketAndPrefix.fromS3Uri(encryptionKeyDirectory);
    }

    @Override
    public void writeEncryptedDataKey(final String encryptedDataKey) throws IOException {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3BucketAndPrefix.getBucketName())
                .key(buildKey(s3BucketAndPrefix.getPrefix()))
                .build();
        final PutObjectResponse putObjectResponse = s3Client.putObject(
                putObjectRequest, RequestBody.fromString(encryptedDataKey));
        if (!putObjectResponse.sdkHttpResponse().isSuccessful()) {
            throw new IOException(String.format(
                    "Failed to write S3 encryption key directory due to status code: %d",
                    putObjectResponse.sdkHttpResponse().statusCode()));
        }
    }

    private String buildKey(final String keyPathPrefix) {
        final Instant now = Instant.now();
        final String iso8601 = DateTimeFormatter.ISO_INSTANT.format(now);
        final String key = String.format(KEY_NAME_FORMAT, iso8601);
        return keyPathPrefix == null ? key : String.format(FULL_KEY_FORMAT, keyPathPrefix, key);
    }
}
