/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class S3EncryptedDataKeySupplier implements EncryptedDataKeySupplier {
    private static final Logger LOG = LoggerFactory.getLogger(S3EncryptedDataKeySupplier.class);

    private final String encryptionKeyDirectory;
    private final S3Client s3Client;
    private final S3BucketAndPrefix s3BucketAndPrefix;
    private final AtomicReference<String> encryptedDataKey = new AtomicReference<>();

    public S3EncryptedDataKeySupplier(final S3Client s3Client,
                                      final String encryptionKeyDirectory) {
        this.s3Client = s3Client;
        this.encryptionKeyDirectory = encryptionKeyDirectory;
        this.s3BucketAndPrefix = S3BucketAndPrefix.fromS3Uri(encryptionKeyDirectory);
        encryptedDataKey.set(retrieveTheLatestFileContent(s3BucketAndPrefix));
    }

    @Override
    public String retrieveValue() {
        return encryptedDataKey.get();
    }

    @Override
    public void refresh() {
        encryptedDataKey.set(retrieveTheLatestFileContent(s3BucketAndPrefix));
    }

    private String retrieveTheLatestFileContent(final S3BucketAndPrefix s3BucketAndPrefix) {
        final String latestFileKey = retrieveTheLatestFileKey(s3BucketAndPrefix);
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3BucketAndPrefix.getBucketName())
                .key(latestFileKey)
                .build();
        try (final ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest)) {
            LOG.info("Object with latest key \"{}\" downloaded.", latestFileKey);
            return IOUtils.toString(s3Object, StandardCharsets.UTF_8);
        } catch (final Exception ex) {
            LOG.error("Error encountered while processing the response from Amazon S3.", ex);
            throw new RuntimeException(ex);
        }
    }

    private String retrieveTheLatestFileKey(final S3BucketAndPrefix s3BucketAndPrefix) {
        final List<S3Object> fileObjects = getAllS3Objects(
                s3Client, s3BucketAndPrefix.getBucketName(), s3BucketAndPrefix.getPrefix())
                .stream().filter(s3Object -> s3Object.size() > 0).collect(Collectors.toList());

        if (fileObjects.isEmpty()) {
            throw new IllegalStateException(String.format("No data key files found in %s.", encryptionKeyDirectory));
        }

        final String latestFileKey = Collections.max(fileObjects, Comparator.comparing(S3Object::key)).key();
        return latestFileKey;
    }

    private List<S3Object> getAllS3Objects(S3Client s3Client, String bucketName, String prefix) {
        final List<S3Object> allObjects = new ArrayList<>();
        String continuationToken = null;

        do {
            final ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix);

            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }

            final ListObjectsV2Response result = s3Client.listObjectsV2(requestBuilder.build());

            allObjects.addAll(result.contents());

            continuationToken = result.nextContinuationToken();
        } while (continuationToken != null);

        return allObjects;
    }
}
