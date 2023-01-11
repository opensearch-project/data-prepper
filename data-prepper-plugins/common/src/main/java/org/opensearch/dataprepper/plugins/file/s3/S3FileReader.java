/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.file.s3;

import org.opensearch.dataprepper.plugins.file.DynamicFileReader;
import org.opensearch.dataprepper.plugins.file.exception.S3ObjectTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class S3FileReader implements DynamicFileReader {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileReader.class);
    private final S3Client s3Client;
    private final String filePath;


    public S3FileReader(final S3Client s3Client,
                                 final String filePath) {
        this.s3Client = Objects.requireNonNull(s3Client);
        this.filePath = Objects.requireNonNull(filePath);
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getFile() {
        try {
            final URI fileUri = new URI(filePath);
            validateS3ObjectSize(fileUri);

            final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(fileUri.getHost())
                    .key(fileUri.getPath().substring(1))
                    .build();

            return s3Client.getObject(getObjectRequest);

        } catch (URISyntaxException ex) {
            LOG.error("Error encountered while parsing the Amazon S3 URI in OpenSearch sink.", ex);
            throw new RuntimeException(ex);
        }
    }

    private void validateS3ObjectSize(final URI fileUri) {
        final GetObjectAttributesRequest getObjectAttributesRequest = GetObjectAttributesRequest.builder()
                .bucket(fileUri.getHost())
                .key(fileUri.getPath().substring(1))
                .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                .build();

        final GetObjectAttributesResponse objectAttributes = s3Client.getObjectAttributes(getObjectAttributesRequest);
        final Long objectSize = objectAttributes.objectSize();

        if (objectSize > 1_000_000L) {
            throw new S3ObjectTooLargeException(String.format("S3 Object size can't be more than 1MB. %s object size is %s", fileUri.getPath().substring(1), objectSize));
        }
    }
}
