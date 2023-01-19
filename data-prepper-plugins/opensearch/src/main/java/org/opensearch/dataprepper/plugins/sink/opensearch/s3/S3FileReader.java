/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

import org.apache.commons.lang3.EnumUtils;
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

public class S3FileReader implements FileReader {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileReader.class);
    private static final long MAX_FILE_SIZE = 5_000_000L;

    private final S3Client s3Client;

    public S3FileReader(final S3Client s3Client) {
        this.s3Client = Objects.requireNonNull(s3Client);
    }

    @Override
    public ResponseInputStream<GetObjectResponse> readFile(final String filePath) {
        try {
            final URI fileUri = new URI(filePath);
            validateFileType(filePath);
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

    private void validateFileType(final String filePath) {
        final int index = filePath.lastIndexOf(".");
        String fileType = null;
        if (index != -1) {
            fileType = filePath.substring(index).substring(1);
        }
        if (!EnumUtils.isValidEnumIgnoreCase(FileType.class, fileType)) {
            throw new UnsupportedFileTypeException("S3 file type provided is not supported");
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

        if (objectSize > MAX_FILE_SIZE) {
            throw new S3ObjectTooLargeException(String.format("S3 Object size can't be more than 5MB. %s object size is %s", fileUri.getPath().substring(1), objectSize));
        }
    }
}
