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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class S3FileReader implements FileReader {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileReader.class);
    static final long ONE_MB = 1024L * 1024L;
    private static final long FIVE_MB = 5 * ONE_MB;
    private static final String MAX_CONTENT_LENGTH = "5 MB";

    private final S3Client s3Client;

    public S3FileReader(final S3Client s3Client) {
        this.s3Client = Objects.requireNonNull(s3Client);
    }

    @Override
    public ResponseInputStream<GetObjectResponse> readFile(final String filePath) {
        try {
            final URI fileUri = new URI(filePath);
            validateURI(fileUri);
            validateFileType(filePath);

            final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(fileUri.getHost())
                    .key(fileUri.getPath().substring(1))
                    .build();

            final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
            validateS3ObjectSize(responseInputStream, fileUri);

            return responseInputStream;

        } catch (URISyntaxException ex) {
            LOG.error("Error encountered while parsing the Amazon S3 URI in OpenSearch sink.", ex);
            throw new RuntimeException(ex);
        }
    }

    private void validateURI(URI fileUri) {
        final String bucketName = fileUri.getHost();
        final String objectKey = fileUri.getPath();

        if (bucketName == null || objectKey.length() < 2) {
            throw new InvalidS3URIException("S3 URi must contain valid bucket and object key name");
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

    private void validateS3ObjectSize(final ResponseInputStream<GetObjectResponse> responseInputStream, final URI uri) {
        final Long contentLength = responseInputStream.response().contentLength();

        if (contentLength > FIVE_MB) {
            throw new S3ObjectTooLargeException(String.format("S3 object content length can't be more than %s. %s object size is %s bytes",
                    MAX_CONTENT_LENGTH, uri.getPath().substring(1), contentLength));
        }
    }
}
