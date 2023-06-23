/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.parquet.io.SeekableInputStream;
import org.opensearch.dataprepper.model.io.InputFile;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.util.concurrent.atomic.LongAdder;

public class S3InputFile implements InputFile {

    private S3Client s3Client;

    private S3ObjectReference s3ObjectReference;

    private LongAdder bytesCounter;

    private HeadObjectResponse metadata;

    public S3InputFile(final S3Client s3Client, final S3ObjectReference s3ObjectReference) {
        this.s3Client = s3Client;
        this.s3ObjectReference = s3ObjectReference;
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
     *
     * @return content length
     */
    @Override
    public long getLength() {
        return getMetadata().contentLength();
    }

    /**
     * Create an input stream from the input file
     * @return an implementation of a SeekableInputStream into the S3 object.
     */
    @Override
    public SeekableInputStream newStream() {
        bytesCounter = new LongAdder();

        return new S3InputStream(s3Client, s3ObjectReference, getMetadata(), bytesCounter);
    }

    /**
     * Get the count of bytes read from the S3 object
     * @return
     */
    public long getBytesCount() {
        if (bytesCounter == null) {
            return 0;
        }

        return bytesCounter.longValue();
    }

    /**
     * Get the metadata of the S3 object. Cache the metadata to avoid subsequent headObject calls to S3
     * @return the metadata of the S3 object
     */
    private synchronized HeadObjectResponse getMetadata() {
        if (metadata == null) {
            final HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(s3ObjectReference.getBucketName())
                    .key(s3ObjectReference.getKey())
                    .build();
            metadata = s3Client.headObject(request);
        }

        return metadata;
    }
}
