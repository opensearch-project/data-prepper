package org.opensearch.dataprepper.plugins.source.s3;

import org.apache.parquet.io.SeekableInputStream;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.time.Duration;
import java.time.Instant;

public class S3InputFile implements InputFile {

    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(6);

    private static final int DEFAULT_RETRIES = 10;

    private final S3Client s3Client;

    private final S3ObjectReference s3ObjectReference;

    private final BucketOwnerProvider bucketOwnerProvider;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;

    private HeadObjectResponse metadata;

    public S3InputFile(
            final S3Client s3Client,
            final S3ObjectReference s3ObjectReference,
            final BucketOwnerProvider bucketOwnerProvider,
            final S3ObjectPluginMetrics s3ObjectPluginMetrics
    ) {
        this.s3Client = s3Client;
        this.s3ObjectReference = s3ObjectReference;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.s3ObjectPluginMetrics = s3ObjectPluginMetrics;
    }

    public S3ObjectReference getObjectReference() {
        return s3ObjectReference;
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
     * Return the last modified time of the file
     *
     * @return last modified time
     */
    public Instant getLastModified() {
        return getMetadata().lastModified();
    }

    /**
     * Create an input stream from the input file
     * @return an implementation of a SeekableInputStream into the S3 object.
     */
    @Override
    public SeekableInputStream newStream() {
        return new S3InputStream(
            s3Client, s3ObjectReference, bucketOwnerProvider, getMetadata(), s3ObjectPluginMetrics, DEFAULT_RETRY_DELAY, DEFAULT_RETRIES);
    }

    /**
     * Get the metadata of the S3 object. Cache the metadata to avoid subsequent headObject calls to S3
     * @return the metadata of the S3 object
     */
    private synchronized HeadObjectResponse getMetadata() {
        if (metadata == null) {
            final HeadObjectRequest.Builder headRequestBuilder = HeadObjectRequest.builder()
                    .bucket(s3ObjectReference.getBucketName())
                    .key(s3ObjectReference.getKey());
            bucketOwnerProvider.getBucketOwner(s3ObjectReference.getBucketName())
                    .ifPresent(headRequestBuilder::expectedBucketOwner);
            final HeadObjectRequest request = headRequestBuilder
                    .build();
            metadata = s3Client.headObject(request);
        }

        return metadata;
    }
}
