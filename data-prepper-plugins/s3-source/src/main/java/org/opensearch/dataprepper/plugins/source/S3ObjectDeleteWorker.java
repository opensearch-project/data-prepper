package org.opensearch.dataprepper.plugins.source;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;

public class S3ObjectDeleteWorker {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectDeleteWorker.class);
    static final String S3_OBJECTS_DELETED_METRIC_NAME = "s3ObjectsDeleted";
    static final String S3_OBJECTS_DELETE_FAILED_METRIC_NAME = "s3ObjectsDeleteFailed";
    private final S3Client s3Client;
    private final Counter s3ObjectsDeletedCounter;
    private final Counter s3ObjectsDeleteFailedCounter;

    public S3ObjectDeleteWorker(final S3Client s3Client, final PluginMetrics pluginMetrics) {
        this.s3Client = s3Client;

        s3ObjectsDeletedCounter = pluginMetrics.counter(S3_OBJECTS_DELETED_METRIC_NAME);
        s3ObjectsDeleteFailedCounter = pluginMetrics.counter(S3_OBJECTS_DELETE_FAILED_METRIC_NAME);
    }

    public void deleteS3Object(final DeleteObjectRequest deleteObjectRequest) {
        try {
            final DeleteObjectResponse deleteObjectResponse = s3Client.deleteObject(deleteObjectRequest);
            if (deleteObjectResponse.sdkHttpResponse().isSuccessful()) {
                LOG.info("Deleted object: {} in S3 bucket: {}. ", deleteObjectRequest.key(), deleteObjectRequest.bucket());
                s3ObjectsDeletedCounter.increment();
            } else {
                s3ObjectsDeleteFailedCounter.increment();
            }
        } catch (final SdkException e) {
            LOG.error("Failed to delete object: {} from S3 bucket: {}. ", deleteObjectRequest.key(), deleteObjectRequest.bucket(), e);
            s3ObjectsDeleteFailedCounter.increment();
        }
    }

    public DeleteObjectRequest buildDeleteObjectRequest(final String bucketName, final String key) {
        return DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
    }

}
