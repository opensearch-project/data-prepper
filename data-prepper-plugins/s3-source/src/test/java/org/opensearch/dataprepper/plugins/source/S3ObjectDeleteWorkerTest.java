package org.opensearch.dataprepper.plugins.source;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.S3ObjectDeleteWorker.S3_OBJECTS_DELETED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.S3ObjectDeleteWorker.S3_OBJECTS_DELETE_FAILED_METRIC_NAME;

@ExtendWith(MockitoExtension.class)
class S3ObjectDeleteWorkerTest {
    @Mock
    private S3Client s3Client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DeleteObjectRequest deleteObjectRequest;

    @Mock
    private DeleteObjectResponse deleteObjectResponse;

    @Mock
    private SdkHttpResponse sdkHttpResponse;

    private Counter s3ObjectsDeletedCounter;

    private Counter s3ObjectsDeleteFailedCounter;

    private S3ObjectDeleteWorker createObjectUnderTest() {
        s3ObjectsDeletedCounter = mock(Counter.class);
        s3ObjectsDeleteFailedCounter = mock(Counter.class);;

        when(pluginMetrics.counter(S3_OBJECTS_DELETED_METRIC_NAME)).thenReturn(s3ObjectsDeletedCounter);
        when(pluginMetrics.counter(S3_OBJECTS_DELETE_FAILED_METRIC_NAME)).thenReturn(s3ObjectsDeleteFailedCounter);

        return new S3ObjectDeleteWorker(s3Client, pluginMetrics);
    }

    @Test
    void buildDeleteObjectRequest_test_should_create_deleteObjectRequest_with_correct_bucket_and_key() {
        final String testBucketName = UUID.randomUUID().toString();
        final String testKey = UUID.randomUUID().toString();
        final S3ObjectDeleteWorker objectUnderTest = createObjectUnderTest();
        final DeleteObjectRequest deleteObjectRequest = objectUnderTest.buildDeleteObjectRequest(testBucketName, testKey);

        assertThat(deleteObjectRequest.bucket(), equalTo(testBucketName));
        assertThat(deleteObjectRequest.key(), equalTo(testKey));
    }

    @Test
    void deleteS3Object_with_successful_response_should_increment_success_metric() {
        final S3ObjectDeleteWorker objectUnderTest = createObjectUnderTest();
        when(s3Client.deleteObject(deleteObjectRequest)).thenReturn(deleteObjectResponse);
        when(deleteObjectResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(true);

        objectUnderTest.deleteS3Object(deleteObjectRequest);

        verify(s3Client).deleteObject(deleteObjectRequest);
        verify(s3ObjectsDeletedCounter).increment();
        verifyNoMoreInteractions(s3ObjectsDeleteFailedCounter);
    }

    @Test
    void deleteS3Object_with_failed_response_should_increment_failed_metric() {
        final S3ObjectDeleteWorker objectUnderTest = createObjectUnderTest();
        when(s3Client.deleteObject(deleteObjectRequest)).thenReturn(deleteObjectResponse);
        when(deleteObjectResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(false);

        objectUnderTest.deleteS3Object(deleteObjectRequest);

        verify(s3Client).deleteObject(deleteObjectRequest);
        verify(s3ObjectsDeleteFailedCounter).increment();
        verifyNoMoreInteractions(s3ObjectsDeletedCounter);
    }

    @Test
    void deleteS3Object_with_exception_should_increment_failed_metric() {
        final S3ObjectDeleteWorker objectUnderTest = createObjectUnderTest();
        when(s3Client.deleteObject(deleteObjectRequest)).thenThrow(SdkException.class);

        objectUnderTest.deleteS3Object(deleteObjectRequest);

        verify(s3Client).deleteObject(deleteObjectRequest);
        verify(s3ObjectsDeleteFailedCounter).increment();
        verifyNoMoreInteractions(s3ObjectsDeletedCounter);
    }

}