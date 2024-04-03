package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.UUID;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;
import static org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferUtilities.ACCESS_DENIED;

@ExtendWith(MockitoExtension.class)
public class BufferUtilitiesTest {

    private String defaultBucket;
    private String targetBucket;

    private String objectKey;

    @Mock
    private RequestBody requestBody;

    @Mock
    private S3Client s3Client;

    @BeforeEach
    void setup() {
        targetBucket = UUID.randomUUID().toString();
        defaultBucket = UUID.randomUUID().toString();
        objectKey = UUID.randomUUID().toString();
    }

    @Test
    void putObjectOrSendToDefaultBucket_with_no_exception_sends_to_target_bucket() {

        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenReturn(mock(PutObjectResponse.class));

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, objectKey, targetBucket, defaultBucket);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client, times(1)).putObject(argumentCaptor.capture(), eq(requestBody));

        assertThat(argumentCaptor.getAllValues().size(), equalTo(1));

        final PutObjectRequest putObjectRequest = argumentCaptor.getValue();
        assertThat(putObjectRequest.bucket(), equalTo(targetBucket));
        assertThat(putObjectRequest.key(), equalTo(objectKey));

    }

    @Test
    void putObjectOrSendToDefaultBucket_with_no_such_bucket_exception_and_null_default_bucket_throws_exception() {
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenThrow(NoSuchBucketException.class);

        assertThrows(NoSuchBucketException.class, () -> BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, objectKey, targetBucket, null));
    }

    @Test
    void putObjectOrSendToDefaultBucket_with_S3Exception_that_is_not_access_denied_or_no_such_bucket_throws_exception() {
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class, () -> BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, objectKey, targetBucket, null));
    }

    @Test
    void putObjectOrSendToDefaultBucket_with_NoSuchBucketException_sends_to_default_bucket() {
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody)))
                .thenThrow(NoSuchBucketException.class)
                .thenReturn(mock(PutObjectResponse.class));

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, objectKey, targetBucket, defaultBucket);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client, times(2)).putObject(argumentCaptor.capture(), eq(requestBody));

        assertThat(argumentCaptor.getAllValues().size(), equalTo(2));

        final List<PutObjectRequest> putObjectRequestList = argumentCaptor.getAllValues();
        final PutObjectRequest putObjectRequest = putObjectRequestList.get(0);
        assertThat(putObjectRequest.bucket(), equalTo(targetBucket));
        assertThat(putObjectRequest.key(), equalTo(objectKey));

        final PutObjectRequest defaultBucketPutObjectRequest = putObjectRequestList.get(1);
        assertThat(defaultBucketPutObjectRequest.bucket(), equalTo(defaultBucket));
        assertThat(defaultBucketPutObjectRequest.key(), equalTo(objectKey));
    }

    @Test
    void putObjectOrSendToDefaultBucket_with_S3Exception_with_access_denied_sends_to_default_bucket() {
        final S3Exception s3Exception = mock(S3Exception.class);
        when(s3Exception.getMessage()).thenReturn(UUID.randomUUID() + ACCESS_DENIED + UUID.randomUUID());

        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody)))
                .thenThrow(s3Exception)
                .thenReturn(mock(PutObjectResponse.class));

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, objectKey, targetBucket, defaultBucket);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client, times(2)).putObject(argumentCaptor.capture(), eq(requestBody));

        assertThat(argumentCaptor.getAllValues().size(), equalTo(2));

        final List<PutObjectRequest> putObjectRequestList = argumentCaptor.getAllValues();
        final PutObjectRequest putObjectRequest = putObjectRequestList.get(0);
        assertThat(putObjectRequest.bucket(), equalTo(targetBucket));
        assertThat(putObjectRequest.key(), equalTo(objectKey));

        final PutObjectRequest defaultBucketPutObjectRequest = putObjectRequestList.get(1);
        assertThat(defaultBucketPutObjectRequest.bucket(), equalTo(defaultBucket));
        assertThat(defaultBucketPutObjectRequest.key(), equalTo(objectKey));
    }
}
