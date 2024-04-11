package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferUtilities.ACCESS_DENIED;
import static org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferUtilities.INVALID_BUCKET;

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

        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), eq(requestBody));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void putObjectOrSendToDefaultBucket_with_S3Exception_that_is_not_access_denied_or_no_such_bucket_throws_exception(final boolean defaultBucketEnabled) {
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class, () -> BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, objectKey, targetBucket,
                defaultBucketEnabled ? defaultBucket : null));

        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), eq(requestBody));
    }

    @ParameterizedTest
    @ArgumentsSource(ExceptionsProvider.class)
    void putObjectOrSendToDefaultBucket_with_NoSuchBucketException_or_access_denied_sends_to_default_bucket(final Exception exception) {
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody)))
                .thenThrow(exception)
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

    private static class ExceptionsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            final S3Exception accessDeniedException = mock(S3Exception.class);
            when(accessDeniedException.getMessage()).thenReturn(UUID.randomUUID() + ACCESS_DENIED + UUID.randomUUID());

            final S3Exception invalidBucketException = mock(S3Exception.class);
            when(invalidBucketException.getMessage()).thenReturn(UUID.randomUUID() + INVALID_BUCKET + UUID.randomUUID());

            final NoSuchBucketException noSuchBucketException = mock(NoSuchBucketException.class);

            return Stream.of(
                    Arguments.arguments(noSuchBucketException),
                    Arguments.arguments(accessDeniedException),
                    Arguments.arguments(invalidBucketException)
            );
        }
    }
}
