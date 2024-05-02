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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    private AsyncRequestBody requestBody;

    @Mock
    private Consumer<Boolean> mockRunOnCompletion;

    @Mock
    private Consumer<Throwable> mockRunOnFailure;

    @Mock
    private S3AsyncClient s3Client;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    @BeforeEach
    void setup() {
        targetBucket = UUID.randomUUID().toString();
        defaultBucket = UUID.randomUUID().toString();
        objectKey = UUID.randomUUID().toString();
    }

    @Test
    void putObjectOrSendToDefaultBucket_with_no_exception_sends_to_target_bucket() {

        final CompletableFuture<PutObjectResponse> successfulFuture = CompletableFuture.completedFuture(mock(PutObjectResponse.class));

        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenReturn(successfulFuture);

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, mockRunOnCompletion, mockRunOnFailure, objectKey, targetBucket, defaultBucket, bucketOwnerProvider).join();

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client, times(1)).putObject(argumentCaptor.capture(), eq(requestBody));

        assertThat(argumentCaptor.getAllValues().size(), equalTo(1));

        final PutObjectRequest putObjectRequest = argumentCaptor.getValue();
        assertThat(putObjectRequest.bucket(), equalTo(targetBucket));
        assertThat(putObjectRequest.key(), equalTo(objectKey));

        verify(mockRunOnFailure, never()).accept(any());
        verify(mockRunOnCompletion).accept(true);
    }

    @Test
    void putObjectOrSendToDefaultBucket_with_no_such_bucket_exception_and_null_default_bucket_completes_with_exception() {

        final CompletableFuture<PutObjectResponse> failedFuture = CompletableFuture.failedFuture(NoSuchBucketException.builder().build());
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenReturn(failedFuture);

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, mockRunOnCompletion, mockRunOnFailure, objectKey, targetBucket, null, bucketOwnerProvider).join();

        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), eq(requestBody));
        verify(mockRunOnCompletion).accept(false);
        verify(mockRunOnFailure).accept(any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void putObjectOrSendToDefaultBucket_with_S3Exception_that_is_not_access_denied_or_no_such_bucket_completes_with_exception(final boolean defaultBucketEnabled) {
        final CompletableFuture<PutObjectResponse> failedFuture = CompletableFuture.failedFuture(new RuntimeException(UUID.randomUUID().toString()));
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenReturn(failedFuture);

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, mockRunOnCompletion, mockRunOnFailure, objectKey, targetBucket,
                defaultBucketEnabled ? defaultBucket : null, bucketOwnerProvider);

        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), eq(requestBody));
        verify(mockRunOnCompletion).accept(false);
        verify(mockRunOnFailure).accept(any());
    }

    @ParameterizedTest
    @ArgumentsSource(ExceptionsProvider.class)
    void putObjectOrSendToDefaultBucket_with_NoSuchBucketException_or_access_denied_sends_to_default_bucket(final Exception exception) {
        final CompletableFuture<PutObjectResponse> successfulFuture = CompletableFuture.completedFuture(mock(PutObjectResponse.class));
        final CompletableFuture<PutObjectResponse> failedFuture = CompletableFuture.failedFuture(exception);
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenReturn(failedFuture).thenReturn(successfulFuture);

        when(bucketOwnerProvider.getBucketOwner(anyString())).thenReturn(Optional.empty());
        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, mockRunOnCompletion, mockRunOnFailure, objectKey, targetBucket, defaultBucket, bucketOwnerProvider);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client, times(2)).putObject(argumentCaptor.capture(), eq(requestBody));

        assertThat(argumentCaptor.getAllValues().size(), equalTo(2));

        final List<PutObjectRequest> putObjectRequestList = argumentCaptor.getAllValues();
        final PutObjectRequest putObjectRequest = putObjectRequestList.get(0);
        assertThat(putObjectRequest.bucket(), equalTo(targetBucket));
        assertThat(putObjectRequest.key(), equalTo(objectKey));
        assertThat(putObjectRequest.expectedBucketOwner(), equalTo(null));

        final PutObjectRequest defaultBucketPutObjectRequest = putObjectRequestList.get(1);
        assertThat(defaultBucketPutObjectRequest.bucket(), equalTo(defaultBucket));
        assertThat(defaultBucketPutObjectRequest.key(), equalTo(objectKey));
        assertThat(defaultBucketPutObjectRequest.expectedBucketOwner(), equalTo(null));

        final InOrder inOrder = Mockito.inOrder(mockRunOnCompletion, mockRunOnFailure);

        inOrder.verify(mockRunOnFailure).accept(exception);
        inOrder.verify(mockRunOnCompletion).accept(true);
    }

    @Test
    void putObject_failing_to_send_to_bucket_and_default_bucket_completes_as_expected() {

        final NoSuchBucketException noSuchBucketException = NoSuchBucketException.builder().build();
        final RuntimeException runtimeException = new RuntimeException();
        final CompletableFuture<PutObjectResponse> failedDefaultBucket = CompletableFuture.failedFuture(runtimeException);
        final CompletableFuture<PutObjectResponse> failedFuture = CompletableFuture.failedFuture(noSuchBucketException);
        when(s3Client.putObject(any(PutObjectRequest.class), eq(requestBody))).thenReturn(failedFuture).thenReturn(failedDefaultBucket);

        final String bucketOwner = UUID.randomUUID().toString();
        final String defaultBucketOwner = UUID.randomUUID().toString();
        when(bucketOwnerProvider.getBucketOwner(targetBucket)).thenReturn(Optional.of(bucketOwner));
        when(bucketOwnerProvider.getBucketOwner(defaultBucket)).thenReturn(Optional.of(defaultBucketOwner));

        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, requestBody, mockRunOnCompletion, mockRunOnFailure, objectKey, targetBucket, defaultBucket, bucketOwnerProvider);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client, times(2)).putObject(argumentCaptor.capture(), eq(requestBody));

        assertThat(argumentCaptor.getAllValues().size(), equalTo(2));

        final List<PutObjectRequest> putObjectRequestList = argumentCaptor.getAllValues();
        final PutObjectRequest putObjectRequest = putObjectRequestList.get(0);
        assertThat(putObjectRequest.bucket(), equalTo(targetBucket));
        assertThat(putObjectRequest.key(), equalTo(objectKey));
        assertThat(putObjectRequest.expectedBucketOwner(), equalTo(bucketOwner));

        final PutObjectRequest defaultBucketPutObjectRequest = putObjectRequestList.get(1);
        assertThat(defaultBucketPutObjectRequest.bucket(), equalTo(defaultBucket));
        assertThat(defaultBucketPutObjectRequest.key(), equalTo(objectKey));
        assertThat(defaultBucketPutObjectRequest.expectedBucketOwner(), equalTo(defaultBucketOwner));

        final InOrder inOrder = Mockito.inOrder(mockRunOnCompletion, mockRunOnFailure);

        inOrder.verify(mockRunOnFailure).accept(noSuchBucketException);
        inOrder.verify(mockRunOnFailure).accept(any(CompletionException.class));
        inOrder.verify(mockRunOnCompletion).accept(false);
    }

    private static class ExceptionsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            final S3Exception accessDeniedException = mock(S3Exception.class);
            when(accessDeniedException.getMessage()).thenReturn(UUID.randomUUID() + ACCESS_DENIED + UUID.randomUUID());

            final S3Exception invalidBucketException = mock(S3Exception.class);
            when(invalidBucketException.getMessage()).thenReturn(UUID.randomUUID() + INVALID_BUCKET + UUID.randomUUID());

            final NoSuchBucketException noSuchBucketException = mock(NoSuchBucketException.class);
            when(noSuchBucketException.getCause()).thenReturn(noSuchBucketException);

            return Stream.of(
                    Arguments.arguments(noSuchBucketException),
                    Arguments.arguments(accessDeniedException),
                    Arguments.arguments(invalidBucketException)
            );
        }
    }
}
