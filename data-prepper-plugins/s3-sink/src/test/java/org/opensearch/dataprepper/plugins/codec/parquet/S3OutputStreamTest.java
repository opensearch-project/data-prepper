/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.parquet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class S3OutputStreamTest {

    @Mock
    private S3AsyncClient s3Client;

    @Mock
    private Consumer<Boolean> runOnCompletion;

    @Mock
    private Consumer<Throwable> runOnError;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    private String bucket;

    private String defaultBucket;

    private String objectKey;

    @BeforeEach
    void setup() {
        bucket = UUID.randomUUID().toString();
        defaultBucket = UUID.randomUUID().toString();
        objectKey = UUID.randomUUID().toString();
    }

    private S3OutputStream createObjectUnderTest() {
        return new S3OutputStream(s3Client, () -> bucket, () -> objectKey, defaultBucket, bucketOwnerProvider);
    }

    @Test
    void close_creates_and_completes_multi_part_upload() {

        final byte[] bytes = new byte[25];
        final String uploadId = UUID.randomUUID().toString();
        final CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(uploadId);
        final CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadResponseCompletableFuture = CompletableFuture.completedFuture(createMultipartUploadResponse);
        when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(createMultipartUploadResponseCompletableFuture);

        final UploadPartResponse uploadPartResponse = mock(UploadPartResponse.class);
        final CompletableFuture<UploadPartResponse> uploadPartResponseCompletableFuture = CompletableFuture.completedFuture(uploadPartResponse);
        when(uploadPartResponse.eTag()).thenReturn(UUID.randomUUID().toString());
        when(s3Client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenReturn(uploadPartResponseCompletableFuture);

        when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(CompletableFuture.completedFuture(mock(CompleteMultipartUploadResponse.class)));


        final S3OutputStream s3OutputStream = createObjectUnderTest();

        s3OutputStream.write(bytes);

        final CompletableFuture<?> completableFuture = s3OutputStream.close(runOnCompletion, runOnError);
        assertThat(completableFuture, notNullValue());
        assertThat(completableFuture.isDone(), equalTo(true));
        assertThat(completableFuture.isCompletedExceptionally(), equalTo(false));

        final ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(s3Client).createMultipartUpload(createMultipartUploadRequestArgumentCaptor.capture());

        final CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestArgumentCaptor.getValue();
        assertThat(createMultipartUploadRequest, notNullValue());
        assertThat(createMultipartUploadRequest.bucket(), equalTo(bucket));
        assertThat(createMultipartUploadRequest.key(), equalTo(objectKey));

        final ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(s3Client).uploadPart(uploadPartRequestArgumentCaptor.capture(), any(AsyncRequestBody.class));

        final UploadPartRequest uploadPartRequest = uploadPartRequestArgumentCaptor.getValue();
        assertThat(uploadPartRequest, notNullValue());
        assertThat(uploadPartRequest.bucket(), equalTo(bucket));
        assertThat(uploadPartRequest.uploadId(), equalTo(uploadId));
        assertThat(uploadPartRequest.key(), equalTo(objectKey));

        final ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        verify(s3Client).completeMultipartUpload(completeMultipartUploadRequestArgumentCaptor.capture());

        final CompleteMultipartUploadRequest completeMultipartUploadRequest = completeMultipartUploadRequestArgumentCaptor.getValue();
        assertThat(completeMultipartUploadRequest, notNullValue());
        assertThat(completeMultipartUploadRequest.bucket(), equalTo(bucket));
        assertThat(completeMultipartUploadRequest.key(), equalTo(objectKey));

        verify(runOnCompletion).accept(true);
    }

    @Test
    void close_with_no_such_bucket_exception_creates_and_completes_multi_part_upload_for_default_bucket() {

        final byte[] bytes = new byte[25];
        final String uploadId = UUID.randomUUID().toString();
        final CompletableFuture<CreateMultipartUploadResponse> failedFuture = CompletableFuture.failedFuture(NoSuchBucketException.builder().build());

        final CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(uploadId);
        final CompletableFuture<CreateMultipartUploadResponse> successfulFuture = CompletableFuture.completedFuture(createMultipartUploadResponse);

        when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(failedFuture)
                .thenReturn(successfulFuture);

        final UploadPartResponse uploadPartResponse = mock(UploadPartResponse.class);
        final CompletableFuture<UploadPartResponse> uploadPartResponseCompletableFuture = CompletableFuture.completedFuture(uploadPartResponse);
        when(uploadPartResponse.eTag()).thenReturn(UUID.randomUUID().toString());
        when(s3Client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenReturn(uploadPartResponseCompletableFuture);


        when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(CompletableFuture.completedFuture(mock(CompleteMultipartUploadResponse.class)));


        final S3OutputStream s3OutputStream = createObjectUnderTest();

        s3OutputStream.write(bytes);

        final CompletableFuture<?> completableFuture = s3OutputStream.close(runOnCompletion, runOnError);
        assertThat(completableFuture, notNullValue());
        assertThat(completableFuture.isDone(), equalTo(true));
        assertThat(completableFuture.isCompletedExceptionally(), equalTo(false));

        final ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(s3Client, times(2)).createMultipartUpload(createMultipartUploadRequestArgumentCaptor.capture());

        final List<CreateMultipartUploadRequest> createMultipartUploadRequests = createMultipartUploadRequestArgumentCaptor.getAllValues();
        assertThat(createMultipartUploadRequests.size(), equalTo(2));

        final CreateMultipartUploadRequest failedCreateMultiPartUploadRequest = createMultipartUploadRequests.get(0);
        assertThat(failedCreateMultiPartUploadRequest, notNullValue());
        assertThat(failedCreateMultiPartUploadRequest.bucket(), equalTo(bucket));
        assertThat(failedCreateMultiPartUploadRequest.key(), equalTo(objectKey));

        final CreateMultipartUploadRequest defaultBucketCreateMultiPartUploadRequest = createMultipartUploadRequests.get(1);
        assertThat(defaultBucketCreateMultiPartUploadRequest, notNullValue());
        assertThat(defaultBucketCreateMultiPartUploadRequest.bucket(), equalTo(defaultBucket));
        assertThat(defaultBucketCreateMultiPartUploadRequest.key(), equalTo(objectKey));

        final ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(s3Client).uploadPart(uploadPartRequestArgumentCaptor.capture(), any(AsyncRequestBody.class));

        final UploadPartRequest uploadPartRequest = uploadPartRequestArgumentCaptor.getValue();
        assertThat(uploadPartRequest, notNullValue());
        assertThat(uploadPartRequest.bucket(), equalTo(defaultBucket));
        assertThat(uploadPartRequest.uploadId(), equalTo(uploadId));
        assertThat(uploadPartRequest.key(), equalTo(objectKey));

        final ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        verify(s3Client).completeMultipartUpload(completeMultipartUploadRequestArgumentCaptor.capture());

        final CompleteMultipartUploadRequest completeMultipartUploadRequest = completeMultipartUploadRequestArgumentCaptor.getValue();
        assertThat(completeMultipartUploadRequest, notNullValue());
        assertThat(completeMultipartUploadRequest.bucket(), equalTo(defaultBucket));
        assertThat(completeMultipartUploadRequest.key(), equalTo(objectKey));

        verify(runOnCompletion).accept(true);
    }

    @Test
    void close_with_upload_part_exception_completes_with_failure_and_returns_null() {
        final byte[] bytes = new byte[25];
        final String uploadId = UUID.randomUUID().toString();
        final CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(uploadId);
        final CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadResponseCompletableFuture = CompletableFuture.completedFuture(createMultipartUploadResponse);
        when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(createMultipartUploadResponseCompletableFuture);

        final RuntimeException mockException = mock(RuntimeException.class);
        final CompletableFuture<UploadPartResponse> uploadPartResponseCompletableFuture = CompletableFuture.failedFuture(mockException);
        when(s3Client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenReturn(uploadPartResponseCompletableFuture);

        final S3OutputStream s3OutputStream = createObjectUnderTest();

        s3OutputStream.write(bytes);

        final CompletableFuture<?> completableFuture = s3OutputStream.close(runOnCompletion, runOnError);
        assertThat(completableFuture, equalTo(null));

        final ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(s3Client).createMultipartUpload(createMultipartUploadRequestArgumentCaptor.capture());

        final CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestArgumentCaptor.getValue();
        assertThat(createMultipartUploadRequest, notNullValue());
        assertThat(createMultipartUploadRequest.bucket(), equalTo(bucket));
        assertThat(createMultipartUploadRequest.key(), equalTo(objectKey));

        final ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(s3Client).uploadPart(uploadPartRequestArgumentCaptor.capture(), any(AsyncRequestBody.class));

        final UploadPartRequest uploadPartRequest = uploadPartRequestArgumentCaptor.getValue();
        assertThat(uploadPartRequest, notNullValue());
        assertThat(uploadPartRequest.bucket(), equalTo(bucket));
        assertThat(uploadPartRequest.uploadId(), equalTo(uploadId));
        assertThat(uploadPartRequest.key(), equalTo(objectKey));

        verify(runOnCompletion).accept(false);

        final ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(runOnError).accept(argumentCaptor.capture());

        final Throwable exception = argumentCaptor.getValue();
        assertThat(exception, notNullValue());
        assertThat(exception, instanceOf(CompletionException.class));
        assertThat(exception.getCause(), equalTo(mockException));
    }
}
