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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.util.UUID;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3OutputStreamTest {

    @Mock
    private S3Client s3Client;

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
        return new S3OutputStream(s3Client, () -> bucket, () -> objectKey, defaultBucket);
    }

    @Test
    void close_creates_and_completes_multi_part_upload() {

        final byte[] bytes = new byte[25];
        final String uploadId = UUID.randomUUID().toString();
        final CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(uploadId);
        when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(createMultipartUploadResponse);

        final UploadPartResponse uploadPartResponse = mock(UploadPartResponse.class);
        when(uploadPartResponse.eTag()).thenReturn(UUID.randomUUID().toString());
        when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(uploadPartResponse);

        when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(mock(CompleteMultipartUploadResponse.class));


        final S3OutputStream s3OutputStream = createObjectUnderTest();

        s3OutputStream.write(bytes);

        s3OutputStream.close();

        final ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(s3Client).createMultipartUpload(createMultipartUploadRequestArgumentCaptor.capture());

        final CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestArgumentCaptor.getValue();
        assertThat(createMultipartUploadRequest, notNullValue());
        assertThat(createMultipartUploadRequest.bucket(), equalTo(bucket));
        assertThat(createMultipartUploadRequest.key(), equalTo(objectKey));

        final ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(s3Client).uploadPart(uploadPartRequestArgumentCaptor.capture(), any(RequestBody.class));

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
    }

    @Test
    void close_with_no_such_bucket_exception_creates_and_completes_multi_part_upload_for_default_bucket() {

        final byte[] bytes = new byte[25];
        final String uploadId = UUID.randomUUID().toString();
        final CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(uploadId);
        when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenThrow(NoSuchBucketException.class)
                .thenReturn(createMultipartUploadResponse);

        final UploadPartResponse uploadPartResponse = mock(UploadPartResponse.class);
        when(uploadPartResponse.eTag()).thenReturn(UUID.randomUUID().toString());
        when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(uploadPartResponse);

        when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(mock(CompleteMultipartUploadResponse.class));


        final S3OutputStream s3OutputStream = createObjectUnderTest();

        s3OutputStream.write(bytes);

        s3OutputStream.close();

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
        verify(s3Client).uploadPart(uploadPartRequestArgumentCaptor.capture(), any(RequestBody.class));

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
    }
}
