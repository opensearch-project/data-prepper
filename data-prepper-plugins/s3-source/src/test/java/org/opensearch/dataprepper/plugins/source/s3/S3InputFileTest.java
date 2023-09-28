package org.opensearch.dataprepper.plugins.source.s3;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3InputFileTest {

    private S3Client s3Client;
    private S3ObjectReference s3ObjectReference;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private String bucketName;
    private String key;
    private BucketOwnerProvider bucketOwnerProvider;

    @BeforeEach
    public void setUp() {
        s3Client = mock(S3Client.class);
        s3ObjectReference = mock(S3ObjectReference.class);
        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);
        bucketOwnerProvider = mock(BucketOwnerProvider.class);
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);
    }

    private S3InputFile createObjectUnderTest() {
        return new S3InputFile(s3Client, s3ObjectReference, bucketOwnerProvider, s3ObjectPluginMetrics);
    }

    @Test
    public void testGetLength() {
        HeadObjectResponse headObjectResponse = mock(HeadObjectResponse.class);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(headObjectResponse.contentLength()).thenReturn(12345L);

        long length = createObjectUnderTest().getLength();

        assertThat(length, equalTo(12345L));
        final ArgumentCaptor<HeadObjectRequest> headObjectRequestArgumentCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(s3Client, times(1)).headObject(headObjectRequestArgumentCaptor.capture());

        final HeadObjectRequest actualHeadObjectRequest = headObjectRequestArgumentCaptor.getValue();
        assertAll(
                () -> assertThat(actualHeadObjectRequest.bucket(), equalTo(bucketName)),
                () -> assertThat(actualHeadObjectRequest.key(), equalTo(key)),
                () -> assertThat(actualHeadObjectRequest.expectedBucketOwner(), nullValue())
        );
    }

    @Test
    public void getLength_requests_head_for_bucket_key_and_owner_when_bucket_has_owner() {
        final HeadObjectResponse headObjectResponse = mock(HeadObjectResponse.class);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(headObjectResponse.contentLength()).thenReturn(12345L);

        final String owner = UUID.randomUUID().toString();
        when(bucketOwnerProvider.getBucketOwner(bucketName)).thenReturn(Optional.of(owner));

        long length = createObjectUnderTest().getLength();

        assertThat(length, equalTo(12345L));

        final ArgumentCaptor<HeadObjectRequest> headObjectRequestArgumentCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(s3Client, times(1)).headObject(headObjectRequestArgumentCaptor.capture());

        final HeadObjectRequest actualHeadObjectRequest = headObjectRequestArgumentCaptor.getValue();
        assertAll(
                () -> assertThat(actualHeadObjectRequest.bucket(), equalTo(bucketName)),
                () -> assertThat(actualHeadObjectRequest.key(), equalTo(key)),
                () -> assertThat(actualHeadObjectRequest.expectedBucketOwner(), equalTo(owner))
        );
    }

    @Test
    public void testNewStream() {
        HeadObjectResponse headObjectResponse = mock(HeadObjectResponse.class);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);

        SeekableInputStream seekableInputStream = createObjectUnderTest().newStream();

        assertThat(seekableInputStream.getClass(), equalTo(S3InputStream.class));
    }

}
