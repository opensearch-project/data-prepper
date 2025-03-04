/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3EncryptedDataKeySupplierTest {
    private static final String TEST_BUCKET_NAME = UUID.randomUUID().toString();
    private static final String TEST_KEY_PREFIX = "test-key";
    private static final String TEST_ENCRYPTED_DATA_KEY_DIRECTORY = String.format(
            "s3://%s/%s", TEST_BUCKET_NAME, TEST_KEY_PREFIX);
    private static final String TEST_ENCRYPTED_DATA_KEY_VALUE = UUID.randomUUID().toString();

    @Mock
    private S3Client s3Client;

    @Mock
    private ListObjectsV2Response listObjectsV2Response;

    @Mock
    private ResponseInputStream<GetObjectResponse> getObjectResponseResponseInputStream;

    @Mock
    private S3Object s3Object;

    @Captor
    private ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor;

    @BeforeEach
    void setUp() {
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(getObjectResponseResponseInputStream);
        when(listObjectsV2Response.contents()).thenReturn(List.of(s3Object));
        when(listObjectsV2Response.nextContinuationToken()).thenReturn(null);
        when(s3Object.size()).thenReturn(1L);
        when(s3Object.key()).thenReturn(TEST_KEY_PREFIX + "-1");
    }

    @Test
    void testRetrieveValue() {
        try (final MockedStatic<IOUtils> ioUtilsMockedStatic = mockStatic(IOUtils.class)) {
            ioUtilsMockedStatic.when(
                    () -> IOUtils.toString(eq(getObjectResponseResponseInputStream), eq(StandardCharsets.UTF_8)))
                    .thenReturn(TEST_ENCRYPTED_DATA_KEY_VALUE);
            final S3EncryptedDataKeySupplier objectUnderTest = new S3EncryptedDataKeySupplier(
                    s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
            assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY_VALUE));
        }
        verify(s3Client).listObjectsV2(listObjectsV2RequestArgumentCaptor.capture());
        final ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestArgumentCaptor.getValue();
        assertThat(listObjectsV2Request.bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(listObjectsV2Request.prefix(), equalTo(TEST_KEY_PREFIX));
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());
        final GetObjectRequest getObjectRequest = getObjectRequestArgumentCaptor.getValue();
        assertThat(getObjectRequest.bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(getObjectRequest.key(), equalTo(TEST_KEY_PREFIX + "-1"));
    }

    @Test
    void testRetrieveAfterRefreshSuccess() {
        try (final MockedStatic<IOUtils> ioUtilsMockedStatic = mockStatic(IOUtils.class)) {
            ioUtilsMockedStatic.when(
                            () -> IOUtils.toString(eq(getObjectResponseResponseInputStream), eq(StandardCharsets.UTF_8)))
                    .thenReturn(TEST_ENCRYPTED_DATA_KEY_VALUE);
            final S3EncryptedDataKeySupplier objectUnderTest = new S3EncryptedDataKeySupplier(
                    s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
            assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY_VALUE));
            final S3Object s3Object1 = mock(S3Object.class);
            when(s3Object1.key()).thenReturn(TEST_KEY_PREFIX + "-2");
            when(s3Object1.size()).thenReturn(2L);
            when(listObjectsV2Response.contents()).thenReturn(List.of(s3Object, s3Object1));
            ioUtilsMockedStatic.when(
                            () -> IOUtils.toString(eq(getObjectResponseResponseInputStream), eq(StandardCharsets.UTF_8)))
                    .thenReturn(TEST_ENCRYPTED_DATA_KEY_VALUE + "-diff");
            objectUnderTest.refresh();
            assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY_VALUE + "-diff"));
        }
        verify(s3Client, times(2)).listObjectsV2(listObjectsV2RequestArgumentCaptor.capture());
        final List<ListObjectsV2Request> listObjectsV2Requests = listObjectsV2RequestArgumentCaptor.getAllValues();
        assertThat(listObjectsV2Requests.get(0).bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(listObjectsV2Requests.get(0).prefix(), equalTo(TEST_KEY_PREFIX));
        assertThat(listObjectsV2Requests.get(1).bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(listObjectsV2Requests.get(1).prefix(), equalTo(TEST_KEY_PREFIX));
        verify(s3Client, times(2)).getObject(getObjectRequestArgumentCaptor.capture());
        final List<GetObjectRequest> getObjectRequests = getObjectRequestArgumentCaptor.getAllValues();
        assertThat(getObjectRequests.get(0).bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(getObjectRequests.get(0).key(), equalTo(TEST_KEY_PREFIX + "-1"));
        assertThat(getObjectRequests.get(1).bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(getObjectRequests.get(1).key(), equalTo(TEST_KEY_PREFIX + "-2"));
    }

    @Test
    void testRefreshThrowsIllegalStateException_when_no_data_key_files_found() {
        try (final MockedStatic<IOUtils> ioUtilsMockedStatic = mockStatic(IOUtils.class)) {
            ioUtilsMockedStatic.when(
                            () -> IOUtils.toString(eq(getObjectResponseResponseInputStream), eq(StandardCharsets.UTF_8)))
                    .thenReturn(TEST_ENCRYPTED_DATA_KEY_VALUE);
            final S3EncryptedDataKeySupplier objectUnderTest = new S3EncryptedDataKeySupplier(
                    s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
            assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY_VALUE));
            when(listObjectsV2Response.contents()).thenReturn(Collections.emptyList());
            assertThrows(IllegalStateException.class, objectUnderTest::refresh);
        }
    }

    @Test
    void testRetrieveLatestDataKeyFileContentThrowsRuntimeException_when_no_IOUtils_throws_IOException() {
        try (final MockedStatic<IOUtils> ioUtilsMockedStatic = mockStatic(IOUtils.class)) {
            ioUtilsMockedStatic.when(
                            () -> IOUtils.toString(eq(getObjectResponseResponseInputStream), eq(StandardCharsets.UTF_8)))
                    .thenThrow(IOException.class);
            assertThrows(RuntimeException.class, () -> new S3EncryptedDataKeySupplier(
                    s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY));
        }
    }
}