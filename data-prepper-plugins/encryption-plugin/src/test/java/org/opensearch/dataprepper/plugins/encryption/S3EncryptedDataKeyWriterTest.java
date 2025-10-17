/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.InvalidRequestException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3EncryptedDataKeyWriterTest {
    private static final String TEST_BUCKET_NAME = UUID.randomUUID().toString();
    private static final String TEST_KEY_PREFIX = "test-key";
    private static final String TEST_ENCRYPTED_DATA_KEY_DIRECTORY = String.format(
            "s3://%s/%s", TEST_BUCKET_NAME, TEST_KEY_PREFIX);
    private static final String TEST_ENCRYPTED_DATA_KEY_VALUE = UUID.randomUUID().toString();

    @Mock
    private S3Client s3Client;

    @Mock
    private PutObjectResponse putObjectResponse;

    @Mock
    private SdkHttpResponse sdkHttpResponse;

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<RequestBody> requestBodyArgumentCaptor;

    @Test
    void testWriteEncryptedDataKeySuccess() throws IOException {
        final S3EncryptedDataKeyWriter objectUnderTest = new S3EncryptedDataKeyWriter(
                s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
        when(putObjectResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(true);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putObjectResponse);
        objectUnderTest.writeEncryptedDataKey(TEST_ENCRYPTED_DATA_KEY_VALUE);
        verify(s3Client).putObject(putObjectRequestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture());
        final PutObjectRequest putObjectRequest = putObjectRequestArgumentCaptor.getValue();
        final RequestBody requestBody = requestBodyArgumentCaptor.getValue();
        assertThat(putObjectRequest.bucket(), equalTo(TEST_BUCKET_NAME));
        assertThat(putObjectRequest.key(), endsWith(".key"));
        final String retrievedEncryptedDataKey = IOUtils.toString(
                requestBody.contentStreamProvider().newStream(), StandardCharsets.UTF_8);
        assertThat(retrievedEncryptedDataKey, equalTo(TEST_ENCRYPTED_DATA_KEY_VALUE));
    }

    @Test
    void testWriteEncryptedDataKeyThrowsIOExceptionWithUnSuccessfulResponse() {
        final S3EncryptedDataKeyWriter objectUnderTest = new S3EncryptedDataKeyWriter(
                s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
        when(putObjectResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(false);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putObjectResponse);
        assertThrows(IOException.class, () -> objectUnderTest.writeEncryptedDataKey(TEST_ENCRYPTED_DATA_KEY_VALUE));
    }

    @Test
    void testWriteEncryptedDataKeyThrowsWhenPutObjectThrows() {
        final S3EncryptedDataKeyWriter objectUnderTest = new S3EncryptedDataKeyWriter(
                s3Client, TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(
                InvalidRequestException.class);
        assertThrows(InvalidRequestException.class,
                () -> objectUnderTest.writeEncryptedDataKey(TEST_ENCRYPTED_DATA_KEY_VALUE));
    }
}