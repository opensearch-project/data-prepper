/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyWithoutPlaintextRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyWithoutPlaintextResponse;
import software.amazon.awssdk.services.kms.model.KmsException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.encryption.KmsEncryptionRotationHandler.ENCRYPTION_ID_TAG;
import static org.opensearch.dataprepper.plugins.encryption.KmsEncryptionRotationHandler.ENCRYPTION_ROTATION_DURATION;
import static org.opensearch.dataprepper.plugins.encryption.KmsEncryptionRotationHandler.ENCRYPTION_ROTATION_FAILURE;
import static org.opensearch.dataprepper.plugins.encryption.KmsEncryptionRotationHandler.ENCRYPTION_ROTATION_SUCCESS;

@ExtendWith(MockitoExtension.class)
class KmsEncryptionRotationHandlerTest {
    private static final String TEST_ENCRYPTION_ID = "test_encryption_id";
    private static final String TEST_KEY_ID = UUID.randomUUID().toString();
    private static final String TEST_CIPHER_TEXT_BLOB = UUID.randomUUID().toString();
    private static final Map<String, String> TEST_ENCRYPTION_CONTEXT = Map.of(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

    @Mock
    private KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    @Mock
    private EncryptedDataKeyWriter encryptedDataKeyWriter;
    @Mock
    private KmsClient kmsClient;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter encryptionRotationSuccessCounter;
    @Mock
    private Counter encryptionRotationFailureCounter;
    @Mock
    private Timer encryptionRotationTimer;
    @Mock
    private GenerateDataKeyWithoutPlaintextResponse generateDataKeyWithoutPlaintextResponse;
    @Captor
    private ArgumentCaptor<GenerateDataKeyWithoutPlaintextRequest> generateDataKeyWithoutPlaintextRequestArgumentCaptor;

    private KmsEncryptionRotationHandler objectUnderTest;

    @BeforeEach
    void setUp() {
        when(kmsEncryptionEngineConfiguration.getKeyId()).thenReturn(TEST_KEY_ID);
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(TEST_ENCRYPTION_CONTEXT);
        when(kmsEncryptionEngineConfiguration.createKmsClient()).thenReturn(kmsClient);
        when(pluginMetrics.counterWithTags(
                eq(ENCRYPTION_ROTATION_SUCCESS), eq(ENCRYPTION_ID_TAG), eq(TEST_ENCRYPTION_ID)))
                .thenReturn(encryptionRotationSuccessCounter);
        when(pluginMetrics.counterWithTags(
                eq(ENCRYPTION_ROTATION_FAILURE), eq(ENCRYPTION_ID_TAG), eq(TEST_ENCRYPTION_ID)))
                .thenReturn(encryptionRotationFailureCounter);
        when(pluginMetrics.timerWithTags(
                eq(ENCRYPTION_ROTATION_DURATION), eq(ENCRYPTION_ID_TAG), eq(TEST_ENCRYPTION_ID)))
                .thenReturn(encryptionRotationTimer);
        doAnswer(a -> {
            a.<Runnable>getArgument(0).run();
            return null;
        }).when(encryptionRotationTimer).record(any(Runnable.class));
        objectUnderTest = new KmsEncryptionRotationHandler(
                TEST_ENCRYPTION_ID, kmsEncryptionEngineConfiguration, encryptedDataKeyWriter, pluginMetrics);
    }

    @Test
    void testHandleRotationSuccess() throws Exception {
        when(kmsClient.generateDataKeyWithoutPlaintext(any(GenerateDataKeyWithoutPlaintextRequest.class)))
                .thenReturn(generateDataKeyWithoutPlaintextResponse);
        when(generateDataKeyWithoutPlaintextResponse.ciphertextBlob()).thenReturn(
                SdkBytes.fromString(TEST_CIPHER_TEXT_BLOB, StandardCharsets.UTF_8));
        assertThat(objectUnderTest.getEncryptionId(), equalTo(TEST_ENCRYPTION_ID));
        objectUnderTest.handleRotation();
        verify(kmsClient).generateDataKeyWithoutPlaintext(
                generateDataKeyWithoutPlaintextRequestArgumentCaptor.capture());
        final GenerateDataKeyWithoutPlaintextRequest generateDataKeyWithoutPlaintextRequest =
                generateDataKeyWithoutPlaintextRequestArgumentCaptor.getValue();
        assertThat(generateDataKeyWithoutPlaintextRequest.encryptionContext(), equalTo(TEST_ENCRYPTION_CONTEXT));
        assertThat(generateDataKeyWithoutPlaintextRequest.keyId(), equalTo(TEST_KEY_ID));
        assertThat(generateDataKeyWithoutPlaintextRequest.keySpec(), equalTo(DataKeySpec.AES_256));
        final String expectedEncryptedDataKey = Base64.getEncoder()
                .withoutPadding()
                .encodeToString(SdkBytes.fromString(TEST_CIPHER_TEXT_BLOB, StandardCharsets.UTF_8).asByteArray());
        verify(encryptedDataKeyWriter).writeEncryptedDataKey(eq(expectedEncryptedDataKey));
        verify(encryptionRotationSuccessCounter).increment();
        verify(encryptionRotationTimer).record(any(Runnable.class));
        verifyNoInteractions(encryptionRotationFailureCounter);
    }

    @Test
    void testHandleRotationFailure_with_kms_client_exception() {
        when(kmsClient.generateDataKeyWithoutPlaintext(any(GenerateDataKeyWithoutPlaintextRequest.class)))
                .thenThrow(KmsException.class);
        assertThat(objectUnderTest.getEncryptionId(), equalTo(TEST_ENCRYPTION_ID));
        objectUnderTest.handleRotation();
        verify(kmsClient).generateDataKeyWithoutPlaintext(
                generateDataKeyWithoutPlaintextRequestArgumentCaptor.capture());
        final GenerateDataKeyWithoutPlaintextRequest generateDataKeyWithoutPlaintextRequest =
                generateDataKeyWithoutPlaintextRequestArgumentCaptor.getValue();
        assertThat(generateDataKeyWithoutPlaintextRequest.encryptionContext(), equalTo(TEST_ENCRYPTION_CONTEXT));
        assertThat(generateDataKeyWithoutPlaintextRequest.keyId(), equalTo(TEST_KEY_ID));
        assertThat(generateDataKeyWithoutPlaintextRequest.keySpec(), equalTo(DataKeySpec.AES_256));
        verifyNoInteractions(encryptedDataKeyWriter);
        verifyNoInteractions(encryptionRotationSuccessCounter);
        verify(encryptionRotationTimer).record(any(Runnable.class));
        verify(encryptionRotationFailureCounter).increment();
    }

    @Test
    void testHandleRotationFailure_with_encrypted_data_key_writer_exception() throws Exception {
        when(kmsClient.generateDataKeyWithoutPlaintext(any(GenerateDataKeyWithoutPlaintextRequest.class)))
                .thenReturn(generateDataKeyWithoutPlaintextResponse);
        when(generateDataKeyWithoutPlaintextResponse.ciphertextBlob()).thenReturn(
                SdkBytes.fromString(TEST_CIPHER_TEXT_BLOB, StandardCharsets.UTF_8));
        assertThat(objectUnderTest.getEncryptionId(), equalTo(TEST_ENCRYPTION_ID));
        doThrow(RuntimeException.class).when(encryptedDataKeyWriter).writeEncryptedDataKey(anyString());
        objectUnderTest.handleRotation();
        verify(kmsClient).generateDataKeyWithoutPlaintext(
                generateDataKeyWithoutPlaintextRequestArgumentCaptor.capture());
        final GenerateDataKeyWithoutPlaintextRequest generateDataKeyWithoutPlaintextRequest =
                generateDataKeyWithoutPlaintextRequestArgumentCaptor.getValue();
        assertThat(generateDataKeyWithoutPlaintextRequest.encryptionContext(), equalTo(TEST_ENCRYPTION_CONTEXT));
        assertThat(generateDataKeyWithoutPlaintextRequest.keyId(), equalTo(TEST_KEY_ID));
        assertThat(generateDataKeyWithoutPlaintextRequest.keySpec(), equalTo(DataKeySpec.AES_256));
        final String expectedEncryptedDataKey = Base64.getEncoder()
                .withoutPadding()
                .encodeToString(SdkBytes.fromString(TEST_CIPHER_TEXT_BLOB, StandardCharsets.UTF_8).asByteArray());
        verify(encryptedDataKeyWriter).writeEncryptedDataKey(eq(expectedEncryptedDataKey));
        verifyNoInteractions(encryptionRotationSuccessCounter);
        verify(encryptionRotationTimer).record(any(Runnable.class));
        verify(encryptionRotationFailureCounter).increment();
    }
}