/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KmsKeyProviderTest {
    @Mock
    private KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    @Mock
    private KmsClient kmsClient;
    @Mock
    private DecryptResponse decryptResponse;
    @Captor
    private ArgumentCaptor<DecryptRequest> decryptRequestArgumentCaptor;

    private KmsKeyProvider createObjectUnderTest() {
        return new KmsKeyProvider(kmsEncryptionEngineConfiguration);
    }

    @BeforeEach
    void setUp() {
        when(kmsEncryptionEngineConfiguration.createKmsClient()).thenReturn(kmsClient);
    }

    @Test
    void apply_returns_plaintext_from_decrypt_request() {
        final String testDecryptedDataKey = "test_decrypted_data_key";
        final String testKeyId = UUID.randomUUID().toString();
        final String testContextKey = UUID.randomUUID().toString();
        final String testContextValue = UUID.randomUUID().toString();
        final Map<String, String> testEncryptionContext = Map.of(
                testContextKey, testContextValue);
        when(kmsEncryptionEngineConfiguration.getKeyId()).thenReturn(testKeyId);
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(testEncryptionContext);
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));
        final String testEncryptionKey = "test_encryption_key";
        final String base64EncodedEncryptionKey = Base64.getEncoder().encodeToString(
                testEncryptionKey.getBytes(StandardCharsets.UTF_8));
        KmsKeyProvider objectUnderTest = createObjectUnderTest();
        final byte[] actualBytes = objectUnderTest.apply(base64EncodedEncryptionKey);

        assertThat(actualBytes, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));
        verify(kmsClient).decrypt(decryptRequestArgumentCaptor.capture());
        final DecryptRequest decryptRequest = decryptRequestArgumentCaptor.getValue();
        assertThat(decryptRequest.keyId(), equalTo(testKeyId));
        assertThat(decryptRequest.ciphertextBlob(),
                equalTo(SdkBytes.fromByteArray(testEncryptionKey.getBytes(StandardCharsets.UTF_8))));
        assertThat(decryptRequest.encryptionContext(), equalTo(testEncryptionContext));
    }

    @Test
    void apply_calls_decrypt_with_correct_values_when_encryption_context_is_null() {
        final String testDecryptedDataKey = "test_decrypted_data_key";
        final String testKeyId = UUID.randomUUID().toString();

        when(kmsEncryptionEngineConfiguration.getKeyId()).thenReturn(testKeyId);
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(null);
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));
        final String testEncryptionKey = "test_encryption_key";
        final String base64EncodedEncryptionKey = Base64.getEncoder().encodeToString(
                testEncryptionKey.getBytes(StandardCharsets.UTF_8));
        KmsKeyProvider objectUnderTest = createObjectUnderTest();
        final byte[] actualBytes = objectUnderTest.apply(base64EncodedEncryptionKey);

        assertThat(actualBytes, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));
        verify(kmsClient).decrypt(decryptRequestArgumentCaptor.capture());
        final DecryptRequest decryptRequest = decryptRequestArgumentCaptor.getValue();
        assertThat(decryptRequest.keyId(), equalTo(testKeyId));
        assertThat(decryptRequest.ciphertextBlob(),
                equalTo(SdkBytes.fromByteArray(testEncryptionKey.getBytes(StandardCharsets.UTF_8))));
        assertThat(decryptRequest.encryptionContext(), equalTo(Collections.emptyMap()));
    }

    @Test
    void apply_on_the_same_encryption_key_returns_result_from_cache() {
        final String testDecryptedDataKey = "test_decrypted_data_key";
        final String testKeyId = UUID.randomUUID().toString();
        final String testContextKey = UUID.randomUUID().toString();
        final String testContextValue = UUID.randomUUID().toString();
        final Map<String, String> testEncryptionContext = Map.of(
                testContextKey, testContextValue);
        when(kmsEncryptionEngineConfiguration.getKeyId()).thenReturn(testKeyId);
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(testEncryptionContext);
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));
        final String testEncryptionKey = "test_encryption_key";
        final String base64EncodedEncryptionKey = Base64.getEncoder().encodeToString(
                testEncryptionKey.getBytes(StandardCharsets.UTF_8));
        KmsKeyProvider objectUnderTest = createObjectUnderTest();
        final byte[] retrievedBytesFirstCall = objectUnderTest.apply(base64EncodedEncryptionKey);
        assertThat(retrievedBytesFirstCall, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));
        verify(kmsClient).decrypt(decryptRequestArgumentCaptor.capture());
        final DecryptRequest decryptRequest = decryptRequestArgumentCaptor.getValue();
        assertThat(decryptRequest.keyId(), equalTo(testKeyId));
        assertThat(decryptRequest.ciphertextBlob(),
                equalTo(SdkBytes.fromByteArray(testEncryptionKey.getBytes(StandardCharsets.UTF_8))));
        assertThat(decryptRequest.encryptionContext(), equalTo(testEncryptionContext));

        final byte[] retrievedBytesSecondCall = objectUnderTest.apply(base64EncodedEncryptionKey);
        assertThat(retrievedBytesSecondCall, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));
        verifyNoMoreInteractions(kmsClient);
    }
}