/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.encryption;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter succeededCounter;
    @Mock
    private Counter failedCounter;
    @Captor
    private ArgumentCaptor<DecryptRequest> decryptRequestArgumentCaptor;
    private String testDecryptedDataKey;
    private String testKeyId;
    private String testContextKey;
    private String testContextValue;
    private String testEncryptionKey;

    private KmsKeyProvider createObjectUnderTest() {
        return new KmsKeyProvider(kmsEncryptionEngineConfiguration, pluginMetrics);
    }

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter("kmsRequestsSucceeded")).thenReturn(succeededCounter);
        when(pluginMetrics.counter("kmsRequestsFailed")).thenReturn(failedCounter);
        when(kmsEncryptionEngineConfiguration.createKmsClient()).thenReturn(kmsClient);

        testDecryptedDataKey = UUID.randomUUID().toString();
        testKeyId = UUID.randomUUID().toString();
        testContextKey = UUID.randomUUID().toString();
        testContextValue = UUID.randomUUID().toString();

        testEncryptionKey = UUID.randomUUID().toString();

        when(kmsEncryptionEngineConfiguration.getKeyId()).thenReturn(testKeyId);
    }

    @Test
    void decryptKey_returns_plaintext_from_decrypt_request() {
        final Map<String, String> testEncryptionContext = Map.of(
                testContextKey, testContextValue);
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(testEncryptionContext);
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));

        final KmsKeyProvider objectUnderTest = createObjectUnderTest();
        final byte[] actualBytes = objectUnderTest.decryptKey(testEncryptionKey.getBytes(StandardCharsets.UTF_8));

        assertThat(actualBytes, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));
        verify(kmsClient).decrypt(decryptRequestArgumentCaptor.capture());
        final DecryptRequest decryptRequest = decryptRequestArgumentCaptor.getValue();
        assertThat(decryptRequest.keyId(), equalTo(testKeyId));
        assertThat(decryptRequest.ciphertextBlob(),
                equalTo(SdkBytes.fromByteArray(testEncryptionKey.getBytes(StandardCharsets.UTF_8))));
        assertThat(decryptRequest.encryptionContext(), equalTo(testEncryptionContext));
    }

    @Test
    void decryptKey_calls_decrypt_with_correct_values_when_encryption_context_is_null() {
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(null);
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));

        final KmsKeyProvider objectUnderTest = createObjectUnderTest();
        final byte[] actualBytes = objectUnderTest.decryptKey(testEncryptionKey.getBytes(StandardCharsets.UTF_8));

        assertThat(actualBytes, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));
        verify(kmsClient).decrypt(decryptRequestArgumentCaptor.capture());
        final DecryptRequest decryptRequest = decryptRequestArgumentCaptor.getValue();
        assertThat(decryptRequest.keyId(), equalTo(testKeyId));
        assertThat(decryptRequest.ciphertextBlob(),
                equalTo(SdkBytes.fromByteArray(testEncryptionKey.getBytes(StandardCharsets.UTF_8))));
        assertThat(decryptRequest.encryptionContext(), equalTo(Collections.emptyMap()));
    }

    @Test
    void decryptKey_on_the_same_encryption_key_returns_result_from_cache() {
        final Map<String, String> testEncryptionContext = Map.of(
                testContextKey, testContextValue);
        when(kmsEncryptionEngineConfiguration.getEncryptionContext()).thenReturn(testEncryptionContext);
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));
        final byte[] testEncryptionKeyBytes = testEncryptionKey.getBytes(StandardCharsets.UTF_8);

        final KmsKeyProvider objectUnderTest = createObjectUnderTest();
        final byte[] retrievedBytesFirstCall = objectUnderTest.decryptKey(
                testEncryptionKeyBytes);
        assertThat(retrievedBytesFirstCall, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));

        final byte[] retrievedBytesSecondCall = objectUnderTest.decryptKey(
                testEncryptionKey.getBytes(StandardCharsets.UTF_8));
        assertThat(retrievedBytesSecondCall, equalTo(testDecryptedDataKey.getBytes(StandardCharsets.UTF_8)));

        verify(kmsClient).decrypt(decryptRequestArgumentCaptor.capture());
        final DecryptRequest decryptRequest = decryptRequestArgumentCaptor.getValue();
        assertThat(decryptRequest.keyId(), equalTo(testKeyId));
        assertThat(decryptRequest.ciphertextBlob(),
                equalTo(SdkBytes.fromByteArray(testEncryptionKeyBytes)));
        assertThat(decryptRequest.encryptionContext(), equalTo(testEncryptionContext));
        verifyNoMoreInteractions(kmsClient);
    }

    @Test
    void decryptKey_should_increment_succeeded_counter() {
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(decryptResponse);
        when(decryptResponse.plaintext()).thenReturn(
                SdkBytes.fromString(testDecryptedDataKey, StandardCharsets.UTF_8));

        createObjectUnderTest().decryptKey(testEncryptionKey.getBytes(StandardCharsets.UTF_8));

        verify(succeededCounter).increment();
        verifyNoInteractions(failedCounter);
    }

    @ParameterizedTest
    @ValueSource(classes = {SdkClientException.class, KmsException.class, StsException.class, RuntimeException.class})
    void decryptKey_should_throw_exception_and_increment_counter_on_failure(final Class<Throwable> exception) {
        when(kmsClient.decrypt(any(DecryptRequest.class))).thenThrow(exception);

        final KmsKeyProvider objectUnderTest = createObjectUnderTest();

        final byte[] testEncryptionKeyBytes = testEncryptionKey.getBytes(StandardCharsets.UTF_8);
        assertThrows(exception, () -> objectUnderTest.decryptKey(testEncryptionKeyBytes));

        verify(failedCounter).increment();
        verifyNoInteractions(succeededCounter);
    }
}