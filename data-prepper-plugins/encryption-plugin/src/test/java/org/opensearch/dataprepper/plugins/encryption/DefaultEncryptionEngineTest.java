/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.encryption.EncryptionEnvelope;
import org.opensearch.dataprepper.model.encryption.KeyProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultEncryptionEngineTest {
    @Mock
    private EncryptionContext encryptionContext;

    @Mock
    private KeyProvider keyProvider;

    @Mock
    private EncryptedDataKeySupplier encryptedDataKeySupplier;

    @Mock
    private Cipher encryptCipher;

    @Mock
    private Cipher decryptCipher;

    private DefaultEncryptionEngine objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new DefaultEncryptionEngine(keyProvider, encryptionContext, encryptedDataKeySupplier);
    }

    @Test
    void testEncrypt_returns_expected_encryption_envelope() throws IllegalBlockSizeException, BadPaddingException {
        final byte[] testData = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testEncryptedData = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testEncryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testUnencryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        when(encryptedDataKeySupplier.retrieveValue()).thenReturn(
                Base64.getEncoder().encodeToString(testEncryptedDataKey));
        when(keyProvider.decryptKey(eq(testEncryptedDataKey))).thenReturn(testUnencryptedDataKey);
        when(encryptionContext.getOrCreateEncryptionCipher(eq(testUnencryptedDataKey))).thenReturn(encryptCipher);
        when(encryptCipher.doFinal(eq(testData))).thenReturn(testEncryptedData);

        final EncryptionEnvelope encryptionEnvelope = objectUnderTest.encrypt(testData);
        assertThat(encryptionEnvelope.getEncryptedData(), equalTo(testEncryptedData));
        assertThat(encryptionEnvelope.getEncryptedDataKey(),
                equalTo(Base64.getEncoder().encodeToString(testEncryptedDataKey)));
    }

    @ParameterizedTest
    @ValueSource(classes = {IllegalBlockSizeException.class, BadPaddingException.class})
    void testEncrypt_throws_RuntimeException_when_cipher_fails(
            final Class<? extends Exception> exception) throws IllegalBlockSizeException, BadPaddingException {
        final byte[] testData = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testEncryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testUnencryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        when(encryptedDataKeySupplier.retrieveValue()).thenReturn(
                Base64.getEncoder().encodeToString(testEncryptedDataKey));
        when(keyProvider.decryptKey(eq(testEncryptedDataKey))).thenReturn(testUnencryptedDataKey);
        when(encryptionContext.getOrCreateEncryptionCipher(eq(testUnencryptedDataKey))).thenReturn(encryptCipher);
        when(encryptCipher.doFinal(eq(testData))).thenThrow(exception);

        assertThrows(RuntimeException.class, () -> objectUnderTest.encrypt(testData));
    }

    @Test
    void testDecrypt_returns_expected_raw_data() throws IllegalBlockSizeException, BadPaddingException {
        final byte[] testData = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testEncryptedData = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testEncryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final EncryptionEnvelope encryptionEnvelope = DefaultEncryptionEnvelope.builder()
                .encryptedData(testEncryptedData)
                .encryptedDataKey(Base64.getEncoder().encodeToString(testEncryptedDataKey))
                .build();
        final byte[] testUnencryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        when(keyProvider.decryptKey(eq(testEncryptedDataKey))).thenReturn(testUnencryptedDataKey);
        when(encryptionContext.getOrCreateDecryptionCipher(eq(testUnencryptedDataKey))).thenReturn(decryptCipher);
        when(decryptCipher.doFinal(eq(testEncryptedData))).thenReturn(testData);

        assertThat(objectUnderTest.decrypt(encryptionEnvelope), equalTo(testData));
    }

    @ParameterizedTest
    @ValueSource(classes = {IllegalBlockSizeException.class, BadPaddingException.class})
    void testDecrypt_throws_RuntimeException_when_cipher_fails(
            final Class<? extends Exception> exception) throws IllegalBlockSizeException, BadPaddingException {
        final byte[] testEncryptedData = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final byte[] testEncryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        final EncryptionEnvelope encryptionEnvelope = DefaultEncryptionEnvelope.builder()
                .encryptedData(testEncryptedData)
                .encryptedDataKey(Base64.getEncoder().encodeToString(testEncryptedDataKey))
                .build();
        final byte[] testUnencryptedDataKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        when(keyProvider.decryptKey(eq(testEncryptedDataKey))).thenReturn(testUnencryptedDataKey);
        when(encryptionContext.getOrCreateDecryptionCipher(eq(testUnencryptedDataKey))).thenReturn(decryptCipher);
        when(decryptCipher.doFinal(eq(testEncryptedData))).thenThrow(exception);

        assertThrows(RuntimeException.class, () -> objectUnderTest.decrypt(encryptionEnvelope));
    }
}