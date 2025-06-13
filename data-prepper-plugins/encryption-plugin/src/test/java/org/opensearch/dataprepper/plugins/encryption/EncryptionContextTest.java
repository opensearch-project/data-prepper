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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.encryption.EncryptionContext.AES_ALGORITHM;

@ExtendWith(MockitoExtension.class)
class EncryptionContextTest {
    @Mock
    private Cipher encryptionCipher;

    @Mock
    private Cipher decryptionCipher;

    @Captor
    private ArgumentCaptor<SecretKeySpec> secretKeySpecArgumentCaptor;

    private EncryptionContext objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new EncryptionContext();
    }

    @Test
    void testGetOrCreateEncryptionCipher_creates_cipher() throws InvalidKeyException {
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenReturn(encryptionCipher);
            assertThat(objectUnderTest.getOrCreateEncryptionCipher(encryptionKey), is(encryptionCipher));
        }
        verify(encryptionCipher).init(eq(Cipher.ENCRYPT_MODE), secretKeySpecArgumentCaptor.capture());
        final SecretKeySpec secretKeySpec = secretKeySpecArgumentCaptor.getValue();
        assertThat(secretKeySpec.getAlgorithm(), equalTo(AES_ALGORITHM));
        assertThat(secretKeySpec.getEncoded(), equalTo(encryptionKey));
    }

    @ParameterizedTest
    @ValueSource(classes = {NoSuchAlgorithmException.class, NoSuchPaddingException.class})
    void testGetOrCreateEncryptionCipher_throws_runtime_exception_when_get_cipher_instance_fails(
            final Class<? extends Exception> exception) {
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenThrow(exception);
            assertThrows(RuntimeException.class, () -> objectUnderTest.getOrCreateEncryptionCipher(encryptionKey));
        }
    }

    @Test
    void testGetOrCreateEncryptionCipher_throws_runtime_exception_when_cipher_init_fails() throws InvalidKeyException {
        doThrow(InvalidKeyException.class).when(encryptionCipher).init(anyInt(), any(Key.class));
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenReturn(encryptionCipher);
            assertThrows(RuntimeException.class, () -> objectUnderTest.getOrCreateEncryptionCipher(encryptionKey));
        }
    }

    @Test
    void testGetOrCreateEncryptionCipher_gets_cached_cipher() throws InvalidKeyException {
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenReturn(encryptionCipher);
            assertThat(objectUnderTest.getOrCreateEncryptionCipher(encryptionKey), is(encryptionCipher));
        }
        objectUnderTest.getOrCreateEncryptionCipher(encryptionKey);
        verify(encryptionCipher, times(1)).init(eq(Cipher.ENCRYPT_MODE), secretKeySpecArgumentCaptor.capture());
        final SecretKeySpec secretKeySpec = secretKeySpecArgumentCaptor.getValue();
        assertThat(secretKeySpec.getAlgorithm(), equalTo(AES_ALGORITHM));
        assertThat(secretKeySpec.getEncoded(), equalTo(encryptionKey));
    }

    @Test
    void testGetOrCreateDecryptionCipher_creates_cipher() throws InvalidKeyException {
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenReturn(decryptionCipher);
            assertThat(objectUnderTest.getOrCreateDecryptionCipher(encryptionKey), is(decryptionCipher));
        }
        verify(decryptionCipher).init(eq(Cipher.DECRYPT_MODE), secretKeySpecArgumentCaptor.capture());
        final SecretKeySpec secretKeySpec = secretKeySpecArgumentCaptor.getValue();
        assertThat(secretKeySpec.getAlgorithm(), equalTo(AES_ALGORITHM));
        assertThat(secretKeySpec.getEncoded(), equalTo(encryptionKey));
    }

    @ParameterizedTest
    @ValueSource(classes = {NoSuchAlgorithmException.class, NoSuchPaddingException.class})
    void testGetOrCreateDecryptionCipher_throws_runtime_exception_when_get_cipher_instance_fails(
            final Class<? extends Exception> exception) {
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenThrow(exception);
            assertThrows(RuntimeException.class, () -> objectUnderTest.getOrCreateDecryptionCipher(encryptionKey));
        }
    }

    @Test
    void testGetOrCreateDecryptionCipher_throws_runtime_exception_when_cipher_init_fails() throws InvalidKeyException {
        doThrow(InvalidKeyException.class).when(decryptionCipher).init(anyInt(), any(Key.class));
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenReturn(decryptionCipher);
            assertThrows(RuntimeException.class, () -> objectUnderTest.getOrCreateDecryptionCipher(encryptionKey));
        }
    }

    @Test
    void testGetOrCreateDecryptionCipher_gets_cached_cipher() throws InvalidKeyException {
        final byte[] encryptionKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (MockedStatic<Cipher> cipherMockedStatic = mockStatic(Cipher.class)) {
            cipherMockedStatic.when(() -> Cipher.getInstance(eq(AES_ALGORITHM))).thenReturn(decryptionCipher);
            assertThat(objectUnderTest.getOrCreateDecryptionCipher(encryptionKey), is(decryptionCipher));
        }
        objectUnderTest.getOrCreateDecryptionCipher(encryptionKey);
        verify(decryptionCipher, times(1)).init(eq(Cipher.DECRYPT_MODE), secretKeySpecArgumentCaptor.capture());
        final SecretKeySpec secretKeySpec = secretKeySpecArgumentCaptor.getValue();
        assertThat(secretKeySpec.getAlgorithm(), equalTo(AES_ALGORITHM));
        assertThat(secretKeySpec.getEncoded(), equalTo(encryptionKey));
    }
}