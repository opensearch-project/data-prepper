/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

class EncryptionContext {
    static final String AES_ALGORITHM = "AES";
    static final Integer MAXIMUM_CACHED_KEYS = 5;

    private final Cache<byte[], Cipher> encryptionCipherCache =
            Caffeine.newBuilder()
                    .maximumSize(MAXIMUM_CACHED_KEYS)
                    .build();
    private final Cache<byte[], Cipher> decryptionCipherCache =
            Caffeine.newBuilder()
                    .maximumSize(MAXIMUM_CACHED_KEYS)
                    .build();

    Cipher createEncryptionCipher(byte[] encryptionKey) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return createCipher(Cipher.ENCRYPT_MODE, encryptionKey);
    }

    Cipher createDecryptionCipher(byte[] encryptionKey) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return createCipher(Cipher.DECRYPT_MODE, encryptionKey);
    }

    private Cipher createCipher(int encryptMode, byte[] encryptionKey)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(encryptionKey, AES_ALGORITHM);
        Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
        cipher.init(encryptMode, secretKeySpec);
        return cipher;
    }

    public Cipher getOrCreateEncryptionCipher(final byte[] encryptionKey) {
        return encryptionCipherCache.asMap().computeIfAbsent(
                encryptionKey, key -> {
                    try {
                        return createEncryptionCipher(encryptionKey);
                    } catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public Cipher getOrCreateDecryptionCipher(final byte[] encryptionKey) {
        return decryptionCipherCache.asMap().computeIfAbsent(
                encryptionKey, key -> {
                    try {
                        return createDecryptionCipher(encryptionKey);
                    } catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
