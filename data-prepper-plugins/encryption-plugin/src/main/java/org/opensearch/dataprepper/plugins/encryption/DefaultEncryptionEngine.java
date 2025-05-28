/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.model.encryption.EncryptionEnvelope;
import org.opensearch.dataprepper.model.encryption.KeyProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.util.Base64;

class DefaultEncryptionEngine implements EncryptionEngine {
    private final EncryptionContext encryptionContext;
    private final KeyProvider keyProvider;
    private final EncryptedDataKeySupplier encryptedDataKeySupplier;

    public DefaultEncryptionEngine(final KeyProvider keyProvider,
                                   final EncryptionContext encryptionContext,
                                   final EncryptedDataKeySupplier encryptedDataKeySupplier) {
        this.keyProvider = keyProvider;
        this.encryptionContext = encryptionContext;
        this.encryptedDataKeySupplier = encryptedDataKeySupplier;
    }

    @Override
    public EncryptionEnvelope encrypt(byte[] data) {
        final String encryptedDataKey = encryptedDataKeySupplier.retrieveValue();
        final byte[] decodedEncryptionKey = Base64.getDecoder().decode(encryptedDataKey);
        final byte[] unencryptedDataKey = keyProvider.decryptKey(decodedEncryptionKey);
        final Cipher encryptionCipher = encryptionContext.getOrCreateEncryptionCipher(unencryptedDataKey);
        try {
            final byte[] encryptedData = encryptionCipher.doFinal(data);
            return DefaultEncryptionEnvelope.builder()
                    .encryptedData(encryptedData)
                    .encryptedDataKey(encryptedDataKey)
                    .build();
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] decrypt(EncryptionEnvelope encryptionEnvelope) {
        final byte[] decodedEncryptionKey = Base64.getDecoder().decode(encryptionEnvelope.getEncryptedDataKey());
        final byte[] unencryptedDataKey = keyProvider.decryptKey(decodedEncryptionKey);
        final Cipher decryptionCipher = encryptionContext.getOrCreateDecryptionCipher(unencryptedDataKey);
        try {
            return decryptionCipher.doFinal(encryptionEnvelope.getEncryptedData());
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }
}
