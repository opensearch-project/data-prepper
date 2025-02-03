/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.model.encryption.KeyProvider;

public class EncryptionEngineFactory {
    private final KeyProviderFactory keyProviderFactory;

    public EncryptionEngineFactory(final KeyProviderFactory keyProviderFactory) {
        this.keyProviderFactory = keyProviderFactory;
    }

    public EncryptionEngine createEncryptionEngine(final EncryptionEngineConfiguration encryptionEngineConfiguration,
                                                   final EncryptedDataKeySupplier encryptedDataKeySupplier) {
        if (encryptionEngineConfiguration instanceof KmsEncryptionEngineConfiguration) {
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration =
                    (KmsEncryptionEngineConfiguration) encryptionEngineConfiguration;
            final KeyProvider keyProvider = keyProviderFactory.createKmsKeyProvider(kmsEncryptionEngineConfiguration);
            final EncryptionContext encryptionContext = new EncryptionContext();
            return new DefaultEncryptionEngine(keyProvider, encryptionContext, encryptedDataKeySupplier);
        } else {
            throw new IllegalArgumentException("Unsupported encryption engine configuration.");
        }
    }
}
