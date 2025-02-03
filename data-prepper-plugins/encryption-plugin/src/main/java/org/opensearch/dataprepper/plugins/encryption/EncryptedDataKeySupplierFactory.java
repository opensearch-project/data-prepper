/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

public class EncryptedDataKeySupplierFactory {
    public EncryptedDataKeySupplier createEncryptedDataKeySupplier(
            final EncryptionEngineConfiguration encryptionEngineConfiguration) {
        if (encryptionEngineConfiguration instanceof KmsEncryptionEngineConfiguration) {
            return new StaticEncryptedDataKeySupplier(
                    ((KmsEncryptionEngineConfiguration) encryptionEngineConfiguration).getEncryptionKey());
        } else {
            throw new IllegalArgumentException("Unsupported encryption engine configuration.");
        }
    }
}
