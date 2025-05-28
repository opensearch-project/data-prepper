/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

class KeyProviderFactory {

    public static KeyProviderFactory create() {
        return new KeyProviderFactory();
    }

    private KeyProviderFactory() {}

    public KmsKeyProvider createKmsKeyProvider(
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration) {
        return new KmsKeyProvider(kmsEncryptionEngineConfiguration);
    }
}
