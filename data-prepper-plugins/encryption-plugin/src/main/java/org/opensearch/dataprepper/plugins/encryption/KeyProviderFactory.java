/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

public class KeyProviderFactory {
    public KmsKeyProvider createKmsKeyProvider(
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration) {
        return new KmsKeyProvider(kmsEncryptionEngineConfiguration);
    }
}
