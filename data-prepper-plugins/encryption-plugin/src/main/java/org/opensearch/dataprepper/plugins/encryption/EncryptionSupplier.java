/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.encryption.EncryptionEngine;

public interface EncryptionSupplier {
    EncryptionEngine getEncryptionEngine(String encryptionId);

    EncryptedDataKeySupplier getEncryptedDataKeySupplier(String encryptionId);
}
