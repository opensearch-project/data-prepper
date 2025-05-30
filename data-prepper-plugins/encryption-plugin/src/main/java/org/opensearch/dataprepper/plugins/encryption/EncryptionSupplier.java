/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.encryption.EncryptionEngine;

/**
 * An interface available to plugins via the encryption plugin extension which provides the encryption engine and
 * the encrypted data key supplier.
 */
public interface EncryptionSupplier {
    EncryptionEngine getEncryptionEngine(String encryptionId);

    EncryptedDataKeySupplier getEncryptedDataKeySupplier(String encryptionId);
}
