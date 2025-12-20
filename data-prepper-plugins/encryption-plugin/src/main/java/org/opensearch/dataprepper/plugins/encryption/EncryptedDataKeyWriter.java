/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

/**
 * An interface available to plugins via the encryption plugin extension which writes encrypted data key to storage.
 */
public interface EncryptedDataKeyWriter {
    /**
     * Writes encrypted data key into storage.
     * @param encryptedDataKey the encrypted data key to write to storage
     * @throws Exception if an error occurs while writing the encrypted data key
     */
    void writeEncryptedDataKey(String encryptedDataKey) throws Exception;
}
