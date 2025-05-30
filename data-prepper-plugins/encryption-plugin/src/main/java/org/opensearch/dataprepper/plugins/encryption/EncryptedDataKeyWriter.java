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
     */
    void writeEncryptedDataKey(String encryptedDataKey) throws Exception;
}
