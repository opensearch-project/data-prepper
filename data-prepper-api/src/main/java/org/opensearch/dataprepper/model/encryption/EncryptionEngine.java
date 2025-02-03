/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.encryption;

public interface EncryptionEngine {
    /**
     * Encrypts raw data into {@link EncryptionEnvelope}.
     *
     * @param data the raw data in bytes
     * @return returns the encryption envelope
     */
    EncryptionEnvelope encrypt(byte[] data);

    /**
     * Decrypts the encryption envelope into raw data.
     *
     * @param encryptionEnvelope the encryption envelope
     * @return returns the raw data in bytes
     */
    byte[] decrypt(EncryptionEnvelope encryptionEnvelope);
}
