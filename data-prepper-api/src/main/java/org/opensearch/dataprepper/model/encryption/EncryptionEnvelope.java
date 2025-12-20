/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.encryption;

public interface EncryptionEnvelope {
    /**
     * The encrypted data.
     * @return the encrypted data as a byte array
     */
    byte[] getEncryptedData();

    /**
     * The encrypted data key.
     * @return the encrypted data key as a String
     */
    String getEncryptedDataKey();
}
