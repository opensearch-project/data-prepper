/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.encryption;

public interface EncryptionEnvelope {
    /**
     * The encrypted data.
     */
    byte[] getEncryptedData();

    /**
     * The encrypted data key.
     */
    String getEncryptedDataKey();
}
