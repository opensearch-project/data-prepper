/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

/**
 * An interface available to plugins via the encryption plugin extension which supplies encrypted data key.
 */
public interface EncryptedDataKeySupplier {
    /**
     * Retrieve the current encrypted data key.
     */
    String retrieveValue();

    /**
     * Refresh the encrypted data key.
     */
    void refresh();
}
