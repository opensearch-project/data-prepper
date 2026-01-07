/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

/**
 * An interface available to plugins via the encryption plugin extension which handles encrypted data key rotation.
 */
public interface EncryptionRotationHandler {
    /**
     * Retrieves encryption configuration ID.
     * @return the encryption configuration ID
     */
    String getEncryptionId();

    /**
     * Handles encrypted data key rotation.
     */
    void handleRotation();
}
