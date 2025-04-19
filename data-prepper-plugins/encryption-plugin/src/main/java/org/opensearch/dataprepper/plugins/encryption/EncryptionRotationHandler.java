/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

public interface EncryptionRotationHandler {
    String getEncryptionId();

    void handleRotation();
}
