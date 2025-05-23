/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

public interface EncryptedDataKeyWriter {
    void writeEncryptedDataKey(String encryptedDataKey) throws Exception;
}
